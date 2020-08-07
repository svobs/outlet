import collections
import logging
import pathlib
import threading
from typing import DefaultDict, Deque, Dict, Iterable, List, Optional

from pydispatch import dispatcher

from constants import TREE_TYPE_GDRIVE, TREE_TYPE_LOCAL_DISK
from index.op_tree_node import DstActionNode, OpTreeNode, RootNode, SrcActionNode
from index.uid.uid import UID
from model.change_action import ChangeAction, ChangeType
from model.node.display_node import DisplayNode, HasParentList
from model.node.gdrive_node import GDriveNode

logger = logging.getLogger(__name__)


# CLASS OpTree
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼

class OpTree:
    """Dependency tree, currently with emphasis on ChangeActions"""

    def __init__(self, application):
        self.cacheman = application.cache_manager
        self.uid_generator = application.uid_generator

        self._lock = threading.Lock()

        self._cv = threading.Condition()
        """Used to help consumers block"""

        self._node_dict: DefaultDict[UID, Deque[OpTreeNode]] = collections.defaultdict(lambda: collections.deque())
        """Contains entries for all nodes have pending changes. Each entry has a queue of pending changes for that node"""

        self._root: OpTreeNode = RootNode()
        """Root of tree. Has no useful internal data; we value it for its children"""

        self._outstanding_actions: Dict[UID, ChangeAction] = {}
        """Contains entries for all ChangeActions which have running operations. Keyed by action UID"""

    def _print_current_state(self):
        lines = self._root.print_recursively()
        logger.debug(f'CURRENT STATE: PendingChangeTree is now {len(lines)} items:')
        for line in lines:
            logger.debug(line)
        self._print_node_dict()

    def _print_node_dict(self):
        logger.debug(f'CURRENT STATE: NodeDict is now: {len(self._node_dict)} items:')
        for node_uid, deque in self._node_dict.items():
            node_list: List[str] = []
            for node in deque:
                node_list.append(node.change_action.tag)
            logger.debug(f'{node_uid}: [{"; ".join(node_list)}]')

    def _derive_parent_uid(self, node: DisplayNode):
        """Derives the UID for the parent of the given node"""
        if isinstance(node, HasParentList):
            assert isinstance(node, GDriveNode) and node.node_identifier.tree_type == TREE_TYPE_GDRIVE, f'Node: {node}'
            parent_uids = node.get_parent_uids()
            assert len(parent_uids) == 1, f'Expected exactly one parent_uid for node: {node}'
            return parent_uids[0]
        else:
            assert node.node_identifier.tree_type == TREE_TYPE_LOCAL_DISK, f'Node: {node}'
            parent_path = str(pathlib.Path(node.full_path).parent)
            return self.cacheman.get_uid_for_path(parent_path)

    def _get_lowest_priority_tree_node(self, uid: UID):
        node_list = self._node_dict[uid]
        if node_list:
            # last element in list is lowest priority:
            return node_list[-1]
        return None

    def get_last_pending_change_for_node(self, node_uid: UID) -> Optional[ChangeAction]:
        tree_node = self._get_lowest_priority_tree_node(node_uid)
        if tree_node:
            return tree_node.change_action
        return None

    def make_tree_to_insert(self, change_batch: Iterable[ChangeAction]) -> RootNode:
        logger.debug(f'Constructing OpNode tree for ChangeAction batch...')

        # Verify batch sort:
        last_uid = 0
        for change in change_batch:
            # Changes MUST be sorted in ascending time of creation!
            if change.action_uid < last_uid:
                raise RuntimeError(f'Batch items are not in order! ({change.action_uid} < {last_uid}')
            last_uid = change.action_uid

        # Put all in dict as wrapped OpTreeNodes
        non_mutex_node_dict: DefaultDict[UID, List[OpTreeNode]] = collections.defaultdict(lambda: list())
        mutex_node_dict: Dict[UID, OpTreeNode] = {}
        for change in change_batch:
            src_node = SrcActionNode(self.uid_generator.next_uid(), change)
            if src_node.is_mutually_exclusive():
                existing = mutex_node_dict.get(src_node.get_target_node().uid, None)
                if existing:
                    raise RuntimeError(f'Duplicate node: {src_node.get_target_node()}')
                mutex_node_dict[src_node.get_target_node().uid] = src_node
            else:
                non_mutex_node_dict[src_node.get_target_node().uid].append(src_node)

            if change.has_dst():
                dst_node = DstActionNode(self.uid_generator.next_uid(), change)
                assert dst_node.is_mutually_exclusive()
                existing = mutex_node_dict.get(dst_node.get_target_node().uid, None)
                if existing:
                    raise RuntimeError(f'Duplicate node: {dst_node.get_target_node()}')
                mutex_node_dict[dst_node.get_target_node().uid] = dst_node

        # Assemble nodes one by one with parent-child relationships.
        root_node = RootNode()

        # non-mutually exclusive nodes: just make them all children of root
        for non_mutex_list in non_mutex_node_dict.values():
            for node in non_mutex_list:
                root_node.add_child(node)

        # mutually exclusive nodes have dependencies on each other:
        for potential_child_node in mutex_node_dict.values():
            parent_uid = self._derive_parent_uid(potential_child_node.get_target_node())
            parent = mutex_node_dict.get(parent_uid, None)
            if parent:
                parent.add_child(potential_child_node)
            else:
                # those with no parent will be children of root:
                root_node.add_child(potential_child_node)

        lines = root_node.print_recursively()
        logger.debug(f'MakeTreeToInsert: constructed tree with {len(lines)} items:')
        for line in lines:
            logger.debug(line)
        return root_node

    def can_add_batch(self, root_of_changes: RootNode) -> bool:
        """
        Takes a tree representing a batch as an arg. The root itself is ignored, but each of its children represent the root of a
        subtree of changes, in which each node of the subtree maps to a node in a directory tree. No intermediate nodes are allowed to be
        omitted from a subtree (e.g. if A is the parent of B which is the parent of C, you cannot copy A and C but exclude B).

        Rules:
        1. Parent for MKDIR_SRC and all DST nodes must be present in tree and not already scheduled for RM
        2. Except for MKDIR, SRC nodes must all be present in tree (OK if exists==False due to pending operation)
        and not already scheduled for RM
        """

        # Invert RM nodes when inserting into tree
        batch_uid = root_of_changes.children[0].change_action.batch_uid

        iterator = iter(root_of_changes.get_all_nodes_in_subtree())
        # skip root
        next(iterator)

        mkdir_dict: Dict[UID, DisplayNode] = {}
        """Keep track of nodes which are to be created, so we can include them in the lookup for valid parents"""

        for tree_node in iterator:
            tgt_node: DisplayNode = tree_node.get_target_node()
            op_type: str = tree_node.change_action.change_type.name

            if tree_node.is_create_type():
                # Enforce Rule 1: ensure parent of target is valid:
                parent_uid = self._derive_parent_uid(tgt_node)
                if not self.cacheman.get_item_for_uid(parent_uid, tgt_node.node_identifier.tree_type) \
                        and not mkdir_dict.get(parent_uid, None):
                    logger.error(f'Could not find parent in cache with UID {parent_uid} for "{op_type}" operation node: {tgt_node}')
                    raise RuntimeError(f'Cannot add batch (UID={batch_uid}): Could not find parent in cache with UID {parent_uid} '
                                       f'for "{op_type}"')

                if tree_node.change_action.change_type == ChangeType.MKDIR:
                    assert not mkdir_dict.get(tree_node.change_action.src_node.uid, None), f'Duplicate MKDIR: {tree_node.change_action.src_node}'
                    mkdir_dict[tree_node.change_action.src_node.uid] = tree_node.change_action.src_node
            else:
                # Enforce Rule 2: ensure target node is valid
                if not self.cacheman.get_item_for_uid(tgt_node.uid, tgt_node.node_identifier.tree_type):
                    logger.error(f'Could not find node in cache for "{op_type}" operation node: {tgt_node}')
                    raise RuntimeError(f'Cannot add batch (UID={batch_uid}): Could not find node in cache with UID {tgt_node.uid} '
                                       f'for "{op_type}"')

            with self._lock:
                pending_change_list = self._node_dict.get(tgt_node.uid, [])
                if pending_change_list:
                    for change_node in pending_change_list:
                        # TODO: for now, just deny anything which tries to work off an RM. Refine in future
                        if change_node.change_action.change_type == ChangeType.RM:
                            raise RuntimeError(f'Cannot add batch (UID={batch_uid}): it depends on a node (UID={tgt_node.uid}) '
                                               f'which is being removed')

        return True

    def _add_single_node(self, tree_node: OpTreeNode):
        """
        The node shall be added as a child dependency of either the last operation which affected its target,
        or as a child dependency of the last operation which affected its parent, whichever has lower priority (i.e. has a lower level
        in the dependency tree). In the case where neither the node nor its parent has a pending operation, we obviously can just add
        to the top of the dependency tree.
        """
        logger.debug(f'Enqueuing single node: {tree_node}')

        # Need to clear out previous relationships before adding to main tree:
        tree_node.children.clear()
        tree_node.parent = None

        target_node: DisplayNode = tree_node.get_target_node()
        target_uid: UID = target_node.uid
        parent_uid: UID = self._derive_parent_uid(target_node)

        # First check whether the target node is known and has pending operations
        last_target_op = self._get_lowest_priority_tree_node(target_uid)
        last_parent_op = self._get_lowest_priority_tree_node(parent_uid)

        if last_target_op and last_parent_op:
            if last_target_op.get_level() > last_parent_op.get_level():
                logger.debug(f'Last target op (for node {target_uid}) is lower level than last parent op (for node {parent_uid});'
                             f' adding as child of last target op')
                last_target_op.add_child(tree_node)
            else:
                logger.debug(f'Last target op is >= level than last parent op; adding as child of last parent op')
                last_parent_op.add_child(tree_node)
        elif last_target_op:
            assert not last_parent_op
            logger.debug(f'Found pending op(s) for target node {target_uid}; adding as child dependency')
            last_target_op.add_child(tree_node)
        elif last_parent_op:
            assert not last_target_op
            logger.debug(f'Found pending op(s) for parent node {parent_uid}; adding as child dependency')
            last_parent_op.add_child(tree_node)
        else:
            assert not last_parent_op and not last_target_op
            logger.debug(f'Found no previous ops for either target node {target_uid} or parent node {parent_uid}; adding to root')
            self._root.add_child(tree_node)

        # Always add pending operation for bookkeeping
        self._node_dict[target_uid].append(tree_node)

    def _add_subtree(self, subtree_root: OpTreeNode):
        all_subtree_nodes = subtree_root.get_all_nodes_in_subtree()
        
        # if subtree_root.change_action.change_type == ChangeType.RM:
        #     # Removing a tree? Do everything in reverse order
        #     all_subtree_nodes = reversed(all_subtree_nodes)
            # FIXME: probably want completely separate logic for this

        for tree_node in all_subtree_nodes:
            with self._lock:
                self._add_single_node(tree_node)

    def add_batch(self, root_of_changes: RootNode):
        # 1. Discard root
        # 2. Examine each child of root. Each shall be treated as its own subtree.
        # 3. For each subtree, look up all its nodes in the master dict. Level...?

        # Disregard the kind of change when building the tree; they are all equal for now (except for RM; see below):
        # Once entire tree is constructed, invert the RM subtree (if any) so that ancestor RMs become descendants

        if not root_of_changes.children:
            raise RuntimeError(f'Batch has no nodes!')

        batch_uid = root_of_changes.children[0].change_action.batch_uid

        for change_subtree_root in root_of_changes.children:
            self._add_subtree(change_subtree_root)

        logger.debug(f'Done adding batch {batch_uid}')
        self._print_current_state()

        with self._cv:
            # notify consumers there is something to get:
            self._cv.notifyAll()

    def _try_get(self) -> Optional[ChangeAction]:
        # We can optimize this later

        for tree_node in self._root.children:
            logger.debug(f'TryGet(): examining {tree_node}')

            if tree_node.change_action.has_dst():
                # If the ChangeAction has both src and dst nodes, *both* must be next in their queues, and also be just below root.
                if tree_node.is_dst():
                    # Dst node is child of root. But verify corresponding src node is also child of root
                    pending_changes_for_src_node = self._node_dict.get(tree_node.change_action.src_node.uid, None)
                    if not pending_changes_for_src_node:
                        logger.error(f'Could not find entry for change src (change={tree_node.change_action}); raising error')
                        raise RuntimeError(f'Serious error: master dict has no entries for change src ({tree_node.change_action.src_node.uid})!')

                    src_tree_node = pending_changes_for_src_node[0]
                    if src_tree_node.change_action.action_uid != tree_node.change_action.action_uid:
                        logger.debug(f'Skipping ChangeAction (UID {tree_node.change_action.action_uid}): it is not next in src node queue')
                        continue

                    level = src_tree_node.get_level()
                    # level 1 == root; level 2 == subroot
                    if level != 2:
                        logger.debug(f'Skipping ChangeAction (UID {tree_node.change_action.action_uid}): src node is level {level}')
                        continue

                else:
                    # Src node is child of root. But verify corresponding dst node is also child of root
                    pending_changes_for_node = self._node_dict.get(tree_node.change_action.dst_node.uid, None)
                    if not pending_changes_for_node:
                        logger.error(f'Could not find entry for change dst (change={tree_node.change_action}); raising error')
                        raise RuntimeError(f'Serious error: master dict has no entries for change dst ({tree_node.change_action.dst_node.uid})!')

                    dst_tree_node = pending_changes_for_node[0]
                    if dst_tree_node.change_action.action_uid != tree_node.change_action.action_uid:
                        logger.debug(f'Skipping ChangeAction (UID {tree_node.change_action.action_uid}): it is not next in dst node queue')
                        continue

                    level = dst_tree_node.get_level()
                    if level != 2:
                        logger.debug(f'Skipping ChangeAction (UID {tree_node.change_action.action_uid}): dst node is level {level}')
                        continue

            # Make sure the node has not already been checked out:
            if not self._outstanding_actions.get(tree_node.change_action.action_uid, None):
                self._outstanding_actions[tree_node.change_action.action_uid] = tree_node.change_action
                return tree_node.change_action
            else:
                logger.debug(f'Skipping node because it is already outstanding')

        return None

    def get_next_change(self) -> Optional[ChangeAction]:
        """Gets and returns the next available ChangeAction from the tree; BLOCKS if no pending changes or all the pending changes have
        already been gotten.

        Internally this class keeps track of what this has returned previously, and will expect to be notified when each is complete
         - think of get_next_change() as a repository checkout, and change_completed() as a commit"""

        # Block until we have a change
        while True:
            with self._lock:
                change = self._try_get()
            if change:
                logger.info(f'Got next pending change: {change}')
                return change
            else:
                logger.debug(f'No pending changes; sleeping until notified')
                with self._cv:
                    self._cv.wait()

    def change_completed(self, change: ChangeAction):
        """Ensure that we were expecting this change to be copmleted, and remove it from the tree."""
        logger.debug(f'Entered change_completed() for change {change}')

        with self._lock:
            if self._outstanding_actions.get(change.action_uid, None):
                self._outstanding_actions.pop(change.action_uid)
            else:
                raise RuntimeError(f'Complated change not found in outstanding change list (action UID {change.action_uid}')

            src_node_list: Deque[OpTreeNode] = self._node_dict.get(change.src_node.uid)
            if not src_node_list:
                raise RuntimeError(f'Src node for completed change not found in master dict (src node UID {change.src_node.uid}')

            src_tree_node = src_node_list.popleft()
            if src_tree_node.change_action.action_uid != change.action_uid:
                raise RuntimeError(f'Completed change (UID {change.action_uid}) does not match first item popped from src queue '
                                   f'(UID {src_tree_node.change_action.action_uid})')

            if src_tree_node.parent.node_uid != self._root.node_uid:
                raise RuntimeError(f'Src node for completed change is not a parent of root (instead found parent {src_tree_node.parent.node_uid}')

            for child in src_tree_node.children:
                # this will change their parent pointers too
                self._root.add_child(child)

            self._root.remove_child(src_tree_node)
            del src_tree_node

            if change.has_dst():
                dst_node_list: Deque[OpTreeNode] = self._node_dict.get(change.dst_node.uid)
                if not dst_node_list:
                    raise RuntimeError(f'Dst node for completed change not found in master dict (dst node UID {change.dst_node.uid}')
                dst_tree_node = dst_node_list.popleft()
                if dst_tree_node.change_action.action_uid != change.action_uid:
                    raise RuntimeError(f'Completed change (UID {change.action_uid}) does not match first item popped from dst queue '
                                       f'(UID {dst_tree_node.change_action.action_uid})')

                if dst_tree_node.parent.node_uid != self._root.node_uid:
                    raise RuntimeError(f'Dst node for completed change is not a parent of root (instead found parent {dst_tree_node.parent.node_uid}')

                for child in dst_tree_node.children:
                    self._root.add_child(child)

                self._root.remove_child(dst_tree_node)
                del dst_tree_node

            logger.debug(f'Done with change_completed() for change: {change}')
            self._print_current_state()

        with self._cv:
            # this may have jostled the tree to make something else free:
            self._cv.notifyAll()


