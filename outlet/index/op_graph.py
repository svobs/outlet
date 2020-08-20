import collections
import logging
import pathlib
import threading
from typing import DefaultDict, Deque, Dict, Iterable, List, Optional

from pydispatch import dispatcher

from constants import ROOT_UID, TREE_TYPE_GDRIVE, TREE_TYPE_LOCAL_DISK
from index.op_graph_node import DstOpNode, OpGraphNode, RmOpNode, RootNode, SrcOpNode
from index.uid.uid import UID
from model.change_action import ChangeAction, ChangeType
from model.node.display_node import DisplayNode, HasParentList
from model.node.gdrive_node import GDriveNode
from util.stopwatch_sec import Stopwatch

logger = logging.getLogger(__name__)


# CLASS OpGraph
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼

class OpGraph:
    """Dependency tree, currently with emphasis on ChangeActions"""

    def __init__(self, application):
        self.cacheman = application.cache_manager
        self.uid_generator = application.uid_generator

        self._lock = threading.Lock()

        self._shutdown: bool = False
        self._cv = threading.Condition()
        """Used to help consumers block"""

        self._node_dict: Dict[UID, Deque[OpGraphNode]] = {}
        """Contains entries for all nodes have pending changes. Each entry has a queue of pending changes for that node"""

        self._root: OpGraphNode = RootNode()
        """Root of tree. Has no useful internal data; we value it for its children"""

        self._outstanding_actions: Dict[UID, ChangeAction] = {}
        """Contains entries for all ChangeActions which have running operations. Keyed by action UID"""

    def __del__(self):
        self.shutdown()

    def shutdown(self):
        """Need to call this for try_get() to return"""
        self._shutdown = True
        with self._cv:
            # unblock any get() task which is waiting
            self._cv.notifyAll()

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
        node_list = self._node_dict.get(uid, None)
        if node_list:
            # last element in list is lowest priority:
            return node_list[-1]
        return None

    def get_last_pending_change_for_node(self, node_uid: UID) -> Optional[ChangeAction]:
        """This is a public method."""
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

        # Put all in dict as wrapped OpGraphNodes
        non_mutex_node_dict: DefaultDict[UID, List[OpGraphNode]] = collections.defaultdict(lambda: list())
        mutex_node_dict: Dict[UID, OpGraphNode] = {}
        for change in change_batch:
            if change.change_type == ChangeType.RM:
                src_node = RmOpNode(self.uid_generator.next_uid(), change)
            else:
                src_node = SrcOpNode(self.uid_generator.next_uid(), change)

            if src_node.is_mutually_exclusive():
                existing = mutex_node_dict.get(src_node.get_target_node().uid, None)
                if existing:
                    raise RuntimeError(f'Duplicate node: {src_node.get_target_node()}')
                mutex_node_dict[src_node.get_target_node().uid] = src_node
            else:
                non_mutex_node_dict[src_node.get_target_node().uid].append(src_node)

            if change.has_dst():
                dst_node = DstOpNode(self.uid_generator.next_uid(), change)
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
                root_node.link_child(node)

        # Need to keep track of RM nodes because we can't identify their topmost nodes the same way as other nodes:
        rm_node_dict: Dict[UID, OpGraphNode] = {}
        
        # mutually exclusive nodes have dependencies on each other:
        for potential_child_op in mutex_node_dict.values():
            parent_uid = self._derive_parent_uid(potential_child_op.get_target_node())
            op_for_parent_node: OpGraphNode = mutex_node_dict.get(parent_uid, None)
            if potential_child_op.is_remove_type():
                # Special handling for RM-type nodes:
                op_parent_unknown = True
                if op_for_parent_node:
                    # Parent node's op is also RM? -> parent node's op becomes op child
                    if op_for_parent_node.is_remove_type():
                        potential_child_op.link_child(op_for_parent_node)
                    else:
                        op_for_parent_node.link_child(potential_child_op)
                        op_parent_unknown = False
                if op_parent_unknown:
                    rm_node_dict[potential_child_op.get_target_node().uid] = potential_child_op
            else:
                # (Nodes which are NOT ChangeType.RM):
                if op_for_parent_node:
                    op_for_parent_node.link_child(potential_child_op)
                else:
                    # those with no parent will be children of root:
                    root_node.link_child(potential_child_op)

        # Filter RM node list so that we only have topmost nodes:
        for uid in list(rm_node_dict.keys()):
            if rm_node_dict[uid].get_parent_list():
                del rm_node_dict[uid]
        # Finally, link topmost RM nodes to root:
        for rm_node in rm_node_dict.values():
            root_node.link_child(rm_node)

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
        batch_uid = root_of_changes.get_first_child().change_action.batch_uid

        iterator = iter(root_of_changes.get_all_nodes_in_subtree())
        # skip root
        next(iterator)

        mkdir_dict: Dict[UID, DisplayNode] = {}
        """Keep track of nodes which are to be created, so we can include them in the lookup for valid parents"""

        for op_node in iterator:
            tgt_node: DisplayNode = op_node.get_target_node()
            op_type: str = op_node.change_action.change_type.name

            if op_node.is_create_type():
                # Enforce Rule 1: ensure parent of target is valid:
                parent_uid = self._derive_parent_uid(tgt_node)
                if not self.cacheman.get_item_for_uid(parent_uid, tgt_node.node_identifier.tree_type) \
                        and not mkdir_dict.get(parent_uid, None):
                    logger.error(f'Could not find parent in cache with UID {parent_uid} for "{op_type}" operation node: {tgt_node}')
                    raise RuntimeError(f'Cannot add batch (UID={batch_uid}): Could not find parent in cache with UID {parent_uid} '
                                       f'for "{op_type}"')

                if op_node.change_action.change_type == ChangeType.MKDIR:
                    assert not mkdir_dict.get(op_node.change_action.src_node.uid, None), f'Duplicate MKDIR: {op_node.change_action.src_node}'
                    mkdir_dict[op_node.change_action.src_node.uid] = op_node.change_action.src_node
            else:
                # Enforce Rule 2: ensure target node is valid
                if not self.cacheman.get_item_for_uid(tgt_node.uid, tgt_node.node_identifier.tree_type):
                    logger.error(f'Could not find node in cache for "{op_type}" operation node: {tgt_node}')
                    raise RuntimeError(f'Cannot add batch (UID={batch_uid}): Could not find node in cache with UID {tgt_node.uid} '
                                       f'for "{op_type}"')

            with self._lock:
                # More of Rule 2: ensure target node is not scheduled for deletion:
                most_recent_op = self.get_last_pending_change_for_node(tgt_node.uid)
                if most_recent_op and most_recent_op.change_type == ChangeType.RM and op_node.is_src() and op_node.change_action.has_dst():
                    # CP, MV, and UP ops cannot logically have a src node which is not present:
                    raise RuntimeError(f'Cannot add batch (UID={batch_uid}): it is attempting to CP/MV/UP from a node (UID={tgt_node.uid}) '
                                       f'which is being removed')

        return True

    def _find_child_nodes_in_tree(self, op_node: OpGraphNode) -> List[OpGraphNode]:
        # TODO: wow this is O(n*m). Find a way to optimize
        sw_total = Stopwatch()

        assert isinstance(op_node, RmOpNode)
        potential_parent: DisplayNode = op_node.get_target_node()

        child_nodes = []

        for existing_op_node in _skip_root(self._root.get_all_nodes_in_subtree()):
            potential_child: DisplayNode = existing_op_node.get_target_node()
            if potential_parent.is_parent(potential_child):
                child_nodes.append(existing_op_node)

        logger.debug(f'{sw_total} Found {len(child_nodes):n} child nodes in tree for op node')
        return child_nodes

    def _add_single_node(self, node_to_insert: OpGraphNode):
        """
        The node shall be added as a child dependency of either the last operation which affected its target,
        or as a child dependency of the last operation which affected its parent, whichever has lower priority (i.e. has a lower level
        in the dependency tree). In the case where neither the node nor its parent has a pending operation, we obviously can just add
        to the top of the dependency tree.
        """
        logger.debug(f'Enqueuing single op node: {node_to_insert}')

        # Need to clear out previous relationships before adding to main tree:
        node_to_insert.clear_relationships()

        target_node: DisplayNode = node_to_insert.get_target_node()
        target_uid: UID = target_node.uid
        parent_uid: UID = self._derive_parent_uid(target_node)

        # First check whether the target node is known and has pending operations
        last_target_op = self._get_lowest_priority_tree_node(target_uid)

        if node_to_insert.is_remove_type():
            # Special handling for RM-type nodes.
            # We want to find the lowest RM node in the tree.

            if last_target_op:
                if last_target_op.is_remove_type():
                    logger.info(f'Op node being enqueued (UID {node_to_insert.node_uid}, tgt UID {target_uid}) is an RM type which is '
                                f'a dup of already enqueued RM (UID {last_target_op.node_uid}); discarding!')
                    return

                # If existing op is found for the target node, add below that.
                if last_target_op.get_child_list():
                    raise RuntimeError(f'While trying to add RM op: did not expect existing op for node to have children! '
                                       f'(Node={last_target_op})')
                logger.debug(f'Found pending op(s) for target node {target_uid} for RM op; adding as child dependency')
                last_target_op.link_child(node_to_insert)
            else:
                # Otherwise see if we can find child nodes of the target node (which in our rules would be the parents of the RM op):
                op_for_child_node_list: List[OpGraphNode] = self._find_child_nodes_in_tree(node_to_insert)
                if op_for_child_node_list:
                    logger.debug(f'Found {len(op_for_child_node_list)} ops for children of node being removed ({target_uid});'
                                 f' adding as child dependency of each')
                    for op_for_child_node in op_for_child_node_list:
                        op_for_child_node.link_child(node_to_insert)
                else:
                    logger.debug(f'Found no previous ops for target node {target_uid} or its children; adding to root')
                    self._root.link_child(node_to_insert)
        else:
            # Not an RM node:
            last_parent_op = self._get_lowest_priority_tree_node(parent_uid)

            if last_target_op and last_parent_op:
                if last_target_op.get_level() > last_parent_op.get_level():
                    logger.debug(f'Last target op (for node {target_uid}) is lower level than last parent op (for node {parent_uid});'
                                 f' adding as child of last target op')
                    last_target_op.link_child(node_to_insert)
                else:
                    logger.debug(f'Last target op is >= level than last parent op; adding as child of last parent op')
                    last_parent_op.link_child(node_to_insert)
            elif last_target_op:
                assert not last_parent_op
                logger.debug(f'Found pending op(s) for target node {target_uid}; adding as child dependency')
                last_target_op.link_child(node_to_insert)
            elif last_parent_op:
                assert not last_target_op
                logger.debug(f'Found pending op(s) for parent node {parent_uid}; adding as child dependency')
                last_parent_op.link_child(node_to_insert)
            else:
                assert not last_parent_op and not last_target_op
                logger.debug(f'Found no previous ops for either target node {target_uid} or parent node {parent_uid}; adding to root')
                self._root.link_child(node_to_insert)

        # Always add pending operation for bookkeeping
        pending_ops = self._node_dict.get(target_uid, None)
        if not pending_ops:
            pending_ops = collections.deque()
            self._node_dict[target_uid] = pending_ops
        pending_ops.append(node_to_insert)

    def add_batch(self, root_of_changes: RootNode):
        # 1. Discard root
        # 2. Examine each child of root. Each shall be treated as its own subtree.
        # 3. For each subtree, look up all its nodes in the master dict. Level...?

        # Disregard the kind of change when building the tree; they are all equal for now (except for RM; see below):
        # Once entire tree is constructed, invert the RM subtree (if any) so that ancestor RMs become descendants

        if not root_of_changes.get_child_list():
            raise RuntimeError(f'Batch has no nodes!')

        batch_uid = root_of_changes.get_first_child().change_action.batch_uid

        breadth_first_list: List[OpGraphNode] = root_of_changes.get_all_nodes_in_subtree()
        for node_to_insert in _skip_root(breadth_first_list):
            with self._lock:
                self._add_single_node(node_to_insert)

        logger.debug(f'Done adding batch {batch_uid}')
        self._print_current_state()

        with self._cv:
            # notify consumers there is something to get:
            self._cv.notifyAll()

    def _try_get(self) -> Optional[ChangeAction]:
        # We can optimize this later

        for tree_node in self._root.get_child_list():
            logger.debug(f'TryGet(): examining {tree_node}')

            if tree_node.change_action.has_dst():
                # If the ChangeAction has both src and dst nodes, *both* must be next in their queues, and also be just below root.
                if tree_node.is_dst():
                    # Dst node is child of root. But verify corresponding src node is also child of root
                    pending_changes_for_src_node: Deque[OpGraphNode] = self._node_dict.get(tree_node.change_action.src_node.uid, None)
                    if not pending_changes_for_src_node:
                        logger.error(f'Could not find entry for change src (change={tree_node.change_action}); raising error')
                        raise RuntimeError(f'Serious error: master dict has no entries for change src ({tree_node.change_action.src_node.uid})!')
                    src_tree_node = pending_changes_for_src_node[0]
                    if src_tree_node.change_action.action_uid != tree_node.change_action.action_uid:
                        logger.debug(f'Skipping ChangeAction (UID {tree_node.change_action.action_uid}): it is not next in src node queue')
                        continue

                    level = src_tree_node.get_level()
                    # level 1 == root; level 2 == sub-root
                    if level != 2:
                        logger.debug(f'Skipping ChangeAction (UID {tree_node.change_action.action_uid}): src node is level {level}')
                        continue

                else:
                    # Src node is child of root. But verify corresponding dst node is also child of root
                    pending_changes_for_node: Deque[OpGraphNode] = self._node_dict.get(tree_node.change_action.dst_node.uid, None)
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
            if self._shutdown:
                return None

            with self._lock:
                change = self._try_get()
            if change:
                logger.info(f'Got next pending change: {change}')
                return change
            else:
                logger.debug(f'No pending changes; sleeping until notified')
                with self._cv:
                    self._cv.wait()

    def _is_child_of_root(self, node: OpGraphNode) -> bool:
        parent = node.get_first_parent()
        return parent and parent.node_uid == self._root.node_uid

    def pop_change(self, change: ChangeAction):
        """Ensure that we were expecting this change to be copmleted, and remove it from the tree."""
        logger.debug(f'Entered pop_change() for change {change}')

        with self._lock:
            if self._outstanding_actions.get(change.action_uid, None):
                self._outstanding_actions.pop(change.action_uid)
            else:
                raise RuntimeError(f'Complated change not found in outstanding change list (action UID {change.action_uid}')

            # I. SRC Node

            # I-1. Remove src node from node dict

            src_node_list: Deque[OpGraphNode] = self._node_dict.get(change.src_node.uid)
            if not src_node_list:
                raise RuntimeError(f'Src node for completed change not found in master dict (src node UID {change.src_node.uid}')

            src_tree_node: OpGraphNode = src_node_list.popleft()
            if src_tree_node.change_action.action_uid != change.action_uid:
                raise RuntimeError(f'Completed change (UID {change.action_uid}) does not match first item popped from src queue '
                                   f'(UID {src_tree_node.change_action.action_uid})')

            # I-2. Remove src node from op graph

            # validate it is a child of root
            if not self._is_child_of_root(src_tree_node):
                parent = src_tree_node.get_first_parent()
                if parent:
                    parent_uid = parent.node_uid
                else:
                    parent_uid = None
                raise RuntimeError(f'Src node for completed change is not a parent of root (instead found parent {parent_uid}')

            # unlink from its parent
            self._root.unlink_child(src_tree_node)

            # unlink its children also. If the child then has no other parents, it moves up to become child of root
            for child in src_tree_node.get_child_list():
                child.unlink_parent(src_tree_node)

                if not child.get_parent_list():
                    self._root.link_child(child)

            # I-3. Delete src node
            del src_tree_node

            # II. DST Node
            if change.has_dst():
                # II-1. Remove dst node from node dict

                dst_node_list: Deque[OpGraphNode] = self._node_dict.get(change.dst_node.uid)
                if not dst_node_list:
                    raise RuntimeError(f'Dst node for completed change not found in master dict (dst node UID {change.dst_node.uid}')
                dst_tree_node = dst_node_list.popleft()
                if dst_tree_node.change_action.action_uid != change.action_uid:
                    raise RuntimeError(f'Completed change (UID {change.action_uid}) does not match first item popped from dst queue '
                                       f'(UID {dst_tree_node.change_action.action_uid})')

                # II-2. Remove dst node from op graph

                # validate it is a child of root
                if not self._is_child_of_root(dst_tree_node):
                    parent = dst_tree_node.get_first_parent()
                    if parent:
                        parent_uid = parent.node_uid
                    else:
                        parent_uid = None
                    raise RuntimeError(f'Dst node for completed change is not a parent of root (instead found parent {parent_uid}')

                # unlink from its parent
                self._root.unlink_child(dst_tree_node)

                # unlink its children also. If the child then has no other parents, it moves up to become child of root
                for child in dst_tree_node.get_child_list():
                    child.unlink_parent(dst_tree_node)

                    if not child.get_parent_list():
                        self._root.link_child(child)

                # II-3. Delete dst node
                del dst_tree_node

            logger.debug(f'Done with change_completed() for change: {change}')
            self._print_current_state()

        with self._cv:
            # this may have jostled the tree to make something else free:
            self._cv.notifyAll()


def _skip_root(node_list: List[OpGraphNode]) -> Iterable[OpGraphNode]:
    """Note: does not support case when root node is second or later in the list"""
    if node_list and node_list[0].node_uid == ROOT_UID:
        node_list_iter = iter(node_list)
        next(node_list_iter)
        return node_list_iter

    return node_list
