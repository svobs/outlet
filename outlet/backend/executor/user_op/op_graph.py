import collections
import logging
import threading
from typing import DefaultDict, Deque, Dict, Iterable, List, Optional, Tuple

from constants import SUPER_DEBUG_ENABLED, SUPER_ROOT_UID, TRACE_ENABLED
from backend.executor.user_op.op_graph_node import DstOpNode, OpGraphNode, RmOpNode, RootNode, SrcOpNode
from model.node_identifier import DN_UID
from model.uid import UID
from model.node.node import Node
from model.user_op import UserOp, UserOpType
from util.has_lifecycle import HasLifecycle
from util.stopwatch_sec import Stopwatch

logger = logging.getLogger(__name__)


class OpGraph(HasLifecycle):
    """
    ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
    CLASS OpGraph

    Flow graph for user ops
    ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼
    """
    def __init__(self, backend):
        HasLifecycle.__init__(self)
        self.backend = backend

        self._struct_lock = threading.Lock()

        self._cv_can_get = threading.Condition(self._struct_lock)
        """Used to help consumers block"""

        self._node_q_dict: Dict[UID, Dict[UID, Deque[OpGraphNode]]] = {}
        """Contains entries for all nodes have pending ops. Each entry has a queue of pending ops for that target node"""

        self._graph_root: OpGraphNode = RootNode()
        """Root of graph. Has no useful internal data; we value it for its children"""

        self._outstanding_actions: Dict[UID, UserOp] = {}
        """Contains entries for all Ops which have running operations. Keyed by action UID"""

    def shutdown(self):
        """Need to call this for try_get() to return"""
        if self.was_shutdown:
            return

        HasLifecycle.shutdown(self)
        with self._cv_can_get:
            # unblock any get() task which is waiting
            self._cv_can_get.notifyAll()

    def __len__(self):
        with self._struct_lock:
            op_set = set()
            # '_node_q_dict' must contain the same number of nodes as the graph, but should be slightly more efficient to iterate over (maybe)
            for device_uid, node_dict in self._node_q_dict.items():
                for node_uid, deque in node_dict.items():
                    for node in deque:
                        op_set.add(node.op.op_uid)

            return len(op_set)

    def _print_current_state(self):
        graph_line_list = self._graph_root.print_recursively()

        op_set = set()
        line_list = []
        queue_count = 0
        for device_uid, node_dict in self._node_q_dict.items():
            queue_count += len(node_dict)
            for node_uid, deque in node_dict.items():
                node_repr_list: List[str] = []
                for node in deque:
                    op_set.add(node.op.op_uid)
                    node_repr_list.append(node.op.get_tag())
                line_list.append(f'NodeUID {node_uid}:')
                for i, node_repr in enumerate(node_repr_list):
                    line_list.append(f'  {i}. {node_repr}')

        logger.debug(f'CURRENT EXECUTION STATE: OpGraph contains {len(graph_line_list)} nodes:')
        for line in graph_line_list:
            logger.debug(line)

        logger.debug(f'CURRENT EXECUTION STATE: NodeQueueDict has {queue_count} queues for {len(self._node_q_dict)} devices '
                     f'({len(op_set)} total ops):')
        for line in line_list:
            logger.debug(line)

    def _get_lowest_priority_op_node(self, device_uid: UID, node_uid: UID):
        node_dict = self._node_q_dict.get(device_uid, None)
        if node_dict:
            node_list = node_dict.get(node_uid, None)
            if node_list:
                # last element in list is lowest priority:
                return node_list[-1]
        return None

    def get_last_pending_op_for_node(self, device_uid: UID, node_uid: UID) -> Optional[UserOp]:
        """This is a public method."""
        op_node: OpGraphNode = self._get_lowest_priority_op_node(device_uid, node_uid)
        if op_node:
            return op_node.op
        return None

    def make_graph_from_batch(self, batch_uid: UID, op_batch: Iterable[UserOp]) -> RootNode:
        logger.debug(f'Constructing OpGraphNode tree for UserOp batch...')

        # Verify batch sort:
        last_op_uid = 0
        for op in op_batch:
            assert op.batch_uid == batch_uid, f'Op is not a part of batch {batch_uid}: {op}'
            # Changes MUST be sorted in ascending time of creation!
            if op.op_uid < last_op_uid:
                op_batch_str = '\n\t'.join([f'{x.op_uid}={repr(x)}' for x in op_batch])
                logger.error(f'OpBatch:\n\t {op_batch_str}')
                raise RuntimeError(f'Batch items are not in order! ({op.op_uid} < {last_op_uid})')
            last_op_uid = op.op_uid

        # Put all in dict as wrapped OpGraphNodes
        reentrant_tgt_node_dict: DefaultDict[DN_UID, List[OpGraphNode]] = collections.defaultdict(lambda: list())
        non_reentrant_tgt_node_dict: Dict[DN_UID, OpGraphNode] = {}
        for op in op_batch:
            # make src node
            if op.op_type == UserOpType.RM:
                src_node: OpGraphNode = RmOpNode(self.backend.uid_generator.next_uid(), op)
            else:
                src_node: OpGraphNode = SrcOpNode(self.backend.uid_generator.next_uid(), op)

            if src_node.is_reentrant():
                assert isinstance(src_node, SrcOpNode) and src_node.op.op_type == UserOpType.CP, f'Not consistent: {src_node}'
                reentrant_tgt_node_dict[src_node.get_tgt_node().dn_uid].append(src_node)
            else:
                existing = non_reentrant_tgt_node_dict.get(src_node.get_tgt_node().dn_uid, None)
                if existing:
                    raise RuntimeError(f'Duplicate node: {src_node.get_tgt_node()}')
                non_reentrant_tgt_node_dict[src_node.get_tgt_node().dn_uid] = src_node

            # make dst node (if op has dst)
            if op.has_dst():
                dst_node = DstOpNode(self.backend.uid_generator.next_uid(), op)
                assert not dst_node.is_reentrant(), f'Expected not reentrant: {dst_node}'
                existing = non_reentrant_tgt_node_dict.get(dst_node.get_tgt_node().dn_uid, None)
                if existing:
                    raise RuntimeError(f'Duplicate node: {dst_node.get_tgt_node()}')
                non_reentrant_tgt_node_dict[dst_node.get_tgt_node().dn_uid] = dst_node

        # Assemble nodes one by one with parent-child relationships.
        root_node = RootNode()

        # reentrant nodes: just make them all children of root
        for tgt_node_dn_uid, reentrant_list in reentrant_tgt_node_dict.items():
            if tgt_node_dn_uid in non_reentrant_tgt_node_dict:
                # cover the last case we missed earlier:
                raise RuntimeError(f'Duplicate target node: {tgt_node_dn_uid}')
            for node in reentrant_list:
                root_node.link_child(node)

        # Need to keep track of RM nodes because we can't identify their topmost nodes the same way as other nodes:
        rm_node_dict: Dict[DN_UID, OpGraphNode] = {}

        # non-reentrant nodes cannot execute concurrently and must have dependencies on each other:
        for potential_child_op in non_reentrant_tgt_node_dict.values():
            parent_device_uid = potential_child_op.get_tgt_node().device_uid
            parent_uid_list: List[UID] = potential_child_op.get_tgt_node().get_parent_uids()
            if SUPER_DEBUG_ENABLED and len(parent_uid_list) > 1:
                logger.debug(f'Target node of op has multiple parents: {potential_child_op}')
            for parent_uid in parent_uid_list:
                parent_dn_uid = Node.format_dn_uid(parent_device_uid, parent_uid)
                op_for_parent_node: OpGraphNode = non_reentrant_tgt_node_dict.get(parent_dn_uid, None)
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
                        rm_node_dict[potential_child_op.get_tgt_node().dn_uid] = potential_child_op
                else:
                    # (Nodes which are NOT UserOpType.RM):
                    if op_for_parent_node:
                        op_for_parent_node.link_child(potential_child_op)
                    else:
                        # those with no parent will be children of root:
                        root_node.link_child(potential_child_op)

        # Filter RM node list so that we only have topmost nodes:
        for rm_node in rm_node_dict.values():
            if not rm_node.get_parent_list():
                # Link topmost RM nodes to root:
                root_node.link_child(rm_node)

        lines = root_node.print_recursively()
        # note: GDrive paths may not be present at this point; this is ok.
        logger.debug(f'[Batch-{batch_uid}] MakeTreeToInsert: constructed tree with {len(lines)} items:')
        for line in lines:
            logger.debug(f'[Batch-{batch_uid}] {line}')
        return root_node

    def can_enqueue_batch(self, op_root: RootNode) -> bool:
        """
        Takes a tree representing a batch as an arg. The root itself is ignored, but each of its children represent the root of a
        subtree of changes, in which each node of the subtree maps to a node in a directory tree. No intermediate nodes are allowed to be
        omitted from a subtree (e.g. if A is the parent of B which is the parent of C, you cannot copy A and C but exclude B).

        Rules:
        1. Parent for MKDIR_SRC and all DST nodes must be present in memcache and not already scheduled for RM
        2. Except for MKDIR, SRC nodes must all be present in memcache (OK if is_live==False due to pending operation)
        and not already scheduled for RM
        """

        assert isinstance(op_root, RootNode)

        # Invert RM nodes when inserting into tree
        batch_uid: UID = op_root.get_first_child().op.batch_uid

        mkdir_node_dict: Dict[DN_UID] = {}
        """Keep track of nodes which are to be created, so we can include them in the lookup for valid parents"""

        for op_node in _skip_root(op_root.get_all_nodes_in_subtree()):
            tgt_node: Node = op_node.get_tgt_node()
            op_type: str = op_node.op.op_type.name

            if op_node.is_create_type():
                # Enforce Rule 1: ensure parent of target is valid:
                tgt_parent_uid_list: List[UID] = tgt_node.get_parent_uids()
                parent_found: bool = False
                for tgt_parent_uid in tgt_parent_uid_list:
                    tgt_parent_dn_uid = Node.format_dn_uid(tgt_node.device_uid, tgt_parent_uid)
                    if self.backend.cacheman.get_node_for_uid(tgt_parent_uid, tgt_node.device_uid) or mkdir_node_dict.get(tgt_parent_dn_uid, None):
                        parent_found = True

                if not parent_found:
                    logger.error(f'Could not find parent(s) in cache with device_uid {tgt_node.device_uid} & UID(s) {tgt_parent_uid_list} '
                                 f'for "{op_type}" operation node: {tgt_node}')
                    raise RuntimeError(f'Cannot add batch (UID={batch_uid}): Could not find any parents in cache with '
                                       f'device_uid {tgt_node.device_uid} & UIDs {tgt_parent_uid_list} for "{op_type}"')

                if op_node.op.op_type == UserOpType.MKDIR:
                    assert not mkdir_node_dict.get(op_node.op.src_node.dn_uid, None), f'Duplicate MKDIR: {op_node.op.src_node}'
                    mkdir_node_dict[op_node.op.src_node.dn_uid] = op_node.op.src_node
            else:
                # Enforce Rule 2: ensure target node is valid
                if not self.backend.cacheman.get_node_for_uid(tgt_node.uid, tgt_node.device_uid):
                    logger.error(f'Could not find node in cache for "{op_type}" operation node: {tgt_node}')
                    raise RuntimeError(f'Cannot add batch (UID={batch_uid}): Could not find node {tgt_node.dn_uid} in cache for "{op_type}"')

            with self._struct_lock:
                # More of Rule 2: ensure target node is not scheduled for deletion:
                most_recent_op = self.get_last_pending_op_for_node(tgt_node.device_uid, tgt_node.uid)
                if most_recent_op and most_recent_op.op_type == UserOpType.RM and op_node.is_src() and op_node.op.has_dst():
                    # CP, MV, and UP ops cannot logically have a src node which is not present:
                    raise RuntimeError(f'Cannot add batch (UID={batch_uid}): it is attempting to CP/MV/UP from a node ({tgt_node.dn_uid}) '
                                       f'which is being removed')

        return True

    def _find_child_nodes_in_tree_for_rm(self, op_node: OpGraphNode) -> List[OpGraphNode]:
        # TODO: probably could optimize a bit more...
        sw_total = Stopwatch()

        assert isinstance(op_node, RmOpNode)
        potential_parent: Node = op_node.get_tgt_node()

        child_nodes = []

        for node_dict in self._node_q_dict.values():
            for node_queue in node_dict.values():
                if node_queue:
                    existing_op_node = node_queue[-1]
                    potential_child: Node = existing_op_node.get_tgt_node()
                    if potential_parent.is_parent_of(potential_child):
                        if not existing_op_node.is_remove_type():
                            # This is not allowed. Cannot remove parent dir (aka child op node) unless *all* its children are first removed
                            raise RuntimeError(f'Found child node for RM-type node which is not RM type: {existing_op_node}')
                        child_nodes.append(existing_op_node)

        logger.debug(f'{sw_total} Found {len(child_nodes):n} child nodes in tree for op node')
        return child_nodes

    def _insert_rm_node_in_tree(self, node_to_insert: OpGraphNode, last_target_op, tgt_parent_uid_list: List[UID]) -> bool:
        # Special handling for RM-type nodes.
        # We want to find the lowest RM node in the tree.
        assert node_to_insert.is_remove_type()
        target_node: Node = node_to_insert.get_tgt_node()
        target_device_uid: UID = target_node.device_uid

        # First, see if we can find child nodes of the target node (which in our rules would be the parents of the RM op):
        op_for_child_node_list: List[OpGraphNode] = self._find_child_nodes_in_tree_for_rm(node_to_insert)
        if op_for_child_node_list:
            logger.debug(f'Found {len(op_for_child_node_list)} ops for children of node being removed ({target_node.uid});'
                         f' adding as child dependency of each')

            existing_child_count = 0
            for op_for_child_node in op_for_child_node_list:
                existing_child: Optional[OpGraphNode] = op_for_child_node.get_first_child()
                if existing_child:
                    assert existing_child.is_remove_type()
                    if existing_child.get_tgt_node().device_uid != target_device_uid or existing_child.get_tgt_node().uid != target_node.uid:
                        raise RuntimeError(f'Found unexpected child node: {existing_child} (attempting to insert: {node_to_insert})')
                    existing_child_count += 1
                else:
                    op_for_child_node.link_child(node_to_insert)

            # Logically, either no children, *or* all child nodes have already been attached to parent
            if not (existing_child_count == 0 or existing_child_count == len(op_for_child_node_list)):
                raise RuntimeError(f'Inconsistency detected in op tree! Only {existing_child_count} of {len(op_for_child_node_list)}'
                                   f'RM nodes in dir have an RM child (attempting to insert: {node_to_insert})')

            if existing_child_count > 0:
                logger.debug(f'All parent ops already have child op attached with correct node UID. Discarding op {node_to_insert.node_uid}')
                return False

        elif last_target_op:
            # If existing op is found for the target node, add below that.
            if last_target_op.is_remove_type():
                logger.info(f'UserOp node being enqueued (UID {node_to_insert.op.src_node.dn_uid}, tgt {target_node.dn_uid}) is an RM type which is '
                            f'a dup of already enqueued RM ({last_target_op.op.src_node.dn_uid}); discarding!')
                return False

            # The node's children MUST be removed first. It is invalid to RM a node which has children.
            if last_target_op.get_child_list():
                raise RuntimeError(f'While trying to add RM op: did not expect existing op for node to have children! (Node={last_target_op})')
            logger.debug(f'Found pending op(s) for target node {target_node.dn_uid} for RM op; adding as child dependency')
            last_target_op.link_child(node_to_insert)
        else:
            logger.debug(f'Found no previous ops for target node {target_node.dn_uid} or its children; adding to root')
            self._graph_root.link_child(node_to_insert)

        for parent_uid in tgt_parent_uid_list:
            # Sometimes the nodes are not inserted in exactly the right order. Link any nodes which fell through cuz their child
            # was inserted first (do this after we have a chance to return False):
            last_parent_op = self._get_lowest_priority_op_node(target_device_uid, parent_uid)
            if last_parent_op and last_parent_op.is_remove_type():
                logger.debug(f'Found potentially unlinked op for parent: {last_parent_op}; linking as child to node being inserted'
                             f' ({node_to_insert})')
                last_parent_op.link_parent(node_to_insert)

        return True

    def _insert_non_rm_node_in_tree(self, node_to_insert: OpGraphNode, last_target_op, tgt_parent_uid_list: List[UID]) -> bool:
        target_node: Node = node_to_insert.get_tgt_node()
        target_device_uid: UID = target_node.device_uid

        for parent_uid in tgt_parent_uid_list:
            parent_last_op = self._get_lowest_priority_op_node(target_device_uid, parent_uid)

            if last_target_op and parent_last_op:
                if last_target_op.get_level() > parent_last_op.get_level():
                    logger.debug(f'Last target op (for node {target_node.dn_uid}) is lower level than last op for parent node ({parent_uid});'
                                 f' adding new node as child of last target op')
                    last_target_op.link_child(node_to_insert)
                else:
                    logger.debug(f'Last target op is >= level than last op for parent node; adding new node as child of last op for parent node')
                    parent_last_op.link_child(node_to_insert)
            elif last_target_op:
                assert not parent_last_op
                logger.debug(f'Found pending op(s) for target node {target_node.dn_uid}; adding new node as child dependency')
                last_target_op.link_child(node_to_insert)
            elif parent_last_op:
                assert not last_target_op
                logger.debug(f'Found pending op(s) for parent node {parent_uid} (device {target_device_uid}); adding new node as child dependency')
                parent_last_op.link_child(node_to_insert)
            else:
                assert not parent_last_op and not last_target_op
                logger.debug(f'Found no previous ops for either target node {target_node.dn_uid} or parent node {parent_uid}; adding to root')
                self._graph_root.link_child(node_to_insert)

        return True

    def _enqueue_single_node(self, node_to_insert: OpGraphNode) -> bool:
        """
        The node shall be added as a child dependency of either the last operation which affected its target,
        or as a child dependency of the last operation which affected its parent, whichever has lower priority (i.e. has a lower level
        in the dependency tree). In the case where neither the node nor its parent has a pending operation, we obviously can just add
        to the top of the dependency tree.

        Returns True if the node was successfully nq'd; returns False if discarded
        """
        logger.debug(f'Enqueuing single op node: {node_to_insert}')

        # Need to clear out previous relationships before adding to main tree:
        node_to_insert.clear_relationships()

        target_node: Node = node_to_insert.get_tgt_node()
        tgt_parent_uid_list: List[UID] = target_node.get_parent_uids()

        # First check whether the target node is known and has pending operations
        last_target_op = self._get_lowest_priority_op_node(target_node.device_uid, target_node.uid)

        if node_to_insert.is_remove_type():
            insert_succeeded = self._insert_rm_node_in_tree(node_to_insert, last_target_op, tgt_parent_uid_list)
        else:
            # Not an RM node:
            insert_succeeded = self._insert_non_rm_node_in_tree(node_to_insert, last_target_op, tgt_parent_uid_list)

        if not insert_succeeded:
            return False

        # Always add to node_queue_dict:
        node_dict = self._node_q_dict.get(target_node.device_uid, None)
        if not node_dict:
            node_dict = dict()
            self._node_q_dict[target_node.device_uid] = node_dict
        pending_op_queue = node_dict.get(target_node.uid)
        if not pending_op_queue:
            pending_op_queue = collections.deque()
            node_dict[target_node.uid] = pending_op_queue
        pending_op_queue.append(node_to_insert)

        logger.info(f'Enqueued single op node: {node_to_insert}')

        return True

    def enqueue_batch(self, op_root: RootNode) -> Tuple[List[UserOp], List[UserOp]]:
        """Returns a tuple of [inserted user ops, discarded user ops]
        Algo:
        1. Discard root
        2. Examine each child of root. Each shall be treated as its own subtree.
        3. For each subtree, look up all its nodes in the master dict. Level...?

        Disregard the kind of op when building the tree; they are all equal for now (except for RM; see below):
        Once entire tree is constructed, invert the RM subtree (if any) so that ancestor RMs become descendants

        Note: it is assumed that the given batch has already been reduced, and stored in the pending ops tree.
        Every op node in the supplied graph must be accounted for.
        """
        if not op_root.get_child_list():
            raise RuntimeError(f'Batch has no nodes!')

        batch_uid: UID = op_root.get_first_child().op.batch_uid

        logger.info(f'Adding batch {batch_uid} to OpGraph')

        breadth_first_list: List[OpGraphNode] = op_root.get_all_nodes_in_subtree()
        inserted_op_dict: Dict[UID, UserOp] = {}
        discarded_op_dict: Dict[UID, UserOp] = {}
        for node_to_nq in _skip_root(breadth_first_list):
            with self._cv_can_get:
                succeeded = self._enqueue_single_node(node_to_nq)
                if succeeded:
                    inserted_op_dict[node_to_nq.op.op_uid] = node_to_nq.op
                else:
                    discarded_op_dict[node_to_nq.op.op_uid] = node_to_nq.op

                # notify consumers there is something to get:
                self._cv_can_get.notifyAll()

        # Wake Central Executor:
        self.backend.executor.notify()

        logger.debug(f'Done adding batch {batch_uid}')
        self._print_current_state()

        return list(inserted_op_dict.values()), list(discarded_op_dict.values())

    def _is_node_ready(self, op: UserOp, node: Node, node_type_str: str) -> bool:
        node_dict: Dict[UID, Deque[OpGraphNode]] = self._node_q_dict.get(node.device_uid, None)
        if not node_dict:
            logger.error(f'Could not find entry in node dict for device_uid {node.device_uid} (from op {node_type_str} node); raising error')
            raise RuntimeError(f'Serious error: master dict has no entries for device_uid={node.device_uid} (op {node_type_str} node) !')
        pending_op_queue: Deque[OpGraphNode] = node_dict.get(node.uid, None)
        if not pending_op_queue:
            logger.error(f'Could not find entry for node UID {node.uid} (op {node_type_str} node: op={op}); raising error')
            raise RuntimeError(f'Serious error: master dict has no entries for op {node_type_str} node (uid={node.uid})!')

        op_graph_node = pending_op_queue[0]
        if op.op_uid != op_graph_node.op.op_uid:
            if SUPER_DEBUG_ENABLED:
                logger.debug(f'Skipping UserOp (UID {op_graph_node.op.op_uid}): it is not next in {node_type_str} node queue')
            return False

        if not op_graph_node.is_child_of_root():
            if SUPER_DEBUG_ENABLED:
                logger.debug(f'Skipping UserOp (UID {op_graph_node.op.op_uid}): {node_type_str} node is not child of root')
            return False

        return True

    def _try_get(self) -> Optional[UserOp]:
        # We can optimize this later

        for op_node in self._graph_root.get_child_list():
            if SUPER_DEBUG_ENABLED:
                logger.debug(f'TryGet(): Examining {op_node}')

            if op_node.op.has_dst():
                # If the UserOp has both src and dst nodes, *both* must be next in their queues, and also be just below root.
                if op_node.is_dst():
                    # Dst node is child of root. But verify corresponding src node is also child of root
                    is_other_node_ready = self._is_node_ready(op_node.op, op_node.op.src_node, 'src')
                else:
                    # Src node is child of root. But verify corresponding dst node is also child of root
                    is_other_node_ready = self._is_node_ready(op_node.op, op_node.op.dst_node, 'dst')

                if not is_other_node_ready:
                    if SUPER_DEBUG_ENABLED:
                        logger.debug(f'TryGet(): Skipping node because other op graph node (is_dst={op_node.is_dst()}) is not ready')
                    continue

            # Make sure the node has not already been checked out:
            if not self._outstanding_actions.get(op_node.op.op_uid, None):
                if SUPER_DEBUG_ENABLED:
                    logger.debug(f'TryGet(): inserting node into OutstandingActionsDict: {op_node}')
                self._outstanding_actions[op_node.op.op_uid] = op_node.op
                return op_node.op
            else:
                if SUPER_DEBUG_ENABLED:
                    logger.debug(f'TryGet(): Skipping node because it is already outstanding')

        if TRACE_ENABLED:
            logger.debug(f'TryGet(): Returning None')
        return None

    def get_next_op(self) -> Optional[UserOp]:
        """Gets and returns the next available UserOp from the tree; BLOCKS if no pending ops or all the pending ops have
        already been gotten.

        Internally this class keeps track of what this has returned previously, and will expect to be notified when each is complete
         - think of get_next_op() as a repository checkout, and op_completed() as a commit"""

        # Block until we have an op
        while True:
            if self.was_shutdown:
                logger.debug(f'get_next_op(): Discovered shutdown flag was set. Returning None')
                return None

            with self._cv_can_get:
                op = self._try_get()
                if op:
                    logger.info(f'Got next pending op: {op}')
                    return op
                else:
                    logger.debug(f'No pending ops; sleeping until notified')
                    self._cv_can_get.wait()

    def get_next_op_nowait(self) -> Optional[UserOp]:
        """Same as get_next_op(), but returns immediately"""
        if self.was_shutdown:
            logger.debug(f'get_next_op(): Discovered shutdown flag was set. Returning None')
            return None

        with self._cv_can_get:
            op = self._try_get()
            if op:
                logger.info(f'Got next pending op: {op}')
                return op

        return None

    def _is_child_of_root(self, node: OpGraphNode) -> bool:
        parent = node.get_first_parent()
        return parent and parent.node_uid == self._graph_root.node_uid

    def pop_op(self, op: UserOp):
        """Ensure that we were expecting this op to be copmleted, and remove it from the tree."""
        logger.debug(f'Entered pop_op() for op {op}')

        with self._cv_can_get:
            if self._outstanding_actions.get(op.op_uid, None):
                self._outstanding_actions.pop(op.op_uid)
            else:
                raise RuntimeError(f'Complated op not found in outstanding op list (action UID {op.op_uid}')

            # I. SRC Node

            # I-1. Remove src node from node dict

            node_dict: Dict[UID, Deque[OpGraphNode]] = self._node_q_dict.get(op.src_node.device_uid)
            if not node_dict:
                # very bad
                raise RuntimeError(f'Completed op src node device_uid ({op.src_node.device_uid}) not found in master dict (for {op.src_node.dn_uid}')

            src_node_list: Deque[OpGraphNode] = node_dict.get(op.src_node.uid)
            if not src_node_list:
                # very bad
                raise RuntimeError(f'Completed op for src node UID ({op.src_node.uid}) not found in master dict!')

            src_op_node: OpGraphNode = src_node_list.popleft()
            if src_op_node.op.op_uid != op.op_uid:
                # very bad
                raise RuntimeError(f'Completed op (UID {op.op_uid}) does not match first node popped from src queue '
                                   f'(UID {src_op_node.op.op_uid})')
            if not src_node_list:
                # Remove queue if it is empty:
                node_dict.pop(op.src_node.uid, None)
            if not node_dict:
                # Remove dict if it is empty:
                self._node_q_dict.pop(op.src_node.uid, None)

            # I-2. Remove src node from op graph

            # validate it is a child of root
            if not self._is_child_of_root(src_op_node):
                parent = src_op_node.get_first_parent()
                parent_uid = parent.node_uid if parent else None
                raise RuntimeError(f'Src node for completed op is not a parent of root (instead found parent op node {parent_uid})')

            # unlink from its parent
            self._graph_root.unlink_child(src_op_node)

            # unlink its children also. If the child then has no other parents, it moves up to become child of root
            for child in src_op_node.get_child_list():
                child.unlink_parent(src_op_node)

                if not child.get_parent_list():
                    self._graph_root.link_child(child)
                else:
                    # Sanity check:
                    for par in child.get_parent_list():
                        if not par.get_parent_list():
                            logger.error(f'Node has no parents: {par}')

            # I-3. Delete src node
            del src_op_node

            # II. DST Node
            if op.has_dst():
                # II-1. Remove dst node from node dict

                node_dict: Dict[UID, Deque[OpGraphNode]] = self._node_q_dict.get(op.dst_node.device_uid)
                if not node_dict:
                    # very bad
                    raise RuntimeError(
                        f'Completed op dst node device_uid ({op.dst_node.device_uid}) not found in master dict (for {op.dst_node.dn_uid})')

                dst_node_list: Deque[OpGraphNode] = node_dict.get(op.dst_node.uid)
                if not dst_node_list:
                    raise RuntimeError(f'Dst node for completed op not found in master dict (dst node UID {op.dst_node.uid})')

                dst_op_node = dst_node_list.popleft()
                if dst_op_node.op.op_uid != op.op_uid:
                    raise RuntimeError(f'Completed op (UID {op.op_uid}) does not match first node popped from dst queue '
                                       f'(UID {dst_op_node.op.op_uid})')
                if not dst_node_list:
                    # Remove queue if it is empty:
                    node_dict.pop(op.dst_node.uid, None)
                if not node_dict:
                    # Remove dict if it is empty:
                    self._node_q_dict.pop(op.dst_node.uid, None)

                # II-2. Remove dst node from op graph

                # validate it is a child of root
                if not self._is_child_of_root(dst_op_node):
                    parent = dst_op_node.get_first_parent()
                    parent_uid = parent.node_uid if parent else None
                    raise RuntimeError(f'Dst node for completed op is not a parent of root (instead found parent {parent_uid})')

                # unlink from its parent
                self._graph_root.unlink_child(dst_op_node)

                # unlink its children also. If the child then has no other parents, it moves up to become child of root
                for child in dst_op_node.get_child_list():
                    child.unlink_parent(dst_op_node)

                    if not child.get_parent_list():
                        self._graph_root.link_child(child)

                # II-3. Delete dst node
                del dst_op_node

            logger.debug(f'Done with pop_op() for op: {op}')
            self._print_current_state()

            # this may have jostled the tree to make something else free:
            self._cv_can_get.notifyAll()

        # Wake Central Executor in case it is in the waiting state:
        self.backend.executor.notify()


def _skip_root(node_list: List[OpGraphNode]) -> Iterable[OpGraphNode]:
    """Note: does not support case when root node is second or later in the list"""
    if node_list and node_list[0].node_uid == SUPER_ROOT_UID:
        node_list_iter = iter(node_list)
        next(node_list_iter)
        return node_list_iter

    return node_list
