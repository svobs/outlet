import collections
import logging
import threading
from typing import Deque, Dict, Iterable, List, Optional, Tuple

from backend.executor.user_op.op_graph_node import OpGraphNode, RmOpNode, RootNode
from constants import NULL_UID, SUPER_DEBUG_ENABLED, SUPER_ROOT_UID, TRACE_ENABLED
from model.node.node import Node
from model.uid import UID
from model.user_op import UserOp
from util.has_lifecycle import HasLifecycle
from util.stopwatch_sec import Stopwatch

logger = logging.getLogger(__name__)


def skip_root(node_list: List[OpGraphNode]) -> Iterable[OpGraphNode]:
    """Note: does not support case when root node is second or later in the list"""
    if node_list and node_list[0].is_root():
        return node_list[1:]

    return node_list


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

        self._cv_can_get = threading.Condition()
        """Used to help consumers block"""

        self._node_q_dict: Dict[UID, Dict[UID, Deque[OpGraphNode]]] = {}
        """Contains entries for all nodes have pending ops. Each entry has a queue of pending ops for that target node"""

        self._graph_root: OpGraphNode = RootNode()
        """Root of graph. Has no useful internal data; we value it for its children"""

        self._outstanding_actions: Dict[UID, UserOp] = {}
        """Contains entries for all Ops which have running operations. Keyed by action UID"""

        self._max_added_op_uid: UID = NULL_UID
        """Sanity check. Keep track of what's been added to the graph, and disallow duplicate or past inserts"""

    def shutdown(self):
        """Need to call this for try_get() to return"""
        if self.was_shutdown:
            return

        HasLifecycle.shutdown(self)
        with self._cv_can_get:
            # unblock any get() task which is waiting
            self._cv_can_get.notifyAll()

    def __len__(self):
        with self._cv_can_get:
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
        qd_line_list = []
        queue_count = 0
        for device_uid, node_dict in self._node_q_dict.items():
            queue_count += len(node_dict)
            for node_uid, deque in node_dict.items():
                node_repr_list: List[str] = []
                for node in deque:
                    op_set.add(node.op.op_uid)
                    node_repr_list.append(node.op.get_tag())
                qd_line_list.append(f'NodeUID {node_uid}:')
                for i, node_repr in enumerate(node_repr_list):
                    qd_line_list.append(f'{i}. {node_repr}')

        logger.debug(f'[MainGraph] CURRENT EXECUTION STATE: OpGraph contains {len(graph_line_list)} nodes:')
        for graph_line in graph_line_list:
            logger.debug(f'[MainGraph] {graph_line}')

        logger.debug(f'[MainGraph] CURRENT EXECUTION STATE: NodeQueueDict has {queue_count} queues for {len(self._node_q_dict)} devices, '
                     f'{len(op_set)} total ops:')
        for qd_line in qd_line_list:
            logger.debug(f'[MainGraph] {qd_line}')

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
        with self._cv_can_get:
            op_node: OpGraphNode = self._get_lowest_priority_op_node(device_uid, node_uid)
        if op_node:
            return op_node.op
        return None

    def get_max_added_op_uid(self) -> UID:
        return self._max_added_op_uid

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
        for graph_node in skip_root(breadth_first_list):
            with self._cv_can_get:
                succeeded = self._enqueue_single_node(graph_node)
                if succeeded:
                    inserted_op_dict[graph_node.op.op_uid] = graph_node.op

                    if self._max_added_op_uid < graph_node.op.op_uid:
                        self._max_added_op_uid = graph_node.op.op_uid

                else:
                    discarded_op_dict[graph_node.op.op_uid] = graph_node.op

                # notify consumers there is something to get:
                self._cv_can_get.notifyAll()

        # Wake Central Executor:
        self.backend.executor.notify()

        with self._cv_can_get:
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

        for og_node in self._graph_root.get_child_list():
            if SUPER_DEBUG_ENABLED:
                logger.debug(f'TryGet(): Examining {og_node}')

            if og_node.op.has_dst():
                # If the UserOp has both src and dst nodes, *both* must be next in their queues, and also be just below root.
                if og_node.is_dst():
                    # Dst node is child of root. But verify corresponding src node is also child of root
                    is_other_node_ready = self._is_node_ready(og_node.op, og_node.op.src_node, 'src')
                else:
                    # Src node is child of root. But verify corresponding dst node is also child of root
                    is_other_node_ready = self._is_node_ready(og_node.op, og_node.op.dst_node, 'dst')

                if not is_other_node_ready:
                    if SUPER_DEBUG_ENABLED:
                        logger.debug(f'TryGet(): Skipping node because other op graph node (is_dst={og_node.is_dst()}) is not ready')
                    continue

            # Make sure the node has not already been checked out:
            if not self._outstanding_actions.get(og_node.op.op_uid, None):
                if SUPER_DEBUG_ENABLED:
                    logger.debug(f'TryGet(): inserting node into OutstandingActionsDict: {og_node}')
                self._outstanding_actions[og_node.op.op_uid] = og_node.op
                return og_node.op
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

    def pop_op(self, op: UserOp):
        """Ensure that we were expecting this op to be copmleted, and remove it from the tree."""
        logger.debug(f'Entered pop_op() for op {op}')

        with self._cv_can_get:
            if not self._outstanding_actions.pop(op.op_uid, None):
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
            if not src_op_node.is_child_of_root():
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
                if not dst_op_node.is_child_of_root():
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
