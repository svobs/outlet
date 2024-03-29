import collections
import copy
import logging
import threading
from typing import Callable, Deque, Dict, Iterable, List, Optional, Set

from be.exec.user_op.op_graph_node import OpGraphNode, RootNode
from constants import IconId, NULL_UID, OP_GRAPH_VALIDATE_AFTER_BATCH_INSERT
from logging_constants import OP_GRAPH_DEBUG_ENABLED, SUPER_DEBUG_ENABLED, TRACE_ENABLED
from error import InvalidInsertOpGraphError, OpGraphError, UnsuccessfulBatchInsertError
from model.node.node import TNode
from model.uid import UID
from model.user_op import ChangeTreeCategoryMeta, UserOp, UserOpResult, UserOpStatus, UserOpCode
from util.has_lifecycle import HasLifecycle, stop_func
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

    Directed acyclic graph for user ops.
    An OpGraph is built out of a set of "OpGraph nodes", aka "OGNs", which I may also refer to as "graph nodes", which have class OpGraphNode.
    These should be distinguished from regular file nodes, dir nodes, etc, which I'll refer to as simply "nodes", with class TNode.

    Every UserOp in the graph will be reprensented by either 1 or 2 OGNs, with one OGN for its src node, and one OGN for its dst node (if it has
    a dst).

    Each OGN can have 1 or more child OGNs, and 1 or more parent OGNs, depending on its type. Parent & child OGNs contain pointers to each other.
    The execution of each child is contigent upon the successful completion of all its parent OGNs. Another way of saying this is that
    an OGN cannot run until all of its parent OGNs' UserOps have completed without error.
    ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼
    """

    def __init__(self, name):
        HasLifecycle.__init__(self)
        self.name = name
        self._ogn_count: int = 0
        self._op_count: int = 0

        self._cv_can_get = threading.Condition()
        """Used to help consumers block"""

        self._node_ogn_q_dict: Dict[UID, Dict[UID, Deque[OpGraphNode]]] = {}
        """Contains entries for all nodes associated with UserOps. Each entry contains a queue of all UserOps for that target node"""

        self.root: RootNode = RootNode()
        """Root of graph. Has no useful internal data; we value it for its children"""

        self._outstanding_op_dict: Dict[UID, UserOp] = {}
        """Contains entries for all UserOps which have running operations. Keyed by action UID"""

        self._max_added_op_uid: UID = NULL_UID
        """Sanity check. Keep track of what's been added to the graph, and disallow duplicate or past inserts"""

        self._ancestor_dict: Dict[UID, Dict[UID, int]] = {}
        # Change tracking for ancestor icons. Calling pop_ancestor_icon_changes() pops and returns both of these:
        self._added_ancestor_dict: Dict[UID, Set[UID]] = {}
        self._removed_ancestor_dict: Dict[UID, Set[UID]] = {}
        self._changed_node_dict: Dict[UID, Set[UID]] = {}

    @stop_func
    def shutdown(self):
        """Need to call this for try_get() to return"""
        if self.was_shutdown:
            return

        with self._cv_can_get:
            # unblock any get() task which is waiting
            self._cv_can_get.notifyAll()

    # Icon stuff
    # ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼

    def get_icon_for_node(self, device_uid: UID, node_uid: UID) -> Optional[IconId]:
        with self._cv_can_get:
            ogn: OpGraphNode = self._get_last_pending_ogn_for_node(device_uid, node_uid)
            if ogn and not ogn.op.is_completed():
                try:
                    icon = ChangeTreeCategoryMeta.get_icon_for_node(ogn.get_tgt_node().is_dir(), is_dst=ogn.is_dst(), op=ogn.op)
                    if icon:
                        if SUPER_DEBUG_ENABLED:
                            op_str = f'{ogn.op.op_uid}: {ogn.op.op_type.name}, {ogn.op.get_status().name}'
                            logger.debug(f'TNode {device_uid}:{node_uid} belongs to pending op ({op_str}): returning icon {icon.name}')
                        return icon
                    else:
                        logger.debug(f'Did not get icon for OGN tgt (OGN={ogn}); returning None')
                        return None
                except RuntimeError:
                    logger.exception(f'Error getting icon for OGN: {ogn}; returning None')
                    return None

            device_ancestor_dict: Dict[UID, int] = self._ancestor_dict.get(device_uid)
            if device_ancestor_dict and device_ancestor_dict.get(node_uid):
                logger.debug(f'TNode {device_uid}:{node_uid} has a downstream op: returning icon {IconId.ICON_DIR_PENDING_DOWNSTREAM_OP.name}')
                return IconId.ICON_DIR_PENDING_DOWNSTREAM_OP

            if TRACE_ENABLED:
                logger.debug(f'TNode {device_uid}:{node_uid}: no custom icon')
            return None

    def pop_ancestor_icon_changes(self):
        """This returns a set of dicts of all the nodes which have changed icon since the last time this method was called."""
        with self._cv_can_get:
            added_ancestor_dict: Dict[UID, Set[UID]] = self._added_ancestor_dict
            removed_ancestor_dict: Dict[UID, Set[UID]] = self._removed_ancestor_dict
            changed_node_dict: Dict[UID, Set[UID]] = self._changed_node_dict
            self._added_ancestor_dict = {}
            self._removed_ancestor_dict = {}
            self._changed_node_dict = {}

        return added_ancestor_dict, removed_ancestor_dict, changed_node_dict

    def _increment_icon_update_counts(self, device_uid: UID, ancestor_node_uid_list: List[UID]):
        if SUPER_DEBUG_ENABLED:
            logger.debug(f'[{self.name}] IncrementIconCounts start: AncestorDict: {self._ancestor_dict}')
        sw = Stopwatch()

        device_ancestor_dict: Dict[UID, int] = self._ancestor_dict.get(device_uid)
        if not device_ancestor_dict:
            device_ancestor_dict = {}
            self._ancestor_dict[device_uid] = device_ancestor_dict
        for node_uid in ancestor_node_uid_list:
            count = device_ancestor_dict.get(node_uid, 0)
            if count == 0:
                added_ancestor_map_for_device = self._added_ancestor_dict.get(device_uid)
                if not added_ancestor_map_for_device:
                    added_ancestor_map_for_device = set()
                    self._added_ancestor_dict[device_uid] = added_ancestor_map_for_device
                added_ancestor_map_for_device.add(node_uid)

                removed_ancestor_map_for_device = self._removed_ancestor_dict.get(device_uid)
                if removed_ancestor_map_for_device and node_uid in removed_ancestor_map_for_device:
                    removed_ancestor_map_for_device.remove(node_uid)
            device_ancestor_dict[node_uid] = count + 1

        if SUPER_DEBUG_ENABLED:
            logger.debug(f'[{self.name}] {sw} IncrementIconCounts done: AncestorDict: {self._ancestor_dict}')

    def _decrement_icon_update_counts(self, ogn: OpGraphNode):
        if SUPER_DEBUG_ENABLED:
            logger.debug(f'[{self.name}] DecrementIconCounts start: Ancestor dict: {self._ancestor_dict}')

        device_uid = ogn.get_tgt_node().device_uid
        device_ancestor_dict: Dict[UID, int] = self._ancestor_dict.get(device_uid)
        if not device_ancestor_dict:
            raise RuntimeError(f'Completed op (UID {ogn.op.op_uid}): no entry for device_uid ({device_uid}) in ancestor dict!')
        for ancestor_uid in ogn.tgt_ancestor_uid_list:
            count = device_ancestor_dict.get(ancestor_uid) - 1
            if count == 0:
                device_ancestor_dict.pop(ancestor_uid)

                added_ancestor_map_for_device = self._added_ancestor_dict.get(device_uid)
                if added_ancestor_map_for_device and ancestor_uid in added_ancestor_map_for_device:
                    added_ancestor_map_for_device.remove(ancestor_uid)

                removed_ancestor_map_for_device = self._removed_ancestor_dict.get(device_uid)
                if not removed_ancestor_map_for_device:
                    removed_ancestor_map_for_device = set()
                    self._removed_ancestor_dict[device_uid] = removed_ancestor_map_for_device
                removed_ancestor_map_for_device.add(ancestor_uid)
            else:
                device_ancestor_dict[ancestor_uid] = count

        if SUPER_DEBUG_ENABLED:
            logger.debug(f'[{self.name}] DecrementIconCounts done: Ancestor dict: {self._ancestor_dict}')

    def _add_tgt_node_to_icon_changes_dict(self, tgt_node: TNode):
        # TODO: merge this structure with ancestor change counts
        logger.debug(f'[{self.name}] Adding tnode {tgt_node.node_identifier} to ChangedIconDict')
        # Add tgt node to change map
        node_map_for_device = self._changed_node_dict.get(tgt_node.device_uid)
        if not node_map_for_device:
            node_map_for_device = set()
            self._changed_node_dict[tgt_node.device_uid] = node_map_for_device
        node_map_for_device.add(tgt_node.uid)

    # Misc non-mutating methods
    # ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼

    def __len__(self):
        """Returns the count of ops in the OpGraph."""

        return self.get_ogn_count()

    def get_ogn_count(self):

        def _visit(_) -> bool:
            _visit.count = _visit.count + 1
            return False

        _visit.count = 0

        with self._cv_can_get:
            self._for_all_ogn_in_graph(_visit)
            return _visit.count

    def get_op_count(self):
        with self._cv_can_get:
            op_set = set()
            self._for_all_ogn_in_graph(lambda _ogn: op_set.add(_ogn.op.op_uid) and False)
            return len(op_set)

    def _print_current_state(self):
        if not OP_GRAPH_DEBUG_ENABLED:
            logger.debug(f'[{self.name}] CURRENT_STATE: OpGraph = [{self._ogn_count} OGNs, {self._op_count} ops]')
            return

        graph_line_list = self.root.print_recursively()

        op_set = set()
        qd_line_list = []
        queue_count = 0
        for device_uid, node_dict in self._node_ogn_q_dict.items():
            queue_count += len(node_dict)
            for node_uid, deque in node_dict.items():
                node_repr_list: List[str] = []
                for node in deque:
                    op_set.add(node.op.op_uid)
                    node_repr_list.append(node)
                qd_line_list.append(f'TNode {device_uid}:{node_uid}:')
                for i, node_repr in enumerate(node_repr_list):
                    qd_line_list.append(f'  [{i}] {node_repr}')

        logger.debug(f'[{self.name}] CURRENT_STATE[0]: OpGraph = [{len(graph_line_list)} OGNs]:')
        for graph_line in graph_line_list:
            logger.debug(f'[{self.name}] {graph_line}')

        logger.debug(f'[{self.name}] CURRENT_STATE[1]: NodeQueues = [{len(op_set)} ops, {len(self._node_ogn_q_dict)} devices, '
                     f'{queue_count} nodes]:')
        for qd_line in qd_line_list:
            logger.debug(f'[{self.name}] {qd_line}')

    def _get_ogn_queue_for_node(self, device_uid: UID, node_uid: UID) -> Optional[Deque[OpGraphNode]]:
        node_dict = self._node_ogn_q_dict.get(device_uid, None)
        if node_dict:
            return node_dict.get(node_uid, None)
        return None

    def _get_last_pending_ogn_for_node(self, device_uid: UID, node_uid: UID) -> Optional[OpGraphNode]:
        ogn_list = self._get_ogn_queue_for_node(device_uid=device_uid, node_uid=node_uid)
        if ogn_list:
            # last element in list is the lowest priority:
            return ogn_list[-1]
        return None

    def _get_next_pending_ogn_for_node(self, device_uid: UID, node_uid: UID) -> Optional[OpGraphNode]:
        ogn_list = self._get_ogn_queue_for_node(device_uid=device_uid, node_uid=node_uid)
        if ogn_list:
            return ogn_list[0]
        return None

    def get_last_pending_op_for_node(self, device_uid: UID, node_uid: UID) -> Optional[UserOp]:
        """This is a public method."""
        with self._cv_can_get:
            op_node: OpGraphNode = self._get_last_pending_ogn_for_node(device_uid, node_uid)
        if op_node:
            return op_node.op
        return None

    def get_max_added_op_uid(self) -> UID:
        with self._cv_can_get:
            return self._max_added_op_uid

    # VALIDATION
    # ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼

    def validate_internal_consistency(self):
        with self._cv_can_get:
            self._validate_internal_consistency()

    def _validate_internal_consistency(self):
        """The caller is responsible for locking the graph before calling this."""
        logger.debug(f'[{self.name}] Validating internal structural consistency of OpGraph...')

        sw = Stopwatch()
        error_count = 0
        ogn_coverage_dict: Dict[UID, OpGraphNode] = {self.root.node_uid: self.root}  # OGN uid -> OGN
        binary_op_src_coverage_dict: Dict[UID, OpGraphNode] = {}
        binary_op_dst_coverage_dict: Dict[UID, OpGraphNode] = {}
        unrecognized_og_node_dict: Dict[UID, str] = {}

        # Iterate through graph using a queue, using ogn_coverage_dict to avoid doing duplicate analysis:
        ogn_queue: Deque[OpGraphNode] = collections.deque()

        for child_of_root in self.root.get_child_list():
            if not child_of_root.is_child_of_root():
                error_count += 1
                logger.error(f'[{self.name}] ValidateGraph: OGN is a child of root but is_child_of_root()==False: {child_of_root}')
            ogn_queue.append(child_of_root)

        while len(ogn_queue) > 0:
            ogn: OpGraphNode = ogn_queue.popleft()
            logger.debug(f'[{self.name}] ValidateGraph: Examining OGN from graph: {ogn}')

            if ogn_coverage_dict.get(ogn.node_uid, None):
                # already processed this node
                continue

            ogn_coverage_dict[ogn.node_uid] = ogn

            # Verify its op:
            if ogn.op.has_dst():
                if ogn.is_src():
                    prev_ogn = binary_op_src_coverage_dict.get(ogn.op.op_uid, None)
                    if prev_ogn:
                        error_count += 1
                        logger.error(f'[{self.name}] ValidateGraph: Duplicate OGNs for src op! Prev={prev_ogn}, current={ogn}')
                    else:
                        binary_op_src_coverage_dict[ogn.op.op_uid] = ogn
                else:
                    assert ogn.is_dst(), f'Expected dst-type OGN but got: {ogn}'
                    prev_ogn = binary_op_dst_coverage_dict.get(ogn.op.op_uid, None)
                    if prev_ogn:
                        error_count += 1
                        logger.error(f'[{self.name}] ValidateGraph: Duplicate OGNs for dst op! Prev={prev_ogn}, current={ogn}')
                    else:
                        binary_op_dst_coverage_dict[ogn.op.op_uid] = ogn

            # Verify parents:
            ogn_parent_list: List[OpGraphNode] = ogn.get_parent_list()
            if not ogn_parent_list:
                error_count += 1
                logger.error(f'ValidateGraph: OG node has no parents: {ogn}')
            else:
                parent_uid_set: Set[UID] = set()
                for ogn_parent in ogn_parent_list:
                    if ogn_parent.node_uid in parent_uid_set:
                        error_count += 1
                        logger.error(f'[{self.name}] ValidateGraph: Duplicate parent listed in OGN! OGN uid={ogn.node_uid}, '
                                     f'parent_uid={ogn_parent.node_uid}')
                        continue
                    else:
                        parent_uid_set.add(ogn_parent.node_uid)

                    if not ogn_coverage_dict.get(ogn_parent.node_uid, None):
                        unrecognized_og_node_dict[ogn_parent.node_uid] = \
                            f'OGN references unrecognized parent! OGN uid={ogn.node_uid}, parent_OGN={ogn_parent}'

                has_multiple_parents = len(ogn_parent_list) > 1
                tgt_node: TNode = ogn.get_tgt_node()

                if not ogn.is_child_of_root():
                    if ogn.is_rm_node():
                        all_parents_must_be_remove_type = False

                        child_node_uid_set = set()
                        for ogn_parent in ogn_parent_list:
                            if ogn_parent.is_remove_type():
                                all_parents_must_be_remove_type = True
                                parent_tgt_node: TNode = ogn_parent.get_tgt_node()
                                if not tgt_node.is_parent_of(parent_tgt_node):
                                    error_count += 1
                                    logger.error(f'[{self.name}] ValidateGraph: Parent of RM OGN is remove-type, but its target node is not '
                                                 f'a child of the RM OGN\'s target node! OGN={ogn}, parent={ogn_parent}')
                                if parent_tgt_node.uid in child_node_uid_set:
                                    error_count += 1
                                    logger.error(f'[{self.name}] ValidateGraph: Parents of RM OGN have duplicate target node! OGN={ogn}, '
                                                 f'parent={ogn_parent}, offending node UID={parent_tgt_node.uid}')
                                child_node_uid_set.add(parent_tgt_node.uid)
                            else:
                                # Parent is not remove-type

                                if all_parents_must_be_remove_type:
                                    error_count += 1
                                    logger.error(f'[{self.name}] ValidateGraph: Some parents of RM OGN are remove-type, but this one is not! '
                                                 f'OG node={ogn}, offending OG parent={ogn_parent}')
                                else:
                                    # Parent's tgt node must be an ancestor of tgt node (or same node).
                                    # Check all paths and confirm that at least one path in parent tgt contains the path of tgt
                                    if not ogn.is_this_tgt_a_descendant_of_ogn_tgt(ogn_parent):
                                        error_count += 1
                                        logger.error(f'[{self.name}] ValidateGraph: Parent of RM OGN is not remove-type, and its '
                                                     f'target node is not an ancestor of its child OGN\'s target! OGN={ogn}, '
                                                     f'offending OGN parent={ogn_parent}')

                    else:  # NOT ogn.is_rm_node()
                        # "finish" types are allowed to have arbitrary number of parents
                        if ogn.is_finish_dir():
                            for ogn_parent in ogn_parent_list:
                                if not ogn_parent.is_this_tgt_a_descendant_of_ogn_tgt(ogn):
                                    # FINISH_DIR OGNs should go in reverse order
                                    error_count += 1
                                    logger.error(f'[{self.name}] ValidateGraph: FINISH_DIR OGN\'s target node is not an ancestor '
                                                 f'of its parent FINISH_DIR\'s target! OGN={ogn}, offending parent={ogn_parent}')

                        else:
                            if has_multiple_parents:
                                if len(ogn_parent_list) > 2:
                                    error_count += 1
                                    logger.error(
                                        f'[{self.name}] ValidateGraph: OGN is not remove-type but has more than 2 parents: {ogn_parent_list}')
                                op_type0 = ogn_parent_list[0].op.op_type
                                op_type1 = ogn_parent_list[1].op.op_type
                                if not op_type0.has_converse() or op_type0.get_converse() != op_type1:
                                    error_count += 1
                                    logger.error(f'[{self.name}] ValidateGraph: Parents of non-RM OGN are not conversely related: {ogn_parent_list}')

                            for ogn_parent in ogn_parent_list:
                                if not ogn.is_this_tgt_a_descendant_of_ogn_tgt(ogn_parent):
                                    error_count += 1
                                    logger.error(f'[{self.name}] ValidateGraph: OGN\'s target node is not a descendent '
                                                 f'of its parent OGN\'s target node! OGN={ogn}, offending parent={ogn_parent}')

            # Verify children:
            child_og_node_list = ogn.get_child_list()
            if child_og_node_list:
                for child_ogn in child_og_node_list:
                    parent_found = False
                    for parent_of_child in child_ogn.get_parent_list():
                        if parent_of_child.node_uid == ogn.node_uid:
                            parent_found = True

                    if not parent_found:
                        error_count += 1
                        logger.error(f'[{self.name}] ValidateGraph: Child of OGN does not list it as parent! OG node={ogn}, '
                                     f'child={child_ogn}')

                    if ogn_coverage_dict.get(child_ogn.node_uid, None):
                        logger.debug(f'[{self.name}] ValidateGraph: Already encountered child of OGN; skipping: {child_ogn}')
                    else:
                        ogn_queue.append(child_ogn)

        for og_node_uid, error_msg in unrecognized_og_node_dict.items():
            if not ogn_coverage_dict.get(og_node_uid, None):
                # still missing: raise original error
                error_count += 1
                logger.error(f'[{self.name}] ValidateGraph: {error_msg}')

        len_ogn_coverage_dict = len(ogn_coverage_dict)
        logger.debug(f'[{self.name}] ValidateGraph: encountered {len_ogn_coverage_dict} OGNs in graph (including root)')

        # Check for missing OGNs for binary ops.
        for ogn_src in binary_op_src_coverage_dict.values():
            if not binary_op_dst_coverage_dict.pop(ogn_src.op.op_uid, None):
                error_count += 1
                logger.error(f'[{self.name}] ValidateGraph: Dst OGN missing for op: {ogn_src.op} (found: {ogn_src})')

        if len(binary_op_dst_coverage_dict) > 0:
            for ogn_dst in binary_op_dst_coverage_dict.values():
                error_count += 1
                logger.error(f'[{self.name}] ValidateGraph: Src OGN missing for op: {ogn_dst.op} (found: {ogn_dst})')

        # Check NodeQueues against graph entries:
        ogn_coverage_dict.pop(self.root.node_uid)

        def _check_for_stranded(_ogn):
            if not ogn_coverage_dict.pop(_ogn.node_uid, None):
                _check_for_stranded.count_found += 1
                logger.error(f'[{self.name}] ValidateGraph: OGN found in NodeQueues which is not in the graph: {_ogn}')
            return False

        _check_for_stranded.count_found = 0
        self._for_all_ogn_in_graph(_check_for_stranded)
        error_count += _check_for_stranded.count_found

        if len(ogn_coverage_dict) > 0:
            for ogn in ogn_coverage_dict.values():
                error_count += 1
                logger.error(f'[{self.name}] ValidateGraph: OGN found in graph which is not present in NodeQueues: {ogn}')

        if error_count > 0:
            raise OpGraphError(f'Validation for OpGraph failed with {error_count} errors!')
        else:
            logger.info(f'[{self.name}] {sw} ValidateGraph: validation done. No errors for {len_ogn_coverage_dict} OGNs')

    # INSERT logic
    # ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼

    def _find_ogn_list_for_children_of(self, potential_parent: TNode) -> List[OpGraphNode]:
        """When adding an RM OGnode, we need to locate its parent OGNs (which relate to the child nodes of its target node).
        We must make sure that all the child nodes are going to be removed before the parent is removed"""
        # TODO: probably could optimize a bit more...maybe make use of the central cache?
        sw_total = Stopwatch()

        ogn_child_list = []

        for node_dict in self._node_ogn_q_dict.values():
            for node_queue in node_dict.values():
                if node_queue:
                    existing_ogn = node_queue[-1]
                    potential_child: TNode = existing_ogn.get_tgt_node()
                    if potential_parent.is_parent_of(potential_child):
                        # We found an OGN for child node
                        ogn_child_list.append(existing_ogn)

        logger.debug(f'[{self.name}] {sw_total} Found {len(ogn_child_list):,} OGNs in graph which are children of parent'
                     f' {potential_parent.node_identifier}')
        return ogn_child_list

    def _find_adopters_for_new_rm_ogn(self, ogn_new: OpGraphNode, prev_ogn_for_target) -> List[OpGraphNode]:
        # Special handling for RM-type nodes.
        # We want to find the lowest RM node in the tree.
        assert ogn_new.is_rm_node()
        target_node: TNode = ogn_new.get_tgt_node()

        if prev_ogn_for_target and prev_ogn_for_target.is_rm_node():
            logger.error(f'[{self.name}] Add_RM_OGN({ogn_new.node_uid}) Op\'s target node ({target_node.dn_uid}) is an RM '
                         f'which is a dup of already enqueued RM ({prev_ogn_for_target.op.src_node.dn_uid})!')
            assert target_node.dn_uid == ogn_new.op.src_node.dn_uid
            assert prev_ogn_for_target.op.src_node.dn_uid == target_node.dn_uid

            raise InvalidInsertOpGraphError(f'Trying to remove node ({target_node.node_identifier}) whose last operation is already an RM operation!')

        # First, see if we can find child nodes of the target node (which would be the parents of the RM OG node):
        ogn_list_for_children_of_tgt: List[OpGraphNode] = self._find_ogn_list_for_children_of(ogn_new.get_tgt_node())
        if ogn_list_for_children_of_tgt:
            # Possibility 1: children
            logger.debug(f'[{self.name}] Add_RM_OGN({ogn_new.node_uid}) Found {len(ogn_list_for_children_of_tgt)} '
                         f'existing OGNs for children of tgt node ({target_node.dn_uid}); examining whether to add as child dependency of each')

            parent_ogn_list: List[OpGraphNode] = []  # Use this list to store parents to link to until we are done validating.
            for ogn_for_child_node in ogn_list_for_children_of_tgt:
                logger.debug(f'[{self.name}] Add_RM_OGN({ogn_new.node_uid}) Examining {ogn_for_child_node}')
                if not ogn_for_child_node.is_remove_type():
                    # This is not allowed. Cannot remove parent dir (aka child op node) unless *all* its children are first removed
                    raise InvalidInsertOpGraphError(f'Found child node for RM-type node which is not RM type: {ogn_for_child_node}')
                if not ogn_for_child_node.is_remove_type():
                    raise InvalidInsertOpGraphError(f'Cannot insert RM OGN: all children of its target must be scheduled for removal first,'
                                                    f'but found: {ogn_for_child_node} (while trying to insert OGN: {ogn_new})')

                # Check if there's an existing OGN which is child of parent OGN.
                conflicting_ogn: Optional[OpGraphNode] = ogn_for_child_node.get_first_child()
                if conflicting_ogn:
                    if not conflicting_ogn.is_remove_type():
                        raise InvalidInsertOpGraphError(f'Found unexpected node which is blocking insert of our RM operation: {conflicting_ogn} '
                                                        f'(while trying to insert: {ogn_new})')
                    else:
                        # extremely rare for this to happen - likely indicates a hole in our logic somewhere
                        raise InvalidInsertOpGraphError(f'Found unexpected child OGN: {conflicting_ogn} (while trying to insert OGN: {ogn_new})')
                else:
                    parent_ogn_list.append(ogn_for_child_node)

            return parent_ogn_list

        elif prev_ogn_for_target:
            # Possibility 2: If existing op is found for the target node, add below that.

            # The node's children MUST be removed first. It is invalid to RM a node which has children.
            # (if the children being added are later scheduled for removal, then they should show up in Possibility 1)
            if prev_ogn_for_target.get_child_list():
                raise InvalidInsertOpGraphError(f'While trying to add RM op: did not expect existing OGN for target to have children! '
                                                f'(tgt={prev_ogn_for_target})')
            logger.debug(f'[{self.name}] Add_RM_OGN({ogn_new.node_uid}) Found pending op(s) for target node {target_node.dn_uid} for RM op; '
                         f'adding as child dependency')
            return [prev_ogn_for_target]
        else:
            # Possibility 3: no previous ops for node or its children
            logger.debug(f'[{self.name}] Add_RM_OGN({ogn_new.node_uid}) Found no previous ops for target node {target_node.dn_uid} or its children; '
                         f'adding to root')
            return [self.root]

    def _find_matching_start_dir_ogn_for_finish_dir_ogn(self, ogn_finish: OpGraphNode) -> Optional[OpGraphNode]:
        tgt_node = ogn_finish.get_tgt_node()
        ogn_queue: Deque[OpGraphNode] = self._get_ogn_queue_for_node(device_uid=tgt_node.device_uid, node_uid=tgt_node.uid)

        if ogn_queue:
            for ogn in reversed(ogn_queue):
                if ogn.node_uid == ogn_finish.node_uid:
                    continue

                if not ogn.is_in_same_batch(ogn_finish):
                    # corrupt graph state!
                    raise RuntimeError(f'Found ancestor OGN ({ogn}) which is not in the same batch as its descendent FINISH ({ogn_finish})')
                if ogn.op.is_start_dir_type():
                    assert ogn.op.src_node.node_identifier == ogn_finish.op.src_node.node_identifier
                    assert ogn.is_src() == ogn_finish.is_src()
                    if ogn_finish.op.op_type == UserOpCode.FINISH_DIR_MV:
                        assert ogn.op.op_type == UserOpCode.START_DIR_MV
                    elif ogn_finish.op.op_type == UserOpCode.FINISH_DIR_CP:
                        assert ogn.op.op_type == UserOpCode.START_DIR_CP
                    else:
                        assert False
                    return ogn

        return None

    def _relink_finish_dir_higher_up(self, ogn_leaf: OpGraphNode, ogn_new: OpGraphNode) -> bool:
        """Returns True if successful"""

        logger.debug(f'[{self.name}] Add_Finish_Dir_OGN({ogn_new.node_uid}): Checking whether leaf {ogn_leaf.node_uid} is '
                     f'a FINISH_DIR OGN for parent of tgt node')
        par_finish_dir: Optional[OpGraphNode] = ogn_leaf

        # is_src() == is_src() is same as saying is_dst() == is_dst(); just make sure they are the same type
        while par_finish_dir and par_finish_dir.is_finish_dir() and par_finish_dir.is_in_same_batch(ogn_new) \
                and par_finish_dir.is_src() == ogn_new.is_src():
            logger.debug(f'[{self.name}] Add_Finish_Dir_OGN({ogn_new.node_uid}): OGN {par_finish_dir.node_uid} '
                         f'is FINISH_DIR (tgt={par_finish_dir.get_tgt_node().node_identifier}): checking if it is parent of OGN target.')

            if par_finish_dir.get_tgt_node().is_parent_of(ogn_new.get_tgt_node()):  # found!
                logger.debug(f'[{self.name}] Add_Finish_Dir_OGN({ogn_new.node_uid}): Found: FINISH_DIR OGN {par_finish_dir.node_uid} '
                             f'is parent of tgt!')
                # need to reverse order
                for parent in par_finish_dir.get_parent_list():
                    # Either parent_ogn is a START_DIR which matches tgt of ogn_new, or parent_ogn is a child node which
                    # goes in the middle of that & ogn_new (START_DIR/FINISH_DIR pair)
                    if parent.get_tgt_node().node_identifier == ogn_new.get_tgt_node().node_identifier or \
                            ogn_new.get_tgt_node().is_parent_of(parent.get_tgt_node()):
                        logger.debug(f'[{self.name}] Add_Finish_Dir_OGN({ogn_new.node_uid}): Re-linking OGN {parent.node_uid} as par of inserted OGN')
                        par_finish_dir.unlink_parent(parent)
                        ogn_new.link_parent(parent)
                    else:
                        # Ignore nodes which are children of parent tgt node
                        assert par_finish_dir.get_tgt_node().is_parent_of(parent.get_tgt_node()), f'UNEXPECTED ($1={par_finish_dir}, $2={parent}'
                        logger.debug(f'[{self.name}] Add_Finish_Dir_OGN({ogn_new.node_uid}): Ignoring irrelevant OGN {parent.node_uid}')
                ogn_new.link_child(par_finish_dir)
                return True
            else:
                logger.debug(f'[{self.name}] Add_Finish_Dir_OGN({ogn_new.node_uid}): OGN {par_finish_dir.node_uid} '
                             f'did not qualify - checking one more level up')
                # check next level up
                ogn_leaf = par_finish_dir
                par_finish_dir = None
                for par_par in ogn_leaf.get_parent_list():
                    if par_par.is_finish_dir() and par_par.is_in_same_batch(ogn_new) and par_par.is_src() == ogn_new.is_src():
                        par_finish_dir = par_par

        # no more FINISH_DIRs up there
        logger.debug(f'[{self.name}] Add_Finish_Dir_OGN({ogn_new.node_uid}): Did not find any qualifying FINISH_DIR OGNs further up')
        return False

    def _find_adopters_for_finish_dir_ogn(self, ogn_new: OpGraphNode) -> List[OpGraphNode]:
        assert ogn_new.op.is_finish_dir_type()

        if ogn_new.get_parent_list():
            logger.debug(f'[{self.name}] Add_Finish_Dir_OGN({ogn_new.node_uid}) Using existing parent list...')
            return ogn_new.get_parent_list()

        logger.debug(f'[{self.name}] Add_Finish_Dir_OGN({ogn_new.node_uid}) New op is FINISH_DIR type. Looking for matching START_DIR...')
        sw_find_start = Stopwatch()
        ogn_start: OpGraphNode = self._find_matching_start_dir_ogn_for_finish_dir_ogn(ogn_finish=ogn_new)
        if ogn_start:
            logger.debug(f'[{self.name}] {sw_find_start} Add_Finish_Dir_OGN({ogn_new.node_uid}) Found ancestor START ({ogn_start}) '
                         f'which appears to match FINISH ({ogn_new})')
            ogn_leaf_list: List[OpGraphNode] = ogn_start.get_all_downstream_leaves()
            if SUPER_DEBUG_ENABLED:
                logger.debug(f'[{self.name}] Add_Finish_Dir_OGN({ogn_new.node_uid}): START_DIR leaf OGNs = {self._uid_list_str(ogn_leaf_list)}')

            # these will be our parents. Validate:
            for ogn_leaf in ogn_leaf_list:
                if ogn_leaf.node_uid == ogn_new.node_uid:
                    raise RuntimeError('something is wrong!')
                if not ogn_leaf.is_in_same_batch(ogn_new):
                    raise RuntimeError(f'Leaf {ogn_leaf} is not in the same batch as FINISH OGN being inserted: {ogn_new}')

                if self._relink_finish_dir_higher_up(ogn_leaf, ogn_new):
                    logger.debug(f'[{self.name}] Add_Finish_Dir_OGN({ogn_new.node_uid}): looks like we linked it higher up. Returning')
                    return ogn_new.get_parent_list()

            return ogn_leaf_list
        else:
            # No matching START_DIR found. This *must* mean that we are merging a batch into the main tree, in which case
            logger.debug(f'[{self.name}] Add_Finish_Dir_OGN({ogn_new.node_uid}) {sw_find_start} No matching START_DIR found (assuming it completed '
                         f' & we are resuming a half-finished batch); using existing OGN parents ({self._uid_list_str(ogn_new.get_parent_list())})')
            if ogn_new.get_parent_list():
                return ogn_new.get_parent_list()
            else:
                logger.debug(f'[{self.name}] Add_Finish_Dir_OGN({ogn_new.node_uid}) actually there are no existing OGN parents; checking whether'
                             f'there is a prev OGN for target')
                target_node = ogn_new.get_tgt_node()
                prev_ogn_for_target = self._get_last_pending_ogn_for_node(target_node.device_uid, target_node.uid)
                if prev_ogn_for_target:
                    logger.debug(f'[{self.name}] Add_Finish_Dir_OGN({ogn_new.node_uid}) Found prev OGN {prev_ogn_for_target.node_uid} '
                                 f'for tgt node {target_node.dn_uid}: will link to it as parent')
                    return [prev_ogn_for_target]
                else:
                    logger.debug(f'[{self.name}] Add_Finish_Dir_OGN({ogn_new.node_uid}) Found no pending ops for tgt node {target_node.dn_uid}'
                                 f'; adding to root')
                    return [self.root]

    def _insert_ogn_between_start_and_finish(self, ogn_new: OpGraphNode, ogn_finish: OpGraphNode) -> OpGraphNode:
        assert ogn_finish.op.is_finish_dir_type()

        sw_find_start = Stopwatch()
        ogn_start = self._find_matching_start_dir_ogn_for_finish_dir_ogn(ogn_finish)
        if not ogn_start:
            # This should never happen
            raise RuntimeError(f'Failed to find earlier queued START OGN matching new FINISH ({ogn_finish})')

        logger.debug(f'[{self.name}] Add_Non_RM_OGN({ogn_new.node_uid}) {sw_find_start} Found START_DIR ({ogn_start.node_uid}): linking it as parent')
        ogn_start.link_child(ogn_new)
        if ogn_finish.is_child_of(ogn_start):
            logger.debug(f'[{self.name}] Add_Non_RM_OGN({ogn_new.node_uid}) Unlinking FINISH_DIR from START_DIR; new OGN will be between')
            ogn_start.unlink_child(ogn_finish)
        logger.debug(f'[{self.name}] Add_Non_RM_OGN({ogn_new.node_uid}) Linking FINISH_DIR as child of new OGN')
        ogn_new.link_child(ogn_finish)
        # Although we hooked everything up completely, we still need to submit the new node's parent for additional processing:
        return ogn_start

    def _find_adopters_for_new_non_rm_ogn(self, ogn_new: OpGraphNode, prev_ogn_for_target: OpGraphNode) -> List[OpGraphNode]:
        target_node: TNode = ogn_new.get_tgt_node()
        target_device_uid: UID = target_node.device_uid
        parent_ogn_list: List[OpGraphNode] = []

        # Check for pending operations for parent node(s) of target:
        for tgt_node_parent_uid in target_node.get_parent_uids():
            logger.debug(f'[{self.name}] Add_Non_RM_OGN({ogn_new.node_uid}) Examining parent '
                         f'{target_node.device_uid}:{tgt_node_parent_uid} of target node ({target_node.dn_uid})')

            prev_ogn_for_target_node_parent: Optional[OpGraphNode] = self._get_last_pending_ogn_for_node(target_device_uid, tgt_node_parent_uid)
            if prev_ogn_for_target_node_parent:

                # Check if parent's last op is FINISH_DIR_*, and if so, find its conjugate START_DIR_*.
                if prev_ogn_for_target_node_parent.op.is_finish_dir_type() and ogn_new.is_in_same_batch(prev_ogn_for_target_node_parent):
                    # If we're in the same batch as START & FINISH: insert this OGN as child of START and parent of FINISH
                    # (reconnecting START & FINISH if needed)
                    # TODO: this should work for one level of dirs, but will it work for nested dirs?
                    logger.debug(f'[{self.name}] Add_Non_RM_OGN({ogn_new.node_uid}) Found existing FINISH_DIR '
                                 f'({prev_ogn_for_target_node_parent.node_uid}): looking for matching START_DIR')
                    prev_ogn_for_target_node_parent = self._insert_ogn_between_start_and_finish(ogn_new=ogn_new,
                                                                                                ogn_finish=prev_ogn_for_target_node_parent)
                    # (kludge): "prev_ogn_for_target_node_parent" is a START_DIR node we have already linked, but need to return something.
                    # else fall through and add as child of "prev_ogn_for_target_node_parent" like normal

                # Sanity check: cannot add to a parent which has been removed
                elif prev_ogn_for_target_node_parent.is_remove_type():
                    raise RuntimeError(f'Invalid operation: cannot {ogn_new.op.op_type.name} {target_node.node_identifier} when its '
                                       f'parent node ({prev_ogn_for_target_node_parent.get_tgt_node().node_identifier}) will first be removed!')

                if prev_ogn_for_target:
                    logger.debug(f'Found both OGN for tgt ({prev_ogn_for_target}) and OGN for parent of tgt ({prev_ogn_for_target_node_parent})')
                    # Sanity check: 99% sure this should never happen, but let's check for it
                    if not prev_ogn_for_target.is_child_of(prev_ogn_for_target_node_parent):
                        logger.error(f'[{self.name}] Add_Non_RM_OGN({ogn_new.node_uid}) Prev OGN ({prev_ogn_for_target}) for tgt node '
                                     f'is not a child of its parent node\'s OGN ({prev_ogn_for_target_node_parent})')
                        raise RuntimeError(f'Invalid state of OpGraph: previous operation for tgt node {target_node.node_identifier} is not connected'
                                           f' to its parent\'s operation!')
                    # else fall through and attach to prev_ogn_for_target
                else:
                    logger.debug(f'[{self.name}] Add_Non_RM_OGN({ogn_new.node_uid}) Found pending op(s) for parent '
                                 f'{target_device_uid}:{tgt_node_parent_uid}; '
                                 f'adding new OGN as child dependency of OGN {prev_ogn_for_target_node_parent.node_uid}')
                    parent_ogn_list.append(prev_ogn_for_target_node_parent)

        if prev_ogn_for_target:
            assert not parent_ogn_list, f'Did not expect: {parent_ogn_list}'

            logger.debug(f'[{self.name}] Add_Non_RM_OGN({ogn_new.node_uid}) Adding new OGN as child of prev OGN {prev_ogn_for_target.node_uid}'
                         f'for tgt node')
            return [prev_ogn_for_target]
        elif parent_ogn_list:
            return parent_ogn_list
        else:
            logger.debug(f'[{self.name}] Add_Non_RM_OGN({ogn_new.node_uid}) Found no pending ops for either target node {target_node.dn_uid} '
                         f'or its parent(s); adding to root')
            return [self.root]

    def insert_ogn(self, ogn_new: OpGraphNode):
        """
        This method is executed as a transaction: if an exception is thrown, the OpGraph can be assumed to be left in a valid state.

        However, this method should not be used if inserting multiple related OGNs is required (such as a batch of OGNs, or even a src-dst pair)
        and this thread is not the sole owner of the OpGraph. To insert a batch of OGNs transactionally, use insert_batch_graph().

        The node shall be added as a child dependency of either the last operation which affected its target, or as a child dependency of the last
        operation which affected its parent, whichever has lower priority (i.e. has a lower level in the dependency tree). In the case where
        neither the node nor its parent has a pending operation, we obviously can just add to the top of the dependency tree.

        A successful return indicates that the node was successfully enqueued; raises InvalidInsertOpGraphError otherwise
        """
        with self._cv_can_get:
            self._insert_ogn(ogn_new)

    def _insert_ogn(self, ogn_new: OpGraphNode):
        logger.debug(f'[{self.name}] InsertOGN({ogn_new.node_uid}) called for: {ogn_new}')
        sw_insert_total_time = Stopwatch()

        # First check whether the target node is known and has pending operations
        target_node: TNode = ogn_new.get_tgt_node()

        sw_find_parents = Stopwatch()
        if ogn_new.is_finish_dir():
            # I. When constructing batch graph, OGNs will typically be inserted in this order: START_DIR, FINISH_DIR, CP, CP, ...
            parent_ogn_list = self._find_adopters_for_finish_dir_ogn(ogn_new)
            logger.debug(f'[{self.name}] InsertOGN({ogn_new.node_uid}) {sw_find_parents} done finding parents for FINISH_DIR')
        else:
            # If not inside a START/FINISH block, need to clear out previous relationships (if any) as we will likely attach them
            # to new parents which are holdovers from a previous batch.
            ogn_new = copy.copy(ogn_new)
            ogn_new.clear_relationships()

            prev_ogn_for_target = self._get_last_pending_ogn_for_node(target_node.device_uid, target_node.uid)

            if ogn_new.is_rm_node():
                parent_ogn_list = self._find_adopters_for_new_rm_ogn(ogn_new, prev_ogn_for_target)
                logger.debug(f'[{self.name}] InsertOGN({ogn_new.node_uid}) {sw_find_parents} done finding parents for RM')
            else:
                # Not an RM node:
                parent_ogn_list = self._find_adopters_for_new_non_rm_ogn(ogn_new, prev_ogn_for_target)
                logger.debug(f'[{self.name}] InsertOGN({ogn_new.node_uid}) {sw_find_parents} done finding parents for NON-RM')

        if not parent_ogn_list:
            # Serious error
            raise InvalidInsertOpGraphError(f'Failed to find parent OGNs to link with: {ogn_new}')

        logger.debug(f'[{self.name}] InsertOGN({ogn_new.node_uid}) Linking OGN as child of OGNs [{self._uid_list_str(parent_ogn_list)}]')

        is_ogn_new_blocked = False
        for parent_ogn in parent_ogn_list:
            parent_ogn.link_child(ogn_new)
            if parent_ogn.op and parent_ogn.op.is_stopped_on_error():
                is_ogn_new_blocked = True

        if is_ogn_new_blocked:
            logger.debug(f'[{self.name}] InsertOGN({ogn_new.node_uid}) New OGN is downstream of an error; setting status of '
                         f'op {ogn_new.op.op_uid} to {UserOpStatus.BLOCKED_BY_ERROR.name}')
            ogn_new.op.set_status(UserOpStatus.BLOCKED_BY_ERROR)

        # Always add to node_queue_dict:
        node_dict = self._node_ogn_q_dict.get(target_node.device_uid, None)
        if not node_dict:
            node_dict = dict()
            self._node_ogn_q_dict[target_node.device_uid] = node_dict
        pending_ogn_queue = node_dict.get(target_node.uid)
        if not pending_ogn_queue:
            pending_ogn_queue = collections.deque()
            node_dict[target_node.uid] = pending_ogn_queue
        pending_ogn_queue.append(ogn_new)

        # Add to ancestor_dict:
        logger.debug(f'[{self.name}] InsertOGN({ogn_new.node_uid}) Tgt node {ogn_new.get_tgt_node().node_identifier}'
                     f' has ancestors: {ogn_new.tgt_ancestor_uid_list}')
        self._increment_icon_update_counts(target_node.device_uid, ogn_new.tgt_ancestor_uid_list)

        if self._max_added_op_uid < ogn_new.op.op_uid:
            self._max_added_op_uid = ogn_new.op.op_uid

        # notify consumers there is something to get:
        self._cv_can_get.notifyAll()

        if logger.isEnabledFor(logging.DEBUG):
            logger.debug(f'[{self.name}] {sw_insert_total_time} InsertOGN({ogn_new.node_uid}): successfully inserted: {ogn_new}')
            self._print_current_state()

    @staticmethod
    def _uid_list_str(ogn_list: List[OpGraphNode]) -> str:
        return ",".join(str(ogn.node_uid) for ogn in ogn_list)

    def insert_batch_graph(self, batch_root: RootNode) -> List[UserOp]:
        """Merges an OpGraph which contains a single batch into this OpGraph by inserting all of
        its OGNs (from its root to its leaves) as descendents of this graph's existing OGNs.
        Tries to make this atomic by locking the graph for the duration of the work [*grits teeth*].
        Tries to make this transactional by failing at standardized intervals and then backing out any already-completed work in the reverse order
        in which it was done."""

        if not batch_root.get_child_list():
            raise RuntimeError(f'Batch has no operations!')

        batch_uid: UID = batch_root.get_first_child().op.batch_uid
        logger.info(f'[{self.name}] Inserting batch {batch_uid} into this OpGraph')

        sw = Stopwatch()

        breadth_first_list: List[OpGraphNode] = batch_root.get_subgraph_bfs_list()
        processed_op_uid_set: Set[UID] = set()
        inserted_op_list: List[UserOp] = []
        inserted_ogn_list: List[OpGraphNode] = []

        if SUPER_DEBUG_ENABLED:
            logger.debug(f'[{self.name}] InsertBatchGraph: About to insert BFS list of OGNs: [{self._uid_list_str(breadth_first_list)}]')

        with self._cv_can_get:
            try:
                for ogn in skip_root(breadth_first_list):
                    if SUPER_DEBUG_ENABLED:
                        logger.debug(f'[{self.name}] InsertBatchGraph: Starting insert of: {ogn}')

                    if ogn.op.is_completed():
                        logger.debug(f'[{self.name}] InsertBatchGraph: Skipping insert of OGN {ogn.node_uid}; its operation is marked as complete')
                        for ogn_child in ogn.get_child_list():
                            ogn.unlink_child(ogn_child)  # this is not for "this" OGN so much as possibly its children

                    else:
                        self._insert_ogn(ogn)
                        inserted_ogn_list.append(ogn)

                        if ogn.op.op_uid not in processed_op_uid_set:
                            inserted_op_list.append(ogn.op)
                            processed_op_uid_set.add(ogn.op.op_uid)

                if OP_GRAPH_VALIDATE_AFTER_BATCH_INSERT:
                    self._validate_internal_consistency()  # this will raise an OpGraphError if validation fails

                logger.info(f'[{self.name}] {sw} InsertBatchGraph done: did insert {len(inserted_ogn_list)} OGNs for {len(inserted_op_list)} ops '
                            f'into OpGraph')
                return inserted_op_list

            except OpGraphError as oge:
                logger.exception(f'[{self.name}] Failed to add batch {batch_uid} to this graph (need to revert insert of {len(inserted_ogn_list)} '
                                 f'OGNs from {len(inserted_op_list)} ops)')
                if inserted_ogn_list:
                    self._rollback(inserted_ogn_list)
                raise UnsuccessfulBatchInsertError(str(oge))
            except RuntimeError as err:
                if not isinstance(err, OpGraphError):
                    # bad bad bad
                    logger.error(f'[{self.name}] Unexpected failure while adding batch {batch_uid} to main graph (after adding '
                                 f'{len(inserted_ogn_list)} OGNs from {len(inserted_op_list)} ops) - rethrowing exception')
                    raise err

    # GET NEXT OP logic
    # ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼

    def _is_node_ready(self, op: UserOp, node: TNode, node_type_str: str, fail_if_not_found: bool = True) -> bool:
        """Note: we allow for the possibilty that not all nodes have been added to the graph yet (i.e. an op's src node is there but not its dst
        node yet) by setting fail_if_not_found to False"""
        node_dict: Dict[UID, Deque[OpGraphNode]] = self._node_ogn_q_dict.get(node.device_uid, None)
        if not node_dict:
            logger.debug(f'[{self.name}] Could not find entry in node dict for device_uid {node.device_uid} (from OGN {node_type_str}, op {op}, '
                         f'fail_if_not_found={fail_if_not_found})')
            if fail_if_not_found:
                raise RuntimeError(f'Serious error: master dict has no entries for device_uid={node.device_uid} (op {node_type_str} node) !')
            else:
                return False
        pending_ogn_queue: Deque[OpGraphNode] = node_dict.get(node.uid, None)
        if not pending_ogn_queue:
            logger.debug(f'[{self.name}] Could not find entry in node dict for device_uid {node.device_uid} (from OGN {node_type_str}, op {op}, '
                         f'fail_if_not_found={fail_if_not_found})')
            if fail_if_not_found:
                raise RuntimeError(f'Serious error: NodeQueueDict has no entries for op {node_type_str} node (uid={node.uid})!')
            else:
                return False

        op_graph_node = pending_ogn_queue[0]
        if op.op_uid != op_graph_node.op.op_uid:
            if OP_GRAPH_DEBUG_ENABLED:
                logger.debug(f'[{self.name}] Skipping UserOp (UID {op_graph_node.op.op_uid}): it is not next in {node_type_str} node queue')
            return False

        if not op_graph_node.is_child_of_root():
            if OP_GRAPH_DEBUG_ENABLED:
                logger.debug(f'[{self.name}] Skipping UserOp (UID {op_graph_node.op.op_uid}): {node_type_str} node is not child of root')
            return False

        return True

    def _try_get(self) -> Optional[UserOp]:
        # We can optimize this later

        for ogn in self.root.get_child_list():
            if OP_GRAPH_DEBUG_ENABLED:
                logger.debug(f'[{self.name}] TryGet(): Examining {ogn}')

            if ogn.op.get_status() != UserOpStatus.NOT_STARTED:
                if OP_GRAPH_DEBUG_ENABLED:
                    logger.debug(f'[{self.name}] TryGet(): Skipping OGN {ogn.node_uid} because it has status {ogn.op.get_status().name}')
                continue

            if ogn.op.has_dst():
                # If the UserOp has both src and dst nodes, *both* must be next in their queues, and also be just below root.
                if ogn.is_dst():
                    # Dst node is child of root. But verify corresponding src node is also child of root
                    is_other_node_ready = self._is_node_ready(ogn.op, ogn.op.src_node, 'src', fail_if_not_found=True)
                else:
                    # Src node is child of root. But verify corresponding dst node is also child of root.
                    is_other_node_ready = self._is_node_ready(ogn.op, ogn.op.dst_node, 'dst', fail_if_not_found=True)

                if not is_other_node_ready:
                    if OP_GRAPH_DEBUG_ENABLED:
                        logger.debug(f'[{self.name}] TryGet(): Skipping OGN {ogn.node_uid} (is_dst={ogn.is_dst()}) because its partner OGN '
                                     f'is not ready')
                    continue

            # Make sure the node has not already been checked out:
            if self._outstanding_op_dict.get(ogn.op.op_uid, None):
                if OP_GRAPH_DEBUG_ENABLED:
                    logger.debug(f'[{self.name}] TryGet(): Skipping op {ogn.op.op_uid} because it is already outstanding')
            else:
                if OP_GRAPH_DEBUG_ENABLED:
                    logger.debug(f'[{self.name}] TryGet(): Returning op for OGN {ogn}')
                self._outstanding_op_dict[ogn.op.op_uid] = ogn.op
                return ogn.op

        if TRACE_ENABLED:
            logger.debug(f'[{self.name}] TryGet(): Returning None')
        return None

    def get_next_op(self) -> Optional[UserOp]:
        """Gets and returns the next available UserOp from the tree; BLOCKS if no pending ops or all the pending ops have
        already been gotten.

        Internally this class keeps track of what this has returned previously, and will expect to be notified when each is complete
         - think of get_next_op() as a repository checkout, and op_completed() as a commit"""

        # Block until we have an op
        while True:
            if self.was_shutdown:
                logger.debug(f'[{self.name}] get_next_op(): Discovered shutdown flag was set. Returning None')
                return None

            with self._cv_can_get:
                sw_get = Stopwatch()
                op = self._try_get()
                if op:
                    logger.info(f'[{self.name}] {sw_get} Got next pending op: {op}')
                    return op
                else:
                    logger.debug(f'[{self.name}] {sw_get} No pending ops; sleeping until notified')
                    self._cv_can_get.wait()

    def get_next_op_nowait(self) -> Optional[UserOp]:
        """Same as get_next_op(), but returns immediately"""
        if self.was_shutdown:
            logger.debug(f'[{self.name}] get_next_op(): Discovered shutdown flag was set. Returning None')
            return None

        with self._cv_can_get:
            op = self._try_get()
            if op:
                logger.info(f'[{self.name}] Got next pending op: {op}')
                return op

        return None

    # POP logic
    # ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼

    def pop_completed_op(self, op_uid: UID) -> bool:
        """
        Ensure that this op was copmleted successfully, and if so, remove it from the tree.
        """
        logger.debug(f'[{self.name}] Entered pop_completed_op() for op {op_uid}')

        with self._cv_can_get:
            op = self._outstanding_op_dict.pop(op_uid, None)
            if not op:
                raise RuntimeError(f'Complated op (UID {op_uid}) not found in outstanding op list!')

            did_remove_op = False
            status = op.get_status()
            if status == UserOpStatus.COMPLETED_OK or status == UserOpStatus.COMPLETED_NO_OP:
                # I. SRC OGN
                self._remove_ogn_for_completed_op(op.src_node, op, 'src')

                # II. DST OGN
                if op.has_dst():
                    self._remove_ogn_for_completed_op(op.dst_node, op, 'dst')

                self._op_count = self._op_count - 1   # TODO: this works fine here, but need a good solution for insert
                did_remove_op = True
            else:
                logger.info(
                    f'[{self.name}] pop_completed_op(): will not pop OGNs for op ({op.op_uid}) as it did not complete OK (status: {status.name}) ')

                if status == UserOpStatus.STOPPED_ON_ERROR:
                    # Mark affected nodes, and also any nodes from dependent OGNs, as needing icon updates.
                    # This merely populates a dict which represents device_uid-node_uid pairs which should be queried for updated icon via the
                    # get_icon_for_node() method.
                    #
                    # Note that we only need to do this at the moment of failure cuz we need to update nodes which are [possibly] already displayed.
                    # Any new nodes (unknown to us now) which need to be displayed thereafter will already call get_icon_for_node() prior to display.
                    logger.debug(f'[{self.name}] pop_completed_op(): Op stopped on error; '
                                 f'will set downstream OGNs to blocked & populate ChangedIconDict (currently = {self._changed_node_dict})')
                    self._block_downstream_ogns_for_failed_op(op)
                    logger.debug(f'[{self.name}] pop_completed_op(): ChangedIconDict is now = {self._changed_node_dict}')

            is_batch_complete: bool = self._is_batch_complete(op.batch_uid)

            logger.debug(f'[{self.name}] Done with pop_completed_op() (did_remove_op={did_remove_op}) for op: {op}')
            if logger.isEnabledFor(logging.DEBUG):
                self._print_current_state()

            # this may have jostled the tree to make something else free:
            self._cv_can_get.notifyAll()

            return is_batch_complete

    def _is_batch_complete(self, batch_uid: UID):
        """NOTE: may want to optimize this later. This is O(N) for OpGraph size"""

        def _found(ogn):
            if ogn.op.batch_uid == batch_uid:
                logger.debug(f'[{self.name}] IsBatchComplete() returning false: found {ogn.op}')
                return True  # this means stop the iteration

        was_found = self._for_all_ogn_in_graph(_found)
        return not was_found

    def _rollback(self, ogn_list: List[OpGraphNode]):
        sw = Stopwatch()
        ogn_count = len(ogn_list)
        while len(ogn_list) > 0:
            # Back out in reverse order in which they were inserted
            ogn = ogn_list.pop()
            logger.debug(f'[{self.name}] Backing out insert of OGN {-(len(ogn_list) - ogn_count)} of {ogn_count}: {ogn}')
            self._uninsert_ogn(ogn)

        logger.info(f'[{self.name}] {sw} Rolled back insert of {ogn_count} OGNs')

    def _uninsert_ogn(self, ogn_to_remove: OpGraphNode):

        def _remove_last(_tgt_ogn_queue):
            last_ogn: OpGraphNode = _tgt_ogn_queue.pop()
            if last_ogn.node_uid != ogn_to_remove.node_uid:
                _tgt_ogn_queue.append(last_ogn)  # put it back
                raise RuntimeError(f'Unexpected failure while trying to remove OGN {ogn_to_remove.node_uid} from tail of node queue: '
                                   f'found OGN {last_ogn.node_uid} instead!')
            return last_ogn

        self._remove_ogn_from_node_queue(tgt_node=ogn_to_remove.get_tgt_node(), label='', remove_func=_remove_last)

        self._decrement_icon_update_counts(ogn_to_remove)

        self._unlink_ogn_from_graph(ogn_to_remove)

        self._ogn_count = self._ogn_count - 1

    def _block_downstream_ogns_for_failed_op(self, failed_op: UserOp):
        assert failed_op.get_status() == UserOpStatus.STOPPED_ON_ERROR, f'Op is not in failed status: {failed_op}'

        def _set_status_to_blocked(_ogn: OpGraphNode):
            tgt_node = _ogn.get_tgt_node()
            self._add_tgt_node_to_icon_changes_dict(_ogn.get_tgt_node())

            if _ogn.op.op_uid != failed_op.op_uid and not _ogn.op.result:  # skip the failed op itself, and don't duplicate work
                logger.debug(f'[{self.name}] Setting status={UserOpStatus.BLOCKED_BY_ERROR.name}: op={_ogn.op.op_uid} tnode={tgt_node.dn_uid}')
                _ogn.op.result = UserOpResult(status=UserOpStatus.BLOCKED_BY_ERROR)
                # the failed op itself

        self._for_op_ogns_and_all_descendent_ogns(failed_op, _set_status_to_blocked)

    def _process_and_enqueue_children(self, tgt_node: TNode, op: UserOp, queue: Deque[UserOp]):
        ogn: OpGraphNode = self._get_next_pending_ogn_for_node(tgt_node.device_uid, tgt_node.uid)
        assert ogn and ogn.op.op_uid == op.op_uid, f'Op (UID {op.op_uid}) does not match last OGN in its target node\'s queue ({ogn})'

        self._add_tgt_node_to_icon_changes_dict(tgt_node)

        # Enqueue downstream ops:
        for child_ogn in ogn.get_child_list():
            queue.append(child_ogn.op)

    def _remove_ogn_for_completed_op(self, tgt_node: TNode, op: UserOp, node_label: str):

        # 1. Remove OGN from node queue dict

        def _remove_from_front(_tgt_ogn_queue):
            ogn: OpGraphNode = _tgt_ogn_queue.popleft()
            if ogn.op.op_uid != op.op_uid:
                # very bad
                self._print_current_state()
                raise RuntimeError(f'Completed op (UID {op.op_uid}) does not match last node popped from its {node_label} node\'s queue '
                                   f'(UID {ogn.op.op_uid})')
            return ogn

        tgt_ogn = self._remove_ogn_from_node_queue(tgt_node=tgt_node, label=f'Completed op for {node_label}', remove_func=_remove_from_front)

        # 2. Remove tgt node ancestor counts

        self._decrement_icon_update_counts(tgt_ogn)

        # 3. Remove OGN from graph

        # 3a. validate it is a child of root
        if not tgt_ogn.is_child_of_root():
            ogn_parent = tgt_ogn.get_first_parent()
            ogn_parent_uid = ogn_parent.node_uid if ogn_parent else None
            raise RuntimeError(f'Src node for completed op is not a parent of root (instead found parent OGN {ogn_parent_uid})')

        # 3b. unlink from its parent
        self._unlink_ogn_from_graph(tgt_ogn)

        # 4. Delete tgt OGN
        del tgt_ogn

    def _remove_ogn_from_node_queue(self, tgt_node: TNode, label: str, remove_func: Callable[[Deque[OpGraphNode]], OpGraphNode]) -> OpGraphNode:
        node_dict_for_device: Dict[UID, Deque[OpGraphNode]] = self._node_ogn_q_dict.get(tgt_node.device_uid)
        if not node_dict_for_device:
            # very bad
            raise RuntimeError(f'{label}: node device_uid ({tgt_node.device_uid}) not found in master dict (for {tgt_node.dn_uid}')

        tgt_ogn_queue: Deque[OpGraphNode] = node_dict_for_device.get(tgt_node.uid)
        if not tgt_ogn_queue:
            # very bad
            self._print_current_state()
            raise RuntimeError(f'{label}: node ({tgt_node.dn_uid}) not found in master dict!')

        tgt_ogn: OpGraphNode = remove_func(tgt_ogn_queue)
        if not tgt_ogn_queue:
            # Remove queue if it is empty:
            node_dict_for_device.pop(tgt_node.uid, None)
        if not node_dict_for_device:
            # Remove device dict if it is empty:
            self._node_ogn_q_dict.pop(tgt_node.device_uid, None)

        return tgt_ogn

    @staticmethod
    def _unlink_ogn_from_graph(tgt_ogn: OpGraphNode):
        ogn_former_parent_list = [] + tgt_ogn.get_parent_list()

        for ogn_parent in tgt_ogn.get_parent_list():
            ogn_parent.unlink_child(tgt_ogn)

        # unlink its children also. If the child then has no other parents, it moves up to become child of root
        for ogn_child in tgt_ogn.get_child_list():
            ogn_child.unlink_parent(tgt_ogn)

            if not ogn_child.get_parent_list():
                for ogn_parent in ogn_former_parent_list:
                    ogn_parent.link_child(ogn_child)

    # RETRY logic
    # ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼

    def retry_failed_op(self, op_uid: UID):
        """
        Finds the op with the given UID, and if the op:
        1. is in STOPPED_ON_ERROR state, in which case the state will be reset and the OpGraph will
           be notified that there is more work to do; or
        2. is in BLOCKED_BY_ERROR state, in which case the op which is the source of the block will be
           found and reset as described in (1); or
        3. is in any other state, or op not found: raises an error
        """
        with self._cv_can_get:
            sw = Stopwatch()

            # NOTE: may want to optimize this later. It goes over the entire graph! If we could look up by op_uid...
            def _found(ogn):
                if ogn.op.op_uid == op_uid:
                    status = ogn.op.get_status()
                    if status == UserOpStatus.STOPPED_ON_ERROR:
                        logger.debug(f'[{self.name}] retry_failed_op(): nulling out result of: {ogn.op}')
                        # For binary ops, we can rely on object reference to change both OGNs
                        self._reset_status_and_all_descendents(ogn.op)
                        return True  # this means stop the iteration.
                    elif status == UserOpStatus.BLOCKED_BY_ERROR:
                        logger.debug(f'[{self.name}] retry_failed_op(): op has status {status.name} (will look for upstream blocker): {ogn.op}')
                        target_op_list = self._find_blocking_op_list(ogn)
                        assert target_op_list, f'Could not find upstream op which is blocking {ogn}'
                        logger.debug(f'[{self.name}] retry_failed_op(): Found blocking op(s): {target_op_list}')
                        for target_op in target_op_list:
                            self._reset_status_and_all_descendents(target_op)
                        return True  # this means stop the iteration.

                    return False

            was_found = self._for_all_ogn_in_graph(_found)
            logger.debug(f'[{self.name}] {sw} retry_failed_op() done: was_found={was_found}')
            if not was_found:
                raise RuntimeError(f'Could not retry: failed to find op in graph with UID: {op_uid}')

            # Found and reset. Notify graph that it has something to do:
            self._cv_can_get.notifyAll()

    def _reset_status_and_all_descendents(self, op: UserOp):
        """Resets the given op's status from STOPPED_ON_ERROR to NOT_STARTED. Then finds all descendents and
        changes their status from BLOCKED_BY_ERROR to NOT_STARTED."""

        def _remove_blocked_status(_ogn: OpGraphNode):
            if _ogn.op.op_uid == op.op_uid:
                pass
                # fall through
            elif _ogn.op.get_status() == UserOpStatus.BLOCKED_BY_ERROR:
                for parent_ogn in _ogn.get_parent_list():
                    if parent_ogn.op.is_stopped_on_error():
                        logger.debug(f'[{self.name}] Will not unblock op {_ogn.op.op_uid}: its OGN ({_ogn.node_uid}) is child of an '
                                     f'OGN in failed state ({parent_ogn.node_uid}: {parent_ogn.op.get_status().name})')
                        return
                logger.debug(f'[{self.name}] Unblocking op: {_ogn.op.op_uid}')
                # fall through

            self._reset_status(_ogn)

        self._for_op_ogns_and_all_descendent_ogns(op, _remove_blocked_status)

    def _reset_status(self, ogn):
        ogn.op.reset_result()
        self._add_tgt_node_to_icon_changes_dict(ogn.get_tgt_node())  # icon changed

    def _find_blocking_op_list(self, ogn: OpGraphNode) -> Iterable[UserOp]:
        found_op_dict: Dict[UID, UserOp] = {}

        queue: Deque[OpGraphNode] = collections.deque()
        queue.append(ogn)

        while queue:
            ogn = queue.popleft()
            if ogn.op.get_status() == UserOpStatus.BLOCKED_BY_ERROR:
                if SUPER_DEBUG_ENABLED:
                    parent_uid_list = [par.node_uid for par in ogn.get_parent_list()]
                    logger.debug(f'[{self.name}] _find_blocking_op_list(): OGN {ogn.node_uid} blocked; checking parents ({parent_uid_list})')
                for parent_ogn in ogn.get_parent_list():
                    queue.append(parent_ogn)
            elif ogn.op.get_status() == UserOpStatus.STOPPED_ON_ERROR:
                if SUPER_DEBUG_ENABLED:
                    logger.debug(f'[{self.name}] _find_blocking_op_list(): found blocking OGN {ogn.node_uid}')
                found_op_dict[ogn.op.op_uid] = ogn.op

        return found_op_dict.values()

    def retry_all_failed_ops(self):
        """
        Finds all ops which have status STOPPED_ON_ERROR, and resets their status to NOT_STARTED. (Also resets the status of all their blocked
        descendents). If no ops have status STOPPED_ON_ERROR, then does nothing.
        """
        logger.debug(f'RetryAllFailedOps() entered')

        with self._cv_can_get:
            # Just go over the graph and wipe out all the errors & blocked statuses in one swoop
            def _process(ogn):
                status = ogn.op.get_status()
                if ogn.op.op_uid in _process.op_set:
                    self._add_tgt_node_to_icon_changes_dict(ogn.get_tgt_node())  # icon changed

                if status == UserOpStatus.STOPPED_ON_ERROR or status == UserOpStatus.BLOCKED_BY_ERROR:
                    logger.debug(f'[{self.name}] RetryAllFailedOps(): nulling out result of: {ogn.op}')
                    # For binary ops, we can rely on object reference to change both OGNs
                    self._reset_status(ogn)
                    _process.op_set.add(ogn.op.op_uid)
                return False

            sw = Stopwatch()
            _process.op_set = set()
            self._for_all_ogn_in_graph(_process)
            logger.info(f'[{self.name}] {sw} RetryAllFailedOps(): Did reset the status of {len(_process.op_set)} failed or blocked ops.')

            # Found and reset. Notify graph that it has something to do:
            self._cv_can_get.notifyAll()

    # Internal utility methods
    # ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼

    def _for_all_ogn_in_graph(self, on_ogn_found: Callable[[OpGraphNode], bool]) -> bool:
        """
        Iterates over all OGNs currently in the graph, in no particular order. Stops when on_ogn_found() returns True.
        :return True iff the last on_ogn_found() returned True; False if they all returned False.
        """
        for device_uid, node_dict in self._node_ogn_q_dict.items():
            for node_uid, deque in node_dict.items():
                for ogn in deque:
                    if on_ogn_found(ogn):
                        return True
        return False

    def _for_op_ogns_and_all_descendent_ogns(self, root_op: UserOp, on_ogn_found: Callable[[OpGraphNode], None]):
        ogn_queue: Deque[OpGraphNode] = collections.deque()

        # Enqueue op's src OGN:
        ogn_src: OpGraphNode = self._get_next_pending_ogn_for_node(root_op.src_node.device_uid, root_op.src_node.uid)
        assert ogn_src, f'Could not find src OGN in OpGraph for {root_op.src_node.node_identifier} (for op: {root_op})'
        assert ogn_src.op.op_uid == root_op.op_uid, \
            f'Expected next op = {root_op.op_uid} but found {ogn_src.op.op_uid} for {root_op.src_node.node_identifier}'
        ogn_queue.append(ogn_src)

        # Enqueue op's dst OGN, if it exists:
        if root_op.has_dst():
            ogn_dst: OpGraphNode = self._get_next_pending_ogn_for_node(root_op.dst_node.device_uid, root_op.dst_node.uid)
            ogn_queue.append(ogn_dst)
            assert ogn_dst, f'Could not find dst OGN in OpGraph for {root_op.dst_node.node_identifier} (for op: {root_op})'
            assert ogn_dst.op.op_uid == root_op.op_uid, \
                f'Expected next op = {root_op.op_uid} but found {ogn_dst.op.op_uid} for {root_op.dst_node.node_identifier}'

        # From here on out we can navigate via OGNs to get the whole downstream graph.
        while len(ogn_queue) > 0:
            ogn: OpGraphNode = ogn_queue.popleft()
            if SUPER_DEBUG_ENABLED:
                logger.debug(f'[{self.name}] ForAllDescendents(): examining OGN {ogn.node_uid}')
            on_ogn_found(ogn)

            # Enqueue downstream OGNs:
            for child_ogn in ogn.get_child_list():
                ogn_queue.append(child_ogn)
