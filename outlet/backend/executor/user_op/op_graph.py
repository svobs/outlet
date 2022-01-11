import collections
import logging
import threading
from typing import Callable, Deque, Dict, Iterable, List, Optional, Set

from backend.executor.user_op.op_graph_node import OpGraphNode, RmOpNode, RootNode
from constants import IconId, NULL_UID, OP_GRAPH_VALIDATE_AFTER_BATCH_INSERT
from logging_constants import SUPER_DEBUG_ENABLED, TRACE_ENABLED
from error import InvalidInsertOpGraphError, OpGraphError, UnsuccessfulBatchInsertError
from model.node.node import Node
from model.uid import UID
from model.user_op import ChangeTreeCategoryMeta, UserOp, UserOpResult, UserOpStatus
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

    Undirected acyclic graph for user ops.
    An OpGraph is built out of a set of "OpGraph nodes", aka "OGNs", which I may also refer to as "graph nodes", which have class OpGraphNode.
    These should be distinguished from regular file nodes, dir nodes, etc, which I'll refer to as simply "nodes", with class Node.

    Every UserOp in the graph will be reprensented by either 1 or 2 OGNs, with one OGN for its src node, and one OGN for its dst node (if it has
    a dst).
    ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼
    """

    def __init__(self, name):
        HasLifecycle.__init__(self)
        self.name = name

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

    def shutdown(self):
        """Need to call this for try_get() to return"""
        if self.was_shutdown:
            return

        HasLifecycle.shutdown(self)
        with self._cv_can_get:
            # unblock any get() task which is waiting
            self._cv_can_get.notifyAll()

    # Icon stuff
    # ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼

    def get_icon_for_node(self, device_uid: UID, node_uid: UID) -> Optional[IconId]:
        with self._cv_can_get:
            ogn: OpGraphNode = self._get_last_pending_ogn_for_node(device_uid, node_uid)
            if ogn and not ogn.op.is_completed():
                icon = ChangeTreeCategoryMeta.get_icon_for_node(ogn.get_tgt_node().is_dir(), is_dst=ogn.is_dst(), op=ogn.op)
                if SUPER_DEBUG_ENABLED:
                    logger.debug(f'Node {device_uid}:{node_uid} belongs to pending op ({ogn.op.op_uid}: {ogn.op.op_type.name}): returning icon')
                return icon

            device_ancestor_dict: Dict[UID, int] = self._ancestor_dict.get(device_uid)
            if device_ancestor_dict and device_ancestor_dict.get(node_uid):
                logger.debug(f'Node {device_uid}:{node_uid} has a downstream op: returning icon {IconId.ICON_DIR_PENDING_DOWNSTREAM_OP.name}')
                return IconId.ICON_DIR_PENDING_DOWNSTREAM_OP

            if SUPER_DEBUG_ENABLED:
                logger.debug(f'Node {device_uid}:{node_uid}: no custom icon')
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
            logger.debug(f'[{self.name}] Increment(before): Ancestor dict: {self._ancestor_dict}')

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
            logger.debug(f'[{self.name}] Increment(after): Ancestor dict: {self._ancestor_dict}')

    def _decrement_icon_update_counts(self, ogn: OpGraphNode):
        if SUPER_DEBUG_ENABLED:
            logger.debug(f'[{self.name}] Decrement(before): Ancestor dict: {self._ancestor_dict}')

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
            logger.debug(f'[{self.name}] Decrement(after): Ancestor dict: {self._ancestor_dict}')

    def _add_tgt_node_to_icon_changes_dict(self, tgt_node: Node):
        # TODO: merge this structure with ancestor change counts
        logger.debug(f'[{self.name}] Adding node {tgt_node.node_identifier} to set of nodes which need UI icon updates')
        # Add tgt node to change map
        node_map_for_device = self._changed_node_dict.get(tgt_node.device_uid)
        if not node_map_for_device:
            node_map_for_device = set()
            self._changed_node_dict[tgt_node.device_uid] = node_map_for_device
        node_map_for_device.add(tgt_node.uid)

    # Misc non-mutating methods
    # ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼

    def __len__(self):
        with self._cv_can_get:
            op_set = set()
            # '_node_ogn_q_dict' must contain the same number of nodes as the graph, but should be slightly more efficient to iterate over (maybe)
            for device_uid, node_dict in self._node_ogn_q_dict.items():
                for node_uid, deque in node_dict.items():
                    for node in deque:
                        op_set.add(node.op.op_uid)

            return len(op_set)

    def _print_current_state(self):
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
                    node_repr_list.append(node.op.get_tag())
                qd_line_list.append(f'Node {device_uid}:{node_uid}:')
                for i, node_repr in enumerate(node_repr_list):
                    qd_line_list.append(f'  [{i}] {node_repr}')

        logger.debug(f'[{self.name}] CURRENT EXECUTION STATE: OpGraph = [{len(graph_line_list)} OGNs]:')
        for graph_line in graph_line_list:
            logger.debug(f'[{self.name}] {graph_line}')

        logger.debug(f'[{self.name}] CURRENT EXECUTION STATE: NodeQueues = [Ops: {len(op_set)} Devices: {len(self._node_ogn_q_dict)} '
                     f'Nodes: {queue_count}]:')
        for qd_line in qd_line_list:
            logger.debug(f'[{self.name}] {qd_line}')

    def _get_last_pending_ogn_for_node(self, device_uid: UID, node_uid: UID) -> Optional[OpGraphNode]:
        node_dict = self._node_ogn_q_dict.get(device_uid, None)
        if node_dict:
            node_list = node_dict.get(node_uid, None)
            if node_list:
                # last element in list is lowest priority:
                return node_list[-1]
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

    def validate_internal_consistency(self):
        with self._cv_can_get:
            self._validate_internal_consistency()

    def _validate_internal_consistency(self):
        """The caller is responsible for locking the graph before calling this."""
        logger.debug(f'[{self.name}] Validating insternal structural consistency of OpGraph...')
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
                logger.error(f'[{self.name}] ValidateGraph: OG node is a child of root but is_child_of_root()==False: {child_of_root}')
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
            ogn_parent_list = ogn.get_parent_list()
            if not ogn_parent_list:
                error_count += 1
                logger.error(f'ValidateGraph: OG node has no parents: {ogn}')
            else:
                parent_uid_set: Set[UID] = set()
                for ogn_parent in ogn_parent_list:
                    if ogn_parent.node_uid in parent_uid_set:
                        error_count += 1
                        logger.error(f'[{self.name}] ValidateGraph: Duplicate parent listed in OG node! OGN uid={ogn.node_uid}, '
                                     f'parent_uid={ogn_parent.node_uid}')
                        continue
                    else:
                        parent_uid_set.add(ogn_parent.node_uid)

                    if not ogn_coverage_dict.get(ogn_parent.node_uid, None):
                        unrecognized_og_node_dict[ogn_parent.node_uid] = \
                            'Unrecognized parent listed in OG node! OGN uid={ogn.node_uid}, ' \
                            'parent_OGN={ogn_parent}'

                has_multiple_parents = len(ogn_parent_list) > 1
                tgt_node: Node = ogn.get_tgt_node()

                if not ogn.is_child_of_root():
                    if ogn.is_rm_node():
                        all_parents_must_be_remove_type = False
                        if has_multiple_parents:
                            all_parents_must_be_remove_type = True
                        child_node_uid_set = set()
                        for ogn_parent in ogn_parent_list:
                            if ogn_parent.is_remove_type():
                                all_parents_must_be_remove_type = True
                                parent_tgt_node: Node = ogn_parent.get_tgt_node()
                                if not tgt_node.is_parent_of(parent_tgt_node):
                                    error_count += 1
                                    logger.error(f'[{self.name}] ValidateGraph: Parent of RM OG node is remove-type, but its target node is not '
                                                 f'a child of its child"s target node! OG node={ogn}, parent={ogn_parent}')
                                if parent_tgt_node.uid in child_node_uid_set:
                                    error_count += 1
                                    logger.error(f'[{self.name}] ValidateGraph: Parents of RM OG node have duplicate target node! OG node={ogn}, '
                                                 f'parent={ogn_parent}, offending node UID={parent_tgt_node.uid}')
                                child_node_uid_set.add(parent_tgt_node.uid)
                            else:
                                # Parent is not remove-type

                                if all_parents_must_be_remove_type:
                                    error_count += 1
                                    logger.error(f'[{self.name}] ValidateGraph: Some parents of RM OG node are remove-type, but this one is not! '
                                                 f'OG node={ogn}, offending OG parent={ogn_parent}')
                                else:
                                    # Parent's tgt node must be an ancestor of tgt node (or same node).
                                    # Check all paths and confirm that at least one path in parent tgt contains the path of tgt
                                    if not ogn.is_tgt_an_ancestor_of_og_node_tgt(ogn_parent):
                                        error_count += 1
                                        logger.error(f'[{self.name}] ValidateGraph: Parent of RM OG node is not remove-type, and its '
                                                     f'target node is not an ancestor of its child OG\'s target! OG node={ogn}, '
                                                     f'offending OG parent={ogn_parent}')

                    else:  # NOT ogn.is_rm_node()
                        assert not has_multiple_parents, \
                            f'Non-RM OG node should not be allowed to have multiple parents: {ogn}, parents={ogn_parent_list}'
                        for ogn_parent in ogn_parent_list:
                            if not ogn_parent.is_tgt_an_ancestor_of_og_node_tgt(ogn):
                                error_count += 1
                                logger.error(f'[{self.name}] ValidateGraph: Parent of OGN has target node which is not an ancestor '
                                             f'of its child\'s target! OG node={ogn}, offending parent={ogn_parent}')

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
                        logger.error(f'[{self.name}] ValidateGraph: Child of OG node does not list it as parent! OG node={ogn}, '
                                     f'child={child_ogn}')

                    if ogn_coverage_dict.get(child_ogn.node_uid, None):
                        logger.debug(f'[{self.name}] ValidateGraph: Already encountered child of OG node; skipping: {child_ogn}')
                    else:
                        ogn_queue.append(child_ogn)

        for og_node_uid, error_msg in unrecognized_og_node_dict.items():
            if not ogn_coverage_dict.get(og_node_uid, None):
                # still missing: raise original error
                error_count += 1
                logger.error(f'[{self.name}] ValidateGraph: {error_msg}')

        len_ogn_coverage_dict = len(ogn_coverage_dict)
        logger.debug(f'[{self.name}] ValidateGraph: encountered {len_ogn_coverage_dict} OGNs in graph')

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
        for device_uid, node_dict in self._node_ogn_q_dict.items():
            for node_uid, deque in node_dict.items():
                for ogn in deque:
                    if not ogn_coverage_dict.pop(ogn.node_uid, None):
                        error_count += 1
                        logger.error(f'[{self.name}] ValidateGraph: OG node found in NodeQueues which is not in the graph: {ogn}')

        if len(ogn_coverage_dict) > 0:
            for ogn in ogn_coverage_dict.values():
                error_count += 1
                logger.error(f'[{self.name}] ValidateGraph: OGN found in graph which is not present in NodeQueues: {ogn}')

        if error_count > 0:
            raise OpGraphError(f'Validation for OpGraph failed with {error_count} errors!')
        else:
            logger.info(f'[{self.name}] ValidateGraph: validation done. No errors for {len_ogn_coverage_dict} OGNs')

    # INSERT logic
    # ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼

    def _find_og_nodes_for_children_of_rm_target_node(self, ogn: OpGraphNode) -> List[OpGraphNode]:
        """When adding an RM OGnode, we need to locate its OGnode parents (which are the child nodes of its target node.
        We must make sure that all the child nodes are going to be removed """
        # TODO: probably could optimize a bit more...
        sw_total = Stopwatch()

        assert isinstance(ogn, RmOpNode), f'Unepxected type: {ogn}'
        potential_parent: Node = ogn.get_tgt_node()

        child_nodes = []

        for node_dict in self._node_ogn_q_dict.values():
            for node_queue in node_dict.values():
                if node_queue:
                    existing_og_node = node_queue[-1]
                    potential_child: Node = existing_og_node.get_tgt_node()
                    if potential_parent.is_parent_of(potential_child):
                        # We found a parent OG node for
                        if not existing_og_node.is_remove_type():
                            # This is not allowed. Cannot remove parent dir (aka child op node) unless *all* its children are first removed
                            raise InvalidInsertOpGraphError(f'Found child node for RM-type node which is not RM type: {existing_og_node}')
                        child_nodes.append(existing_og_node)

        logger.debug(f'[{self.name}] {sw_total} Found {len(child_nodes):,} child nodes in graph for RM node')
        return child_nodes

    def _find_adopters_for_new_rm_ogn(self, new_ogn: OpGraphNode, prev_ogn_for_target) -> List[OpGraphNode]:
        # Special handling for RM-type nodes.
        # We want to find the lowest RM node in the tree.
        assert new_ogn.is_rm_node()
        target_node: Node = new_ogn.get_tgt_node()

        if prev_ogn_for_target and prev_ogn_for_target.is_rm_node():
            logger.error(f'[{self.name}] Add_RM_OGN({new_ogn.node_uid}) Op\'s target node ({target_node.dn_uid}) is an RM '
                         f'which is a dup of already enqueued RM ({prev_ogn_for_target.op.src_node.dn_uid})!')
            assert target_node.dn_uid == new_ogn.op.src_node.dn_uid
            assert prev_ogn_for_target.op.src_node.dn_uid == target_node.dn_uid

            raise InvalidInsertOpGraphError(f'Trying to remove node ({target_node.node_identifier}) whose last operation is already an RM operation!')

        # First, see if we can find child nodes of the target node (which would be the parents of the RM OG node):
        ogn_list_for_children_of_tgt: List[OpGraphNode] = self._find_og_nodes_for_children_of_rm_target_node(new_ogn)
        if ogn_list_for_children_of_tgt:
            # Possibility 1: children
            logger.debug(f'[{self.name}] Add_RM_OGN({new_ogn.node_uid}) Found {len(ogn_list_for_children_of_tgt)} '
                         f'existing OGNs for children of tgt node ({target_node.dn_uid}); examining whether to add as child dependency of each')

            parent_ogn_list: List[OpGraphNode] = []  # Use this list to store parents to link to until we are done validating.
            for og_node_for_child_node in ogn_list_for_children_of_tgt:
                logger.debug(f'[{self.name}] Add_RM_OGN({new_ogn.node_uid}) Examining {og_node_for_child_node}')
                if not og_node_for_child_node.is_remove_type():
                    raise InvalidInsertOpGraphError(f'Cannot insert RM OGN: all children of its target must be scheduled for removal first,'
                                                    f'but found: {og_node_for_child_node} (while trying to insert OGN: {new_ogn})')

                # Check if there's an existing OGN which is child of parent OGN.
                conflicting_ogn: Optional[OpGraphNode] = og_node_for_child_node.get_first_child()
                if conflicting_ogn:
                    if not conflicting_ogn.is_remove_type():
                        raise InvalidInsertOpGraphError(f'Found unexpected node which is blocking insert of our RM operation: {conflicting_ogn} '
                                                        f'(while trying to insert: {new_ogn})')
                    else:
                        # extremely rare for this to happen - likely indicates a hole in our logic somewhere
                        raise InvalidInsertOpGraphError(f'Found unexpected child OGN: {conflicting_ogn} (while trying to insert OGN: {new_ogn})')
                else:
                    parent_ogn_list.append(og_node_for_child_node)

            return parent_ogn_list

        elif prev_ogn_for_target:
            # Possibility 2: If existing op is found for the target node, add below that.

            # The node's children MUST be removed first. It is invalid to RM a node which has children.
            # (if the children being added are later scheduled for removal, then they should should up in Possibility 1
            if prev_ogn_for_target.get_child_list():
                raise InvalidInsertOpGraphError(f'While trying to add RM op: did not expect existing OGN for target to have children! '
                                                f'(tgt={prev_ogn_for_target})')
            logger.debug(f'[{self.name}] Add_RM_OGN({new_ogn.node_uid}) Found pending op(s) for target node {target_node.dn_uid} for RM op; '
                         f'adding as child dependency')
            return [prev_ogn_for_target]
        else:
            # Possibility 3: no previous ops for node or its children
            logger.debug(f'[{self.name}] Add_RM_OGN({new_ogn.node_uid}) Found no previous ops for target node {target_node.dn_uid} or its children; '
                         f'adding to root')
            return [self.root]

    def _find_adopters_for_new_non_rm_ogn(self, new_ogn: OpGraphNode, prev_ogn_for_target: OpGraphNode) -> List[OpGraphNode]:
        target_node: Node = new_ogn.get_tgt_node()
        target_device_uid: UID = target_node.device_uid
        parent_ogn_list: List[OpGraphNode] = []

        # Check for pending operations for parent node(s) of target:
        for tgt_node_parent_uid in target_node.get_parent_uids():
            prev_ogn_for_target_node_parent: Optional[OpGraphNode] = self._get_last_pending_ogn_for_node(target_device_uid, tgt_node_parent_uid)
            if prev_ogn_for_target_node_parent:
                # FIXME: check if parent's last op is FINISH_DIR_MV/CP, and if so, find its companion START_DIR_MV/CP.
                # If we're in the same batch as START & FINISH: insert this OGN as child of START and parent of FINISH
                # (reconnecting START & FINISH if needed)

                # Sanity check:
                if prev_ogn_for_target_node_parent.is_remove_type():
                    raise RuntimeError(f'Invalid operation: cannot {new_ogn.op.op_type.name} {target_node.node_identifier} when its '
                                       f'parent node ({prev_ogn_for_target_node_parent.get_tgt_node().node_identifier}) will first be removed!')

                if prev_ogn_for_target:
                    logger.debug(f'Found both OGN for tgt ({prev_ogn_for_target}) and OGN for parent of tgt ({prev_ogn_for_target_node_parent})')
                    # Sanity check: 99% sure this should never happen, but let's check for it
                    if not prev_ogn_for_target.is_child_of(prev_ogn_for_target_node_parent):
                        logger.error(f'[{self.name}] Add_Non_RM_OGN({new_ogn.node_uid}) Prev OGN ({prev_ogn_for_target}) for tgt node '
                                     f'is not a child of its parent node\'s OGN ({prev_ogn_for_target_node_parent})')
                        raise RuntimeError(f'Invalid state of OpGraph: previous operation for tgt node {target_node.node_identifier} is not connected'
                                           f'to its parent\'s operation!')
                    # else fall through and attach to prev_ogn_for_target
                else:
                    logger.debug(f'[{self.name}] Add_Non_RM_OGN({new_ogn.node_uid}) Found pending op(s) for parent '
                                 f'{target_device_uid}:{tgt_node_parent_uid}; '
                                 f'adding new OGN as child dependency of OGN {prev_ogn_for_target_node_parent.node_uid}')
                    parent_ogn_list.append(prev_ogn_for_target_node_parent)

        if prev_ogn_for_target:
            assert not parent_ogn_list, f'Did not expect: {parent_ogn_list}'
            logger.debug(f'[{self.name}] Add_Non_RM_OGN({new_ogn.node_uid}) Adding new OGN as child of prev OGN {prev_ogn_for_target.node_uid}'
                         f'for tgt node')
            return [prev_ogn_for_target]
        elif parent_ogn_list:
            return parent_ogn_list
        else:
            logger.debug(f'[{self.name}] Add_Non_RM_OGN({new_ogn.node_uid}) Found no pending ops for either target node {target_node.dn_uid} '
                         f'or its parent(s); adding to root')
            return [self.root]

    def insert_ogn(self, new_ogn: OpGraphNode):
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
            self._insert_ogn(new_ogn)

    def _insert_ogn(self, new_ogn: OpGraphNode):
        logger.debug(f'[{self.name}] InsertOGN called for: {new_ogn}')

        target_node: Node = new_ogn.get_tgt_node()

        # First check whether the target node is known and has pending operations
        prev_ogn_for_target = self._get_last_pending_ogn_for_node(target_node.device_uid, target_node.uid)

        if new_ogn.is_rm_node():
            parent_ogn_list = self._find_adopters_for_new_rm_ogn(new_ogn, prev_ogn_for_target)
        else:
            # Not an RM node:
            parent_ogn_list = self._find_adopters_for_new_non_rm_ogn(new_ogn, prev_ogn_for_target)

        if not parent_ogn_list:
            # Serious error
            raise InvalidInsertOpGraphError(f'Failed to find parent OGNs to link with: {new_ogn}')

        # Need to clear out previous relationships before adding to main tree:
        new_ogn.clear_relationships()

        is_new_ogn_blocked = False
        for parent_ogn in parent_ogn_list:
            logger.debug(f'[{self.name}] InsertOGN({new_ogn.node_uid}) Adding OGN as child dependency of OGN {parent_ogn.node_uid}')
            parent_ogn.link_child(new_ogn)
            if parent_ogn.op and parent_ogn.op.is_stopped_on_error():
                is_new_ogn_blocked = True

        if is_new_ogn_blocked:
            logger.debug(f'[{self.name}] InsertOGN({new_ogn.node_uid}) New OGN is downstream of an error; setting status of '
                         f'op {new_ogn.op.op_uid} to {UserOpStatus.BLOCKED_BY_ERROR.name}')
            new_ogn.op.set_status(UserOpStatus.BLOCKED_BY_ERROR)

        # Always add to node_queue_dict:
        node_dict = self._node_ogn_q_dict.get(target_node.device_uid, None)
        if not node_dict:
            node_dict = dict()
            self._node_ogn_q_dict[target_node.device_uid] = node_dict
        pending_ogn_queue = node_dict.get(target_node.uid)
        if not pending_ogn_queue:
            pending_ogn_queue = collections.deque()
            node_dict[target_node.uid] = pending_ogn_queue
        pending_ogn_queue.append(new_ogn)

        # Add to ancestor_dict:
        logger.debug(f'[{self.name}] InsertOGN() Tgt node {new_ogn.get_tgt_node().node_identifier} has ancestors: {new_ogn.tgt_ancestor_uid_list}')
        self._increment_icon_update_counts(target_node.device_uid, new_ogn.tgt_ancestor_uid_list)

        if self._max_added_op_uid < new_ogn.op.op_uid:
            self._max_added_op_uid = new_ogn.op.op_uid

        # notify consumers there is something to get:
        self._cv_can_get.notifyAll()

        logger.info(f'[{self.name}] InsertOGN: successfully inserted: {new_ogn}')
        self._print_current_state()

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

        breadth_first_list: List[OpGraphNode] = batch_root.get_subgraph_bfs_list()
        processed_op_uid_set: Set[UID] = set()
        inserted_op_list: List[UserOp] = []
        inserted_ogn_list: List[OpGraphNode] = []

        with self._cv_can_get:
            try:
                for graph_node in skip_root(breadth_first_list):
                    self._insert_ogn(graph_node)
                    inserted_ogn_list.append(graph_node)

                    if graph_node.op.op_uid not in processed_op_uid_set:
                        inserted_op_list.append(graph_node.op)
                        processed_op_uid_set.add(graph_node.op.op_uid)

                if OP_GRAPH_VALIDATE_AFTER_BATCH_INSERT:
                    self._validate_internal_consistency()  # this will raise an OpGraphError if validation fails

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
                    logger.error(f'Unexpected failure while adding batch {batch_uid} to main graph (after adding {len(inserted_ogn_list)} OGNs from '
                                 f'{len(inserted_op_list)} ops) - rethrowing exception')
                    raise err

    # GET logic
    # ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼

    def _is_node_ready(self, op: UserOp, node: Node, node_type_str: str, fail_if_not_found: bool = True) -> bool:
        """Note: we allow for the possibilty that not all nodes have been added to the graph yet (i.e. an op's src node is there but not its dst
        node yet) by setting fail_if_not_found to False"""
        node_dict: Dict[UID, Deque[OpGraphNode]] = self._node_ogn_q_dict.get(node.device_uid, None)
        if not node_dict:
            if SUPER_DEBUG_ENABLED:
                logger.debug(f'[{self.name}] Could not find entry in node dict for device_uid {node.device_uid} (from OGN {node_type_str}, op {op}, '
                             f'fail_if_not_found={fail_if_not_found})')
            if fail_if_not_found:
                raise RuntimeError(f'Serious error: master dict has no entries for device_uid={node.device_uid} (op {node_type_str} node) !')
            else:
                return False
        pending_ogn_queue: Deque[OpGraphNode] = node_dict.get(node.uid, None)
        if not pending_ogn_queue:
            if SUPER_DEBUG_ENABLED:
                logger.debug(f'[{self.name}] Could not find entry in node dict for device_uid {node.device_uid} (from OGN {node_type_str}, op {op}, '
                             f'fail_if_not_found={fail_if_not_found})')
            if fail_if_not_found:
                raise RuntimeError(f'Serious error: NodeQueueDict has no entries for op {node_type_str} node (uid={node.uid})!')
            else:
                return False

        op_graph_node = pending_ogn_queue[0]
        if op.op_uid != op_graph_node.op.op_uid:
            if SUPER_DEBUG_ENABLED:
                logger.debug(f'[{self.name}] Skipping UserOp (UID {op_graph_node.op.op_uid}): it is not next in {node_type_str} node queue')
            return False

        if not op_graph_node.is_child_of_root():
            if SUPER_DEBUG_ENABLED:
                logger.debug(f'[{self.name}] Skipping UserOp (UID {op_graph_node.op.op_uid}): {node_type_str} node is not child of root')
            return False

        return True

    def _try_get(self) -> Optional[UserOp]:
        # We can optimize this later

        for ogn in self.root.get_child_list():
            if SUPER_DEBUG_ENABLED:
                logger.debug(f'[{self.name}] TryGet(): Examining {ogn}')

            if ogn.op.get_status() != UserOpStatus.NOT_STARTED:
                if SUPER_DEBUG_ENABLED:
                    logger.debug(f'[{self.name}] TryGet(): Skipping OGN {ogn.node_uid} because it has status {ogn.op.get_status().name}')
                continue

            if ogn.op.has_dst():
                # If the UserOp has both src and dst nodes, *both* must be next in their queues, and also be just below root.
                if ogn.is_dst():
                    # Dst node is child of root. But verify corresponding src node is also child of root
                    is_other_node_ready = self._is_node_ready(ogn.op, ogn.op.src_node, 'src', fail_if_not_found=False)
                else:
                    # Src node is child of root. But verify corresponding dst node is also child of root.
                    is_other_node_ready = self._is_node_ready(ogn.op, ogn.op.dst_node, 'dst', fail_if_not_found=False)

                if not is_other_node_ready:
                    if SUPER_DEBUG_ENABLED:
                        logger.debug(f'[{self.name}] TryGet(): Skipping OGN {ogn.node_uid} (is_dst={ogn.is_dst()}) because its partner OGN '
                                     f'is not ready')
                    continue

            # Make sure the node has not already been checked out:
            if self._outstanding_op_dict.get(ogn.op.op_uid, None):
                if SUPER_DEBUG_ENABLED:
                    logger.debug(f'[{self.name}] TryGet(): Skipping op {ogn.op.op_uid} because it is already outstanding')
            else:
                if SUPER_DEBUG_ENABLED:
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
                op = self._try_get()
                if op:
                    logger.info(f'[{self.name}] Got next pending op: {op}')
                    return op
                else:
                    logger.debug(f'[{self.name}] No pending ops; sleeping until notified')
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

    def pop_completed_op(self, op: UserOp):
        """Ensure that we were expecting this op to be copmleted, and remove it from the tree."""
        logger.debug(f'[{self.name}] Entered pop_completed_op() for op {op}')

        with self._cv_can_get:
            if not self._outstanding_op_dict.pop(op.op_uid, None):
                raise RuntimeError(f'Complated op not found in outstanding op list (action UID {op.op_uid}')

            status = op.get_status()
            if status != UserOpStatus.COMPLETED_OK and status != UserOpStatus.COMPLETED_NO_OP:
                logger.info(f'[{self.name}] pop_completed_op(): will not pop OGNs for op ({op.op_uid}) as it did not complete OK (status: {status}) ')

                if status == UserOpStatus.STOPPED_ON_ERROR:
                    # Mark affected nodes, and also any nodes from dependent OGNs, as needing icon updates.
                    # This merely populates a dict which represents device_uid-node_uid pairs which should be queried for updated icon via the
                    # get_icon_for_node() method.
                    #
                    # Note that we only need to do this at the moment of failure cuz we need to update nodes which are [possibly] already displayed.
                    # Any new nodes (unknown to us now) which need to be displayed thereafter will already call get_icon_for_node() prior to display.
                    logger.debug(f'[{self.name}] pop_completed_op(): Op stopped on error; '
                                 f'will populate error dict (currently = {self._changed_node_dict})')
                    self._block_downstream_ogns_for_failed_op(op)
                    logger.debug(f'[{self.name}] pop_completed_op(): Error dict is now = {self._changed_node_dict}')
            else:
                # I. SRC OGN
                self._remove_ogn_for_completed_op(op.src_node, op, 'src')

                # II. DST OGN
                if op.has_dst():
                    self._remove_ogn_for_completed_op(op.dst_node, op, 'dst')

            logger.debug(f'[{self.name}] Done with pop_completed_op() for op: {op}')
            self._print_current_state()

            # this may have jostled the tree to make something else free:
            self._cv_can_get.notifyAll()

    def _rollback(self, ogn_list: List[OpGraphNode]):
        ogn_count = len(ogn_list)
        while len(ogn_list) > 0:
            # Back out in reverse order in which they were inserted
            ogn = ogn_list.pop()
            logger.debug(f'Backing out insert of OGN {-(len(ogn_list) - ogn_count)} of {ogn_count}: {ogn}')
            self._uninsert_ogn(ogn)

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

    def _block_downstream_ogns_for_failed_op(self, failed_op: UserOp):
        assert failed_op.get_status() == UserOpStatus.STOPPED_ON_ERROR, f'Op is not in failed status: {failed_op}'

        queue: Deque[UserOp] = collections.deque()
        queue.append(failed_op)

        while len(queue) > 0:
            op: UserOp = queue.popleft()

            if op.result:
                logger.debug(f'[{self.name}] Op {op.op_uid} already has a result: {op.result}')
            else:
                logger.debug(f'[{self.name}] Setting status of op {op.op_uid} to {UserOpStatus.BLOCKED_BY_ERROR.name}')
                op.result = UserOpResult(status=UserOpStatus.BLOCKED_BY_ERROR)

            self._process_and_enqueue_children(op.src_node, op, queue)

            if op.has_dst():
                self._process_and_enqueue_children(op.dst_node, op, queue)

    def _process_and_enqueue_children(self, tgt_node: Node, op: UserOp, queue: Deque[UserOp]):
        ogn: OpGraphNode = self._get_last_pending_ogn_for_node(tgt_node.device_uid, tgt_node.uid)
        assert ogn and ogn.op.op_uid == op.op_uid, f'Op (UID {op.op_uid}) does not match last OGN in its target node\'s queue ({ogn})'

        self._add_tgt_node_to_icon_changes_dict(tgt_node)

        # Enqueue downstream ops:
        for child_ogn in ogn.get_child_list():
            queue.append(child_ogn.op)

    def _remove_ogn_for_completed_op(self, tgt_node: Node, op: UserOp, node_label: str):

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

    def _remove_ogn_from_node_queue(self, tgt_node: Node, label: str, remove_func: Callable[[Deque[OpGraphNode]], OpGraphNode]) -> OpGraphNode:
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

    def _unlink_ogn_from_graph(self, tgt_ogn: OpGraphNode):
        # FIXME: attach to parents, not root!

        for ogn_parent in tgt_ogn.get_parent_list():
            ogn_parent.unlink_child(tgt_ogn)

        # unlink its children also. If the child then has no other parents, it moves up to become child of root
        for ogn_child in tgt_ogn.get_child_list():
            ogn_child.unlink_parent(tgt_ogn)

            if not ogn_child.get_parent_list():
                self.root.link_child(ogn_child)
            else:
                # Sanity check:
                for par in ogn_child.get_parent_list():
                    if not par.get_parent_list():
                        logger.error(f'[{self.name}] Node has no parents: {par}')
