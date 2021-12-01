import collections
import logging
import threading
from typing import Deque, Dict, Iterable, List, Optional, Set

from backend.executor.user_op.op_graph_node import OpGraphNode, RmOpNode, RootNode
from constants import IconId, NULL_UID, SUPER_DEBUG_ENABLED, TRACE_ENABLED
from error import InvalidInsertOpGraphError
from model.node.node import Node
from model.uid import UID
from model.user_op import OpTypeMeta, UserOp, UserOpResult, UserOpStatus
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

    Flow graph for user ops.
    Note: an OpGraph is built out of "OpGraph nodes", which I may also refer to as "graph nodes", which have class OpGraphNode.
    These should be distinguished from regular file nodes, dir nodes, etc, which I'll refer to as simply "nodes", with class Node.
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

    # Misc.
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

    def _get_last_pending_ogn_for_node(self, device_uid: UID, node_uid: UID):
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

    def get_icon_for_node(self, device_uid: UID, node_uid: UID) -> Optional[IconId]:
        with self._cv_can_get:
            ogn: OpGraphNode = self._get_last_pending_ogn_for_node(device_uid, node_uid)
            if ogn and not ogn.op.is_completed():
                icon = OpTypeMeta.get_icon_for_node(ogn.get_tgt_node().is_dir(), is_dst=ogn.is_dst(), op=ogn.op)
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

    def get_max_added_op_uid(self) -> UID:
        with self._cv_can_get:
            return self._max_added_op_uid

    def validate_graph(self):
        with self._cv_can_get:
            self._validate_graph()

    def _validate_graph(self):
        """The caller is responsible for locking the graph before calling this."""
        logger.debug(f'[{self.name}] Validating graph...')
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
            raise RuntimeError(f'Validation for OpGraph failed with {error_count} errors!')
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

    def _insert_rm_in_graph(self, new_ogn: OpGraphNode, prev_ogn_for_target):
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
        og_nodes_for_child_nodes_list: List[OpGraphNode] = self._find_og_nodes_for_children_of_rm_target_node(new_ogn)
        if og_nodes_for_child_nodes_list:
            # Possibility 1: children
            logger.debug(f'[{self.name}] Add_RM_OGN({new_ogn.node_uid}) Found {len(og_nodes_for_child_nodes_list)} '
                         f'existing OGNs for children of tgt node ({target_node.dn_uid}); examining whether to add as child dependency of each')

            parent_ogn_list: List[OpGraphNode] = []  # Use this list to store parents to link to until we are done validating.
            for og_node_for_child_node in og_nodes_for_child_nodes_list:
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

            for parent_ogn in parent_ogn_list:
                logger.debug(f'[{self.name}] Add_RM_OGN({new_ogn.node_uid}) Adding OGN as child dependency of OGN {parent_ogn.node_uid}')
                parent_ogn.link_child(new_ogn)

        elif prev_ogn_for_target:
            # Possibility 2: If existing op is found for the target node, add below that.

            # The node's children MUST be removed first. It is invalid to RM a node which has children.
            # (if the children being added are later scheduled for removal, then they should should up in Possibility 1
            if prev_ogn_for_target.get_child_list():
                raise InvalidInsertOpGraphError(f'While trying to add RM op: did not expect existing OGN for target to have children! '
                                                f'(tgt={prev_ogn_for_target})')
            logger.debug(f'[{self.name}] Add_RM_OGN({new_ogn.node_uid}) Found pending op(s) for target node {target_node.dn_uid} for RM op; '
                         f'adding as child dependency')
            prev_ogn_for_target.link_child(new_ogn)
        else:
            # Possibility 3: no previous ops for node or its children
            logger.debug(f'[{self.name}] Add_RM_OGN({new_ogn.node_uid}) Found no previous ops for target node {target_node.dn_uid} or its children; '
                         f'adding to root')
            self.root.link_child(new_ogn)

    def _insert_non_rm_in_graph(self, new_ogn: OpGraphNode, prev_ogn_for_target: OpGraphNode, tgt_parent_uid_list: List[UID]):
        target_node: Node = new_ogn.get_tgt_node()
        target_device_uid: UID = target_node.device_uid

        for tgt_node_parent_uid in tgt_parent_uid_list:
            prev_ogn_for_target_node_parent = self._get_last_pending_ogn_for_node(target_device_uid, tgt_node_parent_uid)

            if prev_ogn_for_target and prev_ogn_for_target_node_parent:
                if prev_ogn_for_target.get_level() > prev_ogn_for_target_node_parent.get_level():
                    logger.debug(f'[{self.name}] Add_Non_RM_OGN({new_ogn.node_uid}) ast target op (for node {target_node.dn_uid}) '
                                 f'is lower level than last op for parent node ({tgt_node_parent_uid}); adding new OGN as child of last target op '
                                 f'(OGN {prev_ogn_for_target.node_uid})')
                    prev_ogn_for_target.link_child(new_ogn)
                else:
                    logger.debug(f'[{self.name}] Add_Non_RM_OGN({new_ogn.node_uid}) Last target op is >= level than last op for parent node; '
                                 f'adding new OG node as child of last op for parent node (OG node {prev_ogn_for_target_node_parent.node_uid})')
                    prev_ogn_for_target_node_parent.link_child(new_ogn)
            elif prev_ogn_for_target:
                assert not prev_ogn_for_target_node_parent
                logger.debug(f'[{self.name}] Add_Non_RM_OGN({new_ogn.node_uid}) Found pending op(s) for target node {target_node.dn_uid}; '
                             f'adding new OGN as child dependency of OGN {prev_ogn_for_target.node_uid}')
                prev_ogn_for_target.link_child(new_ogn)
            elif prev_ogn_for_target_node_parent:
                assert not prev_ogn_for_target
                logger.debug(f'[{self.name}] Add_Non_RM_OGN({new_ogn.node_uid}) Found pending op(s) for parent '
                             f'{target_device_uid}:{tgt_node_parent_uid}; '
                             f'adding new OGN as child dependency of OGN {prev_ogn_for_target_node_parent.node_uid}')
                # FIXME: what if this is a remove-type node? What about ancestors of target node? We need a way to look further up the node trees.
                prev_ogn_for_target_node_parent.link_child(new_ogn)
            else:
                assert not prev_ogn_for_target_node_parent and not prev_ogn_for_target
                logger.debug(f'[{self.name}] Add_Non_RM_OGN({new_ogn.node_uid}) Found no pending ops for either target node {target_node.dn_uid} '
                             f'or parent node {target_device_uid}:{tgt_node_parent_uid}; adding to root')
                self.root.link_child(new_ogn)

    def enqueue_single_ogn(self, new_ogn: OpGraphNode):
        """
        The node shall be added as a child dependency of either the last operation which affected its target,
        or as a child dependency of the last operation which affected its parent, whichever has lower priority (i.e. has a lower level
        in the dependency tree). In the case where neither the node nor its parent has a pending operation, we obviously can just add
        to the top of the dependency tree.

        A successful return indicates taht the node was successfully nq'd; raises InvalidInsertOpGraphError otherwise
        """
        logger.debug(f'[{self.name}] InsertOGNode called for: {new_ogn}')

        # Need to clear out previous relationships before adding to main tree:
        new_ogn.clear_relationships()

        target_node: Node = new_ogn.get_tgt_node()
        tgt_parent_uid_list: List[UID] = target_node.get_parent_uids()

        with self._cv_can_get:
            device_uid = new_ogn.get_tgt_node().device_uid

            # First check whether the target node is known and has pending operations
            prev_ogn_for_target = self._get_last_pending_ogn_for_node(device_uid, target_node.uid)
            if prev_ogn_for_target and prev_ogn_for_target.op.is_stopped_on_error():
                # User should resolve this first
                raise InvalidInsertOpGraphError(f'Previous operation for target node {target_node.node_identifier} is already blocked due to error')

            for ancestor_uid in new_ogn.tgt_ancestor_uid_list:
                prev_ogn_for_ancestor = self._get_last_pending_ogn_for_node(target_node.device_uid, ancestor_uid)
                if prev_ogn_for_ancestor and prev_ogn_for_ancestor.op.is_stopped_on_error():
                    raise InvalidInsertOpGraphError(f'An ancestor {target_node.node_identifier} of affected node {target_node.node_identifier}'
                                                    f' is already blocked due to error')

            if new_ogn.is_rm_node():
                self._insert_rm_in_graph(new_ogn, prev_ogn_for_target)
            else:
                # Not an RM node:
                self._insert_non_rm_in_graph(new_ogn, prev_ogn_for_target, tgt_parent_uid_list)

            # Always add to node_queue_dict:
            node_dict = self._node_ogn_q_dict.get(target_node.device_uid, None)
            if not node_dict:
                node_dict = dict()
                self._node_ogn_q_dict[target_node.device_uid] = node_dict
            pending_op_queue = node_dict.get(target_node.uid)
            if not pending_op_queue:
                pending_op_queue = collections.deque()
                node_dict[target_node.uid] = pending_op_queue
            pending_op_queue.append(new_ogn)

            # Add to ancestor_dict:
            device_ancestor_dict: Dict[UID, int] = self._ancestor_dict.get(device_uid)
            if not device_ancestor_dict:
                device_ancestor_dict = {}
                self._ancestor_dict[device_uid] = device_ancestor_dict
            logger.debug(f'[{self.name}] Tgt node {new_ogn.get_tgt_node().node_identifier} has ancestors: {new_ogn.tgt_ancestor_uid_list}')
            for ancestor_uid in new_ogn.tgt_ancestor_uid_list:
                count = device_ancestor_dict.get(ancestor_uid, 0)
                if count == 0:
                    added_ancestor_map_for_device = self._added_ancestor_dict.get(device_uid)
                    if not added_ancestor_map_for_device:
                        added_ancestor_map_for_device = set()
                        self._added_ancestor_dict[device_uid] = added_ancestor_map_for_device
                    added_ancestor_map_for_device.add(ancestor_uid)

                    removed_ancestor_map_for_device = self._removed_ancestor_dict.get(device_uid)
                    if removed_ancestor_map_for_device and ancestor_uid in removed_ancestor_map_for_device:
                        removed_ancestor_map_for_device.remove(ancestor_uid)
                device_ancestor_dict[ancestor_uid] = count + 1

            if SUPER_DEBUG_ENABLED:
                logger.debug(f'[{self.name}] Ancestor dict: {self._ancestor_dict}')

            if self._max_added_op_uid < new_ogn.op.op_uid:
                self._max_added_op_uid = new_ogn.op.op_uid

            # notify consumers there is something to get:
            self._cv_can_get.notifyAll()

            logger.info(f'[{self.name}] InsertOGNode: successfully inserted: {new_ogn}')
            self._print_current_state()

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
        pending_op_queue: Deque[OpGraphNode] = node_dict.get(node.uid, None)
        if not pending_op_queue:
            if SUPER_DEBUG_ENABLED:
                logger.debug(f'[{self.name}] Could not find entry in node dict for device_uid {node.device_uid} (from OGN {node_type_str}, op {op}, '
                             f'fail_if_not_found={fail_if_not_found})')
            if fail_if_not_found:
                raise RuntimeError(f'Serious error: NodeQueueDict has no entries for op {node_type_str} node (uid={node.uid})!')
            else:
                return False

        op_graph_node = pending_op_queue[0]
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

    def pop_op(self, op: UserOp):
        """Ensure that we were expecting this op to be copmleted, and remove it from the tree."""
        logger.debug(f'[{self.name}] Entered pop_op() for op {op}')

        with self._cv_can_get:
            if not self._outstanding_op_dict.pop(op.op_uid, None):
                raise RuntimeError(f'Complated op not found in outstanding op list (action UID {op.op_uid}')

            status = op.get_status()
            if status != UserOpStatus.COMPLETED_OK and status != UserOpStatus.COMPLETED_NO_OP:
                logger.info(f'[{self.name}] pop_op(): will not pop OGNs for op ({op.op_uid}) as it did not complete OK (status: {status}) ')

                if status == UserOpStatus.STOPPED_ON_ERROR:
                    # Mark affected nodes, and also any nodes from dependent OGNs, as needing icon updates.
                    # This merely populates a dict which represents device_uid-node_uid pairs which should be queried for updated icon via the
                    # get_icon_for_node() method.
                    #
                    # Note that we only need to do this at the moment of failure cuz we need to update nodes which are [possibly] already displayed.
                    # Any new nodes (unknown to us now) which need to be displayed thereafter will already call get_icon_for_node() prior to display.
                    logger.debug(f'[{self.name}] pop_op(): Op stopped on error; will populate error dict (currently = {self._changed_node_dict})')
                    self._add_all_affected_nodes_to_changes_dict(op)
                    logger.debug(f'[{self.name}] pop_op(): Error dict is now = {self._changed_node_dict}')
            else:
                # I. SRC OGN
                self._remove_ogn(op.src_node, op, 'src')

                # II. DST OGN
                if op.has_dst():
                    self._remove_ogn(op.dst_node, op, 'dst')

            logger.debug(f'[{self.name}] Done with pop_op() for op: {op}')
            self._print_current_state()

            # this may have jostled the tree to make something else free:
            self._cv_can_get.notifyAll()

    def _add_all_affected_nodes_to_changes_dict(self, op: UserOp):
        queue: Deque[UserOp] = collections.deque()
        queue.append(op)

        while len(queue) > 0:
            op: UserOp = queue.popleft()

            if op.result:
                logger.debug(f'[{self.name}] Op {op.op_uid} already has a result: {op.result}')
            else:
                logger.debug(f'[{self.name}] Setting status of op {op.op_uid} to {UserOpStatus.BLOCKED_BY_ERROR.name}')
                op.result = UserOpResult(status=UserOpStatus.BLOCKED_BY_ERROR)

            self._add_node_to_icon_changes_dict(op.src_node, op, queue)

            if op.has_dst():
                self._add_node_to_icon_changes_dict(op.dst_node, op, queue)

    def _add_node_to_icon_changes_dict(self, tgt_node: Node, op: UserOp, queue: Deque[UserOp]):
        ogn: OpGraphNode = self._get_last_pending_ogn_for_node(tgt_node.device_uid, op.src_node.uid)
        assert ogn and ogn.op.op_uid == op.op_uid, f'Op (UID {op.op_uid}) does not match last node popped from its node\'s queue ({ogn})'

        # Add tgt node to change map
        added_error_node_map_for_device = self._changed_node_dict.get(tgt_node.device_uid)
        if not added_error_node_map_for_device:
            added_error_node_map_for_device = set()
            self._changed_node_dict[tgt_node.device_uid] = added_error_node_map_for_device
        added_error_node_map_for_device.add(tgt_node.uid)

        logger.debug(f'[{self.name}] Adding node {tgt_node.node_identifier} to icon refresh list')

        # Enqueue dependent ops:
        for child_ogn in ogn.get_child_list():
            queue.append(child_ogn.op)

    def _remove_ogn(self, tgt_node: Node, op: UserOp, node_label: str):
        # 1. Remove tgt node from node dict

        node_dict_for_device: Dict[UID, Deque[OpGraphNode]] = self._node_ogn_q_dict.get(tgt_node.device_uid)
        if not node_dict_for_device:
            # very bad
            raise RuntimeError(f'Completed op {node_label} node device_uid ({tgt_node.device_uid}) not found in master dict (for {tgt_node.dn_uid}')

        tgt_ogn_queue: Deque[OpGraphNode] = node_dict_for_device.get(tgt_node.uid)
        if not tgt_ogn_queue:
            # very bad
            self._print_current_state()
            raise RuntimeError(f'Completed op for {node_label} node ({tgt_node.dn_uid}) not found in master dict!')

        tgt_ogn: OpGraphNode = tgt_ogn_queue.popleft()
        if tgt_ogn.op.op_uid != op.op_uid:
            # very bad
            self._print_current_state()
            raise RuntimeError(f'Completed op (UID {op.op_uid}) does not match last node popped from its {node_label} node\'s queue '
                               f'(UID {tgt_ogn.op.op_uid})')
        if not tgt_ogn_queue:
            # Remove queue if it is empty:
            node_dict_for_device.pop(tgt_node.uid, None)
        if not node_dict_for_device:
            # Remove device dict if it is empty:
            self._node_ogn_q_dict.pop(tgt_node.device_uid, None)

        # 2. Remove tgt node ancestor counts

        self._decrement_ancestor_counts(tgt_ogn)

        # 3. Remove tgt node from op graph

        # validate it is a child of root
        if not tgt_ogn.is_child_of_root():
            ogn_parent = tgt_ogn.get_first_parent()
            ogn_parent_uid = ogn_parent.node_uid if ogn_parent else None
            raise RuntimeError(f'Src node for completed op is not a parent of root (instead found parent OGN {ogn_parent_uid})')

        # unlink from its parent
        self.root.unlink_child(tgt_ogn)

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

        # 4. Delete tgt OGN
        del tgt_ogn

    def _decrement_ancestor_counts(self, ogn: OpGraphNode):
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

    def revert_ogn(self, ogn: OpGraphNode):
        # TODO!
        raise NotImplementedError()
