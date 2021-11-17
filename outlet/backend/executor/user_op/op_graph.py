import collections
import logging
import threading
from typing import Deque, Dict, Iterable, List, Optional, Set

from backend.executor.user_op.op_graph_node import OpGraphNode, RmOpNode, RootNode
from constants import NULL_UID, OP_GRAPH_VALIDATE_AFTER_EVERY_INSERTED_OG_NODE, SUPER_DEBUG_ENABLED, TRACE_ENABLED
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

        self._outstanding_ogn_dict: Dict[UID, UserOp] = {}
        """Contains entries for all UserOps which have running operations. Keyed by action UID"""

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

        logger.debug(f'[{self.name}] CURRENT EXECUTION STATE: OpGraph = {len(graph_line_list)} OGNs:')
        for graph_line in graph_line_list:
            logger.debug(f'[{self.name}] {graph_line}')

        logger.debug(f'[{self.name}] CURRENT EXECUTION STATE: NodeQueues = [#Ops: {len(op_set)} #Devices: {len(self._node_ogn_q_dict)} '
                     '#Nodes: {queue_count}]:')
        for qd_line in qd_line_list:
            logger.debug(f'[{self.name}] {qd_line}')

    def _get_lowest_priority_op_node(self, device_uid: UID, node_uid: UID):
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
            op_node: OpGraphNode = self._get_lowest_priority_op_node(device_uid, node_uid)
        if op_node:
            return op_node.op
        return None

    def get_max_added_op_uid(self) -> UID:
        with self._cv_can_get:
            return self._max_added_op_uid

    def _validate_graph(self, last_ogn_added: OpGraphNode):
        """The caller is responsible for locking the graph before calling this."""
        logger.debug(f'[{self.name}] Validating graph...')
        error_count = 0
        ogn_coverage_dict: Dict[UID, OpGraphNode] = {self.root.node_uid: self.root}  # OGN uid -> OGN
        binary_op_src_coverage_dict: Dict[UID, OpGraphNode] = {}
        binary_op_dst_coverage_dict: Dict[UID, OpGraphNode] = {}
        unrecognized_og_node_dict : Dict[UID, str] = {}

        # Iterate through graph using a queue, using ogn_coverage_dict to avoid doing duplicate analysis:
        og_node_queue: Deque[OpGraphNode] = collections.deque()

        for child_of_root in self.root.get_child_list():
            if not child_of_root.is_child_of_root():
                error_count += 1
                logger.error(f'[{self.name}] ValidateGraph: OG node is a child of root but is_child_of_root()==False: {child_of_root}')
            og_node_queue.append(child_of_root)

        while len(og_node_queue) > 0:
            og_node: OpGraphNode = og_node_queue.popleft()
            logger.debug(f'[{self.name}] ValidateGraph: Examining OGN from graph: {og_node}')

            if ogn_coverage_dict.get(og_node.node_uid, None):
                # already processed this node
                continue

            ogn_coverage_dict[og_node.node_uid] = og_node

            # Verify its op:
            if og_node.op.has_dst():
                if og_node.is_src():
                    prev_ogn = binary_op_src_coverage_dict.get(og_node.op.op_uid, None)
                    if prev_ogn:
                        error_count += 1
                        logger.error(f'[{self.name}] ValidateGraph: Duplicate OGNs for src op! Prev={prev_ogn}, current={og_node}')
                    else:
                        binary_op_src_coverage_dict[og_node.op.op_uid] = og_node
                else:
                    assert og_node.is_dst(), f'Expected dst-type OGN but got: {og_node}'
                    prev_ogn = binary_op_dst_coverage_dict.get(og_node.op.op_uid, None)
                    if prev_ogn:
                        error_count += 1
                        logger.error(f'[{self.name}] ValidateGraph: Duplicate OGNs for dst op! Prev={prev_ogn}, current={og_node}')
                    else:
                        binary_op_dst_coverage_dict[og_node.op.op_uid] = og_node

            # Verify parents:
            parent_og_node_list = og_node.get_parent_list()
            if not parent_og_node_list:
                error_count += 1
                logger.error(f'ValidateGraph: OG node has no parents: {og_node}')
            else:
                parent_uid_set: Set[UID] = set()
                for parent_og_node in parent_og_node_list:
                    if parent_og_node.node_uid in parent_uid_set:
                        error_count += 1
                        logger.error(f'[{self.name}] ValidateGraph: Duplicate parent listed in OG node! OGN uid={og_node.node_uid}, '
                                     f'parent_uid={parent_og_node.node_uid}')
                        continue
                    else:
                        parent_uid_set.add(parent_og_node.node_uid)

                    if not ogn_coverage_dict.get(parent_og_node.node_uid, None):
                        unrecognized_og_node_dict[parent_og_node.node_uid] = \
                            'Unrecognized parent listed in OG node! OGN uid={og_node.node_uid}, ' \
                            'parent_OGN={parent_og_node}'

                has_multiple_parents = len(parent_og_node_list) > 1
                tgt_node: Node = og_node.get_tgt_node()

                if not og_node.is_child_of_root():
                    if og_node.is_rm_node():
                        all_parents_must_be_remove_type = False
                        if has_multiple_parents:
                            all_parents_must_be_remove_type = True
                        child_node_uid_set = set()
                        for parent_og_node in parent_og_node_list:
                            if parent_og_node.is_remove_type():
                                all_parents_must_be_remove_type = True
                                parent_tgt_node: Node = parent_og_node.get_tgt_node()
                                if not tgt_node.is_parent_of(parent_tgt_node):
                                    error_count += 1
                                    logger.error(f'[{self.name}] ValidateGraph: Parent of RM OG node is remove-type, but its target node is not '
                                                 f'a child of its child"s target node! OG node={og_node}, parent={parent_og_node}')
                                if parent_tgt_node.uid in child_node_uid_set:
                                    error_count += 1
                                    logger.error(f'[{self.name}] ValidateGraph: Parents of RM OG node have duplicate target node! OG node={og_node}, '
                                                 f'parent={parent_og_node}, offending node UID={parent_tgt_node.uid}')
                                child_node_uid_set.add(parent_tgt_node.uid)
                            else:
                                # Parent is not remove-type

                                if all_parents_must_be_remove_type:
                                    error_count += 1
                                    logger.error(f'[{self.name}] ValidateGraph: Some parents of RM OG node are remove-type, but this one is not! '
                                                 f'OG node={og_node}, offending OG parent={parent_og_node}')
                                else:
                                    # Parent's tgt node must be an ancestor of tgt node (or same node).
                                    # Check all paths and confirm that at least one path in parent tgt contains the path of tgt
                                    if not og_node.is_tgt_an_ancestor_of_og_node_tgt(parent_og_node):
                                        error_count += 1
                                        logger.error(f'[{self.name}] ValidateGraph: Parent of RM OG node is not remove-type, and its '
                                                     f'target node is not an ancestor of its child OG\'s target! OG node={og_node}, '
                                                     f'offending OG parent={parent_og_node}')

                    else:  # NOT og_node.is_rm_node()
                        assert not has_multiple_parents, \
                            f'Non-RM OG node should not be allowed to have multiple parents: {og_node}, parents={parent_og_node_list}'
                        for parent_og_node in parent_og_node_list:
                            if not parent_og_node.is_tgt_an_ancestor_of_og_node_tgt(og_node):
                                error_count += 1
                                logger.error(f'[{self.name}] ValidateGraph: Parent of OGN has target node which is not an ancestor '
                                             f'of its child\'s target! OG node={og_node}, offending parent={parent_og_node}')

            # Verify children:
            child_og_node_list = og_node.get_child_list()
            if child_og_node_list:
                for child_og_node in child_og_node_list:
                    parent_found = False
                    for parent_of_child in child_og_node.get_parent_list():
                        if parent_of_child.node_uid == og_node.node_uid:
                            parent_found = True

                    if not parent_found:
                        error_count += 1
                        logger.error(f'[{self.name}] ValidateGraph: Child of OG node does not list it as parent! OG node={og_node}, '
                                     f'child={child_og_node}')

                    if ogn_coverage_dict.get(child_og_node.node_uid, None):
                        logger.debug(f'[{self.name}] ValidateGraph: Already encountered child of OG node; skipping: {child_og_node}')
                    else:
                        og_node_queue.append(child_og_node)

        for og_node_uid, error_msg in unrecognized_og_node_dict.items():
            if not ogn_coverage_dict.get(og_node_uid, None):
                # still missing: raise original error
                error_count += 1
                logger.error(f'[{self.name}] ValidateGraph: {error_msg}')

        logger.debug(f'[{self.name}] ValidateGraph: encountered {len(ogn_coverage_dict)} OGNs in graph')

        # Check for missing OGNs for binary ops:
        for ogn_src in binary_op_src_coverage_dict.values():
            if not binary_op_dst_coverage_dict.pop(ogn_src.op.op_uid, None):
                # Make an exception for the last OGN added, because it may not have its accompanying node yet
                if ogn_src.node_uid != last_ogn_added.node_uid:
                    error_count += 1
                    logger.error(f'[{self.name}] ValidateGraph: Dst OGN missing for op: {ogn_src.op} (found: {ogn_src})')

        if len(binary_op_dst_coverage_dict) > 0:
            for ogn_dst in binary_op_dst_coverage_dict.values():
                # Make an exception for the last OGN added, because it may not have its accompanying node yet
                if ogn_dst.node_uid != last_ogn_added.node_uid:
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
            logger.info(f'[{self.name}] ValidateGraph: completed with no errors for {len(ogn_coverage_dict)} OGNs')

    # INSERT logic
    # ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼

    def _find_og_nodes_for_children_of_rm_target_node(self, og_node: OpGraphNode) -> List[OpGraphNode]:
        """When adding an RM OGnode, we need to locate its OGnode parents (which are the child nodes of its target node.
        We must make sure that all the child nodes are going to be removed """
        # TODO: probably could optimize a bit more...
        sw_total = Stopwatch()

        assert isinstance(og_node, RmOpNode), f'Unepxected type: {og_node}'
        potential_parent: Node = og_node.get_tgt_node()

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
                            raise RuntimeError(f'Found child node for RM-type node which is not RM type: {existing_og_node}')
                        child_nodes.append(existing_og_node)

        logger.debug(f'[{self.name}] {sw_total} Found {len(child_nodes):,} child nodes in graph for RM node')
        return child_nodes

    def _insert_rm_node_in_tree(self, new_og_node: OpGraphNode, prev_og_node_for_target, tgt_parent_uid_list: List[UID]) -> bool:
        # Special handling for RM-type nodes.
        # We want to find the lowest RM node in the tree.
        assert new_og_node.is_rm_node()
        target_node: Node = new_og_node.get_tgt_node()

        # FIXME: I seem to be checking the op graph for pending ops on the target's parent, but what about further up in its ancestors?

        # First, see if we can find child nodes of the target node (which would be the parents of the RM OG node):
        og_nodes_for_child_nodes_list: List[OpGraphNode] = self._find_og_nodes_for_children_of_rm_target_node(new_og_node)
        if og_nodes_for_child_nodes_list:
            # Possibility 1: children
            logger.debug(f'[{self.name}] Found {len(og_nodes_for_child_nodes_list)} existing OG nodes for children of node being removed '
                         f'({target_node.dn_uid}); examining whether to add as child dependency of each')
            assert new_og_node.get_tgt_node()

            parent_ogn_list: List[OpGraphNode] = []  # Use this list to store parents to link to until we are done validating.
            for og_node_for_child_node in og_nodes_for_child_nodes_list:
                logger.debug(f'[{self.name}] Examining {og_node_for_child_node}')
                if not og_node_for_child_node.is_remove_type():
                    raise RuntimeError(f'Cannot insert RM OGN: all children of its target must be scheduled for removal first,'
                                       f'but found: {og_node_for_child_node} (while trying to insert OGN: {new_og_node})')

                # Check if there's an existing OGN which is child of parent OGN.
                conflicting_ogn: Optional[OpGraphNode] = og_node_for_child_node.get_first_child()
                if conflicting_ogn:
                    if not conflicting_ogn.is_remove_type():
                        raise RuntimeError(f'Found unexpected node which is blocking insert of our RM operation: {conflicting_ogn} '
                                           f'(while trying to insert: {new_og_node})')
                    else:
                        # extremely rare for this to happen - likely indicates a hole in our logic somewhere
                        raise RuntimeError(f'Found unexpected child OGN: {conflicting_ogn} (while trying to insert OGN: {new_og_node})')
                else:
                    parent_ogn_list.append(og_node_for_child_node)

            for parent_ogn in parent_ogn_list:
                logger.debug(f'[{self.name}] Adding OGN {new_og_node.node_uid} as child dependency of OGN {parent_ogn.node_uid}')
                parent_ogn.link_child(new_og_node)

        elif prev_og_node_for_target:
            # Possibility 2: If existing op is found for the target node, add below that.
            if prev_og_node_for_target.is_rm_node():
                logger.warning(f'[{self.name}] UserOp node being enqueued (UID {new_og_node.op.src_node.dn_uid}, tgt {target_node.dn_uid}) is an RM '
                               f'which is a dup of already enqueued RM ({prev_og_node_for_target.op.src_node.dn_uid}); discarding!')
                return False

            # The node's children MUST be removed first. It is invalid to RM a node which has children.
            # (if the children being added are later scheduled for removal, then they should should up in Possibility 1
            if prev_og_node_for_target.get_child_list():
                raise RuntimeError(f'While trying to add RM op: did not expect existing OGN for target to have children! '
                                   f'(tgt={prev_og_node_for_target})')
            logger.debug(f'[{self.name}] Found pending op(s) for target node {target_node.dn_uid} for RM op; adding as child dependency')
            prev_og_node_for_target.link_child(new_og_node)
        else:
            # Possibility 3: no previous ops for node or its children
            logger.debug(f'[{self.name}] Found no previous ops for target node {target_node.dn_uid} or its children; adding to root')
            self.root.link_child(new_og_node)

        return True

    def _insert_non_rm_node_in_tree(self, new_og_node: OpGraphNode, prev_og_node_for_target: OpGraphNode, tgt_parent_uid_list: List[UID]):
        target_node: Node = new_og_node.get_tgt_node()
        target_device_uid: UID = target_node.device_uid

        # FIXME: I seem to be checking the op graph for pending ops on the target's parent, but what about further up in its ancestors?

        for tgt_node_parent_uid in tgt_parent_uid_list:
            prev_og_node_for_tgt_node_parent = self._get_lowest_priority_op_node(target_device_uid, tgt_node_parent_uid)

            if prev_og_node_for_target and prev_og_node_for_tgt_node_parent:
                if prev_og_node_for_target.get_level() > prev_og_node_for_tgt_node_parent.get_level():
                    logger.debug(f'[OGN {new_og_node.node_uid}] Last target op (for node {target_node.dn_uid}) is lower level than last op '
                                 f'for parent node ({tgt_node_parent_uid}); adding new OGN as child of last target op '
                                 f'(OGN {prev_og_node_for_target.node_uid})')
                    prev_og_node_for_target.link_child(new_og_node)
                else:
                    logger.debug(f'[OGN {new_og_node.node_uid}] Last target op is >= level than last op for parent node; adding new OG node '
                                 f'as child of last op for parent node (OG node {prev_og_node_for_tgt_node_parent.node_uid})')
                    prev_og_node_for_tgt_node_parent.link_child(new_og_node)
            elif prev_og_node_for_target:
                assert not prev_og_node_for_tgt_node_parent
                logger.debug(f'[OGN {new_og_node.node_uid}] Found pending op(s) for target node {target_node.dn_uid}; '
                             f'adding new OGN as child dependency of OGN {prev_og_node_for_target.node_uid}')
                prev_og_node_for_target.link_child(new_og_node)
            elif prev_og_node_for_tgt_node_parent:
                assert not prev_og_node_for_target
                logger.debug(f'[OGN {new_og_node.node_uid}] Found pending op(s) for parent {target_device_uid}:{tgt_node_parent_uid}; '
                             f'adding new OGN as child dependency of OGN {prev_og_node_for_tgt_node_parent.node_uid}')
                # FIXME: what if this is a remove-type node? What about ancestors of target node? We need a way to look further up the node trees.
                prev_og_node_for_tgt_node_parent.link_child(new_og_node)
            else:
                assert not prev_og_node_for_tgt_node_parent and not prev_og_node_for_target
                logger.debug(f'[OGN {new_og_node.node_uid}] Found no pending ops for either target node {target_node.dn_uid} '
                             f'or parent node {target_device_uid}:{tgt_node_parent_uid}; adding to root')
                self.root.link_child(new_og_node)

    def enqueue_single_og_node(self, new_og_node: OpGraphNode) -> bool:
        """
        The node shall be added as a child dependency of either the last operation which affected its target,
        or as a child dependency of the last operation which affected its parent, whichever has lower priority (i.e. has a lower level
        in the dependency tree). In the case where neither the node nor its parent has a pending operation, we obviously can just add
        to the top of the dependency tree.

        Returns True if the node was successfully nq'd; returns False if discarded
        """
        logger.debug(f'[{self.name}] InsertOGNode called for: {new_og_node}')

        # Need to clear out previous relationships before adding to main tree:
        new_og_node.clear_relationships()

        target_node: Node = new_og_node.get_tgt_node()
        tgt_parent_uid_list: List[UID] = target_node.get_parent_uids()

        with self._cv_can_get:
            # First check whether the target node is known and has pending operations
            prev_og_node_for_target = self._get_lowest_priority_op_node(target_node.device_uid, target_node.uid)

            if new_og_node.is_rm_node():
                insert_succeeded = self._insert_rm_node_in_tree(new_og_node, prev_og_node_for_target, tgt_parent_uid_list)
            else:
                # Not an RM node:
                self._insert_non_rm_node_in_tree(new_og_node, prev_og_node_for_target, tgt_parent_uid_list)
                insert_succeeded = True

            if not insert_succeeded:
                logger.info(f'[{self.name}] InsertOGNode: failed to enqueue: {new_og_node}')
                return False

            # Always add to node_queue_dict:
            node_dict = self._node_ogn_q_dict.get(target_node.device_uid, None)
            if not node_dict:
                node_dict = dict()
                self._node_ogn_q_dict[target_node.device_uid] = node_dict
            pending_op_queue = node_dict.get(target_node.uid)
            if not pending_op_queue:
                pending_op_queue = collections.deque()
                node_dict[target_node.uid] = pending_op_queue
            pending_op_queue.append(new_og_node)

            if self._max_added_op_uid < new_og_node.op.op_uid:
                self._max_added_op_uid = new_og_node.op.op_uid

            if OP_GRAPH_VALIDATE_AFTER_EVERY_INSERTED_OG_NODE:
                self._validate_graph(last_ogn_added=new_og_node)

            # notify consumers there is something to get:
            self._cv_can_get.notifyAll()

        logger.info(f'[{self.name}] InsertOGNode: successfully enqueued: {new_og_node}')

        return True

    # GET logic
    # ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼

    def _is_node_ready(self, op: UserOp, node: Node, node_type_str: str) -> bool:
        node_dict: Dict[UID, Deque[OpGraphNode]] = self._node_ogn_q_dict.get(node.device_uid, None)
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

        for og_node in self.root.get_child_list():
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
            if not self._outstanding_ogn_dict.get(og_node.op.op_uid, None):
                if SUPER_DEBUG_ENABLED:
                    logger.debug(f'TryGet(): inserting node into OutstandingActionsDict: {og_node}')
                self._outstanding_ogn_dict[og_node.op.op_uid] = og_node.op
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

    # POP logic
    # ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼

    def pop_op(self, op: UserOp):
        """Ensure that we were expecting this op to be copmleted, and remove it from the tree."""
        logger.debug(f'[{self.name}] Entered pop_op() for op {op}')

        with self._cv_can_get:
            if not self._outstanding_ogn_dict.pop(op.op_uid, None):
                raise RuntimeError(f'Complated op not found in outstanding op list (action UID {op.op_uid}')

            # I. SRC Node

            # I-1. Remove src node from node dict

            node_dict: Dict[UID, Deque[OpGraphNode]] = self._node_ogn_q_dict.get(op.src_node.device_uid)
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
                self._node_ogn_q_dict.pop(op.src_node.uid, None)

            # I-2. Remove src node from op graph

            # validate it is a child of root
            if not src_op_node.is_child_of_root():
                parent = src_op_node.get_first_parent()
                parent_uid = parent.node_uid if parent else None
                raise RuntimeError(f'Src node for completed op is not a parent of root (instead found parent op node {parent_uid})')

            # unlink from its parent
            self.root.unlink_child(src_op_node)

            # unlink its children also. If the child then has no other parents, it moves up to become child of root
            for child in src_op_node.get_child_list():
                child.unlink_parent(src_op_node)

                if not child.get_parent_list():
                    self.root.link_child(child)
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

                node_dict: Dict[UID, Deque[OpGraphNode]] = self._node_ogn_q_dict.get(op.dst_node.device_uid)
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
                    self._node_ogn_q_dict.pop(op.dst_node.uid, None)

                # II-2. Remove dst node from op graph

                # validate it is a child of root
                if not dst_op_node.is_child_of_root():
                    parent = dst_op_node.get_first_parent()
                    parent_uid = parent.node_uid if parent else None
                    raise RuntimeError(f'Dst node for completed op is not a parent of root (instead found parent {parent_uid})')

                # unlink from its parent
                self.root.unlink_child(dst_op_node)

                # unlink its children also. If the child then has no other parents, it moves up to become child of root
                for child in dst_op_node.get_child_list():
                    child.unlink_parent(dst_op_node)

                    if not child.get_parent_list():
                        self.root.link_child(child)

                # II-3. Delete dst node
                del dst_op_node

            logger.debug(f'[{self.name}] Done with pop_op() for op: {op}')
            self._print_current_state()

            # this may have jostled the tree to make something else free:
            self._cv_can_get.notifyAll()
