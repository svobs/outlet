import collections
import logging
from typing import Callable, DefaultDict, Deque, Dict, List

from backend.executor.user_op.op_graph import OpGraph, skip_root
from backend.executor.user_op.op_graph_node import DstOpNode, OpGraphNode, RmOpNode, RootNode, SrcOpNode
from constants import NULL_UID, SUPER_DEBUG_ENABLED
from model.node.node import Node
from model.node_identifier import DN_UID
from model.uid import UID
from model.user_op import UserOp, UserOpType

logger = logging.getLogger(__name__)


class BatchBuilder:
    """Support class for OpManager. For reducing and validating a batch of UserOps, and generating a detached OpGraph from them."""
    def __init__(self, backend):
        self.backend = backend

    @staticmethod
    def get_all_nodes_in_batch(batch_op_list: List[UserOp]) -> List[Node]:
        big_node_list: List[Node] = []
        for user_op in batch_op_list:
            big_node_list.append(user_op.src_node)
            if user_op.has_dst():
                big_node_list.append(user_op.dst_node)
        return big_node_list

    # Reduce Changes logic
    # ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼

    @staticmethod
    def _derive_dst_parent_key_list(dst_node: Node) -> List[str]:
        if not dst_node.get_parent_uids():
            raise RuntimeError(f'Node has no parents: {dst_node}')
        return [f'{dst_node.device_uid}:{parent_uid}/{dst_node.name}' for parent_uid in dst_node.get_parent_uids()]

    def reduce_and_validate_ops(self, op_list: List[UserOp]) -> List[UserOp]:
        final_list: List[UserOp] = []

        # Put all affected nodes in map.
        # Is there a hit? Yes == there is overlap
        mkdir_dict: Dict[UID, UserOp] = {}
        rm_dict: Dict[UID, UserOp] = {}
        # Uses _derive_cp_dst_key() to make key:
        cp_dst_dict: Dict[str, UserOp] = {}
        # src node is not necessarily mutually exclusive:
        cp_src_dict: DefaultDict[UID, List[UserOp]] = collections.defaultdict(lambda: list())
        count_ops_orig = 0

        batch_uid: UID = op_list[0].batch_uid

        op_list.sort(key=lambda _op: _op.op_uid)

        for op in op_list:
            # if SUPER_DEBUG_ENABLED:
            logger.debug(f'ReduceChanges(): examining op: {op}')

            if op.batch_uid != batch_uid:
                raise RuntimeError(f'Changes in batch do not all contain the same batch_uid (found {op.batch_uid} and {batch_uid})')

            count_ops_orig += 1
            if op.op_type == UserOpType.MKDIR:
                # remove dup MKDIRs (easy)
                if mkdir_dict.get(op.src_node.uid, None):
                    logger.warning(f'ReduceChanges(): Removing duplicate MKDIR for node: {op.src_node}')
                else:
                    logger.info(f'ReduceChanges(): Adding MKDIR-type: {op}')
                    final_list.append(op)
                    mkdir_dict[op.src_node.uid] = op
            elif op.op_type == UserOpType.RM:
                # remove dups
                if rm_dict.get(op.src_node.uid, None):
                    logger.warning(f'ReduceChanges(): Removing duplicate RM for node: {op.src_node}')
                else:
                    logger.info(f'ReduceChanges(): Adding RM-type: {op}')
                    final_list.append(op)
                    rm_dict[op.src_node.uid] = op
            elif op.has_dst():
                # GDrive nodes' UIDs are derived from their goog_ids; nodes with no goog_id can have different UIDs.
                # So for GDrive nodes with no goog_id, we must rely on a combination of their parent UID and name to check for uniqueness
                for dst_parent_key in self._derive_dst_parent_key_list(op.dst_node):
                    if SUPER_DEBUG_ENABLED:
                        logger.debug(f'Checking parent key: {dst_parent_key}')
                    existing = cp_dst_dict.get(dst_parent_key, None)
                    if existing:
                        # It is an error for anything but an exact duplicate to share the same dst node; if duplicate, then discard
                        if existing.src_node.uid != op.src_node.uid:
                            logger.error(f'ReduceChanges(): Conflict: Change1: {existing}; Change2: {op}')
                            raise RuntimeError(f'Batch op conflict: trying to copy different nodes into the same destination!')
                        elif existing.op_type != op.op_type:
                            logger.error(f'ReduceChanges(): Conflict: Change1: {existing}; Change2: {op}')
                            raise RuntimeError(f'Batch op conflict: trying to copy different op types into the same destination!')
                        elif op.dst_node.uid != existing.dst_node.uid:
                            # GDrive nodes almost certainly
                            raise RuntimeError(f'Batch op conflict: trying to copy same node into the same destination with a different UID!')
                        else:
                            assert op.dst_node.uid == existing.dst_node.uid and existing.src_node.uid == op.src_node.uid and \
                                   existing.op_type == op.op_type, f'Conflict: Change1: {existing}; Change2: {op}'
                            logger.info(f'ReduceChanges(): Discarding op (dup dst): {op}')
                    else:
                        logger.info(f'ReduceChanges(): Adding CP-like type: {op}')
                        cp_src_dict[op.src_node.uid].append(op)
                        cp_dst_dict[dst_parent_key] = op
                        final_list.append(op)
            else:
                assert False, f'Unrecognized op type: {op}'

        logger.debug(f'Reduced {count_ops_orig} ops to {len(final_list)} ops')

        # Validation begin

        def validate_rm_ancestor_func(op_arg: UserOp, ancestor: Node) -> None:
            conflict = mkdir_dict.get(ancestor.uid, None)
            if conflict:
                logger.error(f'ReduceChanges(): Conflict! Op1={conflict}; Op2={op_arg}')
                raise RuntimeError(f'Batch op conflict: trying to create a node and remove its descendant at the same time!')

        def validate_mkdir_ancestor_func(op_arg: UserOp, ancestor: Node) -> None:
            conflict = rm_dict.get(ancestor.uid, None)
            if conflict:
                logger.error(f'ReduceChanges(): Conflict! Op1={conflict}; Op2={op_arg}')
                raise RuntimeError(f'Batch op conflict: trying to remove a node and create its descendant at the same time!')

        def validate_cp_src_ancestor_func(op_arg: UserOp, ancestor: Node) -> None:
            if SUPER_DEBUG_ENABLED:
                logger.debug(f'Validating src ancestor (UserOp={op_arg.op_uid}): {ancestor}')
            if ancestor.uid in mkdir_dict:
                raise RuntimeError(f'Batch op conflict: copy from a descendant of a node being created! (anscestor: {ancestor.node_identifier})')
            rm_op = rm_dict.get(ancestor.uid, None)
            if rm_op and rm_op.op_uid < op_arg.op_uid:
                # we allow a delete of src node AFTER the move, but not before
                raise RuntimeError(f'Batch op conflict: copy from a descendant of a node which is deleted! (anscestor: {ancestor.node_identifier})')
            if ancestor.uid in cp_dst_dict:
                raise RuntimeError(f'Batch op conflict: copy from a descendant of a node being copied to! (anscestor: {ancestor.node_identifier})')

        def validate_cp_dst_ancestor_func(op_arg: UserOp, ancestor: Node) -> None:
            if SUPER_DEBUG_ENABLED:
                logger.debug(f'Validating dst ancestor (op={op.op_uid}): {ancestor}')
            if ancestor.uid in rm_dict:
                raise RuntimeError(f'Batch op conflict: copy to a descendant of a node being deleted! (anscestor: {ancestor.node_identifier})')
            if ancestor.uid in cp_src_dict:
                raise RuntimeError(f'Batch op conflict: copy to a descendant of a node being copied from! (anscestor: {ancestor.node_identifier})')

        # For each element, traverse up the tree and compare each parent node to map
        for op in final_list:
            if SUPER_DEBUG_ENABLED:
                logger.debug(f'_reduce_ops(): Evaluating {op}')
            if op.op_type == UserOpType.RM:
                self._check_ancestors(op, op.src_node, validate_rm_ancestor_func)
            elif op.op_type == UserOpType.MKDIR:
                self._check_ancestors(op, op.src_node, validate_mkdir_ancestor_func)
            elif op.op_type == UserOpType.CP or op.op_type == UserOpType.CP_ONTO or op.op_type == UserOpType.MV or op.op_type == UserOpType.MV_ONTO:
                """Checks all ancestors of both src and dst for mapped Ops. The following are the only valid situations:
                 1. No ancestors of src or dst correspond to any Ops.
                 2. Ancestor(s) of the src node correspond to the src node of a CP or UP action (i.e. they will not change)
                 """
                self._check_ancestors(op, op.src_node, validate_cp_src_ancestor_func)
                self._check_ancestors(op, op.dst_node, validate_cp_dst_ancestor_func)

        # Sort by ascending op_uid
        return sorted(final_list, key=lambda _op: _op.op_uid)

    def _check_ancestors(self, op: UserOp, node: Node, eval_func: Callable[[UserOp, Node], None]):
        queue: Deque[Node] = collections.deque()
        queue.append(node)

        while len(queue) > 0:
            popped_node: Node = queue.popleft()
            for ancestor in self.backend.cacheman.get_parent_list_for_node(popped_node):
                queue.append(ancestor)
                eval_func(op, ancestor)

    # ▲ ▲ ▲ ▲ ▲ ▲ ▲ ▲ ▲ ▲ ▲ ▲ ▲ ▲ ▲
    # Reduce Changes logic

    def build_batch_graph(self, op_batch: List[UserOp]) -> RootNode:
        batch_uid = op_batch[0].batch_uid
        logger.debug(f'Building OpGraph for UserOp batch uid={batch_uid} ({len(op_batch)} ops)...')

        # Verify batch is properly sorted first:
        last_op_uid = 0
        for op in op_batch:
            assert op.batch_uid == batch_uid, f'Op is not a part of batch {batch_uid}: {op}'
            # Changes MUST be sorted in ascending time of creation!
            if op.op_uid < last_op_uid:
                op_batch_str = '\n\t'.join([f'{x.op_uid}={repr(x)}' for x in op_batch])
                logger.error(f'OpBatch:\n\t {op_batch_str}')
                raise RuntimeError(f'Batch items are not in order! ({op.op_uid} < {last_op_uid})')
            last_op_uid = op.op_uid

        batch_graph = OpGraph(f'BatchGraph-{batch_uid}')
        for op in op_batch:
            self.insert_for_op(op, batch_graph)

        lines = batch_graph.root.print_recursively()
        # note: GDrive paths may not be present at this point; this is ok.
        logger.debug(f'[Batch-{batch_uid}] MakeTreeToInsert: constructed tree with {len(lines)} items:')
        for line in lines:
            logger.debug(f'[Batch-{batch_uid}] {line}')
        return batch_graph.root

    def insert_for_op(self, op: UserOp, graph: OpGraph):
        # make src node
        if op.op_type == UserOpType.RM:
            src_node: OpGraphNode = RmOpNode(self.backend.uid_generator.next_uid(), op)
        else:
            src_node: OpGraphNode = SrcOpNode(self.backend.uid_generator.next_uid(), op)

        graph.enqueue_single_og_node(src_node)

        # make dst node (if op has dst)
        if op.has_dst():
            dst_node = DstOpNode(self.backend.uid_generator.next_uid(), op)
            graph.enqueue_single_og_node(dst_node)

    def validate_batch_graph(self, op_root: RootNode, op_manager):
        """
        Takes a tree representing a batch as an arg. The root itself is ignored, but each of its children represent the root of a
        subtree of changes, in which each node of the subtree maps to a node in a directory tree. No intermediate nodes are allowed to be
        omitted from a subtree (e.g. if A is the parent of B which is the parent of C, you cannot copy A and C but exclude B).

        Rules:
        1. Parent for MKDIR_SRC and all DST nodes must be present in memstore and not already scheduled for RM
        2. Except for MKDIR, SRC nodes must all be present in memstore (OK if is_live==False due to pending operation)
        and not already scheduled for RM
        """

        assert isinstance(op_root, RootNode)
        assert op_root.get_child_list(), f'no ops in batch!'

        # Invert RM nodes when inserting into tree
        batch_uid: UID = op_root.get_first_child().op.batch_uid

        mkdir_node_dict: Dict[DN_UID] = {}
        """Keep track of nodes which are to be created, so we can include them in the lookup for valid parents"""

        min_op_uid: UID = op_root.get_first_child().op.op_uid

        for op_node in skip_root(op_root.get_subgraph_bfs_list()):
            tgt_node: Node = op_node.get_tgt_node()
            op_type: str = op_node.op.op_type.name
            if min_op_uid > op_node.op.op_uid:
                min_op_uid = op_node.op.op_uid

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

            # More of Rule 2: ensure target node is not scheduled for deletion:
            most_recent_op = op_manager.get_last_pending_op_for_node(tgt_node.device_uid, tgt_node.uid)
            if most_recent_op and most_recent_op.op_type == UserOpType.RM and op_node.is_src() and op_node.op.has_dst():
                # CP, MV, and UP ops cannot logically have a src node which is not present:
                raise RuntimeError(f'Cannot add batch (UID={batch_uid}): it is attempting to CP/MV/UP from a node ({tgt_node.dn_uid}) '
                                   f'which is being removed')

        # The OpGraph keeps track of the largest op UID it has added to its graph.
        # Refuse to create commands if the UIDs are too small, as a sanity check against running commands repeatedly
        max_added_op_uid = op_manager.get_max_added_op_uid()
        if max_added_op_uid != NULL_UID and max_added_op_uid >= min_op_uid:
            raise RuntimeError(f'Cannot add batch: it appears to contain operation(s) which which are older than those already processed '
                               f'(op_uid={min_op_uid} > {max_added_op_uid})')
