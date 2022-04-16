import collections
import logging
from typing import Callable, DefaultDict, Deque, Dict, List

from be.exec.user_op.op_graph import OpGraph, skip_root
from be.exec.user_op.op_graph_node import DstOpNode, OpGraphNode, RmOpNode, RootNode, SrcOpNode
from constants import is_root, NULL_UID
from logging_constants import SUPER_DEBUG_ENABLED
from model.node.node import TNode
from model.node_identifier import DN_UID
from model.uid import UID
from model.user_op import Batch, UserOp, UserOpCode

logger = logging.getLogger(__name__)


class BatchGraphBuilder:
    """
    ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
    CLASS BatchGraphBuilder
    Support class for OpManager.
    For reducing and validating a batch of UserOps, and generating a standalone OpGraph from them."""
    def __init__(self, backend):
        self.backend = backend

    @staticmethod
    def get_all_nodes_in_batch(batch_op_list: List[UserOp]) -> List[TNode]:
        big_node_list: List[TNode] = []
        for user_op in batch_op_list:
            big_node_list.append(user_op.src_node)
            if user_op.has_dst():
                big_node_list.append(user_op.dst_node)
        return big_node_list

    # Validation & Reduction logic
    # ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼

    @staticmethod
    def _derive_dst_parent_key_list(dst_node: TNode) -> List[str]:
        if not dst_node.get_parent_uids():
            raise RuntimeError(f'Node has no parents: {dst_node}')
        return [f'{dst_node.device_uid}:{parent_uid}/{dst_node.name}' for parent_uid in dst_node.get_parent_uids()]

    def preprocess_batch(self, batch: Batch) -> Batch:
        """Pre=process step before building an OpGraph from the given ops. Validates each op and removes redundencies"""
        final_list: List[UserOp] = []

        # Put all affected nodes in map.
        # Is there a hit? Yes == there is overlap
        mkdir_dict: Dict[UID, UserOp] = {}
        rm_dict: Dict[UID, UserOp] = {}
        # Uses _derive_dst_parent_key_list() to make key:
        dst_op_dict: Dict[str, UserOp] = {}
        # src node is not necessarily mutually exclusive:
        src_op_dict: DefaultDict[UID, List[UserOp]] = collections.defaultdict(lambda: list())
        count_ops_orig = 0

        batch_uid: UID = batch.batch_uid

        op_list = batch.op_list
        op_list.sort(key=lambda _op: _op.op_uid)

        for op in op_list:
            # if SUPER_DEBUG_ENABLED:
            logger.debug(f'ReduceChanges(): examining op: {op}')

            if op.batch_uid != batch_uid:
                raise RuntimeError(f'Changes in batch do not all contain the same batch_uid (found {op.batch_uid} and {batch_uid})')

            count_ops_orig += 1
            if op.op_type == UserOpCode.MKDIR:
                # remove dup MKDIRs (easy)
                if mkdir_dict.get(op.src_node.uid, None):
                    logger.warning(f'ReduceChanges(): Removing duplicate MKDIR for node: {op.src_node}')
                else:
                    logger.info(f'ReduceChanges(): Adding MKDIR-type: {op}')
                    final_list.append(op)
                    mkdir_dict[op.src_node.uid] = op
            elif op.op_type == UserOpCode.RM:
                # remove dups
                if rm_dict.get(op.src_node.uid, None):
                    logger.warning(f'ReduceChanges(): Removing duplicate RM for node: {op.src_node}')
                else:
                    logger.info(f'ReduceChanges(): Adding RM-type: {op}')
                    final_list.append(op)
                    rm_dict[op.src_node.uid] = op
            elif op.has_dst():
                # Check binary ops for consistency.

                # GDrive nodes' UIDs are derived from their goog_ids; nodes with no goog_id can have different UIDs.
                # So for GDrive nodes with no goog_id, we must rely on a combination of their parent UID and name to check for uniqueness
                for dst_parent_key in self._derive_dst_parent_key_list(op.dst_node):
                    if SUPER_DEBUG_ENABLED:
                        logger.debug(f'Checking parent key: "{dst_parent_key}"')
                    existing_op = dst_op_dict.get(dst_parent_key, None)
                    if existing_op:
                        # note: looks like this opens up a hole where ops could theoretically slip through. I deem not worth it at this point
                        if not self._are_equivalent(existing_op.op_type, op.op_type):
                            logger.error(f'ReduceChanges(): Conflict: Change1: {existing_op}; Change2: {op}')
                            raise RuntimeError(f'Batch op conflict: trying to copy different op types into the same destination!')

                        # It is an error for anything but an exact duplicate to share the same dst node; if duplicate, then discard
                        if existing_op.src_node.uid != op.src_node.uid:
                            logger.error(f'ReduceChanges(): Conflict: Change1: {existing_op}; Change2: {op}')
                            raise RuntimeError(f'Batch op conflict: trying to copy different nodes into the same destination!')

                        if op.dst_node.uid != existing_op.dst_node.uid:
                            # GDrive nodes almost certainly
                            raise RuntimeError(f'Batch op conflict: trying to copy same node into the same destination with a different UID!')

                        # fall through:

                    logger.debug(f'ReduceChanges(): Adding binary op: {op}')
                    src_op_dict[op.src_node.uid].append(op)
                    dst_op_dict[dst_parent_key] = op
                    final_list.append(op)
            else:
                assert False, f'Unrecognized op type: {op}'

        logger.debug(f'Reduced {count_ops_orig} ops to {len(final_list)} ops')

        # Validation begin

        def validate_rm_ancestor_func(op_arg: UserOp, ancestor: TNode) -> None:
            conflict = mkdir_dict.get(ancestor.uid, None)
            if conflict:
                logger.error(f'ReduceChanges(): Conflict! Op1={conflict}; Op2={op_arg}')
                raise RuntimeError(f'Batch op conflict: trying to create a node and remove its descendant at the same time!')

        def validate_mkdir_ancestor_func(op_arg: UserOp, ancestor: TNode) -> None:
            conflict = rm_dict.get(ancestor.uid, None)
            if conflict:
                logger.error(f'ReduceChanges(): Conflict! Op1={conflict}; Op2={op_arg}')
                raise RuntimeError(f'Batch op conflict: trying to remove a node and create its descendant at the same time!')

        def validate_cp_src_ancestor_func(op_arg: UserOp, ancestor: TNode) -> None:
            if SUPER_DEBUG_ENABLED:
                logger.debug(f'Validating src ancestor (UserOp={op_arg.op_uid}): {ancestor}')
            if ancestor.uid in mkdir_dict:
                raise RuntimeError(f'Batch op conflict: copy from a descendant of a node being created! (anscestor: {ancestor.node_identifier})')
            rm_op = rm_dict.get(ancestor.uid, None)
            if rm_op and rm_op.op_uid < op_arg.op_uid:
                # we allow a delete of src node AFTER the move, but not before
                raise RuntimeError(f'Batch op conflict: copy from a descendant of a node which is deleted! (anscestor: {ancestor.node_identifier})')
            if ancestor.uid in dst_op_dict:
                raise RuntimeError(f'Batch op conflict: copy from a descendant of a node being copied to! (anscestor: {ancestor.node_identifier})')

        def validate_cp_dst_ancestor_func(op_arg: UserOp, ancestor: TNode) -> None:
            if SUPER_DEBUG_ENABLED:
                logger.debug(f'Validating dst ancestor (op={op.op_uid}): {ancestor}')
            if ancestor.uid in rm_dict:
                raise RuntimeError(f'Batch op conflict: copy to a descendant of a node being deleted! (anscestor: {ancestor.node_identifier})')
            if ancestor.uid in src_op_dict:
                raise RuntimeError(f'Batch op conflict: copy to a descendant of a node being copied from! (anscestor: {ancestor.node_identifier})')

        # For each element, traverse up the tree and compare each parent node to map
        for op in final_list:
            if SUPER_DEBUG_ENABLED:
                logger.debug(f'ReduceChanges(): Evaluating {op}')

            if op.op_type == UserOpCode.RM:
                self._check_ancestors(op, op.src_node, validate_rm_ancestor_func)
            elif op.op_type == UserOpCode.MKDIR:
                self._check_ancestors(op, op.src_node, validate_mkdir_ancestor_func)
            elif op.op_type == UserOpCode.CP or op.op_type == UserOpCode.CP_ONTO or op.op_type == UserOpCode.MV or op.op_type == UserOpCode.MV_ONTO:
                """Checks all ancestors of both src and dst for mapped Ops. The following are the only valid situations:
                 1. No ancestors of src or dst correspond to any Ops.
                 2. Ancestor(s) of the src node correspond to the src node of a CP or UP action (i.e. they will not change)
                 """
                self._check_ancestors(op, op.src_node, validate_cp_src_ancestor_func)
                self._check_ancestors(op, op.dst_node, validate_cp_dst_ancestor_func)

        # Sort by ascending op_uid
        batch.op_list = sorted(final_list, key=lambda _op: _op.op_uid)
        return batch

    @staticmethod
    def _are_equivalent(prev_op_type: UserOpCode, current_op_type: UserOpCode) -> bool:
        if (prev_op_type == UserOpCode.START_DIR_CP or prev_op_type == UserOpCode.FINISH_DIR_CP) and \
                (current_op_type == UserOpCode.START_DIR_CP or current_op_type == UserOpCode.FINISH_DIR_CP):
            return True
        if (prev_op_type == UserOpCode.START_DIR_MV or prev_op_type == UserOpCode.FINISH_DIR_MV) and \
                (current_op_type == UserOpCode.START_DIR_MV or current_op_type == UserOpCode.FINISH_DIR_MV):
            return True

        return prev_op_type == current_op_type

    def _check_ancestors(self, op: UserOp, node: TNode, eval_func: Callable[[UserOp, TNode], None]):
        queue: Deque[TNode] = collections.deque()
        queue.append(node)

        while len(queue) > 0:
            popped_node: TNode = queue.popleft()
            for ancestor in self.backend.cacheman.get_parent_list_for_node(popped_node):
                queue.append(ancestor)
                eval_func(op, ancestor)

    # ▲ ▲ ▲ ▲ ▲ ▲ ▲ ▲ ▲ ▲ ▲ ▲ ▲ ▲ ▲
    # Validation & Reduction logic

    def build_batch_graph(self, op_batch: List[UserOp], op_manager) -> RootNode:
        batch_uid = op_batch[0].batch_uid
        logger.debug(f'[Batch-{batch_uid}] Building OpGraph for {len(op_batch)} ops...')

        batch_graph = OpGraph(f'Batch-{batch_uid}')

        # Verify batch is properly sorted first:
        last_op_uid = 0
        for op in op_batch:
            assert op.batch_uid == batch_uid, f'Op is not a part of batch {batch_uid}: {op}'
            # Changes MUST be sorted in ascending time of creation!
            if op.op_uid < last_op_uid:
                op_batch_str = '\n\t'.join([f'{x.op_uid}={repr(x)}' for x in op_batch])
                logger.error(f'[{batch_graph.name}] OpBatch:\n\t {op_batch_str}')
                raise RuntimeError(f'Batch items are not in order! ({op.op_uid} < {last_op_uid})')
            last_op_uid = op.op_uid

            if SUPER_DEBUG_ENABLED:
                logger.debug(f'[{batch_graph.name}] Found op: {op}')

        # Some ancestors may not be in cacheman, because they are being added by the batch.
        # so, store the batch nodes in a dict for additional lookup. Build the dict beforehand; order of ops is unspecified
        tgt_node_dict = self._build_tgt_node_dict(op_batch)

        for op in op_batch:
            self._insert_for_op(op, batch_graph, tgt_node_dict)

        lines = batch_graph.root.print_recursively()
        # note: GDrive paths may not be present at this point; this is ok.
        logger.debug(f'[Batch-{batch_uid}] BuildBatchGraph: constructed graph with {len(lines)} OGNs:')
        for line in lines:
            logger.debug(f'[Batch-{batch_uid}] {line}')

        # Validate OGN graph is structurally consistent:
        batch_graph.validate_internal_consistency()

        # Reconcile ops against master op tree before adding nodes. This will raise an exception if invalid
        self._validate_batch_graph_against_cache(batch_graph, op_manager)

        return batch_graph.root

    def _build_tgt_node_dict(self, op_batch: List[UserOp]) -> Dict[UID, Dict[UID, TNode]]:
        tgt_node_dict: Dict[UID, Dict[UID, TNode]] = {}
        for op in op_batch:
            self._add_tgt_node_to_dict(op.src_node, tgt_node_dict)
            if op.has_dst():
                self._add_tgt_node_to_dict(op.dst_node, tgt_node_dict)
        return tgt_node_dict

    @staticmethod
    def _add_tgt_node_to_dict(tgt_node, tgt_node_dict):
        device_tgt_node_dict: Dict[UID, TNode] = tgt_node_dict.get(tgt_node.device_uid)
        if not device_tgt_node_dict:
            device_tgt_node_dict = {}
            tgt_node_dict[tgt_node.device_uid] = device_tgt_node_dict
        device_tgt_node_dict[tgt_node.uid] = tgt_node

    def _insert_for_op(self, op: UserOp, graph: OpGraph, tgt_node_dict):
        if SUPER_DEBUG_ENABLED:
            logger.debug(f'[{graph.name}] _insert_for_op() entered for: {op}')

        # 1a. Build src OGN:
        ancestor_uid_list = self._build_ancestor_uid_list(op.src_node, tgt_node_dict)
        if op.op_type == UserOpCode.RM:
            src_ogn: OpGraphNode = RmOpNode(self.backend.uid_generator.next_uid(), op, ancestor_uid_list)
        else:
            src_ogn: OpGraphNode = SrcOpNode(self.backend.uid_generator.next_uid(), op, ancestor_uid_list)

        # 1b. Insert src OGN:
        graph.insert_ogn(src_ogn)

        # 2a. Build dst OGN (if op has dst):
        if op.has_dst():
            ancestor_uid_list = self._build_ancestor_uid_list(op.dst_node, tgt_node_dict)
            dst_node = DstOpNode(self.backend.uid_generator.next_uid(), op, ancestor_uid_list)
            # 2b. Insert dst OGN:
            graph.insert_ogn(dst_node)

    def _build_ancestor_uid_list(self, tgt_node: TNode, tgt_node_dict: Dict[UID, Dict[UID, TNode]]) -> List[UID]:
        if SUPER_DEBUG_ENABLED:
            logger.debug(f'Building ancestor UID list for {tgt_node.node_identifier}')

        ancestor_list: List[UID] = []
        queue = collections.deque()
        queue.append(tgt_node)

        # store tgt node in dict for possible later lookup
        device_tgt_node_dict: Dict[UID, TNode] = tgt_node_dict.get(tgt_node.device_uid)
        if not device_tgt_node_dict:
            raise RuntimeError(f'Tgt node device_id is not in tgt_dict! (tgt_node={tgt_node.node_identifier})')
        while len(queue) > 0:
            node = queue.popleft()
            if is_root(node.uid):
                logger.debug(f'Node is root; stopping: {node.node_identifier}')
                break
            parent_uid_list = node.get_parent_uids()
            if not parent_uid_list:
                raise RuntimeError(f'Node has no parent UIDs listed: {node}')

            if SUPER_DEBUG_ENABLED:
                logger.debug(f'TNode {node.node_identifier} has parent_uid_list: {parent_uid_list}')

            for parent_uid in parent_uid_list:
                ancestor_list.append(parent_uid)
                parent_node = self.backend.cacheman.get_node_for_uid(uid=parent_uid, device_uid=node.device_uid)
                if not parent_node:
                    parent_node = device_tgt_node_dict.get(parent_uid)
                    if not parent_node:
                        raise RuntimeError(f'Failed to find ancestor node {node.device_uid}:{parent_uid} in cacheman or in tgt dict'
                                           f' (for tgt node: {node.node_identifier})')
                queue.append(parent_node)

        if not ancestor_list:
            raise RuntimeError(f'No ancestors for target node: {tgt_node}')
        return ancestor_list

    def _validate_batch_graph_against_cache(self, batch_graph: OpGraph, op_manager):
        """
        Takes an OpGraph representing a monolithic batch as an arg. The root itself is ignored, but each of its children represent the root of a
        subgraph of changes, in which each node of the subgraph maps to a node in a directory tree. No intermediate OpGraph nodes are allowed to be
        omitted from a subgraph (e.g. if A is the parent of B which is the parent of C, you cannot copy A and C but exclude B).

        Rules:
        1. Parent for MKDIR_SRC and all DST nodes must be present in memstore and not already scheduled for RM
        2. Except for MKDIR, SRC nodes must all be present in memstore (OK if is_live==False due to pending operation)
        and not already scheduled for RM
        """

        logger.debug(f'[{batch_graph.name}] Validating OpGraph of batch against central cache...')

        ogn_root = batch_graph.root
        assert isinstance(ogn_root, RootNode)
        assert ogn_root.get_child_list(), f'no ops in batch!'

        # Invert RM nodes when inserting into tree
        batch_uid: UID = ogn_root.get_first_child().op.batch_uid

        mkdir_node_dict: Dict[DN_UID, TNode] = {}
        """Keep track of nodes which are to be created, so we can include them in the lookup for valid parents"""

        # For matching pairs of START & FINISH for dir mv/cp:
        start_x_dst_node_dict: Dict[DN_UID, TNode] = {}
        finish_x_dst_node_dict: Dict[DN_UID, TNode] = {}

        min_op_uid: UID = ogn_root.get_first_child().op.op_uid

        for ogn in skip_root(ogn_root.get_subgraph_bfs_list()):
            tgt_node: TNode = ogn.get_tgt_node()
            op_type_name: str = ogn.op.op_type.name
            if min_op_uid > ogn.op.op_uid:
                min_op_uid = ogn.op.op_uid

            logger.debug(f'[{batch_graph.name}] ValidateOpGraphVsCache: checking OGN: {ogn}')

            if ogn.is_create_type():
                # Enforce Rule 1: ensure parent of target is valid:
                tgt_parent_uid_list: List[UID] = tgt_node.get_parent_uids()
                parent_found: bool = False
                for tgt_parent_uid in tgt_parent_uid_list:
                    tgt_parent_dn_uid = TNode.format_dn_uid(tgt_node.device_uid, tgt_parent_uid)
                    if self.backend.cacheman.get_node_for_uid(tgt_parent_uid, tgt_node.device_uid) \
                            or mkdir_node_dict.get(tgt_parent_dn_uid, None) \
                            or start_x_dst_node_dict.get(tgt_parent_dn_uid, None) \
                            or finish_x_dst_node_dict.get(tgt_parent_dn_uid, None):
                        parent_found = True

                if not parent_found:
                    logger.error(f'Could not find parent(s) in cache with device_uid {tgt_node.device_uid} & UID(s) {tgt_parent_uid_list} '
                                 f'for "{op_type_name}" operation node: {tgt_node}')
                    raise RuntimeError(f'Could not find parent(s) in cache with device_uid {tgt_node.device_uid} & UIDs {tgt_parent_uid_list}'
                                       f' for "{op_type_name}"')

                if tgt_node.is_dir():
                    # Track dir creations in appropriate maps and check for redundancies
                    op_type = ogn.op.op_type

                    exist_mkdir = mkdir_node_dict.get(ogn.op.src_node.dn_uid, None)
                    exist_start_dir = start_x_dst_node_dict.get(ogn.op.src_node.dn_uid, None)
                    exist_finish_dir = finish_x_dst_node_dict.get(ogn.op.src_node.dn_uid, None)

                    if exist_mkdir:
                        raise RuntimeError(f'Batch has redundant operations for: {ogn.op.src_node} (MKDIR and {op_type_name})')
                    if exist_start_dir and not (op_type == UserOpCode.FINISH_DIR_CP or op_type == UserOpCode.FINISH_DIR_MV):
                        raise RuntimeError(f'Batch has redundant operations for: {ogn.op.src_node} (START_DIR_* and {op_type_name})')
                    if exist_finish_dir and not (op_type == UserOpCode.START_DIR_CP or op_type == UserOpCode.START_DIR_MV):
                        raise RuntimeError(f'Batch has redundant operations for: {ogn.op.src_node} (FINISH_DIR_* and {op_type_name})')

                    if op_type == UserOpCode.MKDIR:
                        mkdir_node_dict[tgt_node.dn_uid] = tgt_node
                    elif op_type == UserOpCode.START_DIR_CP or op_type == UserOpCode.START_DIR_MV:
                        assert ogn.is_dst(), f'Expected DstOGN: {ogn}'
                        start_x_dst_node_dict[tgt_node.dn_uid] = tgt_node
                    elif op_type == UserOpCode.FINISH_DIR_CP or op_type == UserOpCode.FINISH_DIR_MV:
                        assert ogn.is_dst(), f'Expected DstOGN: {ogn}'
                        finish_x_dst_node_dict[tgt_node.dn_uid] = tgt_node

            elif not ogn.op.is_completed():
                # Enforce Rule 2: ensure target node is valid (unless op already completed, in which case we don't care)
                if not self.backend.cacheman.get_node_for_uid(tgt_node.uid, tgt_node.device_uid):
                    logger.error(f'Could not find node in cache for "{op_type_name}" operation node: {tgt_node}')
                    raise RuntimeError(f'Cannot add batch (UID={batch_uid}): no node {tgt_node.dn_uid} in cache for "{op_type_name}"')

            # More of Rule 2: ensure target node is not scheduled for deletion:
            most_recent_op = op_manager.get_last_pending_op_for_node(tgt_node.device_uid, tgt_node.uid)
            if most_recent_op and most_recent_op.op_type == UserOpCode.RM and ogn.is_src() and ogn.op.has_dst():
                # CP, MV, and UP ops cannot logically have a src node which is not present:
                raise RuntimeError(f'Invalid operation: attempting to do a {ogn.op.op_type} '
                                   f'from a node which is being removed ({tgt_node.dn_uid}, "{tgt_node.name}")')

        # The OpGraph keeps track of the largest op UID it has added to its graph.
        # Refuse to create commands if the UIDs are too small, as a sanity check against running commands repeatedly
        max_added_op_uid = op_manager.get_max_added_op_uid()
        if max_added_op_uid != NULL_UID and max_added_op_uid >= min_op_uid:
            raise RuntimeError(f'Batch appears to contain operation(s) which which are older than those already submitted '
                               f'(op_uid={min_op_uid} > {max_added_op_uid})')
