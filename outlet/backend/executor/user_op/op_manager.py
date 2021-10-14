import collections
import logging
from collections import defaultdict
from enum import IntEnum
from typing import Callable, DefaultDict, Deque, Dict, Iterable, List, Optional

from backend.executor.central import ExecPriority
from backend.executor.user_op.op_graph import OpGraph
from constants import IconId, SUPER_DEBUG_ENABLED, TRACE_ENABLED
from backend.executor.command.cmd_builder import CommandBuilder
from backend.executor.command.cmd_interface import Command
from backend.executor.user_op.op_disk_store import OpDiskStore
from backend.executor.user_op.op_graph_node import RootNode
from model.node.node import Node
from model.user_op import OpTypeMeta, UserOp, UserOpType
from model.uid import UID
from util.has_lifecycle import HasLifecycle
from util.task_runner import Task

logger = logging.getLogger(__name__)


class ErrorHandlingBehavior(IntEnum):
    """
    ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
    ENUM FailureBehavior
    ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼
    """
    RAISE_ERROR = 1
    IGNORE = 2
    DISCARD = 3


class OpManager(HasLifecycle):
    """
    ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
    CLASS OpManager
    ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼
    """

    def __init__(self, backend, op_db_path):
        HasLifecycle.__init__(self)
        self.backend = backend
        self._cmd_builder: CommandBuilder = CommandBuilder(self.backend.uid_generator)
        self._disk_store: OpDiskStore = OpDiskStore(self.backend, op_db_path=op_db_path)
        self._op_graph: OpGraph = OpGraph(self.backend)
        """Present and future batches, kept in insertion order. Each batch is removed after it is completed."""

    def start(self):
        logger.debug(f'Starting OpManager')
        HasLifecycle.start(self)
        self._disk_store.start()
        self._op_graph.start()

    def shutdown(self):
        logger.debug(f'Shutting down OpManager')
        HasLifecycle.shutdown(self)

        if self._disk_store:
            self._disk_store.shutdown()
            self._disk_store = None
        if self._op_graph:
            self._op_graph.shutdown()
            self._op_graph = None

        self.backend = None
        self._cmd_builder = None

    def _upsert_nodes_in_memstore(self, op: UserOp):
        """Looks at the given UserOp and notifies cacheman so that it can send out update notifications. The nodes involved may not have
        actually changed (i.e., only their statuses have changed).

        Note: we update our input nodes with the returned values, as GDrive node paths in particular may be filled in"""
        src_node = self.backend.cacheman.upsert_single_node(op.src_node)
        assert src_node, f'What happened?! {op}'
        op.src_node = src_node
        if op.has_dst():
            dst_node = self.backend.cacheman.upsert_single_node(op.dst_node)
            assert dst_node, f'What happened?! {op}'
            op.dst_node = dst_node

    # Reduce Changes logic
    # ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼

    @staticmethod
    def _derive_dst_parent_key_list(dst_node: Node) -> List[str]:
        if not dst_node.get_parent_uids():
            raise RuntimeError(f'Node has no parents: {dst_node}')
        return [f'{dst_node.device_uid}:{parent_uid}/{dst_node.name}' for parent_uid in dst_node.get_parent_uids()]

    def _reduce_and_validate_ops(self, op_list: Iterable[UserOp]) -> Iterable[UserOp]:
        final_list: List[UserOp] = []

        # Put all affected nodes in map.
        # Is there a hit? Yes == there is overlap
        mkdir_dict: Dict[UID, UserOp] = {}
        rm_dict: Dict[UID, UserOp] = {}
        # Uses _derive_cp_dst_key() to make key:
        cp_dst_dict: Dict[str, UserOp] = {}
        # src node is not necessarily mutually exclusive:
        cp_src_dict: DefaultDict[UID, List[UserOp]] = defaultdict(lambda: list())
        count_ops_orig = 0
        for op in op_list:
            if SUPER_DEBUG_ENABLED:
                logger.debug(f'ReduceChanges(): examining op: {op}')
            count_ops_orig += 1
            if op.op_type == UserOpType.MKDIR:
                # remove dup MKDIRs (easy)
                if mkdir_dict.get(op.src_node.uid, None):
                    logger.info(f'ReduceChanges(): Removing duplicate MKDIR for node: {op.src_node}')
                else:
                    logger.info(f'ReduceChanges(): Adding MKDIR-type: {op}')
                    final_list.append(op)
                    mkdir_dict[op.src_node.uid] = op
            elif op.op_type == UserOpType.RM:
                # remove dups
                if rm_dict.get(op.src_node.uid, None):
                    logger.info(f'ReduceChanges(): Removing duplicate RM for node: {op.src_node}')
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
                raise RuntimeError(f'Batch op conflict: copy from a descendant of a node being created!')
            if ancestor.uid in rm_dict:
                raise RuntimeError(f'Batch op conflict: copy from a descendant of a node being deleted!')
            if ancestor.uid in cp_dst_dict:
                raise RuntimeError(f'Batch op conflict: copy from a descendant of a node being copied to!')

        def validate_cp_dst_ancestor_func(op_arg: UserOp, ancestor: Node) -> None:
            if SUPER_DEBUG_ENABLED:
                logger.debug(f'Validating dst ancestor (op={op.op_uid}): {ancestor}')
            if ancestor.uid in rm_dict:
                raise RuntimeError(f'Batch op conflict: copy to a descendant of a node being deleted!')
            if ancestor.uid in cp_src_dict:
                raise RuntimeError(f'Batch op conflict: copy to a descendant of a node being copied from!')

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

        return final_list

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

    def cancel_all_pending_ops(self, this_task: Task):
        """Call this at startup, to CANCEL pending ops which have not yet been applied (archive them on disk)."""
        self._disk_store.cancel_all_pending_ops()

    def resume_pending_ops_from_disk(self, this_task: Task):
        """Call this at startup, to RESUME pending ops which have not yet been applied."""

        # Load from disk
        op_list: List[UserOp] = self._disk_store.load_all_pending_ops()
        if not op_list:
            logger.debug(f'resume_pending_ops_from_disk(): No pending ops found in the disk cache')
            return

        logger.info(f'resume_pending_ops_from_disk(): Found {len(op_list)} pending ops from the disk cache')

        # Sort into batches
        batch_dict: DefaultDict[UID, List[UserOp]] = defaultdict(lambda: list())
        for op in op_list:
            batch_dict[op.batch_uid].append(op)

        # Sort batches to make sure they are in correct order
        batch_dict_keys = batch_dict.keys()
        logger.info(f'Sorted ops into {len(batch_dict_keys)} batches')
        sorted_keys = sorted(batch_dict_keys)

        for batch_uid in sorted_keys:
            # Assume batch has already been reduced and reconciled against master tree.
            batch_op_list: List[UserOp] = batch_dict[batch_uid]

            big_node_list: List[Node] = self._get_all_nodes(batch_op_list)

            # Make sure all relevant caches are loaded. Do this via executor tasks:
            self.backend.cacheman.ensure_loaded(this_task, big_node_list)

            # launch this with P7_USER_OP_EXECUTION priority so that it executes after the cache load tasks:
            self.backend.executor.submit_async_task(Task(ExecPriority.P7_USER_OP_EXECUTION, self._append_batch, batch_uid, batch_op_list, False))

    @staticmethod
    def _get_all_nodes(batch_op_list: List[UserOp]) -> List[Node]:
        big_node_list: List[Node] = []
        for user_op in batch_op_list:
            big_node_list.append(user_op.src_node)
            if user_op.has_dst():
                big_node_list.append(user_op.dst_node)
        return big_node_list

    def append_new_pending_op_batch(self, batch_op_list: List[UserOp]):
        """
        Call this after the user requests a new set of ops.

         - First store "planning nodes" to the list of cached nodes (but each will have is_live=False until we execute its associated command).
         - The list of to-be-completed ops is also cached on disk.
         - When each command completes, cacheman is notified of any node updates required as well.
         - When batch completes, we archive the ops on disk.
        """
        if not batch_op_list:
            return

        # Validate batch_uid
        batch_uid: UID = self._validate_batch_uid_consistency(batch_op_list)

        logger.debug(f'append_new_pending_op_batch(): Validating batch {batch_uid} with {len(batch_op_list)} ops )')

        # Simplify and remove redundancies in op_list
        reduced_batch: Iterable[UserOp] = self._reduce_and_validate_ops(batch_op_list)

        def get_op_uid(_op):
            return _op.op_uid

        reduced_batch = sorted(reduced_batch, key=get_op_uid)

        self._append_batch(None, batch_uid, reduced_batch, save_to_disk=True)

        logger.debug(f'append_new_pending_op_batch(): Successfully added batch {batch_uid}')

    @staticmethod
    def _validate_batch_uid_consistency(batch_op_list) -> UID:
        op_iter = iter(batch_op_list)
        batch_uid = next(op_iter).batch_uid
        for op in op_iter:
            if op.batch_uid != batch_uid:
                raise RuntimeError(f'Changes in batch do not all contain the same batch_uid (found {op.batch_uid} and {batch_uid})')

        return batch_uid

    def _append_batch(self, this_task: Optional[Task], batch_uid: UID, batch_op_list: Iterable[UserOp], save_to_disk: bool):

        batch_root: RootNode = self._op_graph.make_graph_from_batch(batch_uid, batch_op_list)

        # Reconcile ops against master op tree before adding nodes
        if not self._op_graph.can_enqueue_batch(batch_root):
            raise RuntimeError('Invalid batch!')

        if save_to_disk:
            # Save ops and their planning nodes to disk
            self._disk_store.upsert_pending_op_list(batch_op_list)

        inserted_op_list, discarded_op_list = self._op_graph.enqueue_batch(batch_root)

        if discarded_op_list:
            logger.debug(f'{len(discarded_op_list)} ops were discarded: removing from disk cache')
            self._disk_store.delete_pending_op_list(discarded_op_list)

        # Upsert src & dst nodes (redraws icons if present; adds missing nodes; fills in GDrive paths).
        # Must do this AFTER adding to OpGraph, because icon determination algo will consult the OpGraph.
        logger.debug(f'Upserting affected nodes in memstore for {len(inserted_op_list)} ops')
        for op in inserted_op_list:
            self._upsert_nodes_in_memstore(op)

    def get_last_pending_op_for_node(self, device_uid: UID, node_uid: UID) -> Optional[UserOp]:
        return self._op_graph.get_last_pending_op_for_node(device_uid, node_uid)

    def get_icon_for_node(self, device_uid: UID, node_uid: UID) -> Optional[IconId]:
        op: Optional[UserOp] = self.get_last_pending_op_for_node(device_uid, node_uid)
        if not op or op.is_completed():
            if TRACE_ENABLED:
                logger.debug(f'Node {device_uid}:{node_uid}: no custom icon (op={op})')
            return None

        icon = OpTypeMeta.get_icon_for(device_uid, node_uid, op)
        if SUPER_DEBUG_ENABLED:
            logger.debug(f'Node {device_uid}:{node_uid} belongs to pending op ({op.op_uid}): {op.op_type.name}): returning icon')
        return icon

    def get_next_command(self) -> Optional[Command]:
        # Call this from Executor. Only returns None if shutting down

        # This will block until a op is ready:
        op: UserOp = self._op_graph.get_next_op()

        if op:
            return self._cmd_builder.build_command(op)
        else:
            logger.debug('Received None; looks like we are shutting down')
            return None

    def get_next_command_nowait(self) -> Optional[Command]:
        # Non-blocking
        op: UserOp = self._op_graph.get_next_op_nowait()

        if op:
            return self._cmd_builder.build_command(op)
        else:
            return None

    def get_pending_op_count(self) -> int:
        return len(self._op_graph)

    def finish_command(self, command: Command):
        logger.debug(f'Archiving op: {command.op}')
        # FIXME: need to save prev dst node for UPDATE op
        self._disk_store.archive_completed_op_list([command.op])

        # Ensure command is one that we are expecting.
        # Important: wait until after we have finished updating cacheman, as popping here will cause the next op to immediately execute:
        self._op_graph.pop_op(command.op)
