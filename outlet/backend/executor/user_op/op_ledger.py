import collections
import logging
from collections import defaultdict
from enum import IntEnum
from typing import Callable, DefaultDict, Deque, Dict, Iterable, List, Optional

from backend.executor.user_op.op_graph import OpGraph
from constants import IconId, SUPER_DEBUG
from backend.executor.command.cmd_builder import CommandBuilder
from backend.executor.command.cmd_interface import Command
from backend.executor.user_op.op_disk_store import OpDiskStore
from backend.executor.user_op.op_graph_node import RootNode
from model.node.node import Node
from model.user_op import UserOp, UserOpType
from model.uid import UID
from signal_constants import Signal
from util.has_lifecycle import HasLifecycle

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


class OpLedger(HasLifecycle):
    """
    ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
    CLASS OpLedger
    ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼
    """

    def __init__(self, backend, op_db_path):
        HasLifecycle.__init__(self)
        self.backend = backend
        self._cmd_builder: CommandBuilder = CommandBuilder(self.backend.uid_generator)
        self._disk_store: OpDiskStore = OpDiskStore(self.backend, op_db_path=op_db_path)
        self._op_graph: OpGraph = OpGraph(self.backend)
        """Present and future batches, kept in insertion order. Each batch is removed after it is completed."""

        self.icon_src_file_dict = {UserOpType.RM: IconId.ICON_FILE_RM,
                                   UserOpType.MV: IconId.ICON_FILE_MV_SRC,
                                   UserOpType.UP: IconId.ICON_FILE_UP_SRC,
                                   UserOpType.CP: IconId.ICON_FILE_CP_SRC}
        self.icon_dst_file_dict = {UserOpType.MV: IconId.ICON_FILE_MV_DST,
                                   UserOpType.UP: IconId.ICON_FILE_UP_DST,
                                   UserOpType.CP: IconId.ICON_FILE_CP_DST}
        self.icon_src_dir_dict = {UserOpType.MKDIR: IconId.ICON_DIR_MK,
                                  UserOpType.RM: IconId.ICON_DIR_RM,
                                  UserOpType.MV: IconId.ICON_DIR_MV_SRC,
                                  UserOpType.UP: IconId.ICON_DIR_UP_SRC,
                                  UserOpType.CP: IconId.ICON_DIR_CP_SRC}
        self.icon_dst_dir_dict = {UserOpType.MV: IconId.ICON_DIR_MV_DST,
                                  UserOpType.UP: IconId.ICON_DIR_UP_DST,
                                  UserOpType.CP: IconId.ICON_DIR_CP_DST}

    def start(self):
        logger.debug(f'Starting OpLedger')
        HasLifecycle.start(self)
        self._disk_store.start()
        self.connect_dispatch_listener(signal=Signal.COMMAND_COMPLETE, receiver=self._on_command_completed)

        self._op_graph.start()

    def shutdown(self):
        HasLifecycle.shutdown(self)

        self.backend = None
        self._cmd_builder = None
        self._op_graph = None

    def _update_nodes_in_memstore(self, op: UserOp):
        """Looks at the given UserOp and notifies cacheman so that it can send out update notifications. The nodes involved may not have
        actually changed (i.e., only their statuses have changed)"""
        self.backend.cacheman.upsert_single_node(op.src_node)
        if op.has_dst():
            self.backend.cacheman.upsert_single_node(op.dst_node)

    # Reduce Changes logic
    # ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼

    @staticmethod
    def _derive_dst_parent_key_list(dst_node: Node) -> List[str]:
        return [f'{parent_uid}/{dst_node.name}' for parent_uid in dst_node.get_parent_uids()]

    def _reduce_ops(self, op_list: Iterable[UserOp]) -> Iterable[UserOp]:
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
            elif op.op_type == UserOpType.CP or op.op_type == UserOpType.UP or op.op_type == UserOpType.MV:
                # GDrive nodes' UIDs are derived from their goog_ids; nodes with no goog_id can have different UIDs.
                # So for GDrive nodes with no goog_id, we must rely on a combination of their parent UID and name to check for uniqueness
                for dst_parent_key in self._derive_dst_parent_key_list(op.dst_node):
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

        def eval_rm_ancestor_func(op_arg: UserOp, ancestor: Node) -> None:
            conflict = mkdir_dict.get(ancestor.uid, None)
            if conflict:
                logger.error(f'ReduceChanges(): Conflict! Op1={conflict}; Op2={op_arg}')
                raise RuntimeError(f'Batch op conflict: trying to create a node and remove its descendant at the same time!')

        def eval_mkdir_ancestor_func(op_arg: UserOp, ancestor: Node) -> None:
            conflict = rm_dict.get(ancestor.uid, None)
            if conflict:
                logger.error(f'ReduceChanges(): Conflict! Op1={conflict}; Op2={op_arg}')
                raise RuntimeError(f'Batch op conflict: trying to remove a node and create its descendant at the same time!')

        def eval_cp_src_ancestor_func(op_arg: UserOp, ancestor: Node) -> None:
            if SUPER_DEBUG:
                logger.debug(f'Evaluating src ancestor (op={op_arg.op_uid}): {ancestor}')
            if ancestor.uid in mkdir_dict:
                raise RuntimeError(f'Batch op conflict: copy from a descendant of a node being created!')
            if ancestor.uid in rm_dict:
                raise RuntimeError(f'Batch op conflict: copy from a descendant of a node being deleted!')
            if ancestor.uid in cp_dst_dict:
                raise RuntimeError(f'Batch op conflict: copy from a descendant of a node being copied to!')

        def eval_cp_dst_ancestor_func(op_arg: UserOp, ancestor: Node) -> None:
            if SUPER_DEBUG:
                logger.debug(f'Evaluating dst ancestor (op={op.op_uid}): {ancestor}')
            if ancestor.uid in rm_dict:
                raise RuntimeError(f'Batch op conflict: copy to a descendant of a node being deleted!')
            if ancestor.uid in cp_src_dict:
                raise RuntimeError(f'Batch op conflict: copy to a descendant of a node being copied from!')

        # For each element, traverse up the tree and compare each parent node to map
        for op in op_list:
            if op.op_type == UserOpType.RM:
                self._check_ancestors(op, op.src_node, eval_rm_ancestor_func)
            elif op.op_type == UserOpType.MKDIR:
                self._check_ancestors(op, op.src_node, eval_mkdir_ancestor_func)
            elif op.op_type == UserOpType.CP or op.op_type == UserOpType.UP or op.op_type == UserOpType.MV:
                """Checks all ancestors of both src and dst for mapped Ops. The following are the only valid situations:
                 1. No ancestors of src or dst correspond to any Ops.
                 2. Ancestor(s) of the src node correspond to the src node of a CP or UP action (i.e. they will not change)
                 """
                self._check_ancestors(op, op.src_node, eval_cp_src_ancestor_func)
                self._check_ancestors(op, op.dst_node, eval_cp_dst_ancestor_func)

        logger.debug(f'Reduced {count_ops_orig} ops to {len(final_list)} ops')
        return final_list

    def _check_ancestors(self, op: UserOp, node: Node, eval_func: Callable[[UserOp, Node], None]):
        queue: Deque[Node] = collections.deque()
        queue.append(node)

        while len(queue) > 0:
            node: Node = queue.popleft()
            for ancestor in self.backend.cacheman.get_parent_list_for_node(node):
                queue.append(ancestor)
                if SUPER_DEBUG:
                    logger.debug(f'(UserOp={op.op_uid}): evaluating ancestor: {ancestor}')
                eval_func(op, ancestor)

    # ▲ ▲ ▲ ▲ ▲ ▲ ▲ ▲ ▲ ▲ ▲ ▲ ▲ ▲ ▲
    # Reduce Changes logic

    def cancel_pending_ops_from_disk(self):
        """Call this at startup, to CANCEL pending ops which have not yet been applied (archive them on disk)."""
        self._disk_store.cancel_pending_ops_from_disk()

    def resume_pending_ops_from_disk(self):
        """Call this at startup, to RESUME pending ops which have not yet been applied."""

        # Load from disk
        op_list: List[UserOp] = self._disk_store.get_pending_ops_from_disk()
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
            self._ensure_batch_nodes_loaded(batch_op_list)

            self._append_batch(batch_uid, batch_op_list, save_to_disk=False)

    def _ensure_batch_nodes_loaded(self, batch_op_list: List[UserOp]):
        big_node_list: List[Node] = []
        for user_op in batch_op_list:
            big_node_list.append(user_op.src_node)
            if user_op.has_dst():
                big_node_list.append(user_op.dst_node)

        # Make sure all relevant caches are loaded:
        self.backend.cacheman.ensure_loaded(big_node_list)

    def append_new_pending_op_batch(self, batch_op_list: Iterable[UserOp]):
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
        op_iter = iter(batch_op_list)
        batch_uid = next(op_iter).batch_uid
        for op in op_iter:
            if op.batch_uid != batch_uid:
                raise RuntimeError(f'Changes in batch do not all contain the same batch_uid (found {op.batch_uid} and {batch_uid})')

        # Simplify and remove redundancies in op_list
        reduced_batch: Iterable[UserOp] = self._reduce_ops(batch_op_list)

        self._append_batch(batch_uid, reduced_batch, save_to_disk=True)

    def _append_batch(self, batch_uid: UID, batch_op_list: Iterable[UserOp], save_to_disk: bool):

        batch_root: RootNode = self._op_graph.make_graph_from_batch(batch_op_list)

        # Reconcile ops against master op tree before adding nodes
        if not self._op_graph.can_enqueue_batch(batch_root):
            raise RuntimeError('Invalid batch!')

        if save_to_disk:
            # Save ops and their planning nodes to disk
            self._disk_store.save_pending_ops_to_disk(batch_op_list)

        # Upsert src & dst nodes (redraws icons if present; adds missing nodes)
        for op in batch_op_list:
            self._update_nodes_in_memstore(op)

        self._add_batch_to_op_graph_and_remove_discarded(batch_root, batch_uid)

    def _add_batch_to_op_graph_and_remove_discarded(self, batch_root, batch_uid):
        logger.info(f'Adding batch {batch_uid} to OpTree')
        discarded_op_list: List[UserOp] = self._op_graph.enqueue_batch(batch_root)
        if discarded_op_list:
            logger.debug(f'{len(discarded_op_list)} ops were discarded: removing from disk cache')
            self._disk_store.remove_pending_ops(discarded_op_list)

    def get_last_pending_op_for_node(self, node_uid: UID) -> Optional[UserOp]:
        return self._op_graph.get_last_pending_op_for_node(node_uid)

    def get_icon_for_node(self, node_uid: UID) -> Optional[IconId]:
        op: Optional[UserOp] = self.get_last_pending_op_for_node(node_uid)
        if not op or not op.is_completed():
            return None

        if SUPER_DEBUG:
            logger.debug(f'Node {node_uid} belongs to pending op ({op.op_uid}): {op.op_type.name}): returning icon')

        if op.has_dst() and op.dst_node.uid == node_uid:
            op_type = op.op_type
            if op_type == UserOpType.MV and not op.dst_node.is_live():
                # Use an add-like icon if nothing there right now:
                op_type = UserOpType.CP

            if op.dst_node.is_dir():
                return self.icon_dst_dir_dict[op_type]
            else:
                return self.icon_dst_file_dict[op_type]

        assert op.src_node.uid == node_uid
        if op.src_node.is_dir():
            return self.icon_src_dir_dict[op.op_type]
        else:
            return self.icon_src_file_dict[op.op_type]

    def get_next_command(self) -> Optional[Command]:
        # Call this from Executor. Only returns None if shutting down

        # This will block until a op is ready:
        op: UserOp = self._op_graph.get_next_op()

        if not op:
            logger.debug('Received None; looks like we are shutting down')
            return None

        return self._cmd_builder.build_command(op)

    def _on_command_completed(self, sender, command: Command):
        logger.debug(f'Received signal: "{Signal.COMMAND_COMPLETE.name}"')

        logger.debug(f'Archiving op: {command.op}')
        self._disk_store.archive_pending_ops_to_disk([command.op])

        # Ensure command is one that we are expecting.
        # Important: wait until after we have finished updating cacheman, as popping here will cause the next op to immediately execute:
        self._op_graph.pop_op(command.op)
