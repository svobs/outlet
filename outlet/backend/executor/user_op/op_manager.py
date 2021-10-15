import collections
import logging
from collections import defaultdict
from enum import IntEnum
from typing import Callable, DefaultDict, Deque, Dict, Iterable, List, Optional

from backend.executor.central import ExecPriority
from backend.executor.user_op.batch_builder import BatchBuilder
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

        self._batch_builder: BatchBuilder = BatchBuilder(self.backend)

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

    def cancel_all_pending_ops(self, this_task: Task):
        """Call this at startup, to CANCEL pending ops which have not yet been applied (archive them on disk)."""
        self._disk_store.cancel_all_pending_ops()

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

        batch_uid = batch_op_list[0].batch_uid
        logger.debug(f'append_new_pending_op_batch(): Validating batch {batch_uid} with {len(batch_op_list)} ops )')

        # Simplify and remove redundancies in op_list, then sort by ascending op_uid:
        reduced_batch: List[UserOp] = self._batch_builder.reduce_and_validate_ops(batch_op_list)

        # TODO: can we safely increase the priority of this task so that the user will see a quicker response?
        add_batch_task = Task(ExecPriority.P3_BACKGROUND_CACHE_LOAD, self._add_batch, reduced_batch, True)
        self.backend.executor.submit_async_task(add_batch_task)
        logger.debug(f'append_new_pending_op_batch(): Enqueued append_batch task for {batch_uid}')

    def resume_pending_ops_from_disk(self, this_task: Task):
        """Call this at startup, to RESUME pending ops which have not yet been applied."""

        # Load from disk
        op_list: List[UserOp] = self._disk_store.load_all_pending_ops()
        if not op_list:
            logger.info(f'resume_pending_ops_from_disk(): No pending ops found in the disk cache')
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

            logger.debug(f'Resuming pending batch uid={batch_uid} with {len(batch_op_list)} ops')

            # Add the batch to the op graph only after the caches are loaded
            add_batch_task = this_task.create_child_task(self._add_batch, batch_op_list, False)
            self.backend.executor.submit_async_task(add_batch_task)

    def _add_batch(self, this_task: Optional[Task], batch_op_list: List[UserOp], save_to_disk: bool):
        """Adds the given batch of UserOps to the graph, which will lead to their eventual execution. Optionally also saves the ops to disk,
        which should only be done if they haven't already been saved.
        This method should only be called via the Executor."""
        assert this_task and this_task.priority == ExecPriority.P3_BACKGROUND_CACHE_LOAD, f'Bad task: {this_task}'

        # Make sure all relevant caches are loaded. Do this via child tasks:
        big_node_list: List[Node] = BatchBuilder.get_all_nodes_in_batch(batch_op_list)
        self.backend.cacheman.ensure_cache_loaded_for_node_list(this_task, big_node_list)

        # Need to make sure we do the rest AFTER any needed cache loads complete
        this_task.add_next_task(self._add_batch_after_cache_ready, batch_op_list, save_to_disk)

    def _add_batch_after_cache_ready(self, this_task: Optional[Task], batch_op_list: List[UserOp], save_to_disk: bool):
        """Part 2 of multi-task procession of adding a batch. Do not call this directly. Call _add_batch(), which will call this."""
        batch_graph_root: RootNode = self._batch_builder.make_graph_from_batch(batch_op_list)

        # Reconcile ops against master op tree before adding nodes. This will raise an exception if invalid
        self._batch_builder.validate_batch_graph(batch_graph_root, self)

        if save_to_disk:
            # Save ops and their planning nodes to disk
            self._disk_store.upsert_pending_op_list(batch_op_list)

        inserted_op_list, discarded_op_list = self._op_graph.enqueue_batch(batch_graph_root)

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

    def get_max_added_op_uid(self) -> UID:
        return self._op_graph.get_max_added_op_uid()

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
