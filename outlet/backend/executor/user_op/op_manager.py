import logging
import threading
from collections import defaultdict
from enum import IntEnum
from typing import DefaultDict, Dict, List, Optional, Set

from pydispatch import dispatcher

from backend.executor.central import ExecPriority
from backend.executor.command.cmd_builder import CommandBuilder
from backend.executor.command.cmd_interface import Command
from backend.executor.user_op.batch_builder import BatchBuilder
from backend.executor.user_op.op_disk_store import OpDiskStore
from backend.executor.user_op.op_graph import OpGraph
from backend.executor.user_op.op_graph_node import RootNode
from constants import DEFAULT_ERROR_HANDLING_STRATEGY, ErrorHandlingStrategy, IconId
from logging_constants import SUPER_DEBUG_ENABLED, TRACE_ENABLED
from error import UnsuccessfulBatchInsertError
from model.node.node import Node
from model.uid import UID
from model.user_op import Batch, UserOp, UserOpStatus
from signal_constants import ID_OP_MANAGER, Signal
from util.has_lifecycle import HasLifecycle
from util.stopwatch_sec import Stopwatch
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
        self._op_graph: OpGraph = OpGraph('MainGraph')
        """Present and future batches, kept in insertion order. Each batch is removed after it is completed."""

        self._batch_builder: BatchBuilder = BatchBuilder(self.backend)
        self._are_batches_loaded_from_last_run: bool = False

        self._lock = threading.Lock()
        self._pending_batch_dict: Dict[UID, Batch] = {}
        self._error_handling_batch_override_dict: Dict[UID, ErrorHandlingStrategy] = {}  # if a batch is not represented here, use default
        self._default_error_handling_strategy: ErrorHandlingStrategy = DEFAULT_ERROR_HANDLING_STRATEGY

    def start(self):
        logger.debug(f'[OpManager] Startup started')
        HasLifecycle.start(self)
        self._disk_store.start()
        self._op_graph.start()

        self.connect_dispatch_listener(signal=Signal.HANDLE_BATCH_FAILED, receiver=self._on_handle_batch_failed)
        self.connect_dispatch_listener(signal=Signal.OP_EXECUTION_PLAY_STATE_CHANGED, receiver=self._on_op_execution_state_changed)
        logger.debug(f'[OpManager] Startup done')

    def shutdown(self):
        logger.debug(f'[OpManager] Shutdown started')
        HasLifecycle.shutdown(self)

        try:
            if self._disk_store:
                self._disk_store.shutdown()
                self._disk_store = None
        except (AttributeError, NameError):
            pass

        try:
            if self._op_graph:
                self._op_graph.shutdown()
                self._op_graph = None
        except (AttributeError, NameError):
            pass

        self.backend = None
        self._cmd_builder = None
        logger.debug(f'[OpManager] Shutdown done')

    def has_pending_batches(self) -> bool:
        with self._lock:
            logger.debug(f'has_pending_batches(): pending_batch_dict size={len(self._pending_batch_dict)}, '
                         f'startup_done={self._are_batches_loaded_from_last_run}')
            return len(self._pending_batch_dict) > 0 or not self._are_batches_loaded_from_last_run

    def try_batch_submit(self):
        self.backend.executor.submit_async_task(Task(ExecPriority.P2_USER_RELEVANT_CACHE_LOAD, self._submit_next_batch))

    def _on_handle_batch_failed(self, sender, batch_uid: UID, error_handling_strategy: ErrorHandlingStrategy):
        """This is triggered when the user indicated a strategy for handling any batch errors."""
        with self._lock:
            if not self._pending_batch_dict.get(batch_uid):
                logger.warning(f'Received signal "{Signal.HANDLE_BATCH_FAILED.name}" with strategy "{error_handling_strategy.name}" '
                               f'but batch {batch_uid} not found. Ignoring')
                return

            logger.info(f'Received signal "{Signal.HANDLE_BATCH_FAILED.name}": Setting error_handling_strategy = {error_handling_strategy} '
                        f'for batch_uid {batch_uid}')
            self._error_handling_batch_override_dict[batch_uid] = error_handling_strategy
            # fall through

        # At this point we know that the batch and all its prerequisites have been loaded:
        self.try_batch_submit()

    def _on_op_execution_state_changed(self, sender: str, is_enabled: bool):
        logger.debug(f'Received signal "{Signal.OP_EXECUTION_PLAY_STATE_CHANGED}" from {sender} with is_enabled={is_enabled}')
        if is_enabled:
            # Kick off task to submit any previously jammed batches.
            if not self.has_pending_batches():
                logger.debug(f'Op execution was enabled but no pending batches in queue')
                return
            logger.debug(f'Op execution was enabled: submitting new task to start submitting queued batches')
            self.try_batch_submit()

    def _upsert_nodes_in_memstore(self, op: UserOp):
        """Looks at the given UserOp and notifies cacheman so that it can send out update notifications. The nodes involved may not have
        actually changed (i.e., only their statuses have changed).

        Note: we update our input nodes with the returned values, as GDrive node paths in particular may be filled in"""
        if SUPER_DEBUG_ENABLED:
            logger.debug(f'Upserting src [and dst] node[s] to CacheMan from op {op.op_uid}')
        src_node = self.backend.cacheman.upsert_single_node(op.src_node)
        assert src_node, f'What happened?! {op}'
        op.src_node = src_node
        if op.has_dst():
            dst_node = self.backend.cacheman.upsert_single_node(op.dst_node)
            assert dst_node, f'What happened?! {op}'
            op.dst_node = dst_node

    def cancel_all_pending_ops(self, this_task: Task):
        """Call this at startup, to CANCEL pending ops which have not yet been applied (archive them on disk)."""
        with self._lock:
            self._disk_store.cancel_all_pending_ops()
            logger.debug(f'cancel_all_pending_ops(): setting startup_done=True')
            self._are_batches_loaded_from_last_run = True

    def enqueue_new_pending_op_batch(self, batch: Batch):
        """
        Call this after the user requests a new set of ops.

         - First do some basic validation of the submitted ops.
         - Persist the list of to-be-completed ops to disk, for robustness.
         - Asynchronously add the batch to the queue of batches to submit.
        """
        batch_uid = batch.batch_uid
        logger.debug(f'enqueue_new_pending_op_batch(): Validating batch {batch_uid} with {len(batch.op_list)} ops')

        # Simplify and remove redundancies in op_list, then sort by ascending op_uid:
        self._batch_builder.preprocess_batch(batch)

        try:
            with self._lock:
                # Save ops and their planning nodes to disk
                self._disk_store.upsert_pending_op_list(batch.op_list)
        except RuntimeError as err:
            self.backend.report_error(ID_OP_MANAGER, f'Failed to save pending ops for batch {batch_uid} to disk', repr(err))
            return

        with self._lock:
            if self._pending_batch_dict.get(batch.batch_uid, None):
                raise RuntimeError(f'Cannot enqueue batch: somehow we already have a pending batch with same UID: {batch.batch_uid}')
            self._pending_batch_dict[batch.batch_uid] = batch

        # Use the same priority as "background cache load" so that we can ensure that relevant caches are loaded...
        # TODO: find a way to increase priority here. User will not see dragged nodes until caches are loaded!
        batch_intake_task = Task(ExecPriority.P2_USER_RELEVANT_CACHE_LOAD, self._batch_intake, batch)
        batch_intake_task.add_next_task(self._submit_next_batch)
        self.backend.executor.submit_async_task(batch_intake_task)
        logger.debug(f'enqueue_new_pending_op_batch(): Enqueued append_batch task for {batch_uid}')

    def resume_pending_ops_from_disk(self, this_task: Task):
        """Call this at startup, to RESUME pending ops which have not yet been applied."""

        # Load from disk
        with self._lock:
            op_list: List[UserOp] = self._disk_store.load_all_pending_ops()
        if not op_list:
            logger.info(f'resume_pending_ops_from_disk(): No pending ops found in the disk cache')
            with self._lock:
                logger.debug(f'resume_pending_ops_from_disk(): setting startup_done=True')
                self._are_batches_loaded_from_last_run = True
            return

        logger.info(f'resume_pending_ops_from_disk(): Found {len(op_list)} pending ops from the disk cache')
        if SUPER_DEBUG_ENABLED:
            logger.info(f'resume_pending_ops_from_disk(): Pending op list UIDs = {",".join([str(op.op_uid) for op in op_list])}')

        # Sort into batches
        batch_dict: DefaultDict[UID, List[UserOp]] = defaultdict(lambda: list())
        for op in op_list:
            batch_dict[op.batch_uid].append(op)

        # Sort batches to make sure they are in correct order
        batch_dict_keys = batch_dict.keys()
        logger.info(f'Sorted ops into {len(batch_dict_keys)} batches')
        sorted_keys = sorted(batch_dict_keys)

        for batch_uid in sorted_keys:
            # Assume batch has already been reduced and reconciled against master tree; no need to call validate_and_reduce_op_list()
            batch_op_list: List[UserOp] = batch_dict[batch_uid]
            batch = Batch(batch_uid, batch_op_list)

            logger.debug(f'Resuming pending batch uid={batch_uid} with {len(batch.op_list)} ops')
            with self._lock:
                if self._pending_batch_dict.get(batch.batch_uid, None):
                    raise RuntimeError(f'Cannot enqueue batch from disk: somehow we already have a pending batch with same UID: {batch.batch_uid}')
                self._pending_batch_dict[batch.batch_uid] = batch

            # Add the batch to the op graph only after the caches are loaded
            batch_intake_task = this_task.create_child_task(self._batch_intake, batch)
            self.backend.executor.submit_async_task(batch_intake_task)

        with self._lock:
            logger.debug(f'resume_pending_ops_from_disk(): setting startup_done=True')
            self._are_batches_loaded_from_last_run = True

        # Call this just once.
        # Need to make sure we do the rest AFTER any needed cache loads complete (this will happen because the cache_load tasks are child tasks
        this_task.add_next_task(self._submit_next_batch)

    def _batch_intake(self, this_task: Task, batch: Batch):
        """Adds the given batch of UserOps to the graph, which will lead to their eventual execution. Optionally also saves the ops to disk,
        which should only be done if they haven't already been saved.
        This method should only be called via the Executor."""
        assert this_task and this_task.priority == ExecPriority.P2_USER_RELEVANT_CACHE_LOAD, f'Bad task: {this_task}'

        batch.op_list.sort(key=lambda _op: _op.op_uid)

        # Make sure all relevant caches are loaded. Do this via child tasks:
        big_node_list: List[Node] = BatchBuilder.get_all_nodes_in_batch(batch.op_list)
        logger.debug(f'Batch {batch.batch_uid} contains {len(big_node_list)} affected nodes. Adding task to ensure they are in memstore')
        self.backend.cacheman.ensure_cache_loaded_for_node_list(this_task, big_node_list)

    def _get_next_batch_in_queue(self) -> Optional[Batch]:
        with self._lock:
            if len(self._pending_batch_dict) == 0:
                logger.debug(f'No pending batches to submit!.')
                return None
            if not self._are_batches_loaded_from_last_run:
                logger.info(f'Startup not finished. Returning for now')
                return None

            min_batch_uid = 0
            for batch in self._pending_batch_dict.values():
                if min_batch_uid == 0 or batch.batch_uid < min_batch_uid:
                    min_batch_uid = batch.batch_uid
            return self._pending_batch_dict[min_batch_uid]

    def _submit_next_batch(self, this_task: Optional[Task]):
        """Part 2 of multi-task procession of adding a batch. Do not call this directly. Start _batch_intake(), which will start this."""
        next_batch: Batch = self._get_next_batch_in_queue()
        if not next_batch:
            return

        logger.info(f'Got next batch to submit: batch_uid={next_batch.batch_uid} with {len(next_batch.op_list)} ops')

        if self._cancel_batch_if_needed(batch=next_batch):
            return

        try:
            batch_graph_root: RootNode = self._batch_builder.build_batch_graph(next_batch.op_list, self)
        except RuntimeError as err:
            logger.exception(f'[Batch-{next_batch.batch_uid}] Failed to build operation graph for batch')
            self._on_batch_error_fight_or_flight('Failed to build operation graph', str(err), batch=next_batch)
            return

        sw = Stopwatch()
        try:
            inserted_op_list = self._add_batch_to_main_graph(batch_graph_root)
        except UnsuccessfulBatchInsertError as ubie:
            msg = str(ubie)
            logger.info(f'[Batch-{next_batch.batch_uid}] {sw} Caught UnsuccessfulBatchInsertError: {msg}')
            self._on_batch_error_fight_or_flight('Failed to add batch to op graph', msg, batch=next_batch)
            return
        except RuntimeError as err:
            logger.exception(f'[Batch-{next_batch.batch_uid}] {sw} Unexpected failure')
            self._on_batch_error_fight_or_flight('Unexpected failure adding batch to op graph', str(err), batch=next_batch)

            self._update_icons_for_nodes()  # just in case
            return

        # The lists are returned in order of BFS of their op graph. However, when upserting to the cache we need them in BFS order of the tree
        # which they are upserting to. Fortunately, the ChangeTree which they came from set their UIDs in the correct order. So sort by that:
        inserted_op_list.sort(key=lambda op: op.op_uid)
        logger.debug(f'[Batch-{next_batch.batch_uid}] {sw} Batch insert succesful. InsertedOpList: {",".join([str(op.op_uid) for op in inserted_op_list])}')
        actual_op_uid_list = [op.op_uid for op in inserted_op_list]
        expected_op_uid_list = [op.op_uid for op in next_batch.op_list]
        if not actual_op_uid_list == expected_op_uid_list:
            # Output to log but keep going
            logger.error(f'[Batch-{next_batch.batch_uid}] List of ops inserted into main graph ({actual_op_uid_list}) do not match '
                         f'planned list of ops ({expected_op_uid_list})')

        with self._lock:
            logger.debug(f'[Batch-{next_batch.batch_uid}] Removing batch {next_batch.batch_uid} from the pending queue')
            if not self._pending_batch_dict.pop(next_batch.batch_uid, None):
                logger.warning(f'Failed to pop batch {next_batch.batch_uid} off of pending batches: was it already removed?')
                # fall through

        try:
            # Upsert src & dst nodes (redraws icons if present; adds missing nodes; fills in GDrive paths).
            # Must do this AFTER adding to OpGraph, because icon determination algo will consult the OpGraph.
            logger.debug(f'[Batch-{next_batch.batch_uid}] Upserting affected nodes in memstore for {len(inserted_op_list)} ops')
            for op in inserted_op_list:
                # NOTE: this REQUIRES that inserted_op_list is in the correct order:
                # any directories which need to be made must come before their children
                self._upsert_nodes_in_memstore(op)
        except RuntimeError as err:
            logger.exception(f'[Batch-{next_batch.batch_uid}] Error while updating nodes in memory store for user ops')
            self.backend.report_error(ID_OP_MANAGER, f'Error while updating nodes in memory store for user ops', repr(err))
            # fall through

        self._update_icons_for_nodes()

        if next_batch.to_select_in_ui:
            self.backend.cacheman.set_selection_in_ui(tree_id=next_batch.select_in_tree_id, selected=next_batch.to_select_in_ui,
                                                      select_ts=next_batch.select_ts)

        logger.debug(f'[Batch-{next_batch.batch_uid}] Done submitting batch; enqueuing another task')
        this_task.add_next_task(self._submit_next_batch)

    def _update_icons_for_nodes(self):
        added_ancestor_dict, removed_ancestor_dict, changed_node_dict = self._op_graph.pop_ancestor_icon_changes()
        logger.debug(f'Got added ancestors: {added_ancestor_dict}; removed ancestors: {removed_ancestor_dict}; other changes: {changed_node_dict}')
        self._update_icons_for_dict(added_ancestor_dict)
        self._update_icons_for_dict(removed_ancestor_dict)
        self._update_icons_for_dict(changed_node_dict)

    def _update_icons_for_dict(self, device_node_dict: Dict[UID, Set[UID]]):
        for device_uid, node_uid_set in device_node_dict.items():
            for ancestor_node_uid in node_uid_set:
                node = self.backend.cacheman.get_node_for_uid(uid=ancestor_node_uid, device_uid=device_uid)
                if node:
                    # This is an in-memory update only:
                    self.backend.cacheman.update_node_icon(node)
                    dispatcher.send(signal=Signal.NODE_UPSERTED_IN_CACHE, sender=ID_OP_MANAGER, node=node)
                elif SUPER_DEBUG_ENABLED:
                    logger.debug(f'Could not find busy ancestor node: {device_uid}:{ancestor_node_uid}')

    def _cancel_batch_if_needed(self, batch: Batch) -> bool:
        with self._lock:
            # Pop the batch override (if any) - it is only good for a single use
            error_strategy: ErrorHandlingStrategy = self._error_handling_batch_override_dict.get(batch.batch_uid)
            if error_strategy == ErrorHandlingStrategy.CANCEL_BATCH:
                logger.debug(f'User cancelled batch {batch.batch_uid}')
                # remove from dict:
                self._error_handling_batch_override_dict.pop(batch.batch_uid)
                self._cancel_batch(batch)
            else:
                if TRACE_ENABLED:
                    logger.debug(f'No user cancellation for batch {batch.batch_uid}')
                return False

        # There may be another batch in the queue: try to process that if applicable
        self.try_batch_submit()
        return True

    def _on_batch_error_fight_or_flight(self, msg: str, secondary_msg: str, batch: Batch):
        """Executed AFTER batch insert error occured"""
        with self._lock:
            # Pop the batch override (if any) - it is only good for a single use
            error_strategy: ErrorHandlingStrategy = self._error_handling_batch_override_dict.pop(batch.batch_uid, None)
            if error_strategy:
                logger.debug(f'OnBatchError(): found error_strategy={error_strategy.name} for batch {batch.batch_uid}')
            else:
                logger.debug(f'OnBatchError(): falling back to default error_strategy ({self._default_error_handling_strategy.name}) '
                             f'for batch {batch.batch_uid}')
                error_strategy = self._default_error_handling_strategy

        if error_strategy == ErrorHandlingStrategy.PROMPT:
            dispatcher.send(signal=Signal.BATCH_FAILED, sender=ID_OP_MANAGER, msg=msg, secondary_msg=secondary_msg,
                            batch_uid=batch.batch_uid)
        elif error_strategy == ErrorHandlingStrategy.CANCEL_BATCH:
            with self._lock:
                self._cancel_batch(batch)

            # There may be another batch in the queue: try to process that if applicable
            self.try_batch_submit()
        elif error_strategy == ErrorHandlingStrategy.CANCEL_FAILED_OPS_AND_ALL_DESCENDANT_OPS:
            # TODO: implement ErrorHandlingStrategy.CANCEL_FAILED_OPS_AND_ALL_DESCENDANT_OPS
            raise NotImplementedError(f'Cannot handle ErrorHandlingStrategy.CANCEL_FAILED_OPS_AND_ALL_DESCENDANT_OPS yet!')
        else:
            raise RuntimeError(f'Invalid error handling strategy for batches: {error_strategy.name}')

    def _cancel_batch(self, batch: Batch):
        logger.debug(f'CancelBatch(): removing batch {batch.batch_uid} from disk')
        self._disk_store.cancel_op_list(batch.op_list, reason_msg=f'User cancel (err on batch insert: {batch.batch_uid})')
        logger.debug(f'CancelBatch(): removing batch {batch.batch_uid} from memory structures')
        self._pending_batch_dict.pop(batch.batch_uid, None)
        self._error_handling_batch_override_dict.pop(batch.batch_uid, None)

    def _add_batch_to_main_graph(self, op_root: RootNode) -> List[UserOp]:
        """Inserts into the main OpGraph a batch which is represented by its own OpGraph.
         Returns a list of inserted user ops if successful, or raises a UnsuccessfulBatchInsertError if it failed but managed to back out any
         data from the batch which was already inserted. Any other exception indicates that something Bad happened and the main OpGraph may
         be corrupted or contain incorrect data.
        """
        inserted_op_list = self._op_graph.insert_batch_graph(op_root)

        # Wake Central Executor on success:
        self.backend.executor.notify()

        return inserted_op_list

    def get_last_pending_op_for_node(self, device_uid: UID, node_uid: UID) -> Optional[UserOp]:
        return self._op_graph.get_last_pending_op_for_node(device_uid, node_uid)

    def get_max_added_op_uid(self) -> UID:
        return self._op_graph.get_max_added_op_uid()

    def get_icon_for_node(self, device_uid: UID, node_uid: UID) -> Optional[IconId]:
        return self._op_graph.get_icon_for_node(device_uid, node_uid)

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
        result = command.op.result

        # TODO: refactor so that we can attempt to create (close to) an atomic operation which combines GDrive and Local functionality

        if result.nodes_to_upsert:
            if SUPER_DEBUG_ENABLED:
                logger.debug(f'Cmd {command.__class__.__name__}:{command.uid} resulted in {len(result.nodes_to_upsert)} nodes to upsert: '
                             f'{result.nodes_to_upsert}')
            else:
                logger.debug(f'Cmd {command.__class__.__name__}:{command.uid} resulted in {len(result.nodes_to_upsert)} nodes to upsert')

            for node_to_upsert in result.nodes_to_upsert:
                self.backend.cacheman.upsert_single_node(node_to_upsert)

        if result.nodes_to_remove:
            if SUPER_DEBUG_ENABLED:
                logger.debug(f'Cmd {command.__class__.__name__}:{command.uid} resulted in {len(result.nodes_to_remove)} nodes to remove: '
                             f'{result.nodes_to_remove}')
            else:
                logger.debug(f'Cmd {command.__class__.__name__}:{command.uid} resulted in {len(result.nodes_to_remove)} nodes to remove')

            for removed_node in result.nodes_to_remove:
                self.backend.cacheman.remove_node(removed_node, to_trash=False)

        if result.status == UserOpStatus.STOPPED_ON_ERROR:
            logger.info(f'Command {command.uid} ({command.op.op_type.name}) stopped on error')
        elif not (result.status == UserOpStatus.COMPLETED_OK or result.status == UserOpStatus.COMPLETED_NO_OP):
            raise RuntimeError(f'Command completed but status ({result.status}) is invalid: {command}')
        else:
            logger.debug(f'Archiving op: {command.op}')
            self._disk_store.archive_completed_op_list([command.op])

        # Ensure command is one that we are expecting.
        # Important: wait until after we have finished updating cacheman, as popping here will cause the next op to immediately execute:
        self._op_graph.pop_completed_op(command.op)

        # Do this after popping the op:
        self._update_icons_for_nodes()

        # Wake Central Executor in case it is in the waiting state:
        self.backend.executor.notify()
