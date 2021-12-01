import logging
import threading
from collections import defaultdict
from enum import IntEnum
from typing import DefaultDict, Dict, List, Optional, Set, Tuple

from pydispatch import dispatcher

from backend.executor.central import ExecPriority
from backend.executor.command.cmd_builder import CommandBuilder
from backend.executor.command.cmd_interface import Command
from backend.executor.user_op.batch_builder import BatchBuilder
from backend.executor.user_op.op_disk_store import OpDiskStore
from backend.executor.user_op.op_graph import OpGraph, skip_root
from backend.executor.user_op.op_graph_node import OpGraphNode, RootNode
from constants import DEFAULT_ERROR_HANDLING_STRATEGY, ErrorHandlingStrategy, IconId, OP_GRAPH_VALIDATE_AFTER_BATCH_INSERT, \
    SUPER_DEBUG_ENABLED, TRACE_ENABLED
from error import OpGraphError, UnsuccessfulBatchInsertError
from model.node.node import Node
from model.uid import UID
from model.user_op import Batch, OpTypeMeta, UserOp, UserOpStatus
from signal_constants import ID_OP_MANAGER, Signal
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
        self.backend.executor.submit_async_task(Task(ExecPriority.P3_BACKGROUND_CACHE_LOAD, self._submit_next_batch))

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
        logger.debug(f'append_new_pending_op_batch(): Validating batch {batch_uid} with {len(batch_op_list)} ops')

        # Simplify and remove redundancies in op_list, then sort by ascending op_uid:
        reduced_batch: List[UserOp] = self._batch_builder.reduce_and_validate_ops(batch_op_list)

        try:
            with self._lock:
                # Save ops and their planning nodes to disk
                self._disk_store.upsert_pending_op_list(reduced_batch)
        except RuntimeError as err:
            self.backend.report_error(ID_OP_MANAGER, f'Failed to save pending ops for batch {batch_uid} to disk', repr(err))
            return

        batch = Batch(batch_uid, reduced_batch)

        with self._lock:
            if self._pending_batch_dict.get(batch.batch_uid, None):
                raise RuntimeError(f'Cannot enqueue batch: somehow we already have a pending batch with same UID: {batch.batch_uid}')
            self._pending_batch_dict[batch.batch_uid] = batch

        # Use the same priority as "background cache load" so that we can ensure that relevant caches are loaded...
        # TODO: find a way to increase priority here. User will not see dragged nodes until caches are loaded!
        batch_intake_task = Task(ExecPriority.P3_BACKGROUND_CACHE_LOAD, self._batch_intake, batch)
        batch_intake_task.add_next_task(self._submit_next_batch)
        self.backend.executor.submit_async_task(batch_intake_task)
        logger.debug(f'append_new_pending_op_batch(): Enqueued append_batch task for {batch_uid}')

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
            # Assume batch has already been reduced and reconciled against master tree; no need to call reduce_and_validate_ops()
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
        assert this_task and this_task.priority == ExecPriority.P3_BACKGROUND_CACHE_LOAD, f'Bad task: {this_task}'

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
        next_batch = self._get_next_batch_in_queue()
        if not next_batch:
            return

        logger.info(f'Got next batch to submit: batch_uid={next_batch.batch_uid} with {len(next_batch.op_list)} ops')

        try:
            batch_graph_root: RootNode = self._batch_builder.build_batch_graph(next_batch.op_list)

            # Reconcile ops against master op tree before adding nodes. This will raise an exception if invalid
            self._batch_builder.validate_batch_graph(batch_graph_root, self)
        except RuntimeError as err:
            logger.exception('Failed to build operation graph')
            self._on_batch_error_fight_or_flight('Failed to build operation graph', str(err), batch=next_batch)
            return

        try:
            inserted_op_list = self._add_batch_to_main_graph(batch_graph_root)
        except UnsuccessfulBatchInsertError as ubie:
            msg = str(ubie)
            logger.info(f'Caught UnsuccessfulBatchInsertError: {msg}')
            self._on_batch_error_fight_or_flight('Failed to add batch to op graph', msg, batch=next_batch)
            return
        except RuntimeError as err:
            logger.exception('Unexpected failure')
            self._on_batch_error_fight_or_flight('Unexpected failure adding batch to op graph', str(err), batch=next_batch)

            self._update_icons_for_nodes()  # just in case
            return

        # The lists are returned in order of BFS of their op graph. However, when upserting to the cache we need them in BFS order of the tree
        # which they are upserting to. Fortunately, the ChangeTree which they came from set their UIDs in the correct order. So sort by that:
        inserted_op_list.sort(key=lambda op: op.op_uid)
        logger.debug(f'Returned from adding batch. InsertedOpList: {",".join([str(op.op_uid) for op in inserted_op_list])}')

        with self._lock:
            logger.debug(f'submit_next_batch(): Popping batch {next_batch.batch_uid} off the pending queue')
            if not self._pending_batch_dict.pop(next_batch.batch_uid, None):
                logger.warning(f'Failed to pop batch {next_batch.batch_uid} off of pending batches: was it already removed?')
                # fall through

        try:
            # Upsert src & dst nodes (redraws icons if present; adds missing nodes; fills in GDrive paths).
            # Must do this AFTER adding to OpGraph, because icon determination algo will consult the OpGraph.
            logger.debug(f'Upserting affected nodes in memstore for {len(inserted_op_list)} ops')
            for op in inserted_op_list:
                # NOTE: this REQUIRES that inserted_op_list is in the correct order:
                # any directories which need to be made must come before their children
                self._upsert_nodes_in_memstore(op)
        except RuntimeError as err:
            logger.exception('Error while updating nodes in memory store for user ops')
            self.backend.report_error(ID_OP_MANAGER, f'Error while updating nodes in memory store for user ops', repr(err))
            # fall through

        self._update_icons_for_nodes()

        logger.debug(f'submit_next_batch(): Done with batch {next_batch.batch_uid}; enqueuing another task')
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

    def _on_batch_error_fight_or_flight(self, msg: str, secondary_msg: str, batch: Batch):
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
                logger.debug(f'OnBatchError(): removing batch {batch.batch_uid} from disk')
                self._disk_store.cancel_op_list(batch.op_list, reason_msg=f'{msg}: {batch.batch_uid}')
                logger.debug(f'OnBatchError(): removing batch {batch.batch_uid} from memory structures')
                self._pending_batch_dict.pop(batch.batch_uid, None)
                self._error_handling_batch_override_dict.pop(batch.batch_uid, None)

            # There may be another batch in the queue: try to process that if applicable
            self.try_batch_submit()
        elif error_strategy == ErrorHandlingStrategy.PAUSE_EXECUTION:
            # TODO: determine if we ever want to support this. Now seems unnecessary and might just confuse the user
            raise NotImplementedError(f'Cannot handle ErrorHandlingStrategy.PAUSE_EXECUTION')
        elif error_strategy == ErrorHandlingStrategy.CANCEL_FAILED_OPS_AND_DEPENDENTS:
            # TODO: implement ErrorHandlingStrategy.CANCEL_FAILED_OPS_AND_DEPENDENTS
            raise NotImplementedError(f'Cannot handle ErrorHandlingStrategy.CANCEL_FAILED_OPS_AND_DEPENDENTS yet!')
        else:
            assert False, f'Unrecognized: {error_strategy.name}'

    def _add_batch_to_main_graph(self, op_root: RootNode) -> List[UserOp]:
        """Returns a tuple of [inserted user ops, discarded user ops]
        Algo:
        1. Discard root
        2. Examine each child of root. Each shall be treated as its own subtree.
        3. For each subtree, look up all its nodes in the master dict. Level...?

        Disregard the kind of op when building the tree; they are all equal for now (except for RM; see below):
        Once entire tree is constructed, invert the RM subtree (if any) so that ancestor RMs become descendants

        Note: it is assumed that the given batch has already been reduced, and stored in the pending ops tree.
        Every op node in the supplied graph must be accounted for.
        """
        if not op_root.get_child_list():
            raise RuntimeError(f'Batch has no nodes!')

        # TODO: put all this logic in OpGraph, make transactional

        batch_uid: UID = op_root.get_first_child().op.batch_uid

        logger.info(f'Adding batch {batch_uid} to OpGraph')

        breadth_first_list: List[OpGraphNode] = op_root.get_subgraph_bfs_list()
        processed_op_uid_set: Set[UID] = set()
        inserted_op_list: List[UserOp] = []
        inserted_ogn_list: List[OpGraphNode] = []
        try:
            for graph_node in skip_root(breadth_first_list):
                self._op_graph.enqueue_single_ogn(graph_node)

                inserted_ogn_list.append(graph_node)
                if SUPER_DEBUG_ENABLED:
                    logger.debug(f'Enqueue of OGN {graph_node.node_uid} (op {graph_node.op.op_uid}) succeeded')

                if graph_node.op.op_uid not in processed_op_uid_set:
                    inserted_op_list.append(graph_node.op)
                    processed_op_uid_set.add(graph_node.op.op_uid)

                # Wake Central Executor for each graph node:
                self.backend.executor.notify()

            if OP_GRAPH_VALIDATE_AFTER_BATCH_INSERT:
                self._op_graph.validate_graph()
        except RuntimeError as err:
            if not isinstance(err, OpGraphError):
                # bad bad bad
                logger.error(f'Unexpected failure while adding batch {batch_uid} to main graph (after adding {len(inserted_ogn_list)} OGNs from '
                             f'{len(inserted_op_list)} ops)')
                raise err

            logger.exception(f'Failed to add batch {batch_uid} to main graph (need to revert add of {len(inserted_ogn_list)} OGNs from '
                             f'{len(inserted_op_list)} ops)')
            if inserted_ogn_list:
                ogn_count = len(inserted_ogn_list)
                while len(inserted_ogn_list) > 0:
                    ogn = inserted_ogn_list.pop()
                    logger.debug(f'Backing out OGN {ogn_count - len(inserted_ogn_list)} of {len(inserted_ogn_list)}: {ogn}')
                    self._op_graph.revert_ogn(ogn)
            raise UnsuccessfulBatchInsertError(str(err))

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
            logger.info(f'Command {command.uid} ({command.op.op_type}) stopped on error')
        elif not (result.status == UserOpStatus.COMPLETED_OK or result.status == UserOpStatus.COMPLETED_NO_OP):
            raise RuntimeError(f'Command completed but status ({result.status}) is invalid: {command}')
        else:
            logger.debug(f'Archiving op: {command.op}')
            self._disk_store.archive_completed_op_list([command.op])

        # Ensure command is one that we are expecting.
        # Important: wait until after we have finished updating cacheman, as popping here will cause the next op to immediately execute:
        self._op_graph.pop_op(command.op)

        # Do this after popping the op:
        self._update_icons_for_nodes()

        # Wake Central Executor in case it is in the waiting state:
        self.backend.executor.notify()
