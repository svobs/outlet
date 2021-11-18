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
from constants import IconId, SUPER_DEBUG_ENABLED, TRACE_ENABLED
from model.node.node import Node
from model.uid import UID
from model.user_op import Batch, OpTypeMeta, UserOp
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

    def start(self):
        logger.debug(f'[OpManager] Startup started')
        HasLifecycle.start(self)
        self._disk_store.start()
        self._op_graph.start()
        logger.debug(f'[OpManager] Startup done')

    def shutdown(self):
        logger.debug(f'[OpManager] Shutdown started')
        HasLifecycle.shutdown(self)

        if self._disk_store:
            self._disk_store.shutdown()
            self._disk_store = None
        if self._op_graph:
            self._op_graph.shutdown()
            self._op_graph = None

        self.backend = None
        self._cmd_builder = None
        logger.debug(f'[OpManager] Shutdown done')

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
        self._disk_store.cancel_all_pending_ops()

        with self._lock:
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
        self.backend.executor.submit_async_task(batch_intake_task)
        logger.debug(f'append_new_pending_op_batch(): Enqueued append_batch task for {batch_uid}')

    def resume_pending_ops_from_disk(self, this_task: Task):
        """Call this at startup, to RESUME pending ops which have not yet been applied."""

        # Load from disk
        op_list: List[UserOp] = self._disk_store.load_all_pending_ops()
        if not op_list:
            logger.info(f'resume_pending_ops_from_disk(): No pending ops found in the disk cache')
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
            self._are_batches_loaded_from_last_run = True

    def _batch_intake(self, this_task: Task, batch: Batch):
        """Adds the given batch of UserOps to the graph, which will lead to their eventual execution. Optionally also saves the ops to disk,
        which should only be done if they haven't already been saved.
        This method should only be called via the Executor."""
        assert this_task and this_task.priority == ExecPriority.P3_BACKGROUND_CACHE_LOAD, f'Bad task: {this_task}'

        batch.op_list.sort(key=lambda _op: _op.op_uid)

        # Make sure all relevant caches are loaded. Do this via child tasks:
        big_node_list: List[Node] = BatchBuilder.get_all_nodes_in_batch(batch.op_list)
        logger.debug(f'Batch {batch.batch_uid} contains {len(big_node_list)} affected nodes. Adding task to ensure they are in memory')
        self.backend.cacheman.ensure_cache_loaded_for_node_list(this_task, big_node_list)

        # Need to make sure we do the rest AFTER any needed cache loads complete
        this_task.add_next_task(self._submit_next_batch)

    def _submit_next_batch(self, this_task: Optional[Task]):
        """Part 2 of multi-task procession of adding a batch. Do not call this directly. Start _batch_intake(), which will start this."""

        with self._lock:
            if len(self._pending_batch_dict) == 0:
                logger.debug(f'No pending batches to submit!.')
                return
            if not self._are_batches_loaded_from_last_run:
                logger.info(f'Startup not finished. Returning for now')
                return

            min_batch_uid = 0
            for batch in self._pending_batch_dict.values():
                if min_batch_uid == 0 or batch.batch_uid < min_batch_uid:
                    min_batch_uid = batch.batch_uid
            next_batch = self._pending_batch_dict[min_batch_uid]

        logger.info(f'Got next batch to submit: batch_uid={next_batch.batch_uid} with {len(next_batch.op_list)} ops')

        try:
            batch_graph_root: RootNode = self._batch_builder.build_batch_graph(next_batch.op_list)

            # Reconcile ops against master op tree before adding nodes. This will raise an exception if invalid
            self._batch_builder.validate_batch_graph(batch_graph_root, self)
        except RuntimeError as err:
            logger.exception('Failed to build operation graph')
            dispatcher.send(signal=Signal.BATCH_FAILED, sender=ID_OP_MANAGER, msg='Failed to build operation graph', secondary_msg=str(err),
                            batch_uid=batch.batch_uid)
            return

        try:
            inserted_op_list, discarded_op_list = self._add_batch_to_main_graph(batch_graph_root)
            # The lists are returned in order of BFS of their op graph. However, when upserting to the cache we need them in BFS order of the tree
            # which they are upserting to. Fortunately, the ChangeTree which they came from set their UIDs in the correct order. So sort by that:
            inserted_op_list.sort(key=lambda op: op.op_uid)
            discarded_op_list.sort(key=lambda op: op.op_uid)
            logger.debug(f'Got list of ops to insert: {",".join([str(op.op_uid) for op in inserted_op_list])}')
        except RuntimeError as err:
            logger.exception('Failed to insert into op graph')
            self.backend.report_error(ID_OP_MANAGER, 'Failed to insert into op graph!', str(err))
            return

        with self._lock:
            logger.debug(f'submit_next_batch(): Popping batch {next_batch.batch_uid} off the pending queue')
            if not self._pending_batch_dict.pop(next_batch.batch_uid, None):
                logger.warning(f'Failed to pop batch {next_batch.batch_uid} off of pending batches: was it already removed?')
                # fall through

        try:
            if discarded_op_list:
                logger.debug(f'{len(discarded_op_list)} ops were discarded: removing from disk cache')
                if SUPER_DEBUG_ENABLED:
                    logger.debug(f'Discarded ops = {",".join([str(op.op_uid) for op in discarded_op_list])}')
                self._disk_store.delete_pending_op_list(discarded_op_list)
        except RuntimeError as err:
            logger.exception('Failed to save discarded ops to disk')
            self.backend.report_error(ID_OP_MANAGER, 'Failed to save discarded ops to disk', str(err))
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
            self.backend.report_error(ID_OP_MANAGER, 'Error while updating nodes in memory store for user ops!', str(err))
            return

        logger.debug(f'submit_next_batch(): Done with batch {next_batch.batch_uid}; enqueuing another task')
        this_task.add_next_task(self._submit_next_batch)

    def _add_batch_to_main_graph(self, op_root: RootNode) -> Tuple[List[UserOp], List[UserOp]]:
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

        batch_uid: UID = op_root.get_first_child().op.batch_uid

        logger.info(f'Adding batch {batch_uid} to OpGraph')

        breadth_first_list: List[OpGraphNode] = op_root.get_subgraph_bfs_list()
        processed_op_uid_set: Set[UID] = set()
        inserted_op_list: List[UserOp] = []
        discarded_op_list: List[UserOp] = []
        for graph_node in skip_root(breadth_first_list):
            succeeded = self._op_graph.enqueue_single_og_node(graph_node)
            if SUPER_DEBUG_ENABLED:
                logger.debug(f'Enqueue of OGNode {graph_node.node_uid} (op {graph_node.op.op_uid}) succeeded={succeeded}')

            if succeeded:
                if graph_node.op.op_uid not in processed_op_uid_set:
                    inserted_op_list.append(graph_node.op)
                    processed_op_uid_set.add(graph_node.op.op_uid)

            else:
                if graph_node.op.op_uid not in processed_op_uid_set:
                    discarded_op_list.append(graph_node.op)
                    processed_op_uid_set.add(graph_node.op.op_uid)

            # Wake Central Executor for each graph node:
            self.backend.executor.notify()

        return inserted_op_list, discarded_op_list

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
            logger.debug(f'Node {device_uid}:{node_uid} belongs to pending op ({op.op_uid}: {op.op_type.name}): returning icon')
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
        self._disk_store.archive_completed_op_list([command.op])

        # Ensure command is one that we are expecting.
        # Important: wait until after we have finished updating cacheman, as popping here will cause the next op to immediately execute:
        self._op_graph.pop_op(command.op)

        # Wake Central Executor in case it is in the waiting state:
        self.backend.executor.notify()
