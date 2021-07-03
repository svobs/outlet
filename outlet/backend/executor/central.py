import threading
import logging
from collections import deque
from concurrent.futures import Future
from typing import Deque, Optional

from pydispatch import dispatcher

from backend.diff.task.tree_diff_task import TreeDiffTask
from backend.executor.command.cmd_executor import CommandExecutor
from constants import COMMAND_EXECUTION_TIMEOUT_SEC, EngineSummaryState
from util.task_runner import TaskRunner
from global_actions import GlobalActions
from model.display_tree.build_struct import DiffResultTreeIds
from signal_constants import ID_CENTRAL_EXEC, ID_LEFT_DIFF_TREE, ID_LEFT_TREE, ID_RIGHT_DIFF_TREE, ID_RIGHT_TREE, Signal
from util.has_lifecycle import HasLifecycle

logger = logging.getLogger(__name__)

OP_EXECUTION_THREAD_NAME = 'OpExecutionThread'


class CentralExecutor(HasLifecycle):
    """
    ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
    CLASS CentralExecutor

    Half-baked proto-module which will at least let me see all execution in one place
    ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼
    """

    def __init__(self, backend):
        HasLifecycle.__init__(self)
        self.backend = backend
        self._command_executor = CommandExecutor(self.backend)
        self._global_actions = GlobalActions(self.backend)
        self._be_task_runner = TaskRunner()
        self.enable_op_execution = backend.get_config('executor.enable_op_execution')
        self._lock = threading.Lock()

        # -- QUEUES --

        self._load_request_priority0_queue: Deque = deque()
        """Highest priority load requests: immediately visible nodes in UI tree.
         For user-initated refresh requests, this queue will be used if the nodes are already visible."""

        self._load_request_priority1_queue: Deque = deque()
        """Second highest priority load requests: visible if filter toggled"""

        self._load_request_priority2_queue: Deque = deque()
        """Third highest priority load requests: dir in UI tree but not yet visible, and not yet in cache.
        For user-initated refresh requests, this queue will be used if the nodes are not already visible."""

        self._cache_load_request_queue: Deque = deque()
        """Fourth highest priority load requests: cache loads from disk into memory (such as during startup)"""

        self._gdrive_download_whole_tree_queue: Deque = deque()
        """Fifth highest priority load requests: donwloading the whole GDrive tree in chunks of nodes."""
        # TODO: maybe combine this with _cache_load_request_queue

        self._signature_calc_queue: Deque = deque()
        """Signature calculations"""

        # Not shown: Op Execution (popped from OpGraph via OpExecutionThread, which blocks until it gets a command)

        # -- End QUEUES --

        self._op_execution_thread: Optional[threading.Thread] = None
        """Executes UserOps as needed in its own thread, which blocks until a UserOp is available."""

    def start(self):
        logger.debug('Central Executor starting')
        HasLifecycle.start(self)

        self._global_actions.start()

        if self.enable_op_execution:
            self.start_op_execution_thread()
        else:
            logger.warning(f'{self._op_execution_thread.name} is disabled!')

        self.connect_dispatch_listener(signal=Signal.PAUSE_OP_EXECUTION, receiver=self._pause_op_execution)
        self.connect_dispatch_listener(signal=Signal.RESUME_OP_EXECUTION, receiver=self._start_op_execution)

    def get_engine_summary_state(self) -> EngineSummaryState:
        with self._lock:
            if len(self._cache_load_request_queue) > 0 or len(self._gdrive_download_whole_tree_queue) > 0:
                # still starting up
                return EngineSummaryState.RED

            total_enqueued = len(self._load_request_priority0_queue) \
                             + len(self._load_request_priority1_queue) \
                             + len(self._load_request_priority2_queue) \
                             + len(self._signature_calc_queue)

            if total_enqueued > 0:
                return EngineSummaryState.YELLOW

            pending_op_count = self.backend.cacheman.get_pending_op_count()
            if pending_op_count > 0:
                return EngineSummaryState.YELLOW

            return EngineSummaryState.GREEN

    def submit_async_task(self, task_func, *args) -> Future:
        """Will expand on this later."""
        # TODO: add mechanism to prioritize some tasks over others

        return self._be_task_runner.enqueue(task_func, *args)

    def shutdown(self):
        if self.was_shutdown:
            return

        HasLifecycle.shutdown(self)
        self.backend = None
        self._command_executor = None
        self._global_actions = None
        self._be_task_runner = None

        logger.debug('CentralExecutor shut down')

    # Op Execution Thread
    # ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼

    def start_op_execution_thread(self):
        with self._lock:
            if not self._op_execution_thread or not self._op_execution_thread.is_alive():
                logger.info(f'Starting {OP_EXECUTION_THREAD_NAME}...')
                self._op_execution_thread = threading.Thread(target=self._run_op_execution_thread, name=OP_EXECUTION_THREAD_NAME, daemon=True)
                self._op_execution_thread.start()

    def _run_op_execution_thread(self):
        """This is a consumer thread for the ChangeManager's dependency tree"""
        while self.enable_op_execution and not self.was_shutdown:
            # Should be ok to do simple infinite loop, because get_next_command() will block until work is available.
            # May need to throttle here in the future however if we are seeing hiccups in the UI for large numbers of operations

            command = None
            try:
                logger.debug(f'[{OP_EXECUTION_THREAD_NAME}] Getting next command...')
                command = self.backend.cacheman.get_next_command()  # Blocks until received
            except RuntimeError as e:
                logger.exception(f'[{OP_EXECUTION_THREAD_NAME}] BAD: caught exception: halting execution')
                self.backend.report_error(sender=ID_CENTRAL_EXEC, msg='Error executing command', secondary_msg=f'{e}')
                self._pause_op_execution(sender=ID_CENTRAL_EXEC)

            # best place to pause is here, after we got unblocked from command (remember: waiting for a command is also blocking behavior)
            if not self.enable_op_execution or self.was_shutdown:
                break

            if command:
                logger.debug(f'[{OP_EXECUTION_THREAD_NAME}] Got a command to execute: {command.__class__.__name__}')
                future: Future = self.submit_async_task(self._command_executor.execute_command, command, None, True)

                try:
                    # Wait for the command to return before enqueuing others. We currently only process 1 command at a time.
                    # TODO: allow parallel processing via forked execution (see README)
                    future.result(timeout=COMMAND_EXECUTION_TIMEOUT_SEC)
                except TimeoutError:
                    logger.error(f'[{OP_EXECUTION_THREAD_NAME}] Timed out waiting for command (moving on): {command}')
            else:
                # This should never happen unless the whole app is shutting down
                logger.debug(f'[{OP_EXECUTION_THREAD_NAME}] Got None for next command. Shutting down')
                self.shutdown()
                break

        logger.info(f'[{OP_EXECUTION_THREAD_NAME}] Execution stopped')
        with self._lock:
            self._op_execution_thread = None

    def _start_op_execution(self, sender):
        logger.debug(f'Received signal "{Signal.RESUME_OP_EXECUTION.name}" from {sender}')
        self.enable_op_execution = True
        self.start_op_execution_thread()
        logger.debug(f'Sending signal "{Signal.OP_EXECUTION_PLAY_STATE_CHANGED.name}" (is_enabled={self.enable_op_execution})')
        dispatcher.send(signal=Signal.OP_EXECUTION_PLAY_STATE_CHANGED, sender=ID_CENTRAL_EXEC, is_enabled=self.enable_op_execution)

    def _pause_op_execution(self, sender):
        logger.debug(f'Received signal "{Signal.PAUSE_OP_EXECUTION.name}" from {sender}')
        self.enable_op_execution = False
        logger.debug(f'Sending signal "{Signal.OP_EXECUTION_PLAY_STATE_CHANGED.name}" (is_enabled={self.enable_op_execution})')
        dispatcher.send(signal=Signal.OP_EXECUTION_PLAY_STATE_CHANGED, sender=ID_CENTRAL_EXEC, is_enabled=self.enable_op_execution)

    # Misc tasks
    # ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼

    def start_tree_diff(self, tree_id_left, tree_id_right) -> DiffResultTreeIds:
        """Starts the Diff Trees task async"""
        assert tree_id_left == ID_LEFT_TREE and tree_id_right == ID_RIGHT_TREE, f'Wrong tree IDs: {ID_LEFT_TREE}, {ID_RIGHT_TREE}'
        tree_id_struct: DiffResultTreeIds = DiffResultTreeIds(ID_LEFT_DIFF_TREE, ID_RIGHT_DIFF_TREE)
        self.submit_async_task(TreeDiffTask.do_tree_diff, self.backend, ID_CENTRAL_EXEC, tree_id_left, tree_id_right, tree_id_struct)
        return tree_id_struct
