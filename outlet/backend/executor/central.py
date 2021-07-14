import logging
import threading
from concurrent.futures import Future
from enum import IntEnum
from functools import partial
from queue import Empty, Queue
from typing import Dict, Optional
from uuid import UUID

from pydispatch import dispatcher

from backend.executor.command.cmd_executor import CommandExecutor
from constants import CENTRAL_EXEC_THREAD_NAME, EngineSummaryState, OP_EXECUTION_THREAD_NAME, SUPER_DEBUG_ENABLED, \
    TASK_EXEC_IMEOUT_SEC, TASK_RUNNER_MAX_WORKERS
from global_actions import GlobalActions
from signal_constants import ID_CENTRAL_EXEC, Signal
from util.has_lifecycle import HasLifecycle
from util.task_runner import Task, TaskRunner

logger = logging.getLogger(__name__)


class ExecPriority(IntEnum):
    # Highest priority load requests: immediately visible nodes in UI tree.
    # For user-initiated refresh requests, this queue will be used if the nodes are already visible.
    LOAD_0 = 1

    # Second highest priority load requests: visible if filter toggled
    # NOTE: Not currently used!
    LOAD_1 = 2

    # Third highest priority load requests: dir in UI tree but not yet visible, and not yet in cache.
    # For user-initiated refresh requests, this queue will be used if the nodes are not already visible.
    LOAD_2 = 3

    # Fourth highest priority load requests: cache loads from disk into memory (such as during startup)
    CACHE_LOAD = 4

    # GDrive whole tree downloads (in chunks)
    LONG_RUNNING_NETWORK_LOAD = 5

    # Updates to the cache based on disk monitoring, in batches:
    LIVE_UPDATE = 6

    # Signature calculations: IO-dominant
    SIGNATURE_CALC = 7

    # This queue stores operations like "resume pending ops on startup", but we also consult the OpLedger
    USER_OP_EXEC = 8


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
        self._running_task_dict: Dict[UUID, Task] = {}
        self.enable_op_execution = backend.get_config('executor.enable_op_execution')
        self._lock = threading.Lock()
        self._running_task_cv = threading.Condition(self._lock)

        # -- QUEUES --
        self._exec_queue_dict: Dict[ExecPriority, Queue[Task]] = {

            ExecPriority.LOAD_0: Queue[Task](),

            ExecPriority.LOAD_1: Queue[Task](),

            ExecPriority.LOAD_2: Queue[Task](),

            ExecPriority.CACHE_LOAD: Queue[Task](),

            ExecPriority.LONG_RUNNING_NETWORK_LOAD: Queue[Task](),

            ExecPriority.LIVE_UPDATE: Queue[Task](),

            ExecPriority.SIGNATURE_CALC: Queue[Task](),

            ExecPriority.USER_OP_EXEC: Queue[Task](),
        }

        self._central_exec_thread: threading.Thread = threading.Thread(target=self._run_central_exec_thread,
                                                                       name=CENTRAL_EXEC_THREAD_NAME, daemon=True)

    def start(self):
        logger.debug('Central Executor starting')
        HasLifecycle.start(self)

        self._global_actions.start()

        self._central_exec_thread.start()

        if not self.enable_op_execution:
            logger.warning(f'{OP_EXECUTION_THREAD_NAME} is disabled!')

        self.connect_dispatch_listener(signal=Signal.PAUSE_OP_EXECUTION, receiver=self._pause_op_execution)
        self.connect_dispatch_listener(signal=Signal.RESUME_OP_EXECUTION, receiver=self._start_op_execution)

    def shutdown(self):
        if self.was_shutdown:
            return

        HasLifecycle.shutdown(self)
        self.backend = None
        self._command_executor = None
        self._global_actions = None
        self._be_task_runner = None

        logger.debug('CentralExecutor shut down')

    def get_engine_summary_state(self) -> EngineSummaryState:
        with self._lock:
            if self._exec_queue_dict[ExecPriority.CACHE_LOAD].qsize() > 0 \
                    or self._exec_queue_dict[ExecPriority.LONG_RUNNING_NETWORK_LOAD].qsize() > 0:
                # still getting up to speed on the BE
                return EngineSummaryState.RED

            total_enqueued = self._exec_queue_dict[ExecPriority.LOAD_0].qsize() + \
                             self._exec_queue_dict[ExecPriority.LOAD_1].qsize() + \
                             self._exec_queue_dict[ExecPriority.LOAD_2].qsize() + \
                             self._exec_queue_dict[ExecPriority.SIGNATURE_CALC].qsize()

            if total_enqueued > 0:
                return EngineSummaryState.YELLOW

            pending_op_count = self.backend.cacheman.get_pending_op_count()
            if pending_op_count > 0:
                return EngineSummaryState.YELLOW

            return EngineSummaryState.GREEN

    # Central Executor Thread
    # ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼
    def _run_central_exec_thread(self):
        logger.info(f'[{CENTRAL_EXEC_THREAD_NAME}] Starting thread...')

        try:
            while not self.was_shutdown:
                task: Optional[Task] = None
                with self._running_task_cv:
                    # wait until we are notified of new task (assuming task queue is not full) or task finished (if task queue is full)
                    if not self._running_task_cv.wait(TASK_EXEC_IMEOUT_SEC):
                        if SUPER_DEBUG_ENABLED:
                            logger.debug(f'[{CENTRAL_EXEC_THREAD_NAME}] Running task CV TIMEOUT!')

                    if len(self._running_task_dict) < TASK_RUNNER_MAX_WORKERS:
                        task = self._get_next_task_to_run_nolock()
                        if task:
                            self._running_task_dict[task.task_uuid] = task
                    else:
                        logger.debug(f'[{CENTRAL_EXEC_THREAD_NAME}] Running task queue is at max capacity ({TASK_RUNNER_MAX_WORKERS}): '
                                     f'{self._running_task_dict}')

                # Do this outside the CV:
                if task:
                    future = self._be_task_runner.enqueue_task(task)
                    callback = partial(self._on_task_done, task)
                    # This will call back immediately if the task already completed:
                    future.add_done_callback(callback)
        finally:
            logger.info(f'[{CENTRAL_EXEC_THREAD_NAME}] Execution stopped')

    def _get_from_queue(self, priority: ExecPriority) -> Optional[Task]:
        try:
            task = self._exec_queue_dict[priority].get_nowait()
            logger.debug(f'[{CENTRAL_EXEC_THREAD_NAME}] Got task with priority={ExecPriority.LOAD_0.name}: {task.task_func.__name__}'
                         f' (task_uuid: {task.task_uuid})')
            return task
        except Empty:
            return None

    def _get_next_task_to_run_nolock(self) -> Optional[Task]:
        task = self._get_from_queue(ExecPriority.LOAD_0)
        if task:
            return task

        task = self._get_from_queue(ExecPriority.LOAD_1)
        if task:
            return task

        task = self._get_from_queue(ExecPriority.LOAD_2)
        if task:
            return task

        task = self._get_from_queue(ExecPriority.CACHE_LOAD)
        if task:
            return task

        task = self._get_from_queue(ExecPriority.LONG_RUNNING_NETWORK_LOAD)
        if task:
            return task

        task = self._get_from_queue(ExecPriority.LIVE_UPDATE)
        if task:
            return task

        task = self._get_from_queue(ExecPriority.SIGNATURE_CALC)
        if task:
            return task

        if self.enable_op_execution:

            # Special case for op execution: we have both a queue and the ledger. The queue takes higher precedence.
            task = self._get_from_queue(ExecPriority.USER_OP_EXEC)
            if task:
                return task

            command = None
            try:
                command = self.backend.cacheman.get_next_command_nowait()
            except RuntimeError as e:
                logger.exception(f'[{CENTRAL_EXEC_THREAD_NAME}] BAD: caught exception while retreiving command: halting execution')
                self.backend.report_error(sender=ID_CENTRAL_EXEC, msg='Error reteiving command', secondary_msg=f'{e}')
                self._pause_op_execution(sender=ID_CENTRAL_EXEC)
            if command:
                logger.debug(f'[{CENTRAL_EXEC_THREAD_NAME}] Got a command to execute: {command.__class__.__name__}')
                return Task(ExecPriority.USER_OP_EXEC, self._command_executor.execute_command, command, None, True)

        return None

    def _on_task_done(self, task: Task, future: Future):
        with self._running_task_cv:
            if SUPER_DEBUG_ENABLED:
                if future.cancelled():
                    logger.debug(f'Task cancelled: "{task.task_func.__name__}" (task_uuid={task.task_uuid}, priority={task.priority})')
                else:
                    logger.debug(f'Task done: "{task.task_func.__name__}" (task_uuid={task.task_uuid}, priority={task.priority})')

            # FIXME: account for child tasks here


            # TODO: DELETE task if completely done, to prevent memory leaks due to circular references

            # Removing based on object identity should work for now, since we are in the same process:
            # Note: this is O(n). Best not to let the deque get too large
            self._running_task_dict.pop(task.task_uuid)
            logger.debug(f'Running task dict now has {len(self._running_task_dict)} tasks')
            self._running_task_cv.notify_all()

    def submit_async_task(self, task: Task, parent_task: Optional[Task] = None):
        """API: enqueue task to be executed, with given priority."""
        priority = task.priority

        if not isinstance(priority, ExecPriority):
            raise RuntimeError(f'Bad arg: {priority}')

        if SUPER_DEBUG_ENABLED:
            logger.debug(f'Enqueuing task with priority={priority.name}: {task.task_func.__name__} (task_uuid: {task.task_uuid}')
        self._exec_queue_dict[priority].put_nowait(task)

        with self._running_task_cv:
            if len(self._running_task_dict) < TASK_RUNNER_MAX_WORKERS:
                self._running_task_cv.notify_all()

    # Op Execution State
    # ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼

    def _start_op_execution(self, sender):
        logger.debug(f'Received signal "{Signal.RESUME_OP_EXECUTION.name}" from {sender}')
        self.enable_op_execution = True
        logger.debug(f'Sending signal "{Signal.OP_EXECUTION_PLAY_STATE_CHANGED.name}" (is_enabled={self.enable_op_execution})')
        dispatcher.send(signal=Signal.OP_EXECUTION_PLAY_STATE_CHANGED, sender=ID_CENTRAL_EXEC, is_enabled=self.enable_op_execution)

    def _pause_op_execution(self, sender):
        logger.debug(f'Received signal "{Signal.PAUSE_OP_EXECUTION.name}" from {sender}')
        self.enable_op_execution = False
        logger.debug(f'Sending signal "{Signal.OP_EXECUTION_PLAY_STATE_CHANGED.name}" (is_enabled={self.enable_op_execution})')
        dispatcher.send(signal=Signal.OP_EXECUTION_PLAY_STATE_CHANGED, sender=ID_CENTRAL_EXEC, is_enabled=self.enable_op_execution)
