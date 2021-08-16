import logging
import threading
from concurrent.futures import Future
from enum import IntEnum
from functools import partial
from queue import Empty, Queue
from typing import Dict, Optional, Set, Tuple
from uuid import UUID

from pydispatch import dispatcher

from backend.executor.command.cmd_executor import CommandExecutor
from constants import CENTRAL_EXEC_THREAD_NAME, EngineSummaryState, OP_EXECUTION_THREAD_NAME, SUPER_DEBUG_ENABLED, \
    TASK_EXEC_IMEOUT_SEC, TASK_RUNNER_MAX_WORKERS, TRACE_ENABLED
from global_actions import GlobalActions
from signal_constants import ID_CENTRAL_EXEC, Signal
from util import time_util
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

    # This queue stores operations like "resume pending ops on startup", but we also consult the OpLedger.
    # Also used for TreeDiffTask presently
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

        self._parent_child_task_dict: Dict[UUID, Set[UUID]] = {}
        self._child_parent_task_dict: Dict[UUID, UUID] = {}
        self._waiting_parent_task_dict: Dict[UUID, Task] = {}

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
                        if TRACE_ENABLED:
                            logger.debug(f'[{CENTRAL_EXEC_THREAD_NAME}] Running task CV timeout!')

                    if len(self._running_task_dict) < TASK_RUNNER_MAX_WORKERS:
                        task = self._get_next_task_to_run_nolock()
                        if task:
                            self._running_task_dict[task.task_uuid] = task
                        elif SUPER_DEBUG_ENABLED:
                            logger.debug(f'[{CENTRAL_EXEC_THREAD_NAME}] No queued tasks to run (currently running: {len(self._running_task_dict)})')
                    else:
                        logger.debug(f'[{CENTRAL_EXEC_THREAD_NAME}] Running task queue is at max capacity ({TASK_RUNNER_MAX_WORKERS}): '
                                     f'{self._print_running_task_dict()}')

                # Do this outside the CV:
                if task:
                    self._enqueue_task(task)
        finally:
            logger.info(f'[{CENTRAL_EXEC_THREAD_NAME}] Execution stopped')

    def _print_running_task_dict(self) -> str:
        str_list = []
        for task in self._running_task_dict.values():
            sec, ms = divmod(time_util.now_ms() - task.task_start_time_ms, 1000)
            elapsed_time_str = f'Runtime={sec}.{ms}s'
            str_list.append(f'{elapsed_time_str}: {task}')
        # TODO: better solution than newline...
        return '\n'.join(str_list)

    def _enqueue_task(self, task: Task):
        future = self._be_task_runner.enqueue_task(task)
        callback = partial(self._on_task_done, task)
        # This will call back immediately if the task already completed:
        future.add_done_callback(callback)

    def _get_from_queue(self, priority: ExecPriority) -> Optional[Task]:
        try:
            task = self._exec_queue_dict[priority].get_nowait()
            logger.debug(f'[{CENTRAL_EXEC_THREAD_NAME}] Got task with priority: {ExecPriority.LOAD_0.name}: {task.task_func.__name__}'
                         f' uuid: {task.task_uuid}')
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

    def _on_task_done(self, done_task: Task, future: Future):

        with self._running_task_cv:
            if SUPER_DEBUG_ENABLED:
                # whether it succeeded, failed or was cancelled is of little difference from our standpoint; just make a note
                if future.cancelled():
                    logger.debug(f'Task cancelled: func="{done_task.task_func.__name__}" uuid={done_task.task_uuid}, priority={done_task.priority.name}')
                else:
                    logger.debug(f'Task done: func="{done_task.task_func.__name__}" uuid={done_task.task_uuid}, priority={done_task.priority.name}')

            # Removing based on object identity should work for now, since we are in the same process:
            # Note: this is O(n). Best not to let the deque get too large
            self._running_task_dict.pop(done_task.task_uuid)
            logger.debug(f'RunningTaskDict now has {len(self._running_task_dict)} tasks')

            # Did this done_task spawn child tasks which need to be waited for?
            child_set_of_done_task = self._parent_child_task_dict.get(done_task.task_uuid, None)
            if child_set_of_done_task:
                logger.debug(f'Task {done_task.task_uuid} has children to run ({",".join([ str(u) for u in child_set_of_done_task])}): '
                             f'will run its next_task after they are done')
                # add to _waiting_parent_task_dict and do not remove it until ready to run its next_task
                self._waiting_parent_task_dict[done_task.task_uuid] = done_task
                self._running_task_cv.notify_all()
                return

            next_task, parent = self._find_next_task(done_task)
            if not next_task:
                # wake up main thread, and allow it to run next task in queue
                self._running_task_cv.notify_all()
                return

        assert next_task
        parent_uuid = parent.task_uuid if parent else None
        logger.debug(f'Submitting next task for completed task ({done_task.task_uuid}): {next_task.task_uuid} with parent={parent_uuid}: ')
        self.submit_async_task(next_task, parent_task=parent)

        # TODO: DELETE task if completely done, to prevent memory leaks due to circular references

    def _find_next_task(self, done_task: Task) -> Tuple[Optional[Task], Optional[Task]]:
        logger.debug(f'Task {done_task.task_uuid} has no children {": will run its" if done_task.next_task else "and no"} next task')
        # just set this here - will call it outside of lock
        next_task = done_task.next_task

        # Was this task a child of some other parent task?
        parent_uuid = self._child_parent_task_dict.pop(done_task.task_uuid, None)
        if parent_uuid:
            logger.debug(f'Task {done_task.task_uuid} was a child of parent task {parent_uuid}')
            parent_child_set = self._parent_child_task_dict.get(parent_uuid, None)
            if parent_child_set is None:
                raise RuntimeError(f'Serious internal error: state of parent & child tasks is inconsistent! '
                                   f'ParentChildDict={self._parent_child_task_dict} ChildParentDic={self._child_parent_task_dict}')
            # update the parent's child set; when it is empty, it is officially complete
            parent_child_set.remove(done_task.task_uuid)

            if next_task:
                # If task was a child of parent, make its next_task also a child of parent and run that before parent's next_task
                parent_child_set.add(next_task.task_uuid)
                # fail if not present
                existing_parent_task = self._waiting_parent_task_dict[parent_uuid]
                assert existing_parent_task.task_uuid == parent_uuid, f'Should be equal: {existing_parent_task.task_uuid} and {parent_uuid}'

                return next_task, existing_parent_task

            if not parent_child_set:
                # No next_task and no children left in parent task: run parent's next_task

                # clean up data structure: children are all completed:
                self._parent_child_task_dict.pop(parent_uuid)

                done_parent_task = self._waiting_parent_task_dict.pop(parent_uuid, None)
                if not done_parent_task:
                    raise RuntimeError(f'Serious internal error: failed to find expected parent task ({parent_uuid}) in waiting_parent_dict!)')
                logger.debug(f'Parent task {parent_uuid} has no children left; recursing')

                # Go up next level in the tree and repeat logic:
                return self._find_next_task(done_parent_task)

            else:
                logger.debug(f'Parent task {parent_uuid} still has {len(parent_child_set)} children left to run')
                return None, None
        else:
            return next_task, None

    def submit_async_task(self, task: Task, parent_task: Optional[Task] = None):
        """API: enqueue task to be executed, with given priority."""
        priority = task.priority

        # TODO: allow child tasks to be added directly to tasks, without needing the central executor

        if not isinstance(priority, ExecPriority):
            raise RuntimeError(f'Bad arg: {priority}')

        with self._running_task_cv:
            if parent_task:
                parent_task_uuid = parent_task.task_uuid
            else:
                parent_task_uuid = None
            logger.debug(f'Enqueuing task (priority: {priority.name}: func_name: "{task.task_func.__name__}" uuid: {task.task_uuid} '
                         f'parent: {parent_task_uuid})')

            if parent_task:
                if not self._running_task_dict.get(parent_task_uuid, None) and not self._waiting_parent_task_dict.get(parent_task_uuid):
                    raise RuntimeError(f'Cannot add task {task.task_uuid}: referenced parent task {parent_task_uuid} was not found! '
                                       f'Maybe it and all its children already completed?')

                # Do this *before* putting in queue, in case it gets picked up too quickly
                child_set = self._parent_child_task_dict.get(parent_task_uuid, None)
                if not child_set:
                    child_set = set()
                    self._parent_child_task_dict[parent_task_uuid] = child_set
                child_set.add(task.task_uuid)

                # Add pointer to parent
                self._child_parent_task_dict[task.task_uuid] = parent_task_uuid

            self._exec_queue_dict[priority].put_nowait(task)

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
