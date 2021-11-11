import logging
import threading
from collections import deque
from concurrent.futures import Future
from enum import IntEnum
from functools import partial
from queue import Empty, Queue
from typing import Deque, Dict, List, Optional, Tuple
from uuid import UUID

from pydispatch import dispatcher

from backend.executor.command.cmd_executor import CommandExecutor
from constants import CENTRAL_EXEC_THREAD_NAME, EngineSummaryState, OP_EXECUTION_THREAD_NAME, SUPER_DEBUG_ENABLED, \
    TASK_EXEC_IMEOUT_SEC, TASK_RUNNER_MAX_CONCURRENT_USER_OP_TASKS, TASK_RUNNER_MAX_COCURRENT_NON_USER_OP_TASKS, \
    TASK_TIME_WARNING_THRESHOLD_SEC, TRACE_ENABLED
from global_actions import GlobalActions
from signal_constants import ID_CENTRAL_EXEC, Signal
from util import time_util
from util.has_lifecycle import HasLifecycle
from util.task_runner import Task, TaskRunner

logger = logging.getLogger(__name__)


class ExecPriority(IntEnum):
    # Highest priority load requests: immediately visible nodes in UI tree, from disk if necessary
    P1_USER_LOAD = 1

    # Cache loads from disk into memory (such as during startup)
    P3_BACKGROUND_CACHE_LOAD = 4

    # Updates to the cache based on disk monitoring, in batches.
    P4_LIVE_UPDATE = 4

    # GDrive whole tree downloads (in chunks), diffs, and other tasks which should be run after the caches have settled.
    P5_LONG_RUNNING_USER_TASK = 5

    # Signature calculations: IO-dominant
    P6_SIGNATURE_CALC = 6

    # This queue stores operations like "resume pending ops on startup", but we also consult the OpManager.
    # Also used for TreeDiffTask presently
    P7_USER_OP_EXECUTION = 7


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
        self._max_workers: int = TASK_RUNNER_MAX_COCURRENT_NON_USER_OP_TASKS + TASK_RUNNER_MAX_CONCURRENT_USER_OP_TASKS
        self._be_task_runner = TaskRunner(max_workers=self._max_workers)
        self.enable_op_execution = backend.get_config('executor.enable_op_execution')
        self._struct_lock = threading.Lock()
        self._running_task_cv = threading.Condition()

        self._PRIORITY_LIST = [ExecPriority.P1_USER_LOAD,
                               ExecPriority.P3_BACKGROUND_CACHE_LOAD,
                               ExecPriority.P4_LIVE_UPDATE,
                               ExecPriority.P5_LONG_RUNNING_USER_TASK,
                               ExecPriority.P6_SIGNATURE_CALC]

        # -- QUEUES --
        self._submitted_task_queue_dict: Dict[ExecPriority, Queue[Task]] = {

            ExecPriority.P1_USER_LOAD: Queue[Task](),

            ExecPriority.P3_BACKGROUND_CACHE_LOAD: Queue[Task](),

            ExecPriority.P4_LIVE_UPDATE: Queue[Task](),

            ExecPriority.P5_LONG_RUNNING_USER_TASK: Queue[Task](),

            ExecPriority.P6_SIGNATURE_CALC: Queue[Task](),
        }

        self._next_task_queue_dict: Dict[ExecPriority, Queue[Task]] = {

            ExecPriority.P1_USER_LOAD: Queue[Task](),

            ExecPriority.P3_BACKGROUND_CACHE_LOAD: Queue[Task](),

            ExecPriority.P5_LONG_RUNNING_USER_TASK: Queue[Task](),

            ExecPriority.P4_LIVE_UPDATE: Queue[Task](),

            ExecPriority.P6_SIGNATURE_CALC: Queue[Task](),
        }

        self._running_task_dict: Dict[UUID, Task] = {}
        self._parent_child_task_dict: Dict[UUID, Deque[UUID]] = {}
        self._dependent_task_dict: Dict[UUID, Task] = {}

        self._central_exec_thread: threading.Thread = threading.Thread(target=self._run_central_exec_thread,
                                                                       name=CENTRAL_EXEC_THREAD_NAME, daemon=True)

        self._was_notified = False

    def start(self):
        logger.debug('[CentralExecutor] Startup started')
        HasLifecycle.start(self)

        self._command_executor.start()

        self._global_actions.start()

        self._central_exec_thread.start()

        if not self.enable_op_execution:
            logger.warning(f'{OP_EXECUTION_THREAD_NAME} is disabled!')

        self.connect_dispatch_listener(signal=Signal.PAUSE_OP_EXECUTION, receiver=self._pause_op_execution)
        self.connect_dispatch_listener(signal=Signal.RESUME_OP_EXECUTION, receiver=self._start_op_execution)

        logger.debug('[CentralExecutor] Startup done')

    def shutdown(self):
        logger.debug('[CentralExecutor] Shutdown started')
        if self.was_shutdown:
            return

        HasLifecycle.shutdown(self)
        self.notify()

        self.backend = None
        if self._command_executor:
            self._command_executor.shutdown()
            self._command_executor = None
        self._global_actions = None
        if self._be_task_runner:
            self._be_task_runner.shutdown()
            self._be_task_runner = None

        logger.debug('[CentralExecutor] Shutdown done')

    def get_engine_summary_state(self) -> EngineSummaryState:
        with self._struct_lock:
            # FIXME: need to revisit these categories
            if self._submitted_task_queue_dict[ExecPriority.P3_BACKGROUND_CACHE_LOAD].qsize() > 0 \
                    or self._submitted_task_queue_dict[ExecPriority.P5_LONG_RUNNING_USER_TASK].qsize() > 0:
                # still getting up to speed on the BE
                return EngineSummaryState.RED

            total_enqueued = self._submitted_task_queue_dict[ExecPriority.P1_USER_LOAD].qsize() + \
                             self._submitted_task_queue_dict[ExecPriority.P6_SIGNATURE_CALC].qsize()

            if total_enqueued > 0:
                return EngineSummaryState.YELLOW

            pending_op_count = self.backend.cacheman.get_pending_op_count()
            if pending_op_count > 0:
                return EngineSummaryState.YELLOW

            return EngineSummaryState.GREEN

    def is_task_or_descendent_running(self, task_uuid: UUID) -> bool:
        with self._struct_lock:
            if self._running_task_dict.get(task_uuid, None):
                logger.debug(f'is_task_or_descendent_running(): task is still running: {task_uuid}')
                return True
            if self._parent_child_task_dict.get(task_uuid, None):
                logger.debug(f'is_task_or_descendent_running(): task is still waiting on descdendent to complete: {task_uuid}')
                return True
            else:
                logger.debug(f'is_task_or_descendent_running(): task is NOT running: {task_uuid}')
                return False

    def wait_until_queue_depleted(self, priority: ExecPriority):
        self._submitted_task_queue_dict.get(priority).join()

    # Central Executor Thread
    # ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼
    def _run_central_exec_thread(self):
        logger.info(f'[{CENTRAL_EXEC_THREAD_NAME}] Starting thread...')

        try:
            while not self.was_shutdown:
                task = self._check_for_queued_task()
                if task:
                    self._enqueue_in_task_runner(task)
                else:
                    if SUPER_DEBUG_ENABLED:
                        logger.debug(f'[{CENTRAL_EXEC_THREAD_NAME}] No tasks or commands. Waiting for running task CV')
                    with self._running_task_cv:
                        if not self._was_notified:  # could have been notified while run loop was doing other work
                            # wait until we are notified of new task (assuming task queue is not full)
                            # or task finished (if task queue is full)
                            if not self._running_task_cv.wait(TASK_EXEC_IMEOUT_SEC):
                                if SUPER_DEBUG_ENABLED:
                                    logger.debug(f'[{CENTRAL_EXEC_THREAD_NAME}] Running task CV timeout!')

        finally:
            logger.info(f'[{CENTRAL_EXEC_THREAD_NAME}] Execution stopped')

    def _get_user_op_count(self) -> int:
        user_op_count = 0
        for running_task in self._running_task_dict.values():
            if running_task.priority == ExecPriority.P7_USER_OP_EXECUTION:
                user_op_count += 1
        return user_op_count

    def _check_for_queued_task(self) -> Optional[Task]:
        if TRACE_ENABLED:
            logger.debug('_check_for_queued_task() entered')
        task: Optional[Task] = None

        with self._struct_lock:
            with self._running_task_cv:
                self._was_notified = False

            if self.was_shutdown:
                # check this again in case shutdown broke us out of our CV:
                return None

            total_count = len(self._running_task_dict)
            if total_count >= self._max_workers:
                # already at max capacity
                logger.debug(f'Already running max number of tasks ({self._max_workers}); will not pull more at this time')
                return None

            # Count number of user ops already running:
            user_op_count = self._get_user_op_count()
            non_user_op_count = total_count - user_op_count

            if non_user_op_count < TASK_RUNNER_MAX_COCURRENT_NON_USER_OP_TASKS:
                if TRACE_ENABLED:
                    logger.debug('Getting next task to run')
                task = self._get_next_task_from_non_user_op_queues()
                if task:
                    if SUPER_DEBUG_ENABLED:
                        logger.debug(f'[{CENTRAL_EXEC_THREAD_NAME}] Got task with priority {task.priority.name}: '
                                     f'"{task.task_func.__name__}" uuid={task.task_uuid}')
                    self._running_task_dict[task.task_uuid] = task
                    # fall through:
                else:
                    logger.debug(f'[{CENTRAL_EXEC_THREAD_NAME}] CheckForQueuedTasks(): task queues are empty ({non_user_op_count} still running)')
            else:
                logger.debug(f'[{CENTRAL_EXEC_THREAD_NAME}] CheckForQueuedTasks(): Already running max number of concurrent regular tasks '
                             f'({TASK_RUNNER_MAX_COCURRENT_NON_USER_OP_TASKS}) ')

        if self.was_shutdown:
            return

        if task:
            return task

        # Now handle user ops. Do this outside the CV:
        if self.enable_op_execution and user_op_count < TASK_RUNNER_MAX_CONCURRENT_USER_OP_TASKS:
            try:
                if TRACE_ENABLED:
                    logger.debug(f'[{CENTRAL_EXEC_THREAD_NAME}] Getting next task from OpGraph')
                command = self.backend.cacheman.get_next_command_nowait()
                if command:
                    logger.debug(f'[{CENTRAL_EXEC_THREAD_NAME}] CheckForQueuedTasks(): returning task for UserOp ({command.op}, '
                                 f'cmd = {command.__class__.__name__})')
                    task = Task(ExecPriority.P7_USER_OP_EXECUTION, self._command_executor.execute_command, command,
                                self._command_executor.global_context, True)
                    with self._struct_lock:
                        self._running_task_dict[task.task_uuid] = task
                    return task
                elif SUPER_DEBUG_ENABLED:
                    logger.debug(f'[{CENTRAL_EXEC_THREAD_NAME}] OpGraph has no tasks for us at this time')

            except RuntimeError as e:
                logger.exception(f'[{CENTRAL_EXEC_THREAD_NAME}] SERIOUS: caught exception while retreiving command: halting execution pipeline')
                self.backend.report_error(sender=ID_CENTRAL_EXEC, msg='Error retreiving command', secondary_msg=f'{e}')
                self._pause_op_execution(sender=ID_CENTRAL_EXEC)

            return task
        else:
            logger.debug(f'[{CENTRAL_EXEC_THREAD_NAME}] CheckForQueuedTasks(): Already running max number of concurrent UserOp tasks '
                         f'({TASK_RUNNER_MAX_CONCURRENT_USER_OP_TASKS}) ')

    def _print_current_state_of_pipeline(self):
        running_tasks_str, problem_tasks_str_list = self._get_running_task_dict_debug_info()

        logger.debug(f'[{CENTRAL_EXEC_THREAD_NAME}] STATE: RunningTaskQueue(count={len(self._running_task_dict)} max={self._max_workers}) tasks='
                     f'[{running_tasks_str}] ParentChildDict(count={len(self._parent_child_task_dict)}) '
                     f'DependentTasksDict(count={len(self._dependent_task_dict)} tasks={[t.task_uuid for t in self._dependent_task_dict.values()]})')

        for problem_task_str in problem_tasks_str_list:
            logger.warning(problem_task_str)

    def _get_running_task_dict_debug_info(self) -> Tuple[str, List[str]]:
        running_tasks_str_list = []
        problem_tasks_str_list = []
        for task in self._running_task_dict.values():
            task_start_time = task.task_start_time_ms
            if task_start_time:
                sec, ms = divmod(time_util.now_ms() - task.task_start_time_ms, 1000)
                elapsed_time_str = f'{sec}.{ms}s'
                if sec > TASK_TIME_WARNING_THRESHOLD_SEC:
                    problem_tasks_str_list.append(f'Task is taking a long time ({elapsed_time_str} so far): {task}')
            else:
                elapsed_time_str = '(not started)'
            running_tasks_str_list.append(f'Task {task.priority.name} {task.task_uuid} ("{task.task_func}"): {elapsed_time_str}')
        running_tasks_str = '; '.join(running_tasks_str_list)

        return running_tasks_str, problem_tasks_str_list

    def _enqueue_in_task_runner(self, task: Task):
        future = self._be_task_runner.enqueue_task(task)
        callback = partial(self._on_task_done, task)
        # This will call back immediately if the task already completed:
        future.add_done_callback(callback)

    def _get_from_queue(self, priority: ExecPriority) -> Optional[Task]:
        assert self._struct_lock.locked()

        try:
            task = self._next_task_queue_dict[priority].get_nowait()
            return task
        except Empty:
            try:
                task = self._submitted_task_queue_dict[priority].get_nowait()
                return task
            except Empty:
                return None

    def _get_next_task_from_non_user_op_queues(self) -> Optional[Task]:
        assert self._struct_lock.locked()

        for priority in self._PRIORITY_LIST:
            task = self._get_from_queue(priority)
            if task:
                return task

        return None

    def _on_task_done(self, done_task: Task, future: Future):
        if TRACE_ENABLED:
            # whether it succeeded, failed or was cancelled is of little difference from our standpoint; just make a note
            if future.cancelled():
                logger.debug(f'Task cancelled: func="{done_task.task_func.__name__}" uuid={done_task.task_uuid}, priority={done_task.priority.name}')
            else:
                logger.debug(f'Task done: "{done_task.task_func.__name__}" uuid={done_task.task_uuid}, priority={done_task.priority.name}')

        with self._struct_lock:

            # Note: this is O(n). Best not to let the deque get too large
            self._running_task_dict.pop(done_task.task_uuid)

            # Did this done_task spawn child tasks which need to be waited for?
            child_deque_of_done_task: Deque[UUID] = self._parent_child_task_dict.get(done_task.task_uuid, None)
            if child_deque_of_done_task:
                logger.debug(f'Task {done_task.task_uuid} has {len(child_deque_of_done_task)} children remaining '
                             f'({", ".join([ str(u) for u in child_deque_of_done_task])}): will enqueue its first child')
                # add to _dependent_task_dict and do not remove it until ready to run its next_task
                self._dependent_task_dict[done_task.task_uuid] = done_task
                # Dereference the first child and add it to the next_task queue
                first_child_uuid = child_deque_of_done_task[0]
                next_task = self._dependent_task_dict[first_child_uuid]
                self._next_task_queue_dict[next_task.priority].put_nowait(next_task)
            else:
                # no need for this reference anymore
                self._dependent_task_dict.pop(done_task.task_uuid, None)

                next_task = self._find_next_task(done_task)
                if next_task:
                    # special queue
                    logger.debug(f'Enqueuing next task {next_task.task_uuid} with parent={next_task.parent_task_uuid} '
                                 f'(for completed task {done_task.task_uuid})')
                    self._next_task_queue_dict[next_task.priority].put_nowait(next_task)

        # wake up main thread, and allow it to run next task in queue
        self.notify()

        # TODO: DELETE task if completely done, to prevent memory leaks due to circular references

    def _find_next_task(self, done_task: Task) -> Optional[Task]:
        logger.debug(f'Task {done_task.priority.name} {done_task.task_uuid} has no children'
                     f'{": will run its" if done_task.next_task else " and no"} next task')
        # just set this here - will call it outside of lock
        next_task = done_task.next_task

        # Was this task a child of some other parent task?
        if done_task.parent_task_uuid:
            logger.debug(f'Task {done_task.priority.name} {done_task.task_uuid} was a child of parent task {done_task.parent_task_uuid}')
            child_deque: Deque[UUID] = self._parent_child_task_dict.get(done_task.parent_task_uuid, None)
            if child_deque is None:
                raise RuntimeError(f'Serious internal error: state of parent & child tasks is inconsistent! '
                                   f'ParentChildDict={self._parent_child_task_dict} ChildTask={done_task}')
            # update the parent's child set; when it is empty, it is officially complete
            child_deque.remove(done_task.task_uuid)

            if next_task:
                # If task was a child of parent, make its next_task also a child of parent and run that before parent's next_task
                child_deque.appendleft(next_task.task_uuid)
                # fail if not present
                existing_parent_task = self._dependent_task_dict[done_task.parent_task_uuid]
                assert existing_parent_task.task_uuid == done_task.parent_task_uuid, \
                    f'Should be equal: {existing_parent_task.task_uuid} and {done_task.parent_task_uuid}'

                if not next_task.parent_task_uuid:
                    # we will forgive this and correct it:
                    next_task.parent_task_uuid = done_task.parent_task_uuid
                elif next_task.parent_task_uuid != done_task.parent_task_uuid:
                    # Programmer error when creating tasks
                    raise RuntimeError(f'Inconsistent state! Task ({done_task}) has a different parent than its next_task ({next_task})')

                self._dependent_task_dict[next_task.task_uuid] = next_task

                return next_task

            if not child_deque:
                # No next_task and no children left in parent task: run parent's next_task

                # clean up data structure: children are all completed:
                self._parent_child_task_dict.pop(done_task.parent_task_uuid)

                done_parent_task = self._dependent_task_dict.pop(done_task.parent_task_uuid, None)
                if not done_parent_task:
                    raise RuntimeError(f'Serious internal error: failed to find expected parent task '
                                       f'({done_task.parent_task_uuid}) in waiting_parent_dict!)')
                logger.debug(f'Parent task {done_parent_task.task_uuid} ({done_parent_task.task_func}) has no children left; recursing')

                # Go up next level in the tree and repeat logic:
                return self._find_next_task(done_parent_task)

            else:
                logger.debug(f'Parent task {done_task.parent_task_uuid} still has {len(child_deque)} children left to run: returning first child')
                child_uuid = child_deque[0]
                return self._dependent_task_dict[child_uuid]  # fail if not found
        else:
            return next_task

    def _add_child_to_parent(self, parent_task_uuid, child_task_uuid):
        child_deque = self._parent_child_task_dict.get(parent_task_uuid, None)
        if not child_deque:
            child_deque = deque()
            self._parent_child_task_dict[parent_task_uuid] = child_deque
        child_deque.append(child_task_uuid)

    def submit_async_task(self, task: Task):
        """API: enqueue task to be executed, with given priority."""
        if TRACE_ENABLED:
            logger.debug(f'Entered submit_async_task() with task: {task}')

        priority = task.priority

        if not isinstance(priority, ExecPriority):
            raise RuntimeError(f'Bad arg: {priority}')

        logger.debug(f'Enqueuing task "{task.task_func.__name__}" {priority.name} {task.task_uuid} parent={task.parent_task_uuid})')

        with self._struct_lock:
            if task.parent_task_uuid:
                if not self._running_task_dict.get(task.parent_task_uuid, None) and not self._dependent_task_dict.get(task.parent_task_uuid):
                    raise RuntimeError(f'Cannot add task {task.task_uuid}: referenced parent task {task.parent_task_uuid} was not found! '
                                       f'Maybe it and all its children already completed?')

                # Child tasks have priority over the _submitted_task_queue_dict and will be enqueued when appropriate
                self._add_child_to_parent(parent_task_uuid=task.parent_task_uuid, child_task_uuid=task.task_uuid)
                self._dependent_task_dict[task.task_uuid] = task
            else:
                # No parent: put in the regular queue:
                self._submitted_task_queue_dict[priority].put_nowait(task)

        # if should_send_notify:
        self.notify()

    def notify(self):
        with self._running_task_cv:
            self._was_notified = True
            self._running_task_cv.notify_all()

    # Op Execution State
    # ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼

    def _start_op_execution(self, sender):
        logger.info(f'Received signal "{Signal.RESUME_OP_EXECUTION.name}" from {sender}')
        self.enable_op_execution = True
        self.notify()
        logger.debug(f'Sending signal "{Signal.OP_EXECUTION_PLAY_STATE_CHANGED.name}" (is_enabled={self.enable_op_execution})')
        dispatcher.send(signal=Signal.OP_EXECUTION_PLAY_STATE_CHANGED, sender=ID_CENTRAL_EXEC, is_enabled=self.enable_op_execution)

    def _pause_op_execution(self, sender):
        logger.info(f'Received signal "{Signal.PAUSE_OP_EXECUTION.name}" from {sender}')
        self.enable_op_execution = False
        logger.debug(f'Sending signal "{Signal.OP_EXECUTION_PLAY_STATE_CHANGED.name}" (is_enabled={self.enable_op_execution})')
        dispatcher.send(signal=Signal.OP_EXECUTION_PLAY_STATE_CHANGED, sender=ID_CENTRAL_EXEC, is_enabled=self.enable_op_execution)
