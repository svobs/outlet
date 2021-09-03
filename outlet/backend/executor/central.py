import logging
import threading
from collections import deque
from concurrent.futures import Future
from enum import IntEnum
from functools import partial
from queue import Empty, Queue
from typing import Deque, Dict, List, Optional, Set, Tuple
from uuid import UUID

from pydispatch import dispatcher

from backend.executor.command.cmd_executor import CommandExecutor
from constants import CENTRAL_EXEC_THREAD_NAME, EngineSummaryState, OP_EXECUTION_THREAD_NAME, SUPER_DEBUG_ENABLED, \
    TASK_EXEC_IMEOUT_SEC, TASK_RUNNER_MAX_WORKERS, TASK_TIME_WARNING_THRESHOLD_SEC, TRACE_ENABLED
from global_actions import GlobalActions
from signal_constants import ID_CENTRAL_EXEC, Signal
from util import time_util
from util.has_lifecycle import HasLifecycle
from util.task_runner import Task, TaskRunner

logger = logging.getLogger(__name__)


class ExecPriority(IntEnum):
    # Highest priority load requests: immediately visible nodes in UI tree.
    # For user-initiated refresh requests, this queue will be used if the nodes are already visible (TODO: this is not currently true)
    P1_USER_LOAD = 1

    # Cache loads from disk into memory (such as during startup)
    P3_BACKGROUND_CACHE_LOAD = 4

    # Updates to the cache based on disk monitoring, in batches:
    P4_LIVE_UPDATE = 4

    # GDrive whole tree downloads (in chunks)
    P5_GDRIVE_TREE_DOWNLOAD = 5

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
        self._be_task_runner = TaskRunner()
        self.enable_op_execution = backend.get_config('executor.enable_op_execution')
        self._lock = threading.Lock()
        self._running_task_cv = threading.Condition(self._lock)

        self._PRIORITIES = [ExecPriority.P1_USER_LOAD, ExecPriority.P3_BACKGROUND_CACHE_LOAD, ExecPriority.P5_GDRIVE_TREE_DOWNLOAD, ExecPriority.P4_LIVE_UPDATE,
                            ExecPriority.P6_SIGNATURE_CALC, ExecPriority.P7_USER_OP_EXECUTION]

        # -- QUEUES --
        self._submitted_task_queue_dict: Dict[ExecPriority, Queue[Task]] = {

            ExecPriority.P1_USER_LOAD: Queue[Task](),

            ExecPriority.P3_BACKGROUND_CACHE_LOAD: Queue[Task](),

            ExecPriority.P5_GDRIVE_TREE_DOWNLOAD: Queue[Task](),

            ExecPriority.P4_LIVE_UPDATE: Queue[Task](),

            ExecPriority.P6_SIGNATURE_CALC: Queue[Task](),

            ExecPriority.P7_USER_OP_EXECUTION: Queue[Task](),
        }

        self._next_task_queue_dict: Dict[ExecPriority, Queue[Task]] = {

            ExecPriority.P1_USER_LOAD: Queue[Task](),

            ExecPriority.P3_BACKGROUND_CACHE_LOAD: Queue[Task](),

            ExecPriority.P5_GDRIVE_TREE_DOWNLOAD: Queue[Task](),

            ExecPriority.P4_LIVE_UPDATE: Queue[Task](),

            ExecPriority.P6_SIGNATURE_CALC: Queue[Task](),

            ExecPriority.P7_USER_OP_EXECUTION: Queue[Task](),
        }

        self._running_task_dict: Dict[UUID, Task] = {}
        self._parent_child_task_dict: Dict[UUID, Deque[UUID]] = {}
        self._dependent_task_dict: Dict[UUID, Task] = {}

        self._central_exec_thread: threading.Thread = threading.Thread(target=self._run_central_exec_thread,
                                                                       name=CENTRAL_EXEC_THREAD_NAME, daemon=True)

    def start(self):
        logger.debug('Central Executor starting')
        HasLifecycle.start(self)

        self._command_executor.start()

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
        with self._running_task_cv:
            self._running_task_cv.notify_all()

        self.backend = None
        if self._command_executor:
            self._command_executor.shutdown()
            self._command_executor = None
        self._global_actions = None
        if self._be_task_runner:
            self._be_task_runner.shutdown()
            self._be_task_runner = None

        logger.debug('CentralExecutor shut down')

    def get_engine_summary_state(self) -> EngineSummaryState:
        with self._lock:
            # FIXME: need to revisit these categories
            if self._submitted_task_queue_dict[ExecPriority.P3_BACKGROUND_CACHE_LOAD].qsize() > 0 \
                    or self._submitted_task_queue_dict[ExecPriority.P5_GDRIVE_TREE_DOWNLOAD].qsize() > 0:
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
        with self._running_task_cv:
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
                task: Optional[Task] = None
                with self._running_task_cv:
                    # wait until we are notified of new task (assuming task queue is not full) or task finished (if task queue is full)
                    if not self._running_task_cv.wait(TASK_EXEC_IMEOUT_SEC):
                        if TRACE_ENABLED:
                            logger.debug(f'[{CENTRAL_EXEC_THREAD_NAME}] Running task CV timeout!')

                    if len(self._running_task_dict) < TASK_RUNNER_MAX_WORKERS:
                        task = self._get_next_task_to_run()
                        if task:
                            if TRACE_ENABLED:
                                logger.debug(f'[{CENTRAL_EXEC_THREAD_NAME}] Got task with priority {task.priority.name}: '
                                             f'"{task.task_func.__name__}" uuid={task.task_uuid}')
                            self._running_task_dict[task.task_uuid] = task
                        elif SUPER_DEBUG_ENABLED:
                            logger.debug(f'[{CENTRAL_EXEC_THREAD_NAME}] No tasks in queue ({len(self._running_task_dict)} currently running)')
                    else:
                        self._print_current_state_of_pipeline()

                # Do this outside the CV:
                if task and not self.was_shutdown:
                    self._enqueue_task(task)
        finally:
            logger.info(f'[{CENTRAL_EXEC_THREAD_NAME}] Execution stopped')

    def _print_current_state_of_pipeline(self):
        running_tasks_str, problem_tasks_str_list = self._get_running_task_dict_debug_info()

        logger.debug(f'[{CENTRAL_EXEC_THREAD_NAME}] STATE: RunningTaskQueue count={len(self._running_task_dict)}/{TASK_RUNNER_MAX_WORKERS} tasks='
                     f'[{running_tasks_str}] ParentChildDict count={len(self._parent_child_task_dict)} '
                     f'DependentTasks count={len(self._dependent_task_dict)} tasks={self._dependent_task_dict.values()})')
        for problem_task_str in problem_tasks_str_list:
            logger.warning(problem_task_str)

    def _get_running_task_dict_debug_info(self) -> Tuple[str, List[str]]:
        running_tasks_str_list = []
        problem_tasks_str_list = []
        for task in self._running_task_dict.values():
            sec, ms = divmod(time_util.now_ms() - task.task_start_time_ms, 1000)
            elapsed_time_str = f'{sec}.{ms}s'
            if sec > TASK_TIME_WARNING_THRESHOLD_SEC:
                problem_tasks_str_list.append(f'Task is taking a long time ({elapsed_time_str} so far): {task}')
            running_tasks_str_list.append(f'Task {task.task_uuid}: {elapsed_time_str}')
        running_tasks_str = '; '.join(running_tasks_str_list)

        return running_tasks_str, problem_tasks_str_list

    def _enqueue_task(self, task: Task):
        future = self._be_task_runner.enqueue_task(task)
        callback = partial(self._on_task_done, task)
        # This will call back immediately if the task already completed:
        future.add_done_callback(callback)

    def _get_from_queue(self, priority: ExecPriority) -> Optional[Task]:
        try:
            task = self._next_task_queue_dict[priority].get_nowait()
            return task
        except Empty:
            try:
                task = self._submitted_task_queue_dict[priority].get_nowait()
                return task
            except Empty:
                return None

    def _get_next_task_to_run(self) -> Optional[Task]:
        for priority in self._PRIORITIES:
            task = self._get_from_queue(priority)
            if task:
                return task

        if self.enable_op_execution:

            # Special case for op execution: we have both a queue and the ledger. The queue takes higher precedence.
            task = self._get_from_queue(ExecPriority.P7_USER_OP_EXECUTION)
            if task:
                return task

            try:
                command = self.backend.cacheman.get_next_command_nowait()
                if command:
                    if TRACE_ENABLED:
                        logger.debug(f'[{CENTRAL_EXEC_THREAD_NAME}] Got a command to execute: {command.__class__.__name__}')
                    return Task(ExecPriority.P7_USER_OP_EXECUTION, self._command_executor.execute_command, command,
                                self._command_executor.global_context, True)
            except RuntimeError as e:
                logger.exception(f'[{CENTRAL_EXEC_THREAD_NAME}] SERIOUS: caught exception while retreiving command: halting execution pipeline')
                self.backend.report_error(sender=ID_CENTRAL_EXEC, msg='Error reteiving command', secondary_msg=f'{e}')
                self._pause_op_execution(sender=ID_CENTRAL_EXEC)

        return None

    def _on_task_done(self, done_task: Task, future: Future):
        if TRACE_ENABLED:
            # whether it succeeded, failed or was cancelled is of little difference from our standpoint; just make a note
            if future.cancelled():
                logger.debug(f'Task cancelled: func="{done_task.task_func.__name__}" uuid={done_task.task_uuid}, priority={done_task.priority.name}')
            else:
                logger.debug(f'Task done: "{done_task.task_func.__name__}" uuid={done_task.task_uuid}, priority={done_task.priority.name}')

        with self._running_task_cv:

            # Removing based on object identity should work for now, since we are in the same process:
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
                first_child_task = self._dependent_task_dict[first_child_uuid]
                self._next_task_queue_dict[first_child_task.priority].put_nowait(first_child_task)
                self._running_task_cv.notify_all()
                return
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
            self._running_task_cv.notify_all()

        # TODO: DELETE task if completely done, to prevent memory leaks due to circular references

    def _find_next_task(self, done_task: Task) -> Optional[Task]:
        logger.debug(f'Task {done_task.task_uuid} ({done_task.task_func}) has no children'
                     f'{": will run its" if done_task.next_task else " and no"} next task')
        # just set this here - will call it outside of lock
        next_task = done_task.next_task

        # Was this task a child of some other parent task?
        if done_task.parent_task_uuid:
            logger.debug(f'Task {done_task.task_uuid} ({done_task.task_func}) was a child of parent task {done_task.parent_task_uuid}')
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
        priority = task.priority

        if not isinstance(priority, ExecPriority):
            raise RuntimeError(f'Bad arg: {priority}')

        with self._running_task_cv:
            logger.debug(f'Enqueuing task (priority: {priority.name}: func_name: "{task.task_func.__name__}" uuid: {task.task_uuid} '
                         f'parent: {task.parent_task_uuid})')

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

            if len(self._running_task_dict) < TASK_RUNNER_MAX_WORKERS:
                self._running_task_cv.notify_all()

    def notify(self):
        with self._running_task_cv:
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
