import uuid
from concurrent.futures import Future, ThreadPoolExecutor
import logging
from typing import Callable, Optional

from constants import SUPER_DEBUG_ENABLED
from global_actions import GlobalActions
from signal_constants import ID_CENTRAL_EXEC
from util import time_util
from util.has_lifecycle import HasLifecycle
from util.stopwatch_sec import Stopwatch

logger = logging.getLogger(__name__)


class Task:
    """
    ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
    CLASS Task
    ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼
    """
    def __init__(self, priority, task_func: Callable, *args):
        self.task_func: Callable = task_func
        self._args = args

        self.priority = priority
        self.parent_task_uuid: Optional[uuid.UUID] = None
        self.next_task: Optional[Task] = None
        self.on_error: Optional[Callable[[Exception], None]] = None
        """Note: the executor has no way of knowing whether a task failed. It is up to the author of the task to ensure that on_error() does
        the appropriate cleanup or notifies the relevant entities."""

        # set internally:
        self.task_uuid: uuid.UUID = uuid.uuid4()
        self.task_start_time_ms: Optional[int] = None

    def create_child_task(self, task_func: Callable, *args):
        child_task = Task(self.priority, task_func, *args)
        child_task.parent_task_uuid = self.task_uuid
        return child_task

    def run(self):
        self.task_start_time_ms = time_util.now_ms()
        logger.info(f'Task starting: "{self.task_func.__name__}", {self.priority.name}, {self.task_uuid}')
        task_time = Stopwatch()  # TODO: maybe just use task_start_time_ms and get rid of this var
        try:
            if not self._args or len(self._args) == 0:
                self.task_func(self)
            else:
                self.task_func(self, *self._args)
        except Exception as err:
            msg = f'Task failed during execution: "{self.task_func.__name__}", {self.task_uuid}'
            logger.exception(msg)

            if self.on_error:
                logger.debug(f'Calling task.on_error() (task_uuid={self.task_uuid})')
                self.on_error(err)
                return

            GlobalActions.display_error_in_ui(ID_CENTRAL_EXEC, msg, repr(err))
            raise
        finally:
            logger.info(f'{task_time} Task returned: "{self.task_func.__name__}", {self.priority.name}, {self.task_uuid}')

    def add_next_task(self, next_task_func: Callable, *args):
        """Adds the given task to the end of the chain of tasks"""
        if not args or len(args) == 0:
            next_task = Task(self.priority, next_task_func)
        else:
            next_task = Task(self.priority, next_task_func, *args)

        task = self

        while True:
            if SUPER_DEBUG_ENABLED:
                logger.debug(f'add_next_task(): looping. Examining next task ({task.next_task.task_uuid if task.next_task else None})')

            if task.next_task:
                task = task.next_task
            else:
                task.next_task = next_task
                return

    def __repr__(self):
        next_task_uuid = self.next_task.task_uuid if self.next_task else 'None'
        return f'Task(uuid={self.task_uuid} pri={self.priority.name} func={self.task_func.__name__} arg_count={len(self._args)} ' \
               f'parent_task={self.parent_task_uuid} next_task={next_task_uuid})'


class TaskRunner(HasLifecycle):
    """
    ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
    CLASS TaskRunner

    Really just a wrapper for ThreadPoolExecutor.
    ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼
    """
    def __init__(self, max_workers: int):
        HasLifecycle.__init__(self)
        self._executor: ThreadPoolExecutor = ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix='TaskRunner-')

    def enqueue_task(self, task: Task) -> Future:
        logger.debug(f'Submitting new task to executor: "{task.task_func.__name__}" {task.priority.name} {task.task_uuid}')
        future: Future = self._executor.submit(task.run)
        return future

    def shutdown(self):
        """Note: setting wait to False will not decrease the shutdown time.
        Rather, wait=True just means block here until all running tasks are completed (i.e. the thread
        pool threads appear to not be daemon threads."""
        HasLifecycle.shutdown(self)

        if self._executor:
            self._executor.shutdown(wait=True)
            self._executor = None
