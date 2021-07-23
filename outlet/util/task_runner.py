import uuid
from concurrent.futures import Future, ThreadPoolExecutor
import logging
from typing import Callable, List, Optional

from constants import TASK_RUNNER_MAX_WORKERS
from global_actions import GlobalActions
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
        self.args = args

        self.priority = priority
        self.next_task: Optional[Task] = None
        self.on_error: Optional[Callable[[Exception], None]] = None
        """Note: the executor has no way of knowing whether a task failed. It is up to the author of the task to ensure that on_error() does
        the appropriate cleanup or notifies the relevant entities."""

        # set internally:
        self.task_uuid: uuid.UUID = uuid.uuid4()
        self.task_start_time_ms: Optional[int] = None

    def run(self):
        self.task_start_time_ms = time_util.now_ms()
        logger.debug(f'Starting task: "{self.task_func.__name__}" with args={self.args}, start_time_ms={self.task_start_time_ms}')
        task_time = Stopwatch()
        try:
            if len(self.args) == 0:
                self.task_func(self)
            else:
                self.task_func(self, *self.args)
        except Exception as err:
            msg = f'Task failed during execution: name="{self.task_func.__name__}" uuid={self.task_uuid}'
            logger.exception(msg)

            if self.on_error:
                logger.debug(f'Calling task.on_error() (task_uuid={self.task_uuid})')
                self.on_error(err)
                return

            GlobalActions.display_error_in_ui(msg, repr(err))
            raise
        finally:
            logger.info(f'{task_time} Task returned: name="{self.task_func.__name__}" uuid={self.task_uuid}')

    def add_next_task(self, next_task_func: Callable, *args):
        """Adds the given task to the end of the chain of tasks"""
        next_task = Task(self.priority, next_task_func, args)

        task = self

        while True:
            logger.debug(f'add_next_task(): looping')

            if task.next_task:
                task = task.next_task
            else:
                task.next_task = next_task
                return

    def __repr__(self):
        return f'Task(uuid={self.task_uuid} start_time_ms={self.task_start_time_ms} priority={self.priority} ' \
               f'func={self.task_func.__name__} args={self.args})'


class TaskRunner(HasLifecycle):
    """
    ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
    CLASS TaskRunner

    Really just a wrapper for ThreadPoolExecutor.
    ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼
    """
    def __init__(self):
        HasLifecycle.__init__(self)
        self._executor: ThreadPoolExecutor = ThreadPoolExecutor(max_workers=TASK_RUNNER_MAX_WORKERS, thread_name_prefix='TaskRunner-')

    def enqueue_task(self, task: Task) -> Future:
        logger.debug(f'Submitting new task to executor: name="{task.task_func.__name__}" uuid={task.task_uuid}')
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
