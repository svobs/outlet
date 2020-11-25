from concurrent.futures import ThreadPoolExecutor
import logging
from typing import Callable

from constants import TASK_RUNNER_MAX_WORKERS
from global_actions import GlobalActions
from util.has_lifecycle import HasLifecycle
from util.stopwatch_sec import Stopwatch

logger = logging.getLogger(__name__)


# CLASS Task
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼

class Task:
    def __init__(self, task_func: Callable, *args, **kwargs):
        self.task_func: Callable = task_func
        self.args = args
        self.kwargs = kwargs

    def run(self):
        logger.debug(f'Starting task: "{self.task_func.__name__}"')
        task_time = Stopwatch()
        try:
            self.task_func(*self.args, **self.kwargs)
        except Exception as err:
            msg = f'Task "{self.task_func.__name__}" failed during execution'
            logger.exception(msg)
            GlobalActions.display_error_in_ui(msg, repr(err))
            raise
        finally:
            logger.info(f'{task_time} Task returned: "{self.task_func.__name__}"')


# CLASS TaskRunner
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼

class TaskRunner(HasLifecycle):
    def __init__(self):
        HasLifecycle.__init__(self)
        self._executor: ThreadPoolExecutor = ThreadPoolExecutor(max_workers=TASK_RUNNER_MAX_WORKERS, thread_name_prefix='TaskRunner-')

    def enqueue(self, task_func, *args, **kwargs):
        logger.debug(f'Submitting new task to executor: "{task_func.__name__}"')
        task = Task(task_func, *args, **kwargs)
        future = self._executor.submit(task.run)
        return future

    def shutdown(self):
        """Note: setting wait to False will not decrease the shutdown time.
        Rather, wait=True just means block here until all running tasks are completed (i.e. the thread
        pool threads appear to not be daemon threads."""
        HasLifecycle.shutdown(self)

        if self._executor:
            self._executor.shutdown(wait=True)
            self._executor = None
