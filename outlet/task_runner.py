from concurrent.futures import ThreadPoolExecutor
import logging
from typing import Callable

from constants import TASK_RUNNER_MAX_WORKERS
from util.has_lifecycle import HasLifecycle
from util.stopwatch_sec import Stopwatch

logger = logging.getLogger(__name__)


# CLASS Task
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼

class Task:
    def __init__(self, app, task_func: Callable, *args):
        self.app = app
        self.task_func: Callable = task_func
        self.args = args

    def run(self):
        logger.debug(f'Starting task: "{self.task_func.__name__}"')
        task_time = Stopwatch()
        try:
            self.task_func(*self.args)
        except Exception as err:
            msg = f'Task "{self.task_func.__name__}" failed during execution'
            logger.exception(msg)
            self.app.window.show_error_ui(msg, repr(err))
            raise
        finally:
            logger.info(f'{task_time} Task returned: "{self.task_func.__name__}"')


# CLASS CentralTaskRunner
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼

class CentralTaskRunner(HasLifecycle):
    def __init__(self, app):
        HasLifecycle.__init__(self)
        self.app = app
        self._executor: ThreadPoolExecutor = ThreadPoolExecutor(max_workers=TASK_RUNNER_MAX_WORKERS, thread_name_prefix='TaskRunner-')

    def enqueue(self, task_func, *args):
        logger.debug(f'Submitting new task to executor: "{task_func.__name__}"')
        task = Task(self.app, task_func, *args)
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

        self.app = None

