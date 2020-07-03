from concurrent.futures import ThreadPoolExecutor
import logging
from typing import Callable

from util.stopwatch_sec import Stopwatch

logger = logging.getLogger(__name__)

MAX_WORKERS = 1


# CLASS Task
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼

class Task:
    def __init__(self, application, task_func: Callable, *args):
        self.application = application
        self.task_func: Callable = task_func
        self.args = args

    def run(self):
        task_time = Stopwatch()
        try:
            self.task_func(*self.args)
        except Exception as err:
            msg = f'Task "{self.task_func.__name__}" failed during execution'
            logger.exception(msg)
            self.application.window.show_error_ui(msg, repr(err))
            raise
        finally:
            logger.info(f'{task_time} Task returned: "{self.task_func.__name__}"')


# CLASS CentralTaskRunner
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼

class CentralTaskRunner:
    def __init__(self, application):
        self.application = application
        self.executor: ThreadPoolExecutor = ThreadPoolExecutor(max_workers=MAX_WORKERS)

    def enqueue(self, task_func, *args):
        logger.debug(f'Submitting new task to executor: "{task_func.__name__}"')
        task = Task(self.application, task_func, *args)
        future = self.executor.submit(task.run)
        return future

    def shutdown(self):
        """Note: setting wait to False will not decrease the shutdown time.
        Rather, wait=True just means block here until all running tasks are completed (i.e. the thread
        pool threads appear to not be daemon threads."""
        self.executor.shutdown(wait=True)

