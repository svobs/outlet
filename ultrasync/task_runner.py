from concurrent.futures import ThreadPoolExecutor
import logging

logger = logging.getLogger(__name__)

MAX_WORKERS = 1


class CentralTaskRunner:
    def __init__(self):
        self.executor = ThreadPoolExecutor(max_workers=MAX_WORKERS)

    def enqueue(self, task_func, *args):
        future = self.executor.submit(task_func, *args)
        return future

    def shutdown(self):
        """Note: setting wait to False will not decrease the shutdown time.
        Rather, wait=True just means block here until all running tasks are completed (i.e. the thread
        pool threads appear to not be daemon threads."""
        self.executor.shutdown(wait=True)

