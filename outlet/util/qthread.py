import logging
import threading
import time
from abc import ABC, abstractmethod
from collections import deque
from typing import Deque

from util.has_lifecycle import HasLifecycle

logger = logging.getLogger(__name__)


# CLASS QThread
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
class QThread(HasLifecycle, threading.Thread, ABC):
    """Daemon thread which processes items from a queue and blocks until items are added to the queue"""
    def __init__(self, name: str, initial_sleep_sec: float = 0):
        HasLifecycle.__init__(self)
        threading.Thread.__init__(self, target=self._run_thread, name=name, daemon=True)
        self.initial_sleep_sec: float = initial_sleep_sec
        self._shutdown: bool = False
        self._queue: Deque = deque()
        self._cv_can_get = threading.Condition()
        self._struct_lock = threading.Lock()

    def start(self):
        HasLifecycle.start(self)
        threading.Thread.start(self)

    def shutdown(self):
        HasLifecycle.shutdown(self)

        if self._shutdown:
            return

        logger.debug(f'Shutting down {self.name}')
        self._shutdown = True

        with self._cv_can_get:
            # unblock thread:
            self._cv_can_get.notifyAll()

    def enqueue(self, item):
        with self._struct_lock:
            self._queue.append(item)

        with self._cv_can_get:
            self._cv_can_get.notifyAll()

    @abstractmethod
    def process_single_item(self, item):
        pass

    def on_thread_start(self):
        pass

    def _run_thread(self):
        logger.info(f'Starting {self.name}...')

        # Wait for CacheMan to finish starting up so as not to deprive it of resources:
        self.on_thread_start()

        while not self._shutdown:
            item = None

            with self._struct_lock:
                if len(self._queue) > 0:
                    item = self._queue.popleft()

            if item:
                try:
                    self.process_single_item(item)
                except RuntimeError:
                    logger.exception(f'Unexpected error while processing item: {item}')
                continue
            else:
                logger.debug(f'[{self.name}] No pending ops; sleeping {self.initial_sleep_sec} sec then waiting till notified...')
                time.sleep(self.initial_sleep_sec)  # in seconds

            with self._cv_can_get:
                self._cv_can_get.wait()
