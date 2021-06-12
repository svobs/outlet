import threading
import time
from typing import Callable, Optional
import logging

logger = logging.getLogger(__name__)
counter: int = 0


class HoldOffTimer:
    """
    ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
    CLASS HoldOffTimer
    ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼
    """

    def __init__(self, holdoff_time_ms: int, task_func: Callable, *args, **kwargs):
        self._initial_delay_sec: float = (holdoff_time_ms / 1000.0)
        self.function = task_func
        self.args = args if args is not None else []
        self.kwargs = kwargs if kwargs is not None else {}
        self._sleep_util_time_sec: float = 0.0
        self._finished = threading.Event()
        self._lock = threading.Lock()
        self._thread: Optional[threading.Thread] = None

    def _reset_timer(self):
        self._sleep_util_time_sec = float(time.time()) + self._initial_delay_sec

    def start_or_delay(self):
        """If a timer has not been started, then it starts one with the configured delay. If one has been started,
        then an additional delay is added so that effectively the timer restarts from now."""

        with self._lock:
            self._reset_timer()
            if self._finished.is_set() or not self._thread:
                self._finished = threading.Event()
                global counter
                counter = counter + 1
                self._thread = threading.Thread(target=self._run, name=f'HoldOffTimer-{counter}', args=self.args, kwargs=self.kwargs, daemon=True)
                logger.debug(f'Starting new timer "{self._thread.name}" for {self._initial_delay_sec}s...')
                self._thread.start()
            # elif SUPER_DEBUG_ENABLED:
            #     # May see a lot of these in a row due to Python's single-threaded nature
            #     logger.debug(f'Set expiry = {self._initial_delay_sec}s from now for existing timer "{self._thread.name}"')

    def cancel(self):
        """Stop the timer if it hasn't finished yet."""
        logger.debug(f'Cancelling timer')
        self._finished.set()

    def _run(self):
        while True:
            with self._lock:
                # Disallow delay less than zero:
                additional_delay = max(self._sleep_util_time_sec - float(time.time()), 0)

            if additional_delay:
                logger.debug(f'{self._thread.name} sleeping for {"{0:.3f}".format(additional_delay)}s...')
                self._finished.wait(additional_delay)
            else:
                break

        with self._lock:
            if not self._finished.is_set():
                logger.debug(f'{self._thread.name} Executing timer task: {self.function.__name__}')
                self.function(*self.args, **self.kwargs)

            logger.debug(f'{self._thread.name} finished')
            self._finished.set()
            self._thread = None
