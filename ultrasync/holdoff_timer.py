import threading
from typing import Callable, Optional
import logging

logger = logging.getLogger(__name__)


class HoldOffTimer:
    def __init__(self, holdoff_time_ms: int, task_func: Callable, *args, **kwargs):
        self._initial_delay_sec: float = (holdoff_time_ms / 1000.0)
        self._additional_delay_sec = 0
        self.function = task_func
        self.args = args if args is not None else []
        self.kwargs = kwargs if kwargs is not None else {}
        self._finished = threading.Event()
        self._lock = threading.Lock()
        self._thread: Optional[threading.Thread] = None

    def start_or_delay(self):
        """If a timer has not been started, then it starts one with the configured delay. If one has been started,
        then an additional delay is added so that effectively the timer restarts from now."""

        with self._lock:
            if self._finished.is_set() or not self._thread:
                logger.debug(f'Starting new timer for {self._initial_delay_sec}s...')
                self._finished = threading.Event()
                self._thread = threading.Thread(target=self._run, name='HoldOffTimer', args=self.args, kwargs=self.kwargs, daemon=True)
                self._thread.start()
            else:
                logger.debug(f'Adding {self._initial_delay_sec}s delay to existing timer...')
                # TODO: more exact timing, like in the function description. This should suffice for now though...
                self._additional_delay_sec = self._initial_delay_sec

    def cancel(self):
        """Stop the timer if it hasn't finished yet."""
        logger.debug(f'Cancelling timer')
        self._finished.set()

    def _run(self):
        logger.debug(f'RunThread started. Sleeping for {self._initial_delay_sec}s...')
        self._finished.wait(self._initial_delay_sec)
        while True:
            more_delay: float = 0
            with self._lock:
                if self._additional_delay_sec == 0:
                    break
                else:
                    more_delay: float = self._additional_delay_sec
                    self._additional_delay_sec = 0
            if more_delay:
                logger.debug(f'RunThread sleeping for another {more_delay}s...')
                self._finished.wait(more_delay)

        if not self._finished.is_set():
            logger.debug(f'Executing timer task: {self.function.__name__}')
            self.function(*self.args, **self.kwargs)

        logger.debug(f'Timer task finished: {self.function.__name__}')
        self._finished.set()
