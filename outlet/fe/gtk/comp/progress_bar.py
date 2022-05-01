import threading
import time

from signal_constants import Signal
import logging

import gi

from constants import PROGRESS_BAR_MAX_MSG_LENGTH, PROGRESS_BAR_PULSE_STEP, PROGRESS_BAR_SLEEP_TIME_SEC
from util.has_lifecycle import HasLifecycle

gi.require_version("Gtk", "3.0")
from gi.repository import GLib, Gtk

logger = logging.getLogger(__name__)


class ProgressBar(HasLifecycle):
    """
    ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
    CLASS ProgressBar
    ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼
    """
    def __init__(self, app_config, sender_list):
        HasLifecycle.__init__(self)
        self._update_interval_ms = float(app_config.get_config('display.progress_bar.max_refresh_interval_sec', 0.001)) * 1000
        self.progressbar = Gtk.ProgressBar()

        # state:
        self._progress = 0
        self._total = 0
        self._indeterminate = False

        # lifecycle:
        self._stop_requested = False
        self._stop_request_count = 0
        self._start_request_count = 0
        self._lock = threading.Lock()

        self.progressbar.set_pulse_step(PROGRESS_BAR_PULSE_STEP)
        self.progressbar.hide()
        """For keeping track of start/stop requests when they can occur in any order"""

        for sender in sender_list:
            logger.debug(f'ProgressBar will listen for siganls from sender: {sender}')
            self.connect_dispatch_listener(signal=Signal.START_PROGRESS_INDETERMINATE, receiver=self.on_start_progress_indeterminate, sender=sender)
            self.connect_dispatch_listener(signal=Signal.START_PROGRESS, receiver=self.on_start_progress, sender=sender)
            self.connect_dispatch_listener(signal=Signal.PROGRESS_MADE, receiver=self.on_progress_made, sender=sender)
            self.connect_dispatch_listener(signal=Signal.STOP_PROGRESS, receiver=self.on_stop_progress, sender=sender)
            self.connect_dispatch_listener(signal=Signal.SET_PROGRESS_TEXT, receiver=self.on_set_progress_text, sender=sender)

        HasLifecycle.start(self)

    def _start_animation(self, total=None):
        self._start_request_count += 1
        logger.debug(f'StartProgress() total={total}: Starts: {self._start_request_count}, Stops: {self._stop_request_count}')

        # FIXME: trying to use the same progress bar for multiple concurrent operations is madness
        if total:
            self._total += total

        if self._start_request_count != self._stop_request_count + 1:
            logger.debug('Discarding start request because we have not received enough stop requests yet')
            return

        def start_animation():
            self.progressbar.show()
            self.progressbar.set_show_text(False)

        GLib.idle_add(start_animation)

        thread = threading.Thread(target=self.run_thread, daemon=True, name='ProgressBarThread')
        thread.start()

    def on_start_progress_indeterminate(self, sender):
        with self._lock:
            self._indeterminate = True
            self._start_animation()

    def on_start_progress(self, sender, total):
        logger.debug(f'Received {Signal.START_PROGRESS} signal from {sender}')

        with self._lock:
            self._indeterminate = False
            self._start_animation(total=total)

    def on_progress_made(self, sender, progress):
        self._progress += progress

    def on_stop_progress(self, sender):
        with self._lock:
            self._stop_request_count += 1
            logger.debug(f'StopProress(): Starts: {self._start_request_count}, Stops: {self._stop_request_count}')
            if self._start_request_count > self._stop_request_count:
                logger.debug('Discarding stop request because we have not received enough start requests yet')
                return
            logger.debug(f'Requesting stop of progress animation')
            self._stop_requested = True

    def on_set_progress_text(self, sender, msg):
        if len(msg) > PROGRESS_BAR_MAX_MSG_LENGTH:
            msg = msg[:(PROGRESS_BAR_MAX_MSG_LENGTH-3)] + '...'

        def set_text(m):
            self.progressbar.set_show_text(True)
            self.progressbar.set_text(m)
        GLib.idle_add(set_text, msg)

    def run_thread(self):
        """
        Update value on the progress bar
        """
        def throb():
            if self._indeterminate:
                self.progressbar.pulse()
            else:
                if self._total == 0:
                    logger.debug('Cannot calculate progress %: total is zero!')
                    return
                new_value = self._progress / self._total
                if new_value > 1:
                    new_value = 1

                self.progressbar.set_fraction(new_value)

        logger.debug('Starting animation')
        while not self._stop_requested:
            time.sleep(PROGRESS_BAR_SLEEP_TIME_SEC)
            GLib.idle_add(throb)

        def stop():
            with self._lock:
                self._stop_requested = False
                self._total = 0
                self._progress = 0
                # Stop looping
                self.progressbar.hide()
                logger.debug('Stopped')

        GLib.idle_add(stop)
