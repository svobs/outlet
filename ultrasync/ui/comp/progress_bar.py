import threading
import time

import ui.actions as actions
import logging

import gi

from constants import PROGRESS_BAR_MAX_MSG_LENGTH, PROGRESS_BAR_PULSE_STEP, PROGRESS_BAR_SLEEP_TIME_SEC

gi.require_version("Gtk", "3.0")
from gi.repository import GLib, Gtk

logger = logging.getLogger(__name__)


class ProgressBar:
    def __init__(self, config, sender_list):
        self._update_interval_ms = float(config.get('display.max_refresh_interval_sec', 0.001)) * 1000
        self.progressbar = Gtk.ProgressBar()

        # state:
        self._progress = 0
        self._total = 0
        self._indeterminate = False

        # lifecycle:
        self._stop_requested = False
        self._stop_request_count = 0
        self._start_request_count = 0

        self.progressbar.set_pulse_step(PROGRESS_BAR_PULSE_STEP)
        self.progressbar.hide()
        """For keeping track of start/stop requests when they can occur in any order"""

        for sender in sender_list:
            logger.debug(f'ProgressBar will listen for siganls from sender: {sender}')
            actions.connect(signal=actions.START_PROGRESS_INDETERMINATE, handler=self.on_start_progress_indeterminate, sender=sender)
            actions.connect(signal=actions.START_PROGRESS, handler=self.on_start_progress, sender=sender)
            actions.connect(signal=actions.PROGRESS_MADE, handler=self.on_progress_made, sender=sender)
            actions.connect(signal=actions.STOP_PROGRESS, handler=self.on_stop_progress, sender=sender)
            actions.connect(signal=actions.SET_PROGRESS_TEXT, handler=self.on_set_progress_text, sender=sender)

    def _start_animaion(self, total=None):
        self._start_request_count += 1
        logger.debug(f'StartProgress(): Starts: {self._start_request_count}, Stops: {self._stop_request_count}')
        if self._start_request_count != self._stop_request_count + 1:
            logger.debug('Discarding start request because we have not received enough stop requests yet')
            return

        if total:
            self._total = total
            self._progress = 0

        def start_animation():
            self.progressbar.show()
            self.progressbar.set_show_text(False)

        GLib.idle_add(start_animation)

        thread = threading.Thread(target=self.run_thread, daemon=True)
        thread.start()

    def on_start_progress_indeterminate(self, sender):
        self._indeterminate = True
        self._start_animaion()

    def on_start_progress(self, sender, total):
        self._indeterminate = False
        self._start_animaion(total=total)

    def on_progress_made(self, sender, progress):
        self._progress += progress

    def on_stop_progress(self, sender):
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
                    logger.error('Cannot calculate progress %: total is zero!')
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
            self._stop_requested = False
            # Stop looping
            self.progressbar.hide()
            logger.debug('Stopped')

        GLib.idle_add(stop)
