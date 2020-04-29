import ui.actions as actions
import logging

import gi
gi.require_version("Gtk", "3.0")
from gi.repository import GLib, Gtk

logger = logging.getLogger(__name__)


class ProgressBarComponent:
    def __init__(self, config, sender_list):
        self.update_interval_ms = float(config.get('display.max_refresh_interval_sec', 0.001)) * 1000
        self.progressbar = Gtk.ProgressBar()
        self.timeout_id = None
        self.progress = 0
        self.total = 0
        self.done = False
        self.progressbar.set_pulse_step(0.001)  # TODO: figure out why pulse is all over the place
        self.progressbar.hide()

        for sender in sender_list:
            logger.debug(f'ProgressBar will listen for siganls from sender: {sender}')
            actions.connect(signal=actions.START_PROGRESS_INDETERMINATE, handler=self.on_start_progress_indeterminate, sender=sender)
            actions.connect(signal=actions.START_PROGRESS, handler=self.on_start_progress, sender=sender)
            actions.connect(signal=actions.PROGRESS_MADE, handler=self.on_progress_made, sender=sender)
            actions.connect(signal=actions.STOP_PROGRESS, handler=self.on_stop_progress, sender=sender)
            actions.connect(signal=actions.SET_PROGRESS_TEXT, handler=self.on_set_progress_text, sender=sender)

    def _start_animaion(self, indeterminate):
        def start_animation():
            self.progressbar.show()
            self.timeout_id = GLib.timeout_add(self.update_interval_ms, self.on_timeout, indeterminate)
            logger.debug(f'Started a progress bar animation with timeout_id: {self.timeout_id}')

        GLib.idle_add(start_animation)

    def on_start_progress_indeterminate(self, sender):
        if self.timeout_id:
            raise RuntimeError(f'Looks like someone is trying to start a ProgressBar which has already been started: {self.timeout_id}')
        self.done = False
        self._start_animaion(True)

    def on_start_progress(self, sender, total):
        if self.timeout_id:
            raise RuntimeError(f'Looks like someone is trying to start a ProgressBar which has already been started: {self.timeout_id}')
        self.done = False
        self.progress = 0
        self.total = float(total)

        self._start_animaion(False)

    def on_progress_made(self, sender, progress):
        self.progress += progress

    def on_stop_progress(self, sender):
        self.done = True
        self.timeout_id = None

    def on_set_progress_text(self, sender, msg):
        def set_text():
            self.progressbar.set_show_text(True)
            self.progressbar.set_text(msg)
        GLib.idle_add(set_text)

    def on_timeout(self, indeterminate):
        """
        Update value on the progress bar
        """
        if self.done:
            self.progressbar.set_show_text(False)
            # Stop looping
            self.progressbar.hide()
            return False

        if indeterminate:
            self.progressbar.pulse()
        else:
            new_value = self.progress / self.total
            if new_value > 1:
                new_value = 1

            self.progressbar.set_fraction(new_value)

        # As this is a timeout function, return True so that it continues to get called
        return True

