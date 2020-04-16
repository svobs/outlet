import time


class ProgressMeter:
    """Abstraction for a progress monitoring mechanism.
       Provides hooks for possible GUI updates, but does not contain any GUI code"""
    def __init__(self, on_progress_made, config, status_delegate=None):
        self.progress = 0
        self.total = 0
        self.progress_made_callback = on_progress_made
        self.max_update_interval_sec = float(config.get('display.max_refresh_interval_sec', 0.0))
        self.status_delegate = status_delegate
        self.last_status_ts = time.time()

    def _is_time_to_update(self):
        current_ts = time.time()
        if current_ts > self.last_status_ts + self.max_update_interval_sec:
            self.last_status_ts = current_ts
            return True
        return False

    def set_status(self, msg):
        if self.status_delegate:
            self.status_delegate.set_status(msg)

    def set_total(self, total):
        self.progress = 0
        self.total = total

    def add_progress(self, amount):
        self.progress += amount
        if self._is_time_to_update():
            self.progress_made_callback(self, self.progress, self.total)
