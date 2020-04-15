class ProgressMeter:
    """Abstraction for a progress monitoring mechanism.
       Provides hooks for possible GUI updates, but does not contain any GUI code"""
    def __init__(self, on_progress_made, status_delegate=None):
        self.progress = 0
        self.total = 0
        self.progress_made_callback = on_progress_made
        self.status_delegate = status_delegate

    def set_status(self, msg):
        # TODO: throttle this by target FPS
        if self.status_delegate:
            self.status_delegate.set_status(msg)

    def set_total(self, total):
        self.progress = 0
        self.total = total

    def add_progress(self, amount):
        # TODO: throttle this by target FPS
        self.progress += amount
        self.progress_made_callback(self, self.progress, self.total)
