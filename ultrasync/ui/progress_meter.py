class ProgressMeter:
    """Abstraction for a progress monitoring mechanism.
       Provides hooks for possible GUI updates, but does not contain any GUI code"""
    def __init__(self, on_progress_made, status_delegate=None):
        self.progress = 0
        self.total = 0
        self.progress_made_callback = on_progress_made
        self.status_delegate = status_delegate

    def set_status(self, msg):
        if self.status_delegate:
            self.status_delegate.set_status(msg)

    def set_total(self, total):
        if self.progress > total:
            raise RuntimeError(f'While setting total: progress ({self.progress} is already greater than total {total}!')
        self.total = total

    def add_progress(self, amount):
        self.progress += amount
        self.progress_made_callback(self, self.progress, self.total)
