# Abstraction for a progress monitoring mechanism.
# Provides hooks for possible GUI updates, but does not contain any GUI code


class ProgressMeter:
    def __init__(self, on_progress_made):
        self.progress = 0
        self.total = 0
        self.progress_made_callback = on_progress_made

    def set_total(self, total):
        self.total = total

    def add_progress(self, amount):
        self.progress += amount
        self.progress_made_callback(self.progress, self.total)