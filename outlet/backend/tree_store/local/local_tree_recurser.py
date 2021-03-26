import os
from typing import Optional, Tuple

from util import file_util


class LocalTreeRecurser:
    """
    ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
    CLASS LocalTreeRecurser
    ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼
    """
    def __init__(self, root_path, valid_suffixes: Optional[Tuple[str]]):
        self.root_path: str = root_path
        self.valid_suffixes: Optional[Tuple[str]] = valid_suffixes

    def recurse_through_dir_tree(self):
        for root, dirs, files in os.walk(self.root_path, topdown=True):
            for name in dirs:
                dir_path = os.path.join(root, name)
                self.handle_dir(dir_path)
            for name in files:
                file_path = os.path.join(root, name)
                if not self.valid_suffixes or file_util.is_target_type(file_path, self.valid_suffixes):
                    self.handle_target_file_type(file_path)
                else:
                    self.handle_non_target_file(file_path)

    def handle_target_file_type(self, file_path):
        # do nothing by default
        return

    def handle_non_target_file(self, file_path):
        # do nothing by default
        return

    def handle_dir(self, dir_path: str):
        # do nothing by default
        return
