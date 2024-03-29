import os
from pathlib import PurePosixPath
from typing import Optional, Tuple

from util import file_util


class LocalDiskTreeRecurser:
    """
    ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
    CLASS LocalDiskTreeRecurser
    ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼
    """
    def __init__(self, root_path, project_dir: str, valid_suffixes: Optional[Tuple[str]]):
        self.root_path: str = root_path
        self.valid_suffixes: Optional[Tuple[str]] = valid_suffixes
        self.project_dir = project_dir

    def recurse_through_dir_tree(self):
        for root, dirs, files in os.walk(self.root_path, topdown=True):
            for name in dirs:
                dir_path = os.path.join(root, name)
                if not self._is_in_project_tree(dir_path):
                    self.handle_dir(dir_path)
            for name in files:
                file_path = os.path.join(root, name)
                if not self._is_in_project_tree(file_path):
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

    def _is_in_project_tree(self, path: str):
        return PurePosixPath(path).is_relative_to(self.project_dir)


class FileCounter(LocalDiskTreeRecurser):
    """
    ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
    CLASS LocalDiskTreeScanner

    Does a quick walk of the filesystem and counts the files which are of interest
    ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼
    """

    def __init__(self, root_path, project_dir: str):
        LocalDiskTreeRecurser.__init__(self, root_path, project_dir, valid_suffixes=None)
        self.files_to_scan = 0
        self.dirs_to_scan = 0

    def handle_target_file_type(self, file_path):
        self.files_to_scan += 1

    def handle_non_target_file(self, file_path):
        self.files_to_scan += 1

    def handle_dir(self, dir_path: str):
        self.dirs_to_scan += 1
