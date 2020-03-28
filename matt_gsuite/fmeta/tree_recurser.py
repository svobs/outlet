import os
import fnmatch
from fmeta.fmeta import FMetaSet


class TreeRecurser:
    def __init__(self, root_path, valid_suffixes):
        self.root_path = root_path
        self.valid_suffixes = valid_suffixes
        self.fmeta_set = FMetaSet()

    def is_target_type(self, file_path):
        file_path_lower = file_path.lower()
        for suffix in self.valid_suffixes:
            regex = '*.' + suffix
            if fnmatch.fnmatch(file_path_lower, regex):
                return True
        return False

    def recurse_through_dir_tree(self):
        for root, dirs, files in os.walk(self.root_path, topdown=True):
            for name in files:
                file_path = os.path.join(root, name)
                if self.is_target_type(file_path):
                    self.handle_target_file_type(file_path)
                else:
                    self.handle_non_target_file(file_path)

        return self.fmeta_set

    def handle_target_file_type(self, file_path):
        # do nothing by default
        return

    def handle_non_target_file(self, file_path):
        # do nothing by default
        return
