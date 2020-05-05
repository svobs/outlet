import os

import file_util


class FileTreeRecurser:
    def __init__(self, root_path, valid_suffixes):
        self.root_path = root_path
        self.valid_suffixes = valid_suffixes

    def recurse_through_dir_tree(self):
        for root, dirs, files in os.walk(self.root_path, topdown=True):
            for name in files:
                file_path = os.path.join(root, name)
                if file_util.is_target_type(file_path, self.valid_suffixes):
                    self.handle_target_file_type(file_path)
                else:
                    self.handle_non_target_file(file_path)

    def handle_target_file_type(self, file_path):
        # do nothing by default
        return

    def handle_non_target_file(self, file_path):
        # do nothing by default
        return
