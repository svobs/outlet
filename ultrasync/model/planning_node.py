from abc import ABC

import file_util
from model.category import Category


class PlanningNode(ABC):
    """
    Planning nodes represent work which has not yet been done, such as copying a file.
    They can be thought of as 'ghosts of a possible future'. As such, they should not be
    cached like other objects.
    """
    def __init__(self):
        pass


class FMetaDecorator(PlanningNode):
    def __init__(self, fmeta):
        super().__init__()
        self.fmeta = fmeta

    @property
    def original_full_path(self):
        return self.fmeta.full_path

    @property
    def md5(self):
        return self.fmeta.md5

    @property
    def sha256(self):
        return self.fmeta.sha256

    @property
    def size_bytes(self):
        return self.fmeta.size_bytes

    @property
    def sync_ts(self):
        return self.fmeta.sync_ts

    @property
    def modify_ts(self):
        return self.fmeta.modify_ts

    @property
    def change_ts(self):
        return self.fmeta.change_ts

    @property
    def full_path(self):
        return self.fmeta.full_path

    def get_name(self):
        return self.fmeta.get_name()

    def get_relative_path(self, root_path):
        return self.fmeta.get_relative_path(root_path)

    @classmethod
    def has_path(cls):
        return True

    @classmethod
    def is_dir(cls):
        return False

    @classmethod
    def is_ignored(cls):
        return False

    @property
    def category(self):
        return Category.NA


class FileToAdd(FMetaDecorator):
    # NOTE: this decorates its enclosed FMeta, EXCEPT for full_path and get_relative_path(),
    # which use the destination path, because this node is placed until a different subtree.
    # TODO: this may be a huge bug bomb
    def __init__(self, fmeta, dest_path):
        super().__init__(fmeta)
        self.dest_path = dest_path

    @property
    def full_path(self):
        return self.dest_path

    def get_relative_path(self, root_path):
        assert self.full_path.startswith(root_path), f'FileToAdd full path ({self.full_path}) does not contain root ({root_path})'
        return file_util.strip_root(self.full_path, root_path)

    # TODO: KLUDGE! Get rid of the category system
    @property
    def category(self):
        return Category.ADDED


class FileToMove(FMetaDecorator):
    # NOTE: this decorates its enclosed FMeta, EXCEPT for full_path and get_relative_path()
    def __init__(self, fmeta, dest_path):
        super().__init__(fmeta)
        self.dest_path = dest_path

    @property
    def full_path(self):
        return self.dest_path

    def get_relative_path(self, root_path):
        assert self.full_path.startswith(root_path), f'FileToMove full path ({self.full_path}) does not contain root ({root_path})'
        return file_util.strip_root(self.full_path, root_path)

    # TODO: KLUDGE! Get rid of the category system
    @property
    def category(self):
        return Category.MOVED
