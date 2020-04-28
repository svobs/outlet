from abc import ABC


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


class FileToAdd(FMetaDecorator):
    def __init__(self, fmeta, dest_path):
        super().__init__(fmeta)
        self.dest_path = dest_path


class FileToMove(FMetaDecorator):
    def __init__(self, fmeta, dest_path):
        super().__init__(fmeta)
        self.dest_path = dest_path
