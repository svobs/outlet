import os

import logging
from typing import Optional

from constants import OBJ_TYPE_LOCAL_DISK
from model.category import Category
from model.display_id import DisplayId
from model.display_node import DisplayNode, ensure_int

logger = logging.getLogger(__name__)

# ⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛


class LocalFsDisplayId(DisplayId):
    def __init__(self, id_string, category: Category):
        super().__init__(id_string=id_string, category=category)

    @property
    def tree_type(self) -> int:
        return OBJ_TYPE_LOCAL_DISK

# ⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛


class FMeta(DisplayNode):
    def __init__(self, md5, sha256, size_bytes, sync_ts, modify_ts, change_ts, full_path, category=Category.NA):
        super().__init__(category)
        self.md5: Optional[str] = md5
        self.sha256: Optional[str] = sha256
        self._size_bytes: int = ensure_int(size_bytes)
        self.sync_ts: int = ensure_int(sync_ts)
        self.modify_ts: int = ensure_int(modify_ts)
        self.change_ts: int = ensure_int(change_ts)
        self.full_path: str = full_path

    def get_name(self):
        return os.path.split(self.full_path)[1]

    def get_relative_path(self, parent_tree):
        return parent_tree.get_relative_path_of(self)

    @classmethod
    def has_path(cls):
        return True

    @property
    def display_id(self):
        return LocalFsDisplayId(self.full_path, self.category)

    def get_icon(self):
        return self.category.name

    @classmethod
    def is_dir(cls):
        return False

    @property
    def size_bytes(self):
        return self._size_bytes

    @classmethod
    def is_ignored(cls):
        return False

    def is_newer_than(self, other):
        if self.sync_ts > other.sync_ts:
            return True
        if self.modify_ts > other.modify_ts:
            return True
        if self.change_ts > other.change_ts:
            return True
        return False

    def is_content_equal(self, other_entry):
        assert isinstance(other_entry, FMeta)
        return self.sha256 == other_entry.sha256 \
            and self.md5 == other_entry.md5 and self._size_bytes == other_entry._size_bytes

    def is_meta_equal(self, other_entry):
        assert isinstance(other_entry, FMeta)
        return self.full_path == other_entry.full_path and \
            self.modify_ts == other_entry.modify_ts and self.change_ts == other_entry.change_ts

    def matches(self, other_entry):
        return self.is_content_equal(other_entry) and self.is_meta_equal(other_entry)

    def __repr__(self):
        return f'FMeta(cat={self.category.name} md5={self.md5} sha256={self.sha256} modify_ts={self.modify_ts} path="{self.full_path}")'


class IgnoredFMeta(FMeta):
    def __init__(self, md5, sha256, size_bytes, sync_ts, modify_ts, change_ts, full_path):
        super().__init__(md5, sha256, size_bytes, sync_ts, modify_ts, change_ts, full_path, Category.Ignored)

    @classmethod
    def is_ignored(cls):
        return True
