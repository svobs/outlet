import os

import logging
from typing import Optional

import file_util
from model.category import Category
from model.display_node import DisplayNode, ensure_int

logger = logging.getLogger(__name__)


class FMeta(DisplayNode):
    def __init__(self, md5, sha256, size_bytes, sync_ts, modify_ts, change_ts, full_path, category=Category.NA, prev_path=None):
        super().__init__(category)
        self.md5: Optional[str] = md5
        self.sha256: Optional[str] = sha256
        self.size_bytes: int = ensure_int(size_bytes)
        self.sync_ts: int = ensure_int(sync_ts)
        self.modify_ts: int = ensure_int(modify_ts)
        self.change_ts: int = ensure_int(change_ts)
        self.full_path: str = full_path
        # Only used if category == ADDED or MOVED
        self.prev_path: Optional[str] = prev_path

    def get_name(self):
        return os.path.split(self.full_path)[1]

    def get_relative_path(self, root_path):
        assert self.full_path.startswith(root_path), f'FMeta full path ({self.full_path}) does not contain root ({root_path})'
        return file_util.strip_root(self.full_path, root_path)

    @classmethod
    def has_path(cls):
        return True

    @classmethod
    def is_dir(cls):
        return False

    def is_content_equal(self, other_entry):
        assert isinstance(other_entry, FMeta)
        return self.sha256 == other_entry.sha256 \
            and self.md5 == other_entry.md5 and self.size_bytes == other_entry.size_bytes

    def is_meta_equal(self, other_entry):
        assert isinstance(other_entry, FMeta)
        return self.full_path == other_entry.full_path and \
            self.modify_ts == other_entry.modify_ts and self.change_ts == other_entry.change_ts

    def matches(self, other_entry):
        return self.is_content_equal(other_entry) and self.is_meta_equal(other_entry)

    def is_ignored(self):
        return self.category == Category.Ignored

    def __repr__(self):
        return f'FMeta(cat={self.category.name} md5={self.md5} sha256={self.sha256} modify_ts={self.modify_ts} path="{self.full_path}")'
