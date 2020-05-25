import logging
from typing import Optional

from index.uid_generator import UID
from model.category import Category
from model.node_identifier import ensure_int, LocalFsIdentifier
from model.display_node import DisplayNode

logger = logging.getLogger(__name__)

# ⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛


class FMeta(DisplayNode):
    def __init__(self, uid: UID, md5, sha256, size_bytes, sync_ts, modify_ts, change_ts, full_path: str, category=Category.NA):
        super().__init__(LocalFsIdentifier(full_path=full_path, uid=uid, category=category))
        self.md5: Optional[str] = md5
        self.sha256: Optional[str] = sha256
        self._size_bytes: int = ensure_int(size_bytes)
        self.sync_ts: int = ensure_int(sync_ts)
        self.modify_ts: int = ensure_int(modify_ts)
        self.change_ts: int = ensure_int(change_ts)

    @classmethod
    def has_path(cls):
        return True

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
        return f'FMeta({self.node_identifier} md5={self.md5} sha256={self.sha256} modify_ts={self.modify_ts})'
