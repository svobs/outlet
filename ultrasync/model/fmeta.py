import logging
import os
from typing import Optional

from constants import NOT_TRASHED
from index.uid import UID
from model.category import Category
from model.node_identifier import ensure_int, LocalFsIdentifier, NodeIdentifier
from model.display_node import ContainerNode, DisplayNode

logger = logging.getLogger(__name__)

SUPER_DEBUG = False


def _ensure_bool(val):
    try:
        return bool(val)
    except ValueError:
        pass
    return val


# CLASS LocalFileNode
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼


class LocalFileNode(DisplayNode):
    def __init__(self, uid: UID, md5, sha256, size_bytes, sync_ts, modify_ts, change_ts, full_path: str, exists: bool, category=Category.NA):
        super().__init__(LocalFsIdentifier(full_path=full_path, uid=uid, category=category))
        self._md5: Optional[str] = md5
        self._sha256: Optional[str] = sha256
        self._size_bytes: int = ensure_int(size_bytes)
        self._sync_ts: int = ensure_int(sync_ts)
        self._modify_ts: int = ensure_int(modify_ts)
        self._change_ts: int = ensure_int(change_ts)
        self._exists = _ensure_bool(exists)

    def get_icon(self):
        return self.category.name

    @classmethod
    def is_file(cls):
        return True

    @classmethod
    def is_dir(cls):
        return False

    def get_size_bytes(self):
        return self._size_bytes

    def get_etc(self):
        return None

    @property
    def md5(self):
        return self._md5

    @md5.setter
    def md5(self, md5):
        self._md5 = md5

    @property
    def sha256(self):
        return self._sha256

    @sha256.setter
    def sha256(self, sha256):
        self._sha256 = sha256

    @property
    def sync_ts(self):
        return self._sync_ts

    @property
    def modify_ts(self):
        return self._modify_ts

    @modify_ts.setter
    def modify_ts(self, modify_ts):
        self._modify_ts = modify_ts

    @property
    def change_ts(self):
        return self._change_ts

    @change_ts.setter
    def change_ts(self, change_ts):
        self._change_ts = change_ts

    @property
    def trashed(self):
        return NOT_TRASHED

    def is_content_equal(self, other_entry):
        assert isinstance(other_entry, LocalFileNode)
        return self.sha256 == other_entry.sha256 \
            and self._md5 == other_entry._md5 and self._size_bytes == other_entry._size_bytes

    def is_meta_equal(self, other_entry):
        assert isinstance(other_entry, LocalFileNode)
        return self.full_path == other_entry.full_path and \
            self._modify_ts == other_entry._modify_ts and self._change_ts == other_entry._change_ts

    def matches(self, other_entry):
        return self.is_content_equal(other_entry) and self.is_meta_equal(other_entry)

    def exists(self) -> bool:
        """Whether the object represented by this node actually exists currently, or it is just planned to exist or is an ephemeral node."""
        return self._exists

    def set_exists(self, does_exist: bool):
        self._exists = does_exist

    def __repr__(self):
        return f'LocalFileNode({self.node_identifier} md5={self._md5} sha256={self.sha256} size_bytes={self._size_bytes} exists={self.exists()} modify_ts={self._modify_ts})'


# CLASS LocalDirNode
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼

class LocalDirNode(ContainerNode):
    """
    Represents a generic local directory.
    """

    def __init__(self, node_identifier: NodeIdentifier, exists: bool):
        super().__init__(node_identifier)
        self._exists = exists

    def exists(self) -> bool:
        """Whether the object represented by this node actually exists currently, or it is just planned to exist or is an ephemeral node."""
        return self._exists

    def set_exists(self, does_exist: bool):
        self._exists = does_exist

    def __repr__(self):
        return f'LocalDirNode({self.node_identifier} exists={self.exists()} {self.get_summary()})'
