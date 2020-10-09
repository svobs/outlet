import logging
import os
import re
from abc import ABC
from typing import Optional, Tuple

from constants import ICON_GENERIC_DIR, NOT_TRASHED, OBJ_TYPE_DIR, OBJ_TYPE_FILE, TREE_TYPE_LOCAL_DISK
from model.node_identifier import ensure_bool, ensure_int, LocalFsIdentifier
from model.node.display_node import DisplayNode, HasChildList

logger = logging.getLogger(__name__)

SUPER_DEBUG = False


# CLASS LocalNode
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼

class LocalNode(DisplayNode, ABC):
    def __init__(self, node_identifier: LocalFsIdentifier, exists: bool):
        super().__init__(node_identifier)
        self._exists = ensure_bool(exists)

    @classmethod
    def get_tree_type(cls) -> int:
        return TREE_TYPE_LOCAL_DISK

    @property
    def trashed(self):
        # TODO: add support for trash
        return NOT_TRASHED

    def exists(self) -> bool:
        """Whether the object represented by this node actually exists currently, or it is just planned to exist or is an ephemeral node."""
        return self._exists

    def set_exists(self, does_exist: bool):
        self._exists = does_exist

    def update_from(self, other_node):
        DisplayNode.update_from(self, other_node)
        self._exists = other_node.exists()

    @property
    def name(self):
        assert self.node_identifier.full_path, f'For {type(self)}, uid={self.uid}'
        return os.path.basename(self.node_identifier.full_path)


# CLASS LocalFileNode
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼

class LocalFileNode(LocalNode):
    def __init__(self, node_identifier: LocalFsIdentifier, md5, sha256, size_bytes, sync_ts, modify_ts, change_ts, exists: bool):
        super().__init__(node_identifier, exists)
        self._md5: Optional[str] = md5
        self._sha256: Optional[str] = sha256
        self._size_bytes: int = ensure_int(size_bytes)
        self._sync_ts: int = ensure_int(sync_ts)
        self._modify_ts: int = ensure_int(modify_ts)
        self._change_ts: int = ensure_int(change_ts)

    def update_from(self, other_node):
        assert isinstance(other_node, LocalFileNode)
        DisplayNode.update_from(self, other_node)
        self._md5: Optional[str] = other_node.md5
        self._sha256: Optional[str] = other_node.sha256
        self._size_bytes: int = ensure_int(other_node.get_size_bytes())
        self._sync_ts: int = ensure_int(other_node.sync_ts)
        self._modify_ts: int = ensure_int(other_node.modify_ts)
        self._change_ts: int = ensure_int(other_node.change_ts)
        self._exists = ensure_bool(other_node.exists())

    def is_parent_of(self, potential_child_node: DisplayNode) -> bool:
        # A file can never be the parent of anything
        return False

    @classmethod
    def get_obj_type(cls):
        return OBJ_TYPE_FILE

    @classmethod
    def is_file(cls):
        return True

    @classmethod
    def is_dir(cls):
        return False

    def get_size_bytes(self):
        return self._size_bytes

    def set_size_bytes(self, size_bytes: int):
        self._size_bytes = size_bytes

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

    @classmethod
    def has_tuple(cls) -> bool:
        return True

    def to_tuple(self) -> Tuple:
        return self.uid, self.md5, self.sha256, self.get_size_bytes(), self.sync_ts, self.modify_ts, self.change_ts, self.full_path, self.exists()

    def __repr__(self):
        return f'LocalFileNode({self.node_identifier} md5={self._md5} sha256={self.sha256} size_bytes={self._size_bytes} ' \
               f'exists={self.exists()} modify_ts={self._modify_ts})'


# CLASS LocalDirNode
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼

class LocalDirNode(HasChildList, LocalNode):
    """
    Represents a generic local directory.
    """

    def __init__(self, node_identifier: LocalFsIdentifier, exists: bool):
        HasChildList.__init__(self)
        LocalNode.__init__(self, node_identifier, exists)

    def update_from(self, other_node):
        assert isinstance(other_node, LocalDirNode)
        HasChildList.update_from(self, other_node)
        LocalNode.update_from(self, other_node)

    def is_parent_of(self, potential_child_node: DisplayNode):
        if potential_child_node.get_tree_type() == TREE_TYPE_LOCAL_DISK:
            rel_path = re.sub(self.full_path, '', potential_child_node.full_path, count=1)
            if len(rel_path) > 0 and rel_path.startswith('/'):
                rel_path = rel_path[1:]
            return rel_path == potential_child_node.name
        return False

    @classmethod
    def has_tuple(cls) -> bool:
        return True

    def to_tuple(self) -> Tuple:
        return self.uid, self.full_path, self.exists()

    def __repr__(self):
        return f'LocalDirNode({self.node_identifier} exists={self.exists()} size_bytes={self.get_size_bytes()} summary="{self.get_summary()}")'

    @classmethod
    def get_obj_type(cls):
        return OBJ_TYPE_DIR

    def get_icon(self):
        return ICON_GENERIC_DIR

    @classmethod
    def is_file(cls):
        return False

    @classmethod
    def is_dir(cls):
        return True

    @property
    def sync_ts(self):
        # Local dirs are not currently synced to disk
        return None

    def __eq__(self, other):
        if not isinstance(other, LocalDirNode):
            return False

        return other.node_identifier == self.node_identifier and other.name == self.name and other.trashed == self.trashed \
            and other.exists() == self.exists()

    def __ne__(self, other):
        return not self.__eq__(other)
