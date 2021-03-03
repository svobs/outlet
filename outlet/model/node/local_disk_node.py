import logging
import pathlib
import re
from abc import ABC
from typing import Optional, Tuple

from constants import IconId, OBJ_TYPE_DIR, OBJ_TYPE_FILE, TrashStatus, TREE_TYPE_LOCAL_DISK
from model.node.directory_stats import DirectoryStats
from model.node.node import Node
from model.node_identifier import LocalNodeIdentifier
from model.uid import UID
from util.ensure import ensure_bool, ensure_int

logger = logging.getLogger(__name__)


class LocalNode(Node, ABC):
    """
    ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
    CLASS LocalNode
    ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼
    """

    def __init__(self, node_identifier: LocalNodeIdentifier, parent_uid: UID, trashed: TrashStatus, is_live: bool):
        super().__init__(node_identifier, parent_uids=parent_uid, trashed=trashed)
        self._is_live = ensure_bool(is_live)

    @classmethod
    def get_tree_type(cls) -> int:
        return TREE_TYPE_LOCAL_DISK

    def is_live(self) -> bool:
        """Whether the object represented by this node actually is live currently, or it is just planned to exist or is an ephemeral node."""
        return self._is_live

    def set_is_live(self, is_live: bool):
        self._is_live = is_live

    def update_from(self, other_node):
        Node.update_from(self, other_node)
        self._is_live = other_node.is_live()

    def derive_parent_path(self) -> str:
        return str(pathlib.Path(self.get_single_path()).parent)

    def get_single_parent(self) -> UID:
        if isinstance(self._parent_uids, list):
            if len(self._parent_uids) != 1:
                raise RuntimeError(f'Missing parent: {self}')
            return self._parent_uids[0]
        assert isinstance(self._parent_uids, UID)
        return self._parent_uids


class LocalDirNode(LocalNode):
    """
    ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
    CLASS LocalDirNode

    Represents a generic local directory.
    ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼
    """

    def __init__(self, node_identifier: LocalNodeIdentifier, parent_uid, trashed: TrashStatus, is_live: bool):
        LocalNode.__init__(self, node_identifier, parent_uid, trashed, is_live)
        self.dir_stats: Optional[DirectoryStats] = None

    def update_from(self, other_node):
        assert isinstance(other_node, LocalDirNode)
        LocalNode.update_from(self, other_node)
        self.dir_stats = other_node.dir_stats

    def is_parent_of(self, potential_child_node: Node):
        if potential_child_node.get_tree_type() == TREE_TYPE_LOCAL_DISK:
            rel_path = re.sub(self.get_single_path(), '', potential_child_node.get_single_path(), count=1)
            if len(rel_path) > 0 and rel_path.startswith('/'):
                rel_path = rel_path[1:]
            return rel_path == potential_child_node.name
        return False

    def get_size_bytes(self):
        if self.dir_stats:
            return self.dir_stats.get_size_bytes()
        return None

    def get_etc(self):
        if self.dir_stats:
            return self.dir_stats.get_etc()
        return None

    @classmethod
    def has_tuple(cls) -> bool:
        return True

    def to_tuple(self) -> Tuple:
        return self.uid, self.get_single_path(), self.get_trashed_status(), self.is_live()

    @classmethod
    def get_obj_type(cls):
        return OBJ_TYPE_DIR

    def get_default_icon(self):
        return IconId.ICON_GENERIC_DIR

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

        return other.node_identifier == self.node_identifier and other.name == self.name and other.get_trashed_status() == self.get_trashed_status() \
            and other._is_live == self._is_live

    def __ne__(self, other):
        return not self.__eq__(other)

    def __repr__(self):
        return f'LocalDirNode({self.node_identifier} parent_uid={self._parent_uids} trashed={self._trashed} is_live={self.is_live()} ' \
               f'size_bytes={self.get_size_bytes()}")'


class LocalFileNode(LocalNode):
    """
    ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
    CLASS LocalFileNode
    ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼
    """

    def __init__(self, node_identifier: LocalNodeIdentifier, parent_uid: UID, md5, sha256, size_bytes, sync_ts, modify_ts, change_ts, trashed,
                 is_live: bool):
        super().__init__(node_identifier, parent_uid, trashed, is_live)
        self._md5: Optional[str] = md5
        self._sha256: Optional[str] = sha256
        self._size_bytes: int = ensure_int(size_bytes)
        self._sync_ts: int = ensure_int(sync_ts)
        self._modify_ts: int = ensure_int(modify_ts)
        self._change_ts: int = ensure_int(change_ts)

    def update_from(self, other_node):
        assert isinstance(other_node, LocalFileNode)
        Node.update_from(self, other_node)
        self._md5: Optional[str] = other_node.md5
        self._sha256: Optional[str] = other_node.sha256
        self._size_bytes: int = ensure_int(other_node.get_size_bytes())
        self._sync_ts: int = ensure_int(other_node.sync_ts)
        self._modify_ts: int = ensure_int(other_node.modify_ts)
        self._change_ts: int = ensure_int(other_node.change_ts)
        self._is_live = ensure_bool(other_node.is_live())

    def is_parent_of(self, potential_child_node: Node) -> bool:
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

    @classmethod
    def has_tuple(cls) -> bool:
        return True

    def to_tuple(self) -> Tuple:
        return self.uid, self.md5, self.sha256, self._size_bytes, self.sync_ts, self.modify_ts, self.change_ts, self.get_single_path(), \
               self._trashed, self._is_live

    def __eq__(self, other):
        if not isinstance(other, LocalFileNode):
            return False

        return other.node_identifier == self.node_identifier and other._md5 == self._md5 and other._sha256 == self._sha256 \
               and other._modify_ts == self._modify_ts and other._change_ts == self._change_ts \
               and other.get_trashed_status() == self.get_trashed_status() and other._is_live == self._is_live

    def __ne__(self, other):
        return not self.__eq__(other)

    def __repr__(self):
        return f'LocalFileNode({self.node_identifier} parent_uid={self._parent_uids} md5={self._md5} sha256={self.sha256} ' \
               f'size_bytes={self._size_bytes} trashed={self._trashed} is_live={self.is_live()} modify_ts={self._modify_ts})'
