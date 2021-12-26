import logging
import pathlib
from abc import ABC
from typing import Optional, Tuple

from constants import IconId, IS_MACOS, OBJ_TYPE_DIR, OBJ_TYPE_FILE, TrashStatus
from logging_constants import SUPER_DEBUG_ENABLED
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

    def __init__(self, node_identifier: LocalNodeIdentifier, parent_uid: UID, trashed: TrashStatus, is_live: bool, sync_ts: int,
                 create_ts: int, modify_ts: int, change_ts: int):
        super().__init__(node_identifier, parent_uids=parent_uid)
        self._trashed: TrashStatus = trashed
        self._is_live = ensure_bool(is_live)

        self._sync_ts: int = ensure_int(sync_ts)
        self._create_ts: int = ensure_int(create_ts)
        self._modify_ts: int = ensure_int(modify_ts)

        # "Metadata Change Time" (UNIX "ctime") - not available on Windows
        self._change_ts: int = ensure_int(change_ts)

    def is_live(self) -> bool:
        """Whether the object represented by this node actually is live currently, or it is just planned to exist or is an ephemeral node."""
        return self._is_live

    def set_is_live(self, is_live: bool):
        self._is_live = is_live

    def update_from(self, other_node):
        Node.update_from(self, other_node)
        self._trashed = other_node.get_trashed_status()
        self._is_live = other_node.is_live()
        self._sync_ts: int = ensure_int(other_node.sync_ts)
        self._create_ts: int = ensure_int(other_node.create_ts)
        self._modify_ts: int = ensure_int(other_node.modify_ts)
        self._change_ts: int = ensure_int(other_node.change_ts)

    def derive_parent_path(self) -> str:
        return str(pathlib.Path(self.get_single_path()).parent)

    def get_single_parent_uid(self) -> UID:
        if isinstance(self._parent_uids, list):
            if len(self._parent_uids) != 1:
                raise RuntimeError(f'Missing parent: {self}')
            return self._parent_uids[0]
        assert isinstance(self._parent_uids, UID)
        return self._parent_uids

    @property
    def sync_ts(self):
        return self._sync_ts

    @sync_ts.setter
    def sync_ts(self, sync_ts: int):
        self._sync_ts = sync_ts

    @property
    def create_ts(self):
        return self._create_ts

    @create_ts.setter
    def create_ts(self, create_ts):
        self._create_ts = create_ts

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


class LocalDirNode(LocalNode):
    """
    ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
    CLASS LocalDirNode

    Represents a generic local directory.
    ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼
    """

    def __init__(self, node_identifier: LocalNodeIdentifier, parent_uid, trashed: TrashStatus, is_live: bool, sync_ts: int,
                 create_ts: int, modify_ts: int, change_ts: int, all_children_fetched: bool):
        LocalNode.__init__(self, node_identifier, parent_uid, trashed, is_live, sync_ts, create_ts, modify_ts, change_ts)
        self.dir_stats: Optional[DirectoryStats] = None
        self.all_children_fetched: bool = ensure_bool(all_children_fetched)

    def update_from(self, other_node):
        assert isinstance(other_node, LocalDirNode)
        LocalNode.update_from(self, other_node)
        self.dir_stats = other_node.dir_stats
        self.set_is_live(ensure_bool(other_node.is_live()))
        self.all_children_fetched = other_node.all_children_fetched

    def is_parent_of(self, potential_child_node: Node):
        if potential_child_node.device_uid == self.device_uid:
            rel_path = potential_child_node.get_single_path().replace(self.get_single_path(), '')
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
        return self.uid, self.get_single_parent_uid(), self.get_single_path(), self.get_trashed_status(), self.is_live(), \
               self.sync_ts, self.create_ts, self.modify_ts, self.change_ts, self.all_children_fetched

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

    def meta_matches(self, other_node) -> bool:
        return isinstance(other_node, LocalDirNode) and \
               other_node.create_ts == self.create_ts and \
               other_node.modify_ts == self.modify_ts and \
               other_node.change_ts == self.change_ts

    def __eq__(self, other):
        """Compares against the node's metadata. Matches ONLY the node's identity and content; not its parents, children, or derived path"""
        if isinstance(other, LocalDirNode) and \
                other.node_identifier.node_uid == self.node_identifier.node_uid and \
                other.node_identifier.device_uid == self.node_identifier.device_uid and \
                other.name == self.name and \
                other._create_ts == self._create_ts and \
                other._modify_ts == self._modify_ts and \
                other._change_ts == self._change_ts and \
                other.get_trashed_status() == self.get_trashed_status() and \
                other._is_live == self._is_live and \
                other.all_children_fetched == self.all_children_fetched and \
                other.get_icon() == self.get_icon():
            return True

        return False

    def __ne__(self, other):
        return not self.__eq__(other)

    def __repr__(self):
        return f'LocalDirNode({self.node_identifier} parent_uid={self.get_single_parent_uid()} trashed={self._trashed} is_live={self.is_live()} ' \
               f'create_ts={self._create_ts} modify_ts={self._modify_ts} change_ts={self._change_ts} size_bytes={self.get_size_bytes()} ' \
               f'all_children_fetched={self.all_children_fetched}")'


class LocalFileNode(LocalNode):
    """
    ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
    CLASS LocalFileNode
    ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼
    """

    def __init__(self, node_identifier: LocalNodeIdentifier, parent_uid: UID, md5, sha256, size_bytes, sync_ts,
                 create_ts, modify_ts, change_ts, trashed, is_live: bool):
        super().__init__(node_identifier, parent_uid, trashed, is_live, sync_ts, create_ts, modify_ts, change_ts)
        self._md5: Optional[str] = md5
        self._sha256: Optional[str] = sha256
        self._size_bytes: int = ensure_int(size_bytes)

    def update_from(self, other_node):
        assert isinstance(other_node, LocalFileNode)
        LocalNode.update_from(self, other_node)
        self._md5: Optional[str] = other_node.md5
        self._sha256: Optional[str] = other_node.sha256
        self._size_bytes: int = ensure_int(other_node.get_size_bytes())

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

    @classmethod
    def has_tuple(cls) -> bool:
        return True

    def to_tuple(self) -> Tuple:
        return self.uid, self.get_single_parent_uid(), self.md5, self.sha256, self._size_bytes, self.sync_ts, \
               self.create_ts, self.modify_ts, self.change_ts,  self.get_single_path(), self._trashed, self._is_live

    def has_signature(self) -> bool:
        return self._md5 is not None and self._sha256 is not None

    def copy_signature_if_meta_matches(self, other) -> bool:
        if self.meta_matches(other) and (other.md5 or other.sha256):
            if other.md5:
                if SUPER_DEBUG_ENABLED:
                    if self._md5 and self._md5 != other.md5:
                        logger.error(f'copy_signature_if_meta_matches(): meta matches but MD5s differ! this={self}, other={other}')
                    else:
                        logger.debug(f'Copying MD5 from node: {other.node_identifier}')

                self._md5 = other.md5

            if other.sha256:
                if SUPER_DEBUG_ENABLED:
                    if self._sha256 and self._sha256 != other.sha256:
                        logger.error(f'copy_signature_if_meta_matches(): meta matches but SHA256s differ! this={self}, other={other}')
                    else:
                        logger.debug(f'Copying SHA256 from node: {other.node_identifier}')

                self._sha256 = other.sha256

            if SUPER_DEBUG_ENABLED:
                _check_update_sanity(other, self)
            return True

        return False

    def meta_matches(self, other_node) -> bool:
        return isinstance(other_node, LocalFileNode) and \
                other_node.create_ts == self.create_ts and \
                other_node.modify_ts == self.modify_ts and \
                other_node.change_ts == self.change_ts and \
                other_node.get_size_bytes() == self.get_size_bytes()

    def update_signature_and_timestamps_from(self, other):
        assert isinstance(other, Node), f'Not a node: {other}'
        self.md5 = other.md5
        self.set_size_bytes(other.get_size_bytes())
        self.modify_ts = other.modify_ts

        if isinstance(other, LocalFileNode):
            self.change_ts = other.change_ts
            self.sha256 = other.sha256

    def __eq__(self, other):
        """Compares against the node's metadata. Matches ONLY the node's identity and content; not its parents, children, or derived path"""
        if isinstance(other, LocalFileNode) and \
                other._md5 == self._md5 and \
                other._sha256 == self._sha256 and \
                other.node_identifier.node_uid == self.node_identifier.node_uid and \
                other.node_identifier.device_uid == self.node_identifier.device_uid and \
                other._create_ts == self._create_ts and \
                other._modify_ts == self._modify_ts and \
                other._change_ts == self._change_ts and \
                other.get_trashed_status() == self.get_trashed_status() and \
                other._is_live == self._is_live and \
                other.get_icon() == self.get_icon():
            return True

        return False

    def __ne__(self, other):
        return not self.__eq__(other)

    def __repr__(self):
        return f'LocalFileNode({self.node_identifier} parent_uid={self.get_single_parent_uid()} md5={self._md5} sha256={self.sha256} ' \
               f'size_bytes={self._size_bytes} trashed={self._trashed} is_live={self.is_live()} ' \
               f'create_ts={self._create_ts} modify_ts={self._modify_ts} change_ts={self._change_ts})'


def _check_update_sanity(old_node: LocalFileNode, new_node: LocalFileNode):
    try:
        if not old_node:
            raise RuntimeError(f'old_node is empty!')

        if not isinstance(old_node, LocalFileNode):
            # Internal error; try to recover
            logger.error(f'Invalid node type for old_node: {type(old_node)}. Will overwrite cache entry')
            return

        if not new_node:
            raise RuntimeError(f'new_node is empty!')

        if not isinstance(new_node, LocalFileNode):
            raise RuntimeError(f'Invalid node type for new_node: {type(new_node)}')

        if not old_node.create_ts:
            logger.debug(f'old_node has no create_ts. Skipping create_ts comparison (Old={old_node} New={new_node}')
        elif not new_node.create_ts:
            raise RuntimeError(f'new_node is missing create_ts!')
        elif new_node.create_ts < old_node.create_ts:
            if IS_MACOS:
                # Known bug in MacOS
                logger.debug(
                    f'File {new_node.node_identifier}: update has older create_ts ({new_node.create_ts}) than prev version ({old_node.create_ts})'
                    f'(probably MacOS bug)')
            else:
                logger.warning(
                    f'File {new_node.node_identifier}: update has older create_ts ({new_node.create_ts}) than prev version ({old_node.create_ts})')

        if not old_node.modify_ts:
            logger.debug(f'old_node has no modify_ts. Skipping modify_ts comparison (Old={old_node} New={new_node}')
        elif not new_node.modify_ts:
            raise RuntimeError(f'new_node is missing modify_ts!')
        elif new_node.modify_ts < old_node.modify_ts:
            if IS_MACOS:
                # Known bug in MacOS
                logger.debug(
                    f'File {new_node.node_identifier}: update has older modify_ts ({new_node.modify_ts}) than prev version ({old_node.modify_ts})'
                    f'(probably MacOS bug)')
            else:
                logger.warning(
                    f'File {new_node.node_identifier}: update has older modify_ts ({new_node.modify_ts}) than prev version ({old_node.modify_ts})')

        if not old_node.change_ts:
            logger.debug(f'old_node has no change_ts. Skipping change_ts comparison (Old={old_node} New={new_node}')
        elif not new_node.change_ts:
            raise RuntimeError(f'new_node is missing change_ts!')
        elif new_node.change_ts < old_node.change_ts:
            if IS_MACOS:
                # Known bug in MacOS
                logger.debug(
                    f'File {new_node.node_identifier}: update has older modify_ts ({new_node.modify_ts}) than prev version ({old_node.modify_ts})'
                    f'(probably MacOS bug)')
            else:
                logger.warning(
                    f'File {new_node.node_identifier}: update has older change_ts ({new_node.change_ts}) than prev version ({old_node.change_ts})')

        if new_node.get_size_bytes() != old_node.get_size_bytes() and new_node.md5 == old_node.md5 and old_node.md5:
            logger.warning(f'File {new_node.node_identifier}: update has same MD5 ({new_node.md5}) ' +
                           f'but different size: (old={old_node.get_size_bytes()}, new={new_node.get_size_bytes()})')
    except Exception as e:
        logger.error(f'Error checking update sanity! Old={old_node} New={new_node}: {repr(e)}')
        raise
