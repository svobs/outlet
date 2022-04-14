import logging
import pathlib
from abc import ABC
from typing import Optional, Tuple

from backend.sqlite.content_meta_db import ContentMeta
from constants import IconId, IS_MACOS, NULL_UID, OBJ_TYPE_DIR, OBJ_TYPE_FILE, TrashStatus
from logging_constants import SUPER_DEBUG_ENABLED, TRACE_ENABLED
from model.node.directory_stats import DirectoryStats
from model.node.node import TNode
from model.node_identifier import LocalNodeIdentifier
from model.uid import UID
from util.ensure import ensure_bool, ensure_int

logger = logging.getLogger(__name__)


class LocalNode(TNode, ABC):
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
        TNode.update_from(self, other_node)
        self._trashed = other_node.get_trashed_status()
        self._is_live = other_node.is_live()
        self._sync_ts: int = ensure_int(other_node.sync_ts)
        self._create_ts: int = ensure_int(other_node.create_ts)
        self._modify_ts: int = ensure_int(other_node.modify_ts)
        self._change_ts: int = ensure_int(other_node.change_ts)

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

    def __init__(self, node_identifier: LocalNodeIdentifier, parent_uid, trashed: TrashStatus, is_live: bool, sync_ts: Optional[int],
                 create_ts: Optional[int], modify_ts: Optional[int], change_ts: Optional[int], all_children_fetched: bool):
        LocalNode.__init__(self, node_identifier, parent_uid, trashed, is_live, sync_ts, create_ts, modify_ts, change_ts)
        self.dir_stats: Optional[DirectoryStats] = None
        self.all_children_fetched: bool = ensure_bool(all_children_fetched)

    def update_from(self, other_node):
        assert isinstance(other_node, LocalDirNode)
        LocalNode.update_from(self, other_node)
        self.dir_stats = other_node.dir_stats
        self.set_is_live(ensure_bool(other_node.is_live()))
        self.all_children_fetched = other_node.all_children_fetched

    def is_parent_of(self, potential_child_node: TNode):
        if potential_child_node.device_uid == self.device_uid:
            child_path = pathlib.PurePosixPath(potential_child_node.get_single_path())
            if child_path.is_relative_to(self.get_single_path()):
                rel_path: pathlib.PurePosixPath = child_path.relative_to(self.get_single_path())
                return str(rel_path) == potential_child_node.name
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

    def __init__(self, node_identifier: LocalNodeIdentifier, parent_uid: UID, content_meta: ContentMeta, size_bytes: int, sync_ts: Optional[int],
                 create_ts: Optional[int], modify_ts: Optional[int], change_ts: Optional[int], trashed, is_live: bool):
        super().__init__(node_identifier, parent_uid, trashed, is_live, sync_ts, create_ts, modify_ts, change_ts)
        self.content_meta: ContentMeta = content_meta
        self._size_bytes: Optional[int] = size_bytes

    def update_from(self, other_node):
        assert isinstance(other_node, LocalFileNode)
        LocalNode.update_from(self, other_node)
        self.content_meta = other_node.content_meta
        self._size_bytes = other_node.get_size_bytes()

    def is_parent_of(self, potential_child_node: TNode) -> bool:
        # A file can never be the parent of anything
        return False

    @classmethod
    def get_obj_type(cls):
        return OBJ_TYPE_FILE

    @classmethod
    def is_file(cls) -> bool:
        return True

    @classmethod
    def is_dir(cls) -> bool:
        return False

    @property
    def content_meta_uid(self):
        return self.content_meta.uid if self.content_meta else NULL_UID

    def has_signature(self) -> bool:
        return self.content_meta_uid and self.content_meta.has_signature()

    def get_size_bytes(self) -> int:
        return self.content_meta.size_bytes if self.content_meta_uid else self._size_bytes

    @property
    def md5(self) -> Optional[str]:
        return self.content_meta.md5 if self.content_meta_uid else None

    @property
    def sha256(self) -> Optional[str]:
        return self.content_meta.sha256 if self.content_meta_uid else None

    @classmethod
    def has_tuple(cls) -> bool:
        return True

    def to_tuple(self) -> Tuple:
        return self.uid, self.get_single_parent_uid(), self.content_meta_uid, self.get_size_bytes(), \
               self.sync_ts, self.create_ts, self.modify_ts, self.change_ts,  self.get_single_path(), self._trashed, self._is_live

    def copy_signature_if_is_meta_equal(self, other) -> bool:
        """If meta is equal for this node & other, then copy signature from other to this."""
        if self.is_meta_equal(other) and (other.has_signature()):
            if TRACE_ENABLED:
                logger.debug(f'copy_signature_if_is_meta_equal(): Meta is equal for {self.node_identifier}')
            self.content_meta = other.content_meta

            if SUPER_DEBUG_ENABLED:
                self._check_update_sanity(other)
            return True
        else:
            if TRACE_ENABLED:
                logger.debug(f'copy_signature_if_is_meta_equal(): NOT equal: this={self}; other={other}')
            return False

    def _check_update_sanity(self, old_node):
        try:
            if not old_node:
                raise RuntimeError(f'old_node is empty!')

            if not isinstance(old_node, LocalFileNode):
                # Internal error; try to recover
                logger.error(f'Invalid node type for old_node: {type(old_node)}. Will overwrite cache entry')
                return

            if not old_node.create_ts:
                logger.debug(f'old_node has no create_ts. Skipping create_ts comparison (Old={old_node} New={self}')
            elif not self.create_ts:
                raise RuntimeError(f'self is missing create_ts!')
            elif self.create_ts < old_node.create_ts:
                if IS_MACOS:
                    # Known bug in MacOS
                    logger.debug(
                        f'File {self.node_identifier}: update has older create_ts ({self.create_ts}) than prev version ({old_node.create_ts})'
                        f'(probably MacOS bug)')
                else:
                    logger.warning(
                        f'File {self.node_identifier}: update has older create_ts ({self.create_ts}) than prev version ({old_node.create_ts})')

            if not old_node.modify_ts:
                logger.debug(f'old_node has no modify_ts. Skipping modify_ts comparison (Old={old_node} New={self}')
            elif not self.modify_ts:
                raise RuntimeError(f'self is missing modify_ts!')
            elif self.modify_ts < old_node.modify_ts:
                if IS_MACOS:
                    # Known bug in MacOS
                    logger.debug(
                        f'File {self.node_identifier}: update has older modify_ts ({self.modify_ts}) than prev version ({old_node.modify_ts}) '
                        f'(probably MacOS bug)')
                else:
                    logger.warning(
                        f'File {self.node_identifier}: update has older modify_ts ({self.modify_ts}) than prev version ({old_node.modify_ts}) ')

            if not old_node.change_ts:
                logger.debug(f'old_node has no change_ts. Skipping change_ts comparison (Old={old_node} New={self}')
            elif not self.change_ts:
                raise RuntimeError(f'self is missing change_ts!')
            elif self.change_ts < old_node.change_ts:
                if IS_MACOS:
                    # Known bug in MacOS
                    logger.debug(
                        f'File {self.node_identifier}: update has older modify_ts ({self.modify_ts}) than prev version ({old_node.modify_ts}) '
                        f'(probably MacOS bug)')
                else:
                    logger.warning(
                        f'File {self.node_identifier}: update has older change_ts ({self.change_ts}) than prev version ({old_node.change_ts})')

            if self.get_size_bytes() != old_node.get_size_bytes() and old_node.md5 and self.md5 == old_node.md5:
                logger.warning(f'File {self.node_identifier}: update has same MD5 ({self.md5}) ' +
                               f'but different size: (old={old_node.get_size_bytes()}, new={self.get_size_bytes()})')
        except Exception as e:
            logger.error(f'Error checking update sanity! Old={old_node} New={self}: {repr(e)}')
            raise

    def __eq__(self, other):
        """Compares against the node's metadata. Matches ONLY the node's identity and content; not its parents, children, or derived path"""
        if isinstance(other, LocalFileNode) and \
                other.content_meta_uid == self.content_meta_uid and \
                other.get_size_bytes() == other.get_size_bytes() and \
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
        return f'LocalFileNode({self.node_identifier} parent_uid={self.get_single_parent_uid()} content_uid={self.content_meta_uid} ' \
               f'size_bytes={self.get_size_bytes()} md5={self.md5} is_live={self.is_live()} ' \
               f'create_ts={self._create_ts} modify_ts={self._modify_ts} change_ts={self._change_ts})'
