import logging
import os
from abc import ABC, abstractmethod
from typing import List, Optional, Tuple

from treelib import Node

from util import format
from constants import ICON_ADD_FILE, ICON_GENERIC_FILE, NOT_TRASHED
from index.uid.uid_generator import UID
from model.node_identifier import NodeIdentifier

logger = logging.getLogger(__name__)


# ABSTRACT CLASS DisplayNode
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼

class DisplayNode(Node, ABC):
    """Base class for nodes which are meant to be displayed in a UI tree"""

    def __init__(self, node_identifier: NodeIdentifier):
        # Look at this next line. It is very important.
        Node.__init__(self, identifier=node_identifier.uid)
        self.node_identifier = node_identifier

        self._update_tag()

    def _update_tag(self):
        self.tag = f'{self.node_identifier}: "{self.identifier}"'

    @abstractmethod
    def is_parent(self, potential_child_node) -> bool:
        # TODO: custom exception class, 'InvalidOperationError'
        raise RuntimeError('Not allowed!')

    @classmethod
    def get_tree_type(cls) -> int:
        # TODO: custom exception class, 'InvalidOperationError'
        raise RuntimeError('Not allowed!')

    @classmethod
    @abstractmethod
    def get_obj_type(cls):
        return None

    @classmethod
    def is_file(cls):
        return False

    @classmethod
    @abstractmethod
    def is_dir(cls):
        return False

    @classmethod
    def is_ephemereal(cls) -> bool:
        return False

    @classmethod
    def exists(cls) -> bool:
        """Whether the object represented by this node actually exists currently, or it is just planned to exist or is an ephemeral node."""
        return False

    @classmethod
    def has_tuple(cls) -> bool:
        return False

    def to_tuple(self) -> Tuple:
        raise RuntimeError('Operation not supported for this object: "to_tuple()"')

    @property
    def name(self):
        assert type(self.node_identifier.full_path) == str, f'Not a string: {self.node_identifier.full_path} (this={self})'
        return os.path.basename(self.node_identifier.full_path)

    @property
    def trashed(self):
        return NOT_TRASHED

    @abstractmethod
    def get_etc(self):
        return None

    @property
    def md5(self):
        return None

    @property
    def sha256(self):
        return None

    @abstractmethod
    def get_size_bytes(self):
        return None

    @property
    def sync_ts(self):
        return None

    @property
    def modify_ts(self):
        return None

    @property
    def change_ts(self):
        return None

    @property
    def full_path(self):
        return self.node_identifier.full_path

    @property
    def uid(self) -> UID:
        return self.node_identifier.uid

    @uid.setter
    def uid(self, uid: UID):
        self.node_identifier.uid = uid
        self.identifier = uid
        self._update_tag()

    def get_relative_path(self, parent_tree):
        return parent_tree.get_relative_path_for_item(self)

    def get_icon(self):
        if self.exists():
            return ICON_GENERIC_FILE
        return ICON_ADD_FILE


# ABSTRACT CLASS HasParentList
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼

class HasParentList(ABC):
    def __init__(self, parent_uids: Optional[List[UID]] = None):
        self._parent_uids: Optional[List[UID]] = parent_uids

    def get_parent_uids(self) -> List[UID]:
        if self._parent_uids:
            if isinstance(self._parent_uids, list):
                return self._parent_uids
            elif isinstance(self._parent_uids, UID):
                return [self._parent_uids]
            assert False, f'Expected list or UID for parent_uids but got: type={type(self._parent_uids)}; val={self._parent_uids} '
        return []

    def set_parent_uids(self, parent_uids):
        """Can be a list of GoogFolders' UIDs, or a single UID, or None"""
        if not parent_uids:
            self._parent_uids = None
        elif isinstance(parent_uids, list):
            if len(parent_uids) == 1:
                assert isinstance(parent_uids[0], UID), f'Found instead: {parent_uids[0]}, type={type(parent_uids[0])}'
                self._parent_uids = parent_uids[0]
            else:
                self._parent_uids = parent_uids
        else:
            assert isinstance(parent_uids, UID), f'Found instead: {parent_uids}, type={type(parent_uids)}'
            self._parent_uids = parent_uids

    def add_parent(self, parent_uid: UID):
        current_parent_ids: List[UID] = self.get_parent_uids()
        if len(current_parent_ids) == 0:
            self._parent_uids = parent_uid
        else:
            for current_parent_id in current_parent_ids:
                if current_parent_id == parent_uid:
                    logger.debug(f'Parent is already in list; skipping: {parent_uid}')
                    return
            current_parent_ids.append(parent_uid)
            self._parent_uids = current_parent_ids


# CLASS HasChildren
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼

class HasChildren:
    """
    Represents a generic directory (i.e. not an LocalFileNode or domain object)
    """

    def __init__(self):
        self.file_count = 0
        self.trashed_file_count = 0
        self.trashed_dir_count = 0
        self.dir_count = 0
        self.trashed_bytes = 0
        self._size_bytes = None
        """Set this to None to signify that stats are not yet calculated"""

    def zero_out_stats(self):
        self._size_bytes = None
        self.file_count = 0
        self.dir_count = 0

    def add_meta_metrics(self, child_node: DisplayNode):
        if self._size_bytes is None:
            self._size_bytes = 0

        if child_node.trashed == NOT_TRASHED:
            # not trashed:
            if child_node.get_size_bytes():
                self._size_bytes += child_node.get_size_bytes()

            if child_node.is_dir():
                assert isinstance(child_node, HasChildren)
                self.dir_count += child_node.dir_count + 1
                self.file_count += child_node.file_count
            else:
                self.file_count += 1
        else:
            # trashed:
            if child_node.is_dir():
                assert isinstance(child_node, HasChildren)
                if child_node.get_size_bytes():
                    self.trashed_bytes += child_node.get_size_bytes()
                if child_node.trashed_bytes:
                    self.trashed_bytes += child_node.trashed_bytes
                self.trashed_dir_count += child_node.dir_count + child_node.trashed_dir_count + 1
                self.trashed_file_count += child_node.file_count + child_node.trashed_file_count
            else:
                self.trashed_file_count += 1
                if child_node.get_size_bytes():
                    self.trashed_bytes += child_node.get_size_bytes()

    def get_etc(self):
        if self._size_bytes is None:
            return ''
        files = self.file_count + self.trashed_file_count
        folders = self.trashed_dir_count + self.dir_count
        if folders:
            if folders == 1:
                multi = ''
            else:
                multi = 's'
            folders_str = f', {folders:n} folder{multi}'
        else:
            folders_str = ''
        if files == 1:
            multi = ''
        else:
            multi = 's'
        return f'{files:n} file{multi}{folders_str}'

    def get_summary(self):
        if self._size_bytes is None:
            return ''
        if not self._size_bytes and not self.file_count:
            return '0 items'
        size = format.humanfriendlier_size(self._size_bytes)
        return f'{size} in {self.file_count:n} files and {self.dir_count:n} dirs'

    def get_size_bytes(self):
        return self._size_bytes

