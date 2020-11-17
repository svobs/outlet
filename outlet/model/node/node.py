import collections
import logging
import os
from abc import ABC, abstractmethod
from typing import List, Optional, Tuple

import treelib

from error import InvalidOperationError
from util import format
from constants import ICON_FILE_CP_DST, ICON_GENERIC_FILE, TrashStatus
from store.uid.uid_generator import UID
from model.node_identifier import NodeIdentifier

logger = logging.getLogger(__name__)

# TYPEDEF SPIDNodePair
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
SPIDNodePair = collections.namedtuple('SPIDNodePair', 'spid node')


# ABSTRACT CLASS Node
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼

class Node(treelib.Node, ABC):
    """Base class for all data nodes."""

    def __init__(self, node_identifier: NodeIdentifier, nid=None, trashed: TrashStatus = TrashStatus.NOT_TRASHED):
        # Look at these next 3 lines. They are very important.
        if not nid:
            nid = node_identifier.uid
        self.node_identifier: NodeIdentifier = node_identifier

        if not trashed:
            self._trashed: TrashStatus = TrashStatus.NOT_TRASHED
        elif trashed < TrashStatus.NOT_TRASHED or trashed > TrashStatus.DELETED:
            raise RuntimeError(f'Invalid value for "trashed": {trashed}')
        else:
            self._trashed: TrashStatus = TrashStatus(trashed)

        treelib.Node.__init__(self, identifier=nid)
        self._update_tag()

    def _update_tag(self):
        self.tag = f'{self.node_identifier}: "{self.identifier}"'

    @abstractmethod
    def is_parent_of(self, potential_child_node) -> bool:
        raise InvalidOperationError('is_parent_of')

    def get_tree_type(self) -> int:
        return self.node_identifier.tree_type

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
    def is_display_only(cls):
        return False

    @classmethod
    def is_ephemereal(cls) -> bool:
        return False

    @classmethod
    def is_live(cls) -> bool:
        """Whether the object represented by this node actually exists currently; or it is just e.g. planned to exist or is an ephemeral node."""
        return False

    @classmethod
    def has_tuple(cls) -> bool:
        return False

    @property
    def is_shared(self):
        return False

    def to_tuple(self) -> Tuple:
        raise RuntimeError('Operation not supported for this object: "to_tuple()"')

    def set_node_identifier(self, node_identifier: NodeIdentifier):
        self.node_identifier = node_identifier
        # This will call self._set_identifier():
        self.identifier = node_identifier.uid

    def _set_identifier(self, value):
        super()._set_identifier(value)
        self._update_tag()

    @property
    def name(self):
        assert self.node_identifier.get_path_list(), f'Oops - for {self}'
        return os.path.basename(self.node_identifier.get_path_list()[0])

    def get_trashed_status(self) -> TrashStatus:
        return self._trashed

    @staticmethod
    def get_etc():
        return None

    @property
    def md5(self):
        return None

    @property
    def sha256(self):
        return None

    def get_size_bytes(self):
        return None

    def set_size_bytes(self, size_bytes: int):
        pass

    @property
    @abstractmethod
    def sync_ts(self):
        raise RuntimeError('sync_ts(): if you are seeing this msg you forgot to implement this in subclass of Node!')

    @property
    def modify_ts(self):
        return None

    @property
    def change_ts(self):
        return None

    def get_path_list(self):
        return self.node_identifier.get_path_list()

    def get_single_path(self):
        return self.node_identifier.get_single_path()

    @property
    def uid(self) -> UID:
        return self.node_identifier.uid

    @uid.setter
    def uid(self, uid: UID):
        self.node_identifier.uid = uid
        self.identifier = uid
        self._update_tag()

    def get_icon(self):
        if self.is_live():
            return ICON_GENERIC_FILE
        return ICON_FILE_CP_DST

    @abstractmethod
    def update_from(self, other_node):
        assert isinstance(other_node, Node), f'Invalid type: {type(other_node)}'
        assert other_node.node_identifier.uid == self.node_identifier.uid and other_node.node_identifier.tree_type == self.node_identifier.tree_type,\
            f'Other identifier ({other_node.node_identifier}) does not match: {self.node_identifier}'
        # do not change UID or tree type
        self.node_identifier.set_path_list(other_node.get_path_list())
        self.identifier = other_node.identifier
        self._trashed = other_node._trashed


# ABSTRACT CLASS HasParentList
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼

class HasParentList(ABC):
    def __init__(self, parent_uids: Optional[List[UID]] = None):
        self._parent_uids: Optional[List[UID]] = parent_uids

    def update_from(self, other_node):
        if not isinstance(other_node, HasParentList):
            raise RuntimeError(f'Bad: {other_node} (we are: {self})')
        self._parent_uids = other_node.get_parent_uids()

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

    def remove_parent(self, parent_uid_to_remove: UID):
        current_parent_list: List[UID] = self.get_parent_uids()
        for current_parent_id in current_parent_list:
            if current_parent_id == parent_uid_to_remove:
                current_parent_list.remove(current_parent_id)
                self.set_parent_uids(current_parent_list)
                return

        logger.warning(f'Could not remove parent ({parent_uid_to_remove}): it was not found in parent list ({current_parent_list})')

    def has_same_parents(self, other):
        assert isinstance(other, HasParentList)
        my_parents = self.get_parent_uids()
        other_parents = other.get_parent_uids()
        num_parents = len(my_parents)
        if num_parents != len(other_parents):
            return False

        if num_parents == 0:
            return True

        if num_parents == 1:
            return my_parents[0] == other_parents[0]

        return sorted(my_parents) == sorted(other_parents)

    def has_no_parents(self):
        return not self._parent_uids


# ABSTRACT CLASS HasChildStats
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼

class HasChildStats(ABC):
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

    def update_from(self, other_node):
        if not isinstance(other_node, HasChildStats):
            raise RuntimeError(f'Bad: {other_node} (we are: {self})')
        self.file_count = other_node.file_count
        self.trashed_file_count = other_node.trashed_file_count
        self.trashed_dir_count = other_node.trashed_dir_count
        self.dir_count = other_node.dir_count
        self.trashed_bytes = other_node.trashed_bytes
        self._size_bytes = other_node.get_size_bytes()

    def zero_out_stats(self):
        self._size_bytes = None
        self.file_count = 0
        self.dir_count = 0

    def set_stats_for_no_children(self):
        self._size_bytes = 0
        self.file_count = 0
        self.dir_count = 0

    def add_meta_metrics(self, child_node: Node):
        if self._size_bytes is None:
            self._size_bytes = 0

        if child_node.get_trashed_status() == TrashStatus.NOT_TRASHED:
            # not trashed:
            if child_node.get_size_bytes():
                self._size_bytes += child_node.get_size_bytes()

            if child_node.is_dir():
                assert isinstance(child_node, HasChildStats)
                self.dir_count += child_node.dir_count + 1
                self.file_count += child_node.file_count
            else:
                self.file_count += 1
        else:
            # trashed:
            if child_node.is_dir():
                assert isinstance(child_node, HasChildStats)
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

    def set_size_bytes(self, size_bytes: int):
        self._size_bytes = size_bytes

