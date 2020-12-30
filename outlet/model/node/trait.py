import logging
from abc import ABC
from typing import List, Optional, Union

from constants import TrashStatus
from backend.store.uid.uid_generator import UID
from util import format
from util.ensure import ensure_uid

logger = logging.getLogger(__name__)


class HasParentList(ABC):
    """
    ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
    TRAIT HasParentList

    Base class for all data nodes.
    ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼
    """

    def __init__(self, parent_uids: Optional[Union[UID, List[UID]]] = None):
        if type(parent_uids) == int:
            parent_uids = UID(parent_uids)
        self._parent_uids: Optional[Union[UID, List[UID]]] = parent_uids

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
        parent_uid = ensure_uid(parent_uid)
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
    ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
    TRAIT HasChildStats

    Represents a generic directory (i.e. not an LocalFileNode or domain object)
    ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼
    """

    def __init__(self):
        self.file_count: int = 0
        self.trashed_file_count: int = 0
        self.trashed_dir_count: int = 0
        self.dir_count: int = 0
        self.trashed_bytes: int = 0
        self._size_bytes: Optional[int] = None
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

    def add_meta_metrics(self, child_node):
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

    def get_etc(self) -> str:
        if not self.is_stats_loaded():
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

    def get_summary(self) -> str:
        if not self.is_stats_loaded():
            return ''
        if not self._size_bytes and not self.file_count:
            return '0 items'
        size = format.humanfriendlier_size(self._size_bytes)
        return f'{size} in {self.file_count:n} files and {self.dir_count:n} dirs'

    def get_size_bytes(self) -> Optional[int]:
        return self._size_bytes

    def set_size_bytes(self, size_bytes: int):
        self._size_bytes = size_bytes

    def is_stats_loaded(self):
        return self._size_bytes is not None
