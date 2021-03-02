import logging
from abc import ABC
from typing import List, Optional, Union

from backend.store.uid.uid_generator import UID
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
                assert isinstance(parent_uids[0], UID), f'set_parent_uids(): Found instead: {parent_uids[0]}, type={type(parent_uids[0])}'
                self._parent_uids = parent_uids[0]
            else:
                self._parent_uids = parent_uids
        else:
            assert isinstance(parent_uids, UID), f'set_parent_uids(): Found instead: {parent_uids}, type={type(parent_uids)}'
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

