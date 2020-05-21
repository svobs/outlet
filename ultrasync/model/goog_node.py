import os
from abc import ABC, abstractmethod
import logging
from typing import List, Optional, Tuple, Union

from constants import ICON_ADD_DIR, NOT_TRASHED, TRASHED_STATUS
from index.uid_generator import UID
from model.category import Category
from model.node_identifier import GDriveIdentifier
from model.display_node import DisplayNode, ensure_int
from model.planning_node import PlanningNode
from constants import ICON_GENERIC_DIR, ICON_TRASHED_DIR, ICON_TRASHED_FILE

logger = logging.getLogger(__name__)


"""
◤━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━◥
    CLASS GoogNode
◣━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━◢
"""


class GoogNode(DisplayNode, ABC):

    def __init__(self, uid: UID, goog_id: Optional[str], item_name: str, trashed: int, drive_id: Optional[str],
                 my_share: bool, sync_ts: Optional[int], category: Category = Category.NA):
        super().__init__(GDriveIdentifier(uid, None, category))

        self.goog_id: str = goog_id
        """The Google ID - long string. Need this for syncing with Google Drive,
        although the (int) uid will be used internally."""

        self._parent_ids:  Optional[Union[UID, List[UID]]] = None
        """ Most items will have only one parent, so store that way for efficiency"""

        self._name = item_name

        if trashed < 0 or trashed > 2:
            raise RuntimeError(f'Invalid value for "trashed": {trashed}')
        self.trashed = trashed

        self.drive_id = drive_id
        """This will only ever contain other users' drive_ids."""

        self.my_share = my_share
        """If true, I own it but I have shared it with other users"""

        self.sync_ts = sync_ts

    # TODO: rewrite this as compare_to()
    def is_newer_than(self, other_folder):
        return self.sync_ts > other_folder.sync_ts

    @property
    def parent_ids(self) -> List[UID]:
        if not self._parent_ids:
            return []
        if isinstance(self._parent_ids, list):
            return self._parent_ids
        return [self._parent_ids]

    @parent_ids.setter
    def parent_ids(self, parent_ids: Optional[Union[UID, List[UID]]]):
        """Can be a list of GoogFolders, or a single instance, or None"""
        if not parent_ids:
            self._parent_ids = None
        elif isinstance(parent_ids, list):
            if len(parent_ids) == 0:
                self._parent_ids = None
            elif len(parent_ids) == 1:
                self._parent_ids = parent_ids[0]
            else:
                self._parent_ids = parent_ids
        else:
            self._parent_ids = parent_ids

    def add_parent(self, parent_id: UID):
        current_parent_ids: List[UID] = self.parent_ids
        if len(current_parent_ids) == 0:
            self.parent_ids = parent_id
        else:
            for current_parent_id in current_parent_ids:
                if current_parent_id == parent_id:
                    logger.debug(f'Parent is already in list; skipping: {parent_id}')
                    return
            current_parent_ids.append(parent_id)
            self.parent_ids = current_parent_ids

    def get_icon(self):
        if self.trashed == NOT_TRASHED:
            return ICON_GENERIC_DIR
        return ICON_TRASHED_DIR

    @classmethod
    def is_file(cls):
        return False

    @classmethod
    def is_dir(cls):
        return False

    @property
    def name(self):
        return self._name

    @name.setter
    def name(self, name):
        self._name = name

    @classmethod
    def has_path(cls):
        return True

    @property
    def trashed_str(self):
        if self.trashed is None:
            return ' '
        return TRASHED_STATUS[self.trashed]

    @abstractmethod
    def to_tuple(self):
        pass

"""
◤━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━◥
    CLASS GoogFolder
◣━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━◢
"""


class GoogFolder(GoogNode):
    def __init__(self, uid, goog_id, item_name, trashed, drive_id, my_share, sync_ts, all_children_fetched):
        super().__init__(uid, goog_id, item_name, trashed, drive_id, my_share, sync_ts)

        self.all_children_fetched = all_children_fetched
        """If true, all its children have been fetched from Google"""

    def __repr__(self):
        return f'Folder:(uid="{self.uid}" goog_id="{self.goog_id}" name="{self.name}" trashed={self.trashed_str} drive_id={self.drive_id} ' \
               f'my_share={self.my_share} sync_ts={self.sync_ts} parent_ids={self.parent_ids} children_fetched={self.all_children_fetched} ]'

    def to_tuple(self):
        return self.uid, self.goog_id, self.name, self.trashed, self.drive_id, self.my_share, self.sync_ts, self.all_children_fetched

    @classmethod
    def is_file(cls):
        return False

    @classmethod
    def is_dir(cls):
        return True


"""
◤━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━◥
    CLASS GoogFile
◣━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━◢
"""


class GoogFile(GoogNode):
    # TODO: handling of shortcuts... does a shortcut have an ID?
    # TODO: handling of special chars in file systems

    def __init__(self, uid, goog_id, item_name, trashed, drive_id, version, head_revision_id, md5,
                 my_share, create_ts, modify_ts, size_bytes, owner_id, sync_ts):
        super().__init__(uid=uid, goog_id=goog_id, item_name=item_name, trashed=trashed,
                         drive_id=drive_id, my_share=my_share, sync_ts=sync_ts)
        self.version = version
        self.head_revision_id = head_revision_id
        self.md5 = md5
        self.my_share = my_share
        self.create_ts = ensure_int(create_ts)
        self.modify_ts = ensure_int(modify_ts)
        self._size_bytes = ensure_int(size_bytes)
        self.owner_id = owner_id

    def __repr__(self):
        return f'GoogFile(id={self.node_identifier} goog_id="{self.goog_id}" name="{self.name}" trashed={self.trashed_str}  size={self.size_bytes} ' \
               f'md5="{self.md5} create_ts={self.create_ts} modify_ts={self.modify_ts} owner_id={self.owner_id} ' \
               f'drive_id={self.drive_id} my_share={self.my_share} version={self.version} head_rev_id="{self.head_revision_id}" ' \
               f'sync_ts={self.sync_ts} parent_ids={self.parent_ids})'

    def is_newer_than(self, other_file):
        if self.modify_ts and other_file.modify_ts:
            delta = self.modify_ts - other_file.modify_ts
            if delta != 0:
                return delta
        return self.sync_ts > other_file.sync_ts

    @property
    def size_bytes(self):
        return self._size_bytes

    @classmethod
    def is_file(cls):
        return True

    @classmethod
    def is_dir(cls):
        return False

    def get_icon(self):
        if self.trashed != NOT_TRASHED:
            return ICON_TRASHED_FILE
        return self.category.name

    def to_tuple(self):
        return (self.uid, self.goog_id, self.name, self.trashed, self._size_bytes, self.md5, self.create_ts, self.modify_ts,
                self.owner_id, self.drive_id, self.my_share, self.version, self.head_revision_id, self.sync_ts)


"""
◤━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━◥
    CLASS FolderToAdd
◣━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━◢
"""


class FolderToAdd(PlanningNode, GoogNode):
    def __init__(self, uid: UID, dest_path: str):
        GoogNode.__init__(self, uid=uid, goog_id=None, item_name=os.path.basename(dest_path), trashed=NOT_TRASHED,
                          drive_id=None, my_share=False, sync_ts=None, category=Category.ADDED)
        self.node_identifier.full_path = dest_path

    def get_icon(self):
        return ICON_ADD_DIR

    @classmethod
    def is_file(cls):
        return False

    @classmethod
    def is_dir(cls):
        return True

    @classmethod
    def has_path(cls):
        return True

    def __repr__(self):
        return f'FolderToAdd(dest_path={self.full_path} parent_ids={self.parent_ids})'

    def to_tuple(self):
        pass
