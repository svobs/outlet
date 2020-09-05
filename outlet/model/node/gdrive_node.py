import logging
from abc import ABC, abstractmethod
from typing import Optional

from util import format
from constants import ICON_ADD_DIR, ICON_ADD_FILE, ICON_GENERIC_DIR, ICON_GENERIC_FILE, ICON_TRASHED_DIR, ICON_TRASHED_FILE, NOT_TRASHED, \
    OBJ_TYPE_DIR, OBJ_TYPE_FILE, TRASHED_STATUS, TREE_TYPE_GDRIVE
from model.node.display_node import DisplayNode, HasChildList, HasParentList
from model.node_identifier import ensure_int, GDriveIdentifier

logger = logging.getLogger(__name__)


"""
◤━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━◥
    CLASS GDriveNode
◣━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━◢
"""


class GDriveNode(HasParentList, DisplayNode, ABC):
    # ▲▲ Remember, Method Resolution Order places greatest priority to the first in the list, then goes down ▲▲

    def __init__(self, node_identifier: GDriveIdentifier, goog_id: Optional[str], node_name: str, trashed: int, drive_id: Optional[str],
                 my_share: bool, sync_ts: Optional[int]):
        DisplayNode.__init__(self, node_identifier)
        HasParentList.__init__(self, None)
        self.goog_id: Optional[str] = goog_id
        """The Google ID - long string. Need this for syncing with Google Drive,
        although the (int) uid will be used internally."""

        self._name = node_name

        if trashed < 0 or trashed > 2:
            raise RuntimeError(f'Invalid value for "trashed": {trashed}')
        self._trashed: int = trashed

        self.drive_id = drive_id
        """This will only ever contain other users' drive_ids."""

        self.my_share = my_share
        """If true, I own it but I have shared it with other users"""

        self._sync_ts = ensure_int(sync_ts)

    @property
    def sync_ts(self):
        return self._sync_ts

    @classmethod
    def get_tree_type(cls) -> int:
        return TREE_TYPE_GDRIVE

    @property
    def name(self) -> str:
        return self._name

    @name.setter
    def name(self, name):
        self._name = name

    @property
    def trashed(self) -> int:
        return self._trashed

    @trashed.setter
    def trashed(self, trashed: int):
        self._trashed = trashed

    @property
    def trashed_str(self):
        if self.trashed is None:
            return 'None'
        return TRASHED_STATUS[self.trashed]

    @abstractmethod
    def to_tuple(self):
        pass

    def exists(self) -> bool:
        return bool(self.goog_id)


"""
◤━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━◥
    CLASS GDriveFolder
◣━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━◢
"""


class GDriveFolder(HasChildList, GDriveNode):
    def __init__(self, node_identifier: GDriveIdentifier, goog_id, node_name, trashed, drive_id, my_share, sync_ts, all_children_fetched):
        GDriveNode.__init__(self, node_identifier, goog_id, node_name, trashed, drive_id, my_share, sync_ts)
        HasChildList.__init__(self)

        self.all_children_fetched = all_children_fetched
        """If true, all its children have been fetched from Google"""

    def __repr__(self):
        return f'GDriveFolder:(uid="{self.uid}" goog_id="{self.goog_id}" name="{self.name}" trashed={self.trashed_str} drive_id={self.drive_id} ' \
               f'my_share={self.my_share} sync_ts={self.sync_ts} parent_uids={self.get_parent_uids()} children_fetched={self.all_children_fetched}]'

    @classmethod
    def has_tuple(cls) -> bool:
        return True

    def to_tuple(self):
        return self.uid, self.goog_id, self.name, self.trashed, self.drive_id, self.my_share, self.sync_ts, self.all_children_fetched

    def is_parent(self, potential_child_node: DisplayNode) -> bool:
        if potential_child_node.get_tree_type() == TREE_TYPE_GDRIVE:
            assert isinstance(potential_child_node, GDriveNode)
            return self.uid in potential_child_node.get_parent_uids()
        return False

    @classmethod
    def get_obj_type(cls):
        return OBJ_TYPE_DIR

    @classmethod
    def is_file(cls):
        return False

    @classmethod
    def is_dir(cls):
        return True

    def get_icon(self):
        if self.trashed == NOT_TRASHED:
            if self.exists():
                return ICON_GENERIC_DIR
            else:
                return ICON_ADD_DIR
        return ICON_TRASHED_DIR

    def get_summary(self):
        if not self._size_bytes and not self.file_count and not self.dir_count:
            return '0 items'
        size = format.humanfriendlier_size(self._size_bytes)
        return f'{size} in {self.file_count:n} files and {self.dir_count:n} folders'

    def __eq__(self, other):
        if not isinstance(other, GDriveFolder):
            return False

        return other.uid == self.uid and other.goog_id == self.goog_id and other.name == self.name and other.trashed == self.trashed \
            and other.drive_id == self.drive_id and other.my_share == self.my_share and other.all_children_fetched == self.all_children_fetched

    def __ne__(self, other):
        return not self.__eq__(other)


"""
◤━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━◥
    CLASS GDriveFile
◣━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━◢
"""


class GDriveFile(GDriveNode):
    # TODO: handling of shortcuts... does a shortcut have an ID?
    # TODO: handling of special chars in file systems

    def __init__(self, node_identifier: GDriveIdentifier, goog_id, node_name, trashed, drive_id, version, head_revision_id, md5,
                 my_share, create_ts, modify_ts, size_bytes, owner_id, sync_ts):
        GDriveNode.__init__(self, node_identifier, goog_id, node_name, trashed, drive_id, my_share, sync_ts)

        self.version = ensure_int(version)
        self.head_revision_id = head_revision_id
        self._md5 = md5
        self.my_share = my_share
        self.create_ts = ensure_int(create_ts)
        self._modify_ts = ensure_int(modify_ts)
        self._size_bytes = ensure_int(size_bytes)
        self.owner_id = owner_id
        """OwnerID if it's not me"""

    def __repr__(self):
        return f'GDriveFile(id={self.node_identifier} goog_id="{self.goog_id}" name="{self.name}" trashed={self.trashed_str} ' \
               f'size={self.get_size_bytes()} md5={self._md5} create_ts={self.create_ts} modify_ts={self.modify_ts} owner_id={self.owner_id} ' \
               f'drive_id={self.drive_id} my_share={self.my_share} version={self.version} head_rev_id="{self.head_revision_id}" ' \
               f'sync_ts={self.sync_ts} parent_uids={self.get_parent_uids()})'

    def __eq__(self, other):
        if not isinstance(other, GDriveFile):
            return False

        return other.uid == self.uid and other.goog_id == self.goog_id and other.name == self.name and other.md5 == self._md5 and \
            other.trashed == self.trashed and other.drive_id == self.drive_id and other.version == self.version and \
            other.head_revision_id == self.head_revision_id and other.my_share == self.my_share and other.get_size_bytes() == self.get_size_bytes() \
            and other.create_ts == self.create_ts and other.modify_ts == self.modify_ts

    def __ne__(self, other):
        return not self.__eq__(other)

    def is_parent(self, potential_child_node: DisplayNode) -> bool:
        # A file can never be the parent of anything
        return False

    @property
    def md5(self):
        return self._md5

    @md5.setter
    def md5(self, md5):
        self._md5 = md5

    def set_modify_ts(self, modify_ts: int):
        self._modify_ts = ensure_int(modify_ts)

    @property
    def modify_ts(self):
        return self._modify_ts

    @modify_ts.setter
    def modify_ts(self, modify_ts):
        self._modify_ts = modify_ts

    def get_size_bytes(self):
        return self._size_bytes

    def set_size_bytes(self, size_bytes: int):
        self._size_bytes = size_bytes

    @classmethod
    def get_obj_type(cls):
        return OBJ_TYPE_FILE

    @classmethod
    def is_file(cls):
        return True

    @classmethod
    def is_dir(cls):
        return False

    def get_icon(self):
        if self.trashed == NOT_TRASHED:
            if self.exists():
                return ICON_GENERIC_FILE
            else:
                return ICON_ADD_FILE
        return ICON_TRASHED_FILE

    @classmethod
    def has_tuple(cls) -> bool:
        return True

    def to_tuple(self):
        return (self.uid, self.goog_id, self.name, self.trashed, self._size_bytes, self._md5, self.create_ts, self.modify_ts,
                self.owner_id, self.drive_id, self.my_share, self.version, self.head_revision_id, self.sync_ts)
