import logging
from abc import ABC, abstractmethod
from typing import Optional

from util import format
from constants import ICON_DIR_MK, ICON_DIR_TRASHED, ICON_FILE_CP_DST, ICON_FILE_TRASHED, ICON_GENERIC_DIR, ICON_GENERIC_FILE, \
    NOT_TRASHED, OBJ_TYPE_DIR, OBJ_TYPE_FILE, TRASHED_STATUS, TREE_TYPE_GDRIVE
from model.node.display_node import DisplayNode, HasChildList, HasParentList
from model.node_identifier import ensure_bool, ensure_int, GDriveIdentifier

logger = logging.getLogger(__name__)


"""
◤━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━◥
    CLASS GDriveNode
◣━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━◢
"""


class GDriveNode(HasParentList, DisplayNode, ABC):
    # ▲▲ Remember, Method Resolution Order places greatest priority to the first in the list, then goes down ▲▲

    def __init__(self, node_identifier: GDriveIdentifier, goog_id: Optional[str], node_name: str, trashed: int,
                 create_ts: Optional[int], modify_ts: Optional[int],
                 owner_id: Optional[int], drive_id: Optional[str], is_shared: bool, shared_by_user_id: Optional[int], sync_ts: Optional[int]):
        DisplayNode.__init__(self, node_identifier)
        HasParentList.__init__(self, None)
        self.goog_id: Optional[str] = goog_id
        """The Google ID - long string. Need this for syncing with Google Drive,
        although the (int) uid will be used internally."""

        self._name = node_name

        if trashed < 0 or trashed > 2:
            raise RuntimeError(f'Invalid value for "trashed": {trashed}')
        self._trashed: int = trashed

        self.create_ts = ensure_int(create_ts)
        self._modify_ts = ensure_int(modify_ts)

        self.owner_id: Optional[int] = owner_id
        """OwnerID if it's not me"""

        self.drive_id = drive_id
        """This will only ever contain other users' drive_ids."""

        self.is_shared: bool = ensure_bool(is_shared)
        """If true, item is shared by shared_by_user_id"""

        self.shared_by_user_id: int = ensure_int(shared_by_user_id)

        self._sync_ts = ensure_int(sync_ts)

    @abstractmethod
    def update_from(self, other_node):
        if not isinstance(other_node, GDriveNode):
            raise RuntimeError(f'Bad: {other_node} (we are: {self})')
        HasParentList.update_from(self, other_node)
        DisplayNode.update_from(self, other_node)
        self.goog_id = other_node.goog_id
        self._name = other_node.name
        self._trashed = other_node.trashed
        self.create_ts = other_node.create_ts
        self._modify_ts = other_node._modify_ts
        self.owner_id = other_node.owner_id
        self.drive_id = other_node.drive_id
        self.is_shared = other_node.is_shared
        self.shared_by_user_id = other_node.shared_by_user_id
        self._sync_ts = other_node.sync_ts

    def set_modify_ts(self, modify_ts: int):
        self._modify_ts = ensure_int(modify_ts)

    @property
    def modify_ts(self):
        return self._modify_ts

    @modify_ts.setter
    def modify_ts(self, modify_ts):
        self._modify_ts = ensure_int(modify_ts)

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
    def __init__(self, node_identifier: GDriveIdentifier, goog_id, node_name, trashed, create_ts, modify_ts, owner_id, drive_id,
                 is_shared, shared_by_user_id, sync_ts, all_children_fetched):
        GDriveNode.__init__(self, node_identifier, goog_id, node_name, trashed, create_ts, modify_ts, owner_id, drive_id, is_shared,
                            shared_by_user_id, sync_ts)
        HasChildList.__init__(self)

        self.all_children_fetched = all_children_fetched
        """If true, all its children have been fetched from Google"""

    def __repr__(self):
        return f'GDriveFolder:(uid="{self.uid}" goog_id="{self.goog_id}" name="{self.name}" trashed={self.trashed_str} ' \
               f'owner_id={self.owner_id} drive_id={self.drive_id} is_shared={self.is_shared} shared_by_user_id={self.shared_by_user_id} ' \
               f'sync_ts={self.sync_ts} parent_uids={self.get_parent_uids()} children_fetched={self.all_children_fetched}]'

    def update_from(self, other_node):
        if not isinstance(other_node, GDriveFolder):
            raise RuntimeError(f'Bad: {other_node} (we are: {self})')
        GDriveNode.update_from(self, other_node)
        HasChildList.update_from(self, other_node)
        self.all_children_fetched = other_node.all_children_fetched

    @classmethod
    def has_tuple(cls) -> bool:
        return True

    def to_tuple(self):
        return self.uid, self.goog_id, self.name, self.trashed, self.create_ts, self._modify_ts, self.owner_id, self.drive_id, self.is_shared, \
               self.shared_by_user_id, self.sync_ts, self.all_children_fetched

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
                return ICON_DIR_MK
        return ICON_DIR_TRASHED

    def get_summary(self):
        if not self._size_bytes and not self.file_count and not self.dir_count:
            return '0 items'
        size = format.humanfriendlier_size(self._size_bytes)
        return f'{size} in {self.file_count:n} files and {self.dir_count:n} folders'

    def __eq__(self, other):
        if not isinstance(other, GDriveFolder):
            return False

        return other.uid == self.uid and other.goog_id == self.goog_id and other.name == self.name and other.trashed == self.trashed \
            and other.create_ts == self.create_ts and other._modify_ts == self._modify_ts and other.owner_id == self.owner_id \
            and other.drive_id == self.drive_id and other.is_shared == self.is_shared and other.shared_by_user_id \
            and other.all_children_fetched == self.all_children_fetched

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
                 is_shared, create_ts, modify_ts, size_bytes, owner_id, shared_by_user_id, sync_ts):
        GDriveNode.__init__(self, node_identifier, goog_id, node_name, trashed, create_ts, modify_ts, owner_id, drive_id, is_shared,
                            shared_by_user_id, sync_ts)

        self.version = ensure_int(version)
        self.head_revision_id = head_revision_id
        self._md5 = md5
        self._size_bytes = ensure_int(size_bytes)

    def __repr__(self):
        return f'GDriveFile(id={self.node_identifier} goog_id="{self.goog_id}" name="{self.name}" trashed={self.trashed_str} ' \
               f'size={self.get_size_bytes()} md5={self._md5} create_ts={self.create_ts} modify_ts={self.modify_ts} owner_id={self.owner_id} ' \
               f'drive_id={self.drive_id} is_shared={self.is_shared} shared_by_user_id={self.shared_by_user_id} version={self.version} ' \
               f'head_rev_id="{self.head_revision_id}" sync_ts={self.sync_ts} parent_uids={self.get_parent_uids()})'

    def __eq__(self, other):
        if not isinstance(other, GDriveFile):
            return False

        return other.uid == self.uid and other.goog_id == self.goog_id and other.name == self.name and other.md5 == self._md5 and \
            other.trashed == self.trashed and other.drive_id == self.drive_id and other.version == self.version and \
            other.head_revision_id == self.head_revision_id and other.is_shared == self.is_shared and \
            other.get_size_bytes() == self.get_size_bytes() and other.owner_id == self.owner_id and \
            other.shared_by_user_id == self.shared_by_user_id and other.create_ts == self.create_ts and other.modify_ts == self.modify_ts

    def __ne__(self, other):
        return not self.__eq__(other)

    def update_from(self, other_node):
        if not isinstance(other_node, GDriveFile):
            raise RuntimeError(f'Bad: {other_node} (we are: {self})')
        GDriveNode.update_from(self, other_node)
        self.version = other_node.version
        self.head_revision_id = other_node.head_revision_id
        self._md5 = other_node.md5
        self._size_bytes = other_node.get_size_bytes()

    def is_parent(self, potential_child_node: DisplayNode) -> bool:
        # A file can never be the parent of anything
        return False

    @property
    def md5(self):
        return self._md5

    @md5.setter
    def md5(self, md5):
        self._md5 = md5

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
                return ICON_FILE_CP_DST
        return ICON_FILE_TRASHED

    @classmethod
    def has_tuple(cls) -> bool:
        return True

    def to_tuple(self):
        return (self.uid, self.goog_id, self.name, self.trashed, self._size_bytes, self._md5, self.create_ts, self.modify_ts,
                self.owner_id, self.drive_id, self.is_shared, self.shared_by_user_id, self.version, self.head_revision_id, self.sync_ts)
