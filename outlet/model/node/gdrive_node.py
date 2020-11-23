import logging
from abc import ABC, abstractmethod
from typing import Optional

from util import format
from constants import GDRIVE_FOLDER_MIME_TYPE_UID, ICON_DIR_MK, ICON_DIR_TRASHED, ICON_FILE_CP_DST, ICON_FILE_TRASHED, ICON_GENERIC_DIR, \
    ICON_GENERIC_FILE, OBJ_TYPE_DIR, OBJ_TYPE_FILE, TRASHED_STATUS_STR, TrashStatus, TREE_TYPE_GDRIVE
from model.node.node import Node, HasChildStats, HasParentList
from model.node_identifier import ensure_bool, ensure_int, GDriveIdentifier

logger = logging.getLogger(__name__)


"""
◤━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━◥
    CLASS GDriveNode
◣━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━◢
"""


class GDriveNode(HasParentList, Node, ABC):
    # ▲▲ Remember, Method Resolution Order places greatest priority to the first in the list, then goes down ▲▲

    def __init__(self, node_identifier: GDriveIdentifier, goog_id: Optional[str], node_name: str, trashed: TrashStatus,
                 create_ts: Optional[int], modify_ts: Optional[int],
                 owner_uid: Optional[int], drive_id: Optional[str], is_shared: bool, shared_by_user_uid: Optional[int], sync_ts: Optional[int]):
        Node.__init__(self, node_identifier, trashed=trashed)
        HasParentList.__init__(self, None)
        self.goog_id: Optional[str] = goog_id
        """The Google ID - long string. Need this for syncing with Google Drive,
        although the (int) uid will be used internally."""

        self._name = node_name

        self.create_ts = ensure_int(create_ts)
        self._modify_ts = ensure_int(modify_ts)

        self.owner_uid: Optional[int] = owner_uid
        """OwnerID if it's not me"""

        self.drive_id = drive_id
        """This will only ever contain other users' drive_ids."""

        self._is_shared: bool = ensure_bool(is_shared)
        """If true, item is shared by shared_by_user_uid"""

        self.shared_by_user_uid: int = ensure_int(shared_by_user_uid)

        self._sync_ts = ensure_int(sync_ts)

    @abstractmethod
    def update_from(self, other_node):
        if not isinstance(other_node, GDriveNode):
            raise RuntimeError(f'Bad: {other_node} (we are: {self})')
        HasParentList.update_from(self, other_node)
        Node.update_from(self, other_node)
        self.goog_id = other_node.goog_id
        self._name = other_node.name
        self._trashed = other_node.get_trashed_status()
        self.create_ts = other_node.create_ts
        self._modify_ts = other_node._modify_ts
        self.owner_uid = other_node.owner_uid
        self.drive_id = other_node.drive_id
        self._is_shared = other_node.is_shared
        self.shared_by_user_uid = other_node.shared_by_user_uid
        self._sync_ts = other_node.sync_ts

    @property
    def is_shared(self):
        return self._is_shared

    @is_shared.setter
    def is_shared(self, val: bool):
        self._is_shared = val

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

    def set_sync_ts(self, sync_ts: int):
        self._sync_ts = sync_ts

    @classmethod
    def get_tree_type(cls) -> int:
        return TREE_TYPE_GDRIVE

    @property
    def name(self) -> str:
        return self._name

    @name.setter
    def name(self, name):
        self._name = name

    def get_trashed_status(self) -> TrashStatus:
        assert not self._trashed or isinstance(self._trashed, TrashStatus)
        return self._trashed

    def set_trashed_status(self, trashed: TrashStatus):
        self._trashed = trashed

    def is_live(self) -> bool:
        """Whether the object represented by this node actually exists currently; or it is just e.g. planned to exist or is an ephemeral node."""
        return self.goog_id is not None

    @property
    def trashed_str(self):
        if self.get_trashed_status() is None:
            return 'None'
        return TRASHED_STATUS_STR[self.get_trashed_status()]

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


class GDriveFolder(HasChildStats, GDriveNode):
    def __init__(self, node_identifier: GDriveIdentifier, goog_id, node_name, trashed, create_ts, modify_ts, owner_uid, drive_id,
                 is_shared, shared_by_user_uid, sync_ts, all_children_fetched):
        GDriveNode.__init__(self, node_identifier, goog_id, node_name, trashed, create_ts, modify_ts, owner_uid, drive_id, is_shared,
                            shared_by_user_uid, sync_ts)
        HasChildStats.__init__(self)

        self.all_children_fetched = all_children_fetched
        """If true, all its children have been fetched from Google"""

    def __repr__(self):
        return f'GDriveFolder(id={self.node_identifier} goog_id="{self.goog_id}" name="{self.name}" trashed={self.trashed_str} ' \
               f'owner_uid={self.owner_uid} drive_id={self.drive_id} is_shared={self._is_shared} shared_by_user_uid={self.shared_by_user_uid} ' \
               f'sync_ts={self.sync_ts} parent_uids={self.get_parent_uids()} children_fetched={self.all_children_fetched}]'

    def update_from(self, other_node):
        if not isinstance(other_node, GDriveFolder):
            raise RuntimeError(f'Bad: {other_node} (we are: {self})')
        GDriveNode.update_from(self, other_node)
        HasChildStats.update_from(self, other_node)
        self.all_children_fetched = other_node.all_children_fetched

    @classmethod
    def has_tuple(cls) -> bool:
        return True

    def to_tuple(self):
        return self.uid, self.goog_id, self.name, self.get_trashed_status(), self.create_ts, self._modify_ts, self.owner_uid, \
               self.drive_id, self._is_shared, self.shared_by_user_uid, self.sync_ts, self.all_children_fetched

    def is_parent_of(self, potential_child_node: Node) -> bool:
        if potential_child_node.get_tree_type() == TREE_TYPE_GDRIVE:
            assert isinstance(potential_child_node, GDriveNode)
            return self.uid in potential_child_node.get_parent_uids()
        return False

    @property
    def mime_type_uid(self) -> int:
        return GDRIVE_FOLDER_MIME_TYPE_UID

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
        if self.get_trashed_status() == TrashStatus.NOT_TRASHED:
            if self.is_live():
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

        if not other.has_same_parents(self):
            return False

        return other.uid == self.uid and other.goog_id == self.goog_id and other.name == self.name \
            and other.get_trashed_status() == self.get_trashed_status() and other.create_ts == self.create_ts \
            and other._modify_ts == self._modify_ts and other.owner_uid == self.owner_uid and other.drive_id == self.drive_id \
            and other.is_shared == self._is_shared and other.shared_by_user_uid

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

    def __init__(self, node_identifier: GDriveIdentifier, goog_id, node_name, mime_type_uid, trashed, drive_id, version, md5,
                 is_shared, create_ts, modify_ts, size_bytes, owner_uid, shared_by_user_uid, sync_ts):
        GDriveNode.__init__(self, node_identifier, goog_id, node_name, trashed, create_ts, modify_ts, owner_uid, drive_id, is_shared,
                            shared_by_user_uid, sync_ts)

        self.mime_type_uid = ensure_int(mime_type_uid)
        self.version = ensure_int(version)
        self._md5 = md5
        self._size_bytes = ensure_int(size_bytes)

    def __repr__(self):
        return f'GDriveFile(id={self.node_identifier} goog_id="{self.goog_id}" name="{self.name}" mime_type_uid={self.mime_type_uid} ' \
               f'trashed={self.trashed_str} size={self.get_size_bytes()} md5={self._md5} create_ts={self.create_ts} modify_ts={self.modify_ts} ' \
               f'owner_uid={self.owner_uid} drive_id={self.drive_id} is_shared={self.is_shared} shared_by_user_uid={self.shared_by_user_uid} ' \
               f'version={self.version} sync_ts={self.sync_ts} parent_uids={self.get_parent_uids()})'

    def __eq__(self, other):
        if not isinstance(other, GDriveFile):
            return False

        if not other.has_same_parents(self):
            return False

        return other.uid == self.uid and other.goog_id == self.goog_id and other.name == self.name and other.md5 == self._md5 and \
            other.mime_type_uid == self.mime_type_uid and other.get_trashed_status() == self.get_trashed_status() and \
            other.drive_id == self.drive_id and other.version == self.version and other.is_shared == self.is_shared and \
            other.get_size_bytes() == self.get_size_bytes() and other.owner_uid == self.owner_uid and \
            other.shared_by_user_uid == self.shared_by_user_uid and other.create_ts == self.create_ts and other.modify_ts == self.modify_ts

    def __ne__(self, other):
        return not self.__eq__(other)

    def update_from(self, other_node):
        if not isinstance(other_node, GDriveFile):
            raise RuntimeError(f'Bad: {other_node} (we are: {self})')
        GDriveNode.update_from(self, other_node)
        self.mime_type_uid = other_node.mime_type_uid
        self.version = other_node.version
        self._md5 = other_node.md5
        self._size_bytes = other_node.get_size_bytes()

    def is_parent_of(self, potential_child_node: Node) -> bool:
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
        if self.get_trashed_status() == TrashStatus.NOT_TRASHED:
            if self.is_live():
                return ICON_GENERIC_FILE
            else:
                return ICON_FILE_CP_DST
        return ICON_FILE_TRASHED

    @classmethod
    def has_tuple(cls) -> bool:
        return True

    def to_tuple(self):
        return (self.uid, self.goog_id, self.name, self.mime_type_uid, self.get_trashed_status(), self._size_bytes, self._md5,
                self.create_ts, self.modify_ts, self.owner_uid, self.drive_id, self.is_shared, self.shared_by_user_uid, self.version,
                self.sync_ts)
