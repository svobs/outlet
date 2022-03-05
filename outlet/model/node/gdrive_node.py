import logging
from abc import ABC, abstractmethod
from typing import Optional

from backend.sqlite.content_meta_db import ContentMeta
from constants import GDRIVE_FOLDER_MIME_TYPE_UID, IconId, OBJ_TYPE_DIR, OBJ_TYPE_FILE, TRASHED_STATUS_STR, TrashStatus, TreeType
from error import InvalidOperationError
from model.node.directory_stats import DirectoryStats
from model.node.node import Node
from model.node_identifier import GDriveIdentifier
from model.uid import UID
from util.ensure import ensure_bool, ensure_int, ensure_trash_status, ensure_uid

logger = logging.getLogger(__name__)


class GDriveNode(Node, ABC):
    """
    ◤━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━◥
        CLASS GDriveNode
    ◣━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━◢
    """
    # ▲▲ Remember, Method Resolution Order places greatest priority to the first in the list, then goes down ▲▲
    def __init__(self, node_identifier: GDriveIdentifier, goog_id: Optional[str], node_name: str, trashed: TrashStatus,
                 create_ts: Optional[int], modify_ts: Optional[int],
                 owner_uid: Optional[UID], drive_id: Optional[str], is_shared: bool, shared_by_user_uid: Optional[UID], sync_ts: Optional[int]):
        Node.__init__(self, node_identifier)

        self._trashed: TrashStatus = ensure_trash_status(trashed)
        self.goog_id: Optional[str] = goog_id
        """The Google ID - long string. Need this for syncing with Google Drive,
        although the (int) uid will be used internally."""

        assert node_name, f'Node name is null! node_identifier={node_identifier} goog_id={goog_id}'
        self._name = node_name

        self._create_ts = ensure_int(create_ts)
        self._modify_ts = ensure_int(modify_ts)

        self.owner_uid: Optional[UID] = ensure_uid(owner_uid)
        """OwnerID if it's not me"""

        self.drive_id = drive_id
        """This will only ever contain other users' drive_ids."""

        self._is_shared: bool = ensure_bool(is_shared)
        """If true, item is shared by shared_by_user_uid"""

        self.shared_by_user_uid: Optional[UID] = ensure_uid(shared_by_user_uid)

        self._sync_ts = ensure_int(sync_ts)

    @abstractmethod
    def update_from(self, other_node):
        if not isinstance(other_node, GDriveNode):
            raise RuntimeError(f'Bad: {other_node} (we are: {self})')
        Node.update_from(self, other_node)
        self._trashed: TrashStatus = other_node.get_trashed_status()
        self.goog_id = other_node.goog_id
        self._name = other_node.name
        self._trashed = other_node.get_trashed_status()
        self._create_ts = other_node._create_ts
        self._modify_ts = other_node._modify_ts
        self.owner_uid = other_node.owner_uid
        self.drive_id = other_node.drive_id
        self._is_shared = other_node.is_shared
        self.shared_by_user_uid = other_node.shared_by_user_uid
        self._sync_ts = other_node.sync_ts

    @property
    def mime_type_uid(self) -> UID:
        raise InvalidOperationError

    @property
    def is_shared(self):
        return self._is_shared

    @is_shared.setter
    def is_shared(self, val: bool):
        self._is_shared = val

    def set_modify_ts(self, modify_ts: int):
        self._modify_ts = ensure_int(modify_ts)

    @property
    def create_ts(self):
        return self._create_ts

    @create_ts.setter
    def create_ts(self, create_ts):
        self._create_ts = ensure_int(create_ts)

    @property
    def modify_ts(self):
        return self._modify_ts

    @modify_ts.setter
    def modify_ts(self, modify_ts):
        self._modify_ts = ensure_int(modify_ts)

    @property
    def sync_ts(self):
        return self._sync_ts

    @sync_ts.setter
    def sync_ts(self, sync_ts: int):
        self._sync_ts = sync_ts

    @property
    def tree_type(self) -> TreeType:
        return TreeType.GDRIVE

    @property
    def name(self) -> str:
        return self._name

    @name.setter
    def name(self, name):
        self._name = name

    def get_trashed_status(self) -> TrashStatus:
        assert not self._trashed or isinstance(self._trashed, TrashStatus), f'Invalid trashed status: {self._trashed}'
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


class GDriveFolder(GDriveNode):
    """
    ◤━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━◥
        CLASS GDriveFolder
    ◣━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━◢
    """
    def __init__(self, node_identifier: GDriveIdentifier, goog_id, node_name, trashed, create_ts, modify_ts, owner_uid, drive_id,
                 is_shared, shared_by_user_uid, sync_ts, all_children_fetched):
        GDriveNode.__init__(self, node_identifier, goog_id, node_name, trashed, create_ts, modify_ts, owner_uid, drive_id, is_shared,
                            shared_by_user_uid, sync_ts)
        self.dir_stats: Optional[DirectoryStats] = None

        self.all_children_fetched: bool = ensure_bool(all_children_fetched)
        """If true, all its children have been fetched from Google"""

    def __repr__(self):
        return f'GDriveFolder(id={self.node_identifier} goog_id="{self.goog_id}" name="{repr(self.name)}" trashed={self.trashed_str} ' \
               f'owner_uid={self.owner_uid} drive_id={self.drive_id} is_shared={self._is_shared} shared_by_user_uid={self.shared_by_user_uid} ' \
               f'sync_ts={self.sync_ts} parent_uids={self.get_parent_uids()} all_children_fetched={self.all_children_fetched}]'

    def update_from(self, other_node):
        if not isinstance(other_node, GDriveFolder):
            raise RuntimeError(f'Bad: {other_node} (we are: {self})')
        GDriveNode.update_from(self, other_node)
        self.dir_stats = other_node.dir_stats
        self.all_children_fetched = other_node.all_children_fetched

    @classmethod
    def has_tuple(cls) -> bool:
        return True

    def to_tuple(self):
        return self.uid, self.goog_id, self.name, self.get_trashed_status(), self.create_ts, self._modify_ts, self.owner_uid, \
               self.drive_id, self._is_shared, self.shared_by_user_uid, self.sync_ts, self.all_children_fetched

    def is_parent_of(self, potential_child_node: Node) -> bool:
        if potential_child_node.device_uid == self.device_uid:
            assert isinstance(potential_child_node, GDriveNode)
            return self.uid in potential_child_node.get_parent_uids()
        return False

    def get_size_bytes(self):
        if self.dir_stats:
            return self.dir_stats.get_size_bytes()
        return None

    def get_etc(self):
        if self.dir_stats:
            return self.dir_stats.get_etc()
        return None

    @property
    def mime_type_uid(self) -> UID:
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

    def get_default_icon(self) -> IconId:
        if self.get_trashed_status() == TrashStatus.NOT_TRASHED:
            if self.is_live():
                return IconId.ICON_GENERIC_DIR
            else:
                return IconId.ICON_DIR_MK
        return IconId.ICON_DIR_TRASHED

    def __eq__(self, other):
        """Compares against the node's metadata. Matches ONLY the node's identity and content; not its parents, children, or derived path"""
        if isinstance(other, GDriveFolder) and \
                other.uid == self.uid and \
                other.goog_id == self.goog_id and \
                other.name == self.name and \
                other.get_trashed_status() == self.get_trashed_status() and \
                other._create_ts == self._create_ts and \
                other._modify_ts == self._modify_ts and \
                other.owner_uid == self.owner_uid and \
                other.drive_id == self.drive_id and \
                other.is_shared == self._is_shared and \
                other.shared_by_user_uid and \
                other.get_icon() == self.get_icon():
            return True

        return False

    def __ne__(self, other):
        return not self.__eq__(other)


class GDriveFile(GDriveNode):
    """
    ◤━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━◥
        CLASS GDriveFile
    ◣━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━◢
    """
    # TODO: handling of shortcuts... does a shortcut have an ID?
    # TODO: handling of special chars in file systems

    def __init__(self, node_identifier: GDriveIdentifier, goog_id, node_name, mime_type_uid, trashed, drive_id, version, content_meta,
                 is_shared, create_ts, modify_ts, owner_uid, shared_by_user_uid, sync_ts):
        GDriveNode.__init__(self, node_identifier, goog_id, node_name, trashed, create_ts, modify_ts, owner_uid, drive_id, is_shared,
                            shared_by_user_uid, sync_ts)

        self._mime_type_uid = ensure_uid(mime_type_uid)
        self.version = ensure_int(version)
        self.content_meta: ContentMeta = content_meta

    def __repr__(self):
        return f'GDriveFile(id={self.node_identifier} goog_id="{self.goog_id}" name="{repr(self.name)}" mime_type_uid={self.mime_type_uid} ' \
               f'trashed={self.trashed_str} size={self.get_size_bytes()} content_uid={self.content_meta.uid} ' \
               f'create_ts={self.create_ts} modify_ts={self.modify_ts} ' \
               f'owner_uid={self.owner_uid} drive_id={self.drive_id} is_shared={self.is_shared} shared_by_user_uid={self.shared_by_user_uid} ' \
               f'version={self.version} sync_ts={self.sync_ts} icon={self.get_icon()} parent_uids={self.get_parent_uids()})'

    def __eq__(self, other):
        """Compares against the node's metadata. Matches ONLY the node's identity and content; not its parents, children, or derived path"""
        if isinstance(other, GDriveFile) and \
                other.uid == self.uid and \
                other.device_uid == other.device_uid and \
                other.goog_id == self.goog_id and \
                other.name == self.name and \
                other.mime_type_uid == self.mime_type_uid and \
                other.get_trashed_status() == self.get_trashed_status() and \
                other.content_meta == self.content_meta and \
                other.version == self.version and \
                other._create_ts == self._create_ts and \
                other._modify_ts == self._modify_ts and \
                other.owner_uid == self.owner_uid and \
                other.drive_id == self.drive_id and \
                other.is_shared == self.is_shared and \
                other.shared_by_user_uid == self.shared_by_user_uid and \
                other.get_icon() == self.get_icon():
            return True

        return False

    def __ne__(self, other):
        return not self.__eq__(other)

    @property
    def mime_type_uid(self) -> UID:
        return self._mime_type_uid

    def update_from(self, other_node):
        if not isinstance(other_node, GDriveFile):
            raise RuntimeError(f'Bad: {other_node} (we are: {self})')
        GDriveNode.update_from(self, other_node)
        self._mime_type_uid = other_node.mime_type_uid
        self.version = other_node.version
        self.content_meta = other_node.content_meta
        self._trashed: TrashStatus = other_node.get_trashed_status()

    def is_parent_of(self, potential_child_node: Node) -> bool:
        # A file can never be the parent of anything
        return False

    def has_signature(self) -> bool:
        return self.content_meta.has_signature()

    @property
    def md5(self):
        return self.content_meta.md5

    def get_size_bytes(self):
        return self.content_meta.size_bytes

    @classmethod
    def get_obj_type(cls):
        return OBJ_TYPE_FILE

    @classmethod
    def is_file(cls):
        return True

    @classmethod
    def is_dir(cls):
        return False

    def get_default_icon(self) -> IconId:
        if self.get_trashed_status() == TrashStatus.NOT_TRASHED:
            if self.is_live():
                return IconId.ICON_GENERIC_FILE
            else:
                return IconId.ICON_FILE_CP_DST
        return IconId.ICON_FILE_TRASHED

    @classmethod
    def has_tuple(cls) -> bool:
        return True

    def to_tuple(self):
        return (self.uid, self.goog_id, self.name, self.mime_type_uid, self.get_trashed_status(), self.content_meta.uid,
                self.create_ts, self.modify_ts, self.owner_uid, self.drive_id, self.is_shared, self.shared_by_user_uid, self.version,
                self.sync_ts)
