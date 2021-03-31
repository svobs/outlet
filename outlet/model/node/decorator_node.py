import copy
from typing import Tuple

from constants import IconId, TrashStatus, TreeType
from model.node.node import Node
from model.node_identifier import NodeIdentifier
from model.uid import UID
from util.ensure import ensure_uid


class DecoNode(Node):
    """
    ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
    CLASS DecoNode

    Wraps a delegate node and passes all methods to the delegate, except for node_identifier and parent_uid, which instead point to their
    display-tree-dependent values.
    ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼
    """
    def __init__(self, uid: UID, parent_uid: UID, delegate_node):
        node_identifier: NodeIdentifier = copy.copy(delegate_node.node_identifier)
        node_identifier.uid = uid
        super().__init__(node_identifier, parent_uid, delegate_node.get_trashed_status())
        self.delegate = delegate_node

    @classmethod
    def is_decorator(cls):
        return True

    def update_from(self, other_node):
        self.delegate.update_from(other_node)

    def get_trashed_status(self) -> TrashStatus:
        return self.delegate.get_trashed_status()

    def set_trashed_status(self, trashed: TrashStatus):
        self.delegate.set_trashed_status(trashed)

    def is_parent_of(self, potential_child_node: Node) -> bool:
        # A file can never be the parent of anything
        return self.delegate.is_parent_of(potential_child_node)

    def get_obj_type(self):
        return self.delegate.get_obj_type()

    def is_file(self):
        return self.delegate.is_file()

    def is_dir(self):
        return self.delegate.is_dir()

    def get_size_bytes(self):
        return self.delegate.get_size_bytes()

    def set_size_bytes(self, size_bytes: int):
        self.delegate.set_size_bytes(size_bytes)

    @property
    def tree_type(self) -> TreeType:
        return self.delegate.tree_type

    def get_default_icon(self) -> IconId:
        return self.delegate.get_default_icon()

    @property
    def trashed_str(self):
        return self.delegate.trashed_str

    def is_live(self) -> bool:
        return self.delegate.is_live()

    def set_is_live(self, is_live) -> bool:
        return self.delegate.set_is_live(is_live)

    @property
    def md5(self):
        return self.delegate.md5

    @md5.setter
    def md5(self, md5):
        self.delegate.md5 = md5

    @property
    def sha256(self):
        return self.delegate.sha256

    @sha256.setter
    def sha256(self, sha256):
        self.delegate.sha256 = sha256

    @property
    def sync_ts(self):
        return self.delegate.sync_ts

    @property
    def modify_ts(self):
        return self.delegate.modify_ts

    @modify_ts.setter
    def modify_ts(self, modify_ts):
        self.delegate.modify_ts = modify_ts

    @property
    def create_ts(self):
        return self.delegate.create_ts

    @create_ts.setter
    def create_ts(self, create_ts):
        self.delegate.create_ts = create_ts

    @property
    def change_ts(self):
        return self.delegate.change_ts

    @change_ts.setter
    def change_ts(self, change_ts):
        self.delegate.change_ts = change_ts

    def get_icon(self) -> IconId:
        return self.delegate.get_icon()

    def get_custom_icon(self):
        return self.delegate.get_custom_icon()

    def set_icon(self, icon: IconId):
        self._icon = icon

    @classmethod
    def has_tuple(cls) -> bool:
        # This should not be serialized
        return False

    def to_tuple(self) -> Tuple:
        return self.delegate.to_tuple()

    def __eq__(self, other):
        return self.delegate == other

    def __ne__(self, other):
        return not self.__eq__(other)

    def __repr__(self):
        return f'DecoNode({self.node_identifier} parent_uid={self._parent_uids} delegate={self.delegate})'

    # GDrive-only (if applicable):
    # ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼

    @property
    def mime_type_uid(self):
        return self.delegate.mime_type_uid

    @mime_type_uid.setter
    def mime_type_uid(self, mime_type_uid):
        self.delegate.mime_type_uid = mime_type_uid

    @property
    def version(self):
        return self.delegate.version

    @version.setter
    def version(self, version):
        self.delegate.version = version

    @property
    def owner_uid(self):
        return self.delegate.owner_uid

    @owner_uid.setter
    def owner_uid(self, owner_uid):
        self.delegate.owner_uid = owner_uid

    @property
    def shared_by_user_uid(self):
        return self.delegate.shared_by_user_uid

    @shared_by_user_uid.setter
    def shared_by_user_uid(self, shared_by_user_uid):
        self.delegate.shared_by_user_uid = shared_by_user_uid

    @property
    def goog_id(self):
        return self.delegate.goog_id

    @goog_id.setter
    def goog_id(self, goog_id):
        self.delegate.goog_id = goog_id

    @property
    def name(self):
        return self.delegate.name

    @name.setter
    def name(self, name):
        self.delegate.name = name

    @property
    def is_shared(self):
        return self.delegate.is_shared

    @is_shared.setter
    def is_shared(self, is_shared):
        self.delegate.is_shared = is_shared

    @property
    def drive_id(self):
        return self.delegate.drive_id

    @drive_id.setter
    def drive_id(self, drive_id):
        self.delegate.drive_id = drive_id

    # Local-only (if applicable):
    # ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼

    def derive_parent_path(self) -> str:
        return self.delegate.derive_parent_path()

    def get_single_parent(self) -> UID:
        return self.delegate.get_single_parent()


def decorate_node(uid: UID, parent_uid: UID, delegate_node: Node):
    assert not delegate_node.is_decorator()
    uid = ensure_uid(uid)
    parent_uid = ensure_uid(parent_uid)

    return DecoNode(uid, parent_uid, delegate_node)
