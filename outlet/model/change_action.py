import time
from enum import IntEnum

from treelib import Node

from constants import ICON_ADD_DIR, ICON_ADD_FILE, ICON_GENERIC_DIR, ICON_GENERIC_FILE, ICON_MODIFY_FILE
from index.uid.uid import UID
from model.node.display_node import DisplayNode


# ENUM ChangeType
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼

class ChangeType(IntEnum):
    RM = 1
    """Remove src node"""

    CP = 2
    """Copy content of src node to dst node"""

    MKDIR = 3
    """Make dir represented by src node"""

    MV = 4
    """Equivalent to CP followed by RM: copy src node to dst node, then delete src node"""

    UP = 5
    """Essentially equivalent to CP, but intention is different. Copy content of src node to dst node, overwriting the contents of dst"""

    def has_dst(self) -> bool:
        return self == ChangeType.CP or self == ChangeType.MV or self == ChangeType.UP


# Class ChangeActionRef
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼

class ChangeActionRef:
    def __init__(self, action_uid: UID, batch_uid: UID, change_type: ChangeType, src_uid: UID, dst_uid: UID = None, create_ts: int = None):
        self.action_uid: UID = action_uid
        self.batch_uid: UID = batch_uid
        self.change_type: ChangeType = change_type
        self.src_uid: UID = src_uid
        self.dst_uid: UID = dst_uid
        self.create_ts: int = create_ts
        if not self.create_ts:
            self.create_ts = int(time.time())

    def __repr__(self):
        return f'ChangeActionRef(uid={self.action_uid} type={self.change_type.name} src={self.src_uid} dst={self.dst_uid}'


# Class ChangeAction
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼

class ChangeAction(Node):
    # TODO: add additional icons
    icon_src_file_dict = {ChangeType.RM: ChangeType.RM.name,
                          ChangeType.MV: ICON_GENERIC_FILE,
                          ChangeType.UP: ICON_GENERIC_FILE,
                          ChangeType.CP: ICON_GENERIC_FILE}
    icon_dst_file_dict = {ChangeType.MV: ICON_ADD_FILE,
                          ChangeType.UP: ICON_MODIFY_FILE,
                          ChangeType.CP: ICON_ADD_FILE}
    icon_src_dir_dict = {ChangeType.MKDIR: ICON_ADD_DIR,
                         ChangeType.RM: ICON_GENERIC_DIR,
                         ChangeType.MV: ICON_GENERIC_DIR,
                         ChangeType.UP: ICON_GENERIC_DIR,
                         ChangeType.CP: ICON_GENERIC_DIR}
    icon_dst_dir_dict = {ChangeType.MV: ICON_ADD_DIR,
                         ChangeType.UP: ICON_GENERIC_DIR,
                         ChangeType.CP: ICON_ADD_DIR}

    def __init__(self, action_uid: UID, batch_uid: UID, change_type: ChangeType, src_node: DisplayNode,
                 dst_node: DisplayNode = None, create_ts: int = None):
        assert src_node, 'No src node!'
        Node.__init__(self, identifier=action_uid)
        self.action_uid: UID = action_uid
        self.batch_uid: UID = batch_uid
        self.change_type: ChangeType = change_type
        self.src_node: DisplayNode = src_node
        self.dst_node: DisplayNode = dst_node
        """If it exists, this is the target. Otherwise the target is the src node"""

        self.create_ts = create_ts
        if not self.create_ts:
            self.create_ts = int(time.time())

        self.tag = repr(self)

    def has_dst(self) -> bool:
        return self.change_type.has_dst()

    def get_icon_for_node(self, node_uid: UID):
        if self.has_dst() and self.dst_node.uid == node_uid:
            if self.dst_node.is_dir():
                return ChangeAction.icon_dst_dir_dict[self.change_type]
            else:
                return ChangeAction.icon_dst_file_dict[self.change_type]

        assert self.src_node.uid == node_uid
        if self.src_node.is_dir():
            return ChangeAction.icon_src_dir_dict[self.change_type]
        else:
            return ChangeAction.icon_src_file_dict[self.change_type]

    def __repr__(self):
        if self.dst_node:
            dst = self.dst_node.node_identifier
        else:
            dst = 'None'
        return f'ChangeAction(uid={self.action_uid} batch={self.batch_uid} type={self.change_type.name} src={self.src_node.node_identifier} ' \
               f'dst={dst}'
