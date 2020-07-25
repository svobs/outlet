import time
from enum import IntEnum

from treelib import Node

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
    def __init__(self, action_uid: UID, batch_uid: UID, change_type: ChangeType, src_node: DisplayNode,
                 dst_node: DisplayNode = None, create_ts: int = None):
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

    def __repr__(self):
        return f'ChangeAction(uid={self.action_uid} batch={self.batch_uid} type={self.change_type.name} src={self.src_node.node_identifier} ' \
               f'dst={self.dst_node.node_identifier}'
