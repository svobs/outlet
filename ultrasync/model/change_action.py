from enum import IntEnum
from index.uid.uid import UID
from model.display_node import DisplayNode


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


# Class ChangeActionRef
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼

class ChangeActionRef:
    def __init__(self, action_uid: UID, change_type: ChangeType, src_uid: UID, dst_uid: UID = None):
        self.action_uid: UID = action_uid
        self.change_type: ChangeType = change_type
        self.src_uid: UID = src_uid
        self.dst_uid: UID = dst_uid

    def __repr__(self):
        return f'ChangeActionRef(uid={self.action_uid} type={self.change_type.name} src={self.src_uid} dst={self.dst_uid}'


# Class ChangeAction
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼

class ChangeAction:
    def __init__(self, action_uid: UID, change_type: ChangeType, src_node: DisplayNode, dst_node: DisplayNode = None):
        self.action_uid: UID = action_uid
        self.change_type: ChangeType = change_type
        self.src_node: DisplayNode = src_node
        self.dst_node: DisplayNode = dst_node
        """If it exists, this is the target. Otherwise the target is the src node"""

    def __repr__(self):
        return f'ChangeAction(uid={self.action_uid} type={self.change_type.name} src={self.src_node.node_identifier} ' \
               f'dst={self.dst_node.node_identifier}'
