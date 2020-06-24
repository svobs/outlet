
from enum import IntEnum

from index.uid import UID


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


# Class ChangeAction
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼

class ChangeAction:
    def __init__(self, change_type: ChangeType, action_uid: UID, src_uid: UID, dst_uid: UID = None):
        self.change_type: ChangeType = change_type
        self.action_uid: UID = action_uid
        self.src_uid: UID = src_uid
        self.dst_uid: UID = dst_uid

    def __repr__(self):
        return f'ChangeAction(uid={self.action_uid} type={self.change_type.name} src={self.src_uid} dst={self.dst_uid}'
