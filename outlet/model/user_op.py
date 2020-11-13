import time
from enum import IntEnum
import logging

import treelib
from treelib import Node

from constants import ICON_DIR_CP_DST, ICON_DIR_CP_SRC, ICON_DIR_MK, ICON_DIR_MV_DST, ICON_DIR_MV_SRC, ICON_DIR_RM, \
    ICON_DIR_UP_DST, ICON_DIR_UP_SRC, ICON_FILE_CP_DST, ICON_FILE_CP_SRC, \
    ICON_FILE_MV_DST, ICON_FILE_MV_SRC, ICON_FILE_RM, ICON_FILE_UP_DST, ICON_FILE_UP_SRC
from model.uid import UID
from model.node.node import Node

logger = logging.getLogger(__name__)

# ENUM UserOpType
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼

class UserOpType(IntEnum):
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
        return self == UserOpType.CP or self == UserOpType.MV or self == UserOpType.UP


USER_OP_TYPES = [UserOpType.CP, UserOpType.RM, UserOpType.UP, UserOpType.MV]


# Class UserOpRef
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼

class UserOpRef:
    def __init__(self, op_uid: UID, batch_uid: UID, op_type: UserOpType, src_uid: UID, dst_uid: UID = None, create_ts: int = None):
        self.op_uid: UID = op_uid
        self.batch_uid: UID = batch_uid
        self.op_type: UserOpType = op_type
        self.src_uid: UID = src_uid
        self.dst_uid: UID = dst_uid
        self.create_ts: int = create_ts
        if not self.create_ts:
            self.create_ts = int(time.time())

    def __repr__(self):
        return f'UserOpRef(uid={self.op_uid} type={self.op_type.name} src={self.src_uid} dst={self.dst_uid}'


# Class UserOp
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼

class UserOp(treelib.Node):
    icon_src_file_dict = {UserOpType.RM: ICON_FILE_RM,
                          UserOpType.MV: ICON_FILE_MV_SRC,
                          UserOpType.UP: ICON_FILE_UP_SRC,
                          UserOpType.CP: ICON_FILE_CP_SRC}
    icon_dst_file_dict = {UserOpType.MV: ICON_FILE_MV_DST,
                          UserOpType.UP: ICON_FILE_UP_DST,
                          UserOpType.CP: ICON_FILE_CP_DST}
    icon_src_dir_dict = {UserOpType.MKDIR: ICON_DIR_MK,
                         UserOpType.RM: ICON_DIR_RM,
                         UserOpType.MV: ICON_DIR_MV_SRC,
                         UserOpType.UP: ICON_DIR_UP_SRC,
                         UserOpType.CP: ICON_DIR_CP_SRC}
    icon_dst_dir_dict = {UserOpType.MV: ICON_DIR_MV_DST,
                         UserOpType.UP: ICON_DIR_UP_DST,
                         UserOpType.CP: ICON_DIR_CP_DST}

    def __init__(self, op_uid: UID, batch_uid: UID, op_type: UserOpType, src_node: Node,
                 dst_node: Node = None, create_ts: int = None):
        assert src_node, 'No src node!'
        treelib.Node.__init__(self, identifier=op_uid)
        self.op_uid: UID = op_uid
        self.batch_uid: UID = batch_uid
        self.op_type: UserOpType = op_type
        self.src_node: Node = src_node
        self.dst_node: Node = dst_node
        """If it exists, this is the target. Otherwise the target is the src node"""

        self.create_ts = create_ts
        if not self.create_ts:
            self.create_ts = int(time.time())

        self._completed: bool = False
        """This is only briefly used in a brief interval right after the op has completed, but not yet updated everywhere"""

        self.tag = repr(self)

    def is_completed(self) -> bool:
        return self._completed

    def set_completed(self):
        logger.debug(f'Setting op complete: {self}')
        self._completed = True

    def has_dst(self) -> bool:
        return self.op_type.has_dst()

    def get_icon_for_node(self, node_uid: UID):
        if self.has_dst() and self.dst_node.uid == node_uid:
            op_type = self.op_type
            if op_type == UserOpType.MV and not self.dst_node.is_live():
                # Use an add-like icon if nothing there right now:
                op_type = UserOpType.CP

            if self.dst_node.is_dir():
                return UserOp.icon_dst_dir_dict[op_type]
            else:
                return UserOp.icon_dst_file_dict[op_type]

        assert self.src_node.uid == node_uid
        if self.src_node.is_dir():
            return UserOp.icon_src_dir_dict[self.op_type]
        else:
            return UserOp.icon_src_file_dict[self.op_type]

    def __repr__(self):
        if self.dst_node:
            dst = self.dst_node.node_identifier
        else:
            dst = 'None'
        return f'UserOp(uid={self.op_uid} batch={self.batch_uid} type={self.op_type.name} src={self.src_node.node_identifier} ' \
               f'dst={dst}'
