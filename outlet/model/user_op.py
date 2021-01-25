from enum import IntEnum
import logging
from typing import Dict, List, Optional

from constants import IconId
from model.uid import UID
from model.node.node import Node
from util import time_util
from util.simple_tree import BaseNode

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


DISPLAYED_USER_OP_TYPES: Dict[UserOpType, str] = {
    UserOpType.CP: 'To Add',
    UserOpType.RM: 'To Delete',
    UserOpType.UP: 'To Update',
    UserOpType.MV: 'To Move'
}


def get_uid_for_op_and_tree_type(op_type: UserOpType, tree_type: int):
    return UID(tree_type*10 + op_type)


# ENUM UserOpStatus
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼

class UserOpStatus(IntEnum):
    NOT_STARTED = 1
    EXECUTING = 2
    STOPPED_ON_ERROR = 8
    COMPLETED_NO_OP = 9
    COMPLETED_OK = 10


class UserOpResult:
    """
    ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
    CLASS UserOpResult
    ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼
    """
    def __init__(self, status: UserOpStatus, error=None, to_upsert=None, to_delete=None):
        self.status = status
        self.error = error
        self.nodes_to_upsert: List[Node] = to_upsert
        self.nodes_to_delete: List[Node] = to_delete

    def is_completed(self) -> bool:
        return self.status >= UserOpStatus.STOPPED_ON_ERROR


# Class UserOp
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼

class UserOp(BaseNode):

    def __init__(self, op_uid: UID, batch_uid: UID, op_type: UserOpType, src_node: Node,
                 dst_node: Node = None, create_ts: int = None):
        assert src_node, 'No src node!'
        BaseNode.__init__(self, identifier=op_uid)
        self.op_uid: UID = op_uid
        self.batch_uid: UID = batch_uid
        self.op_type: UserOpType = op_type
        self.src_node: Node = src_node
        self.dst_node: Node = dst_node
        """If it exists, this is the target. Otherwise the target is the src node"""

        self.create_ts = create_ts
        if not self.create_ts:
            self.create_ts = time_util.now_sec()

        self.result: Optional[UserOpResult] = None

        self.tag = repr(self)

    def is_completed(self) -> bool:
        return self.result and self.result.is_completed()

    def status(self) -> UserOpStatus:
        if self.result:
            return self.result.status
        return UserOpStatus.NOT_STARTED

    def has_dst(self) -> bool:
        return self.op_type.has_dst()

    def __repr__(self):
        if self.dst_node:
            dst = self.dst_node.node_identifier
        else:
            dst = 'None'
        return f'UserOp(uid={self.op_uid} batch={self.batch_uid} type={self.op_type.name} src={self.src_node.node_identifier} ' \
               f'dst={dst}'
