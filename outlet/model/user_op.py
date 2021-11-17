from enum import IntEnum
import logging
from typing import Dict, List, Optional, Union

from constants import IconId
from model.uid import UID
from model.node.node import BaseNode, Node
from util import time_util

logger = logging.getLogger(__name__)


# ENUM UserOpType
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼

class UserOpType(IntEnum):
    RM = 1
    """Remove src node (single-node op)"""

    UNLINK = 2
    """Will (a) just remove from parent, for GDrive nodes, or (b) unlink shortcuts/links, if those type"""

    MKDIR = 3
    """Make dir represented by src node (single-node op)"""

    CP = 4
    """Copy content of src node to dst node, where dst node does not yet exist"""

    CP_ONTO = 5
    """Copy content of src node to existing dst node, overwriting the previous contents of dst"""

    MV = 6
    """Equivalent to CP followed by RM: copy src node to dst node, then delete src node.
    I would actually get rid of this and replace it with a CP followed with an RM, but most file systems provide an atomic operation for this,
    so let's honor that."""

    MV_ONTO = 7
    """Similar to MV, but replace node at dst with src. Copy content of src node to dst node, overwriting the contents of dst, then delete src"""

    def has_dst(self) -> bool:
        return self == UserOpType.CP or self == UserOpType.MV or self == UserOpType.CP_ONTO or self == UserOpType.MV_ONTO


# ENUM UserOpStatus
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼

class UserOpStatus(IntEnum):
    NOT_STARTED = 1
    EXECUTING = 2
    COMPLETED_OK = 10
    COMPLETED_NO_OP = 11
    STOPPED_ON_ERROR = 12

    def is_completed(self) -> bool:
        """Note: "completed" set includes possible errors"""
        return self.value >= UserOpStatus.COMPLETED_OK


class UserOpResult:
    """
    ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
    CLASS UserOpResult
    ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼
    """

    def __init__(self, status: UserOpStatus, error: Optional[Union[str, Exception]] = None,
                 to_upsert: Optional[List[Node]] = None, to_remove: Optional[List[Node]] = None):
        self.status: UserOpStatus = status
        self.error: Optional[Union[str, Exception]] = error
        self.nodes_to_upsert: Optional[List[Node]] = to_upsert
        self.nodes_to_remove: Optional[List[Node]] = to_remove

    def is_completed(self) -> bool:
        return self.status.is_completed()


class UserOp(BaseNode):
    """
    ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
    CLASS UserOp
    ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼
    """

    def __init__(self, op_uid: UID, batch_uid: UID, op_type: UserOpType, src_node: Node, dst_node: Optional[Node] = None, create_ts: int = None):
        assert src_node, 'No src node!'
        BaseNode.__init__(self)
        self.op_uid: UID = op_uid
        self.batch_uid: UID = batch_uid
        self.op_type: UserOpType = op_type
        self.src_node: Node = src_node
        self.dst_node: Optional[Node] = dst_node
        """If it exists, this is the target. Otherwise the target is the src node"""

        self.create_ts = create_ts
        if not self.create_ts:
            self.create_ts = time_util.now_sec()

        self.result: Optional[UserOpResult] = None

    def get_tag(self) -> str:
        return repr(self)

    @property
    def identifier(self):
        return self.op_uid

    def is_completed(self) -> bool:
        return self.result and self.result.is_completed()

    def get_status(self) -> UserOpStatus:
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
        return f'UserOp(uid={self.op_uid} batch={self.batch_uid} type={self.op_type.name} status={self.get_status().name} ' \
               f'src={self.src_node.node_identifier} dst={dst}'


class OpTypeMeta:
    # currently we only use these labels for displaying diff previews - thus the 'To Update' name. Also 'To Replace' will never be used for diff prev
    _display_label_dict: Dict[UserOpType, str] = {
        UserOpType.RM: 'To Delete',
        UserOpType.CP: 'To Add',
        UserOpType.CP_ONTO: 'To Update',
        UserOpType.MV: 'To Move',
        UserOpType.MV_ONTO: 'To Replace'
    }

    _icon_src_file_dict = {
        UserOpType.RM: IconId.ICON_FILE_RM,
        UserOpType.MV: IconId.ICON_FILE_MV_SRC,
        UserOpType.MV_ONTO: IconId.ICON_FILE_MV_SRC,
        UserOpType.CP: IconId.ICON_FILE_CP_SRC,
        UserOpType.CP_ONTO: IconId.ICON_FILE_UP_SRC
    }
    _icon_dst_file_dict = {
        UserOpType.MV: IconId.ICON_FILE_MV_DST,
        UserOpType.MV_ONTO: IconId.ICON_FILE_MV_DST,
        UserOpType.CP: IconId.ICON_FILE_CP_DST,
        UserOpType.CP_ONTO: IconId.ICON_FILE_UP_DST
    }
    _icon_src_dir_dict = {
        UserOpType.MKDIR: IconId.ICON_DIR_MK,
        UserOpType.RM: IconId.ICON_DIR_RM,
        UserOpType.MV: IconId.ICON_DIR_MV_SRC,
        UserOpType.MV_ONTO: IconId.ICON_DIR_MV_SRC,
        UserOpType.CP: IconId.ICON_DIR_CP_SRC,
        UserOpType.CP_ONTO: IconId.ICON_DIR_UP_SRC
    }
    _icon_dst_dir_dict = {
        UserOpType.MV: IconId.ICON_DIR_MV_DST,
        UserOpType.MV_ONTO: IconId.ICON_DIR_MV_DST,
        UserOpType.CP: IconId.ICON_DIR_CP_DST,
        UserOpType.CP_ONTO: IconId.ICON_DIR_UP_DST
    }
    _icon_cat_node = {
        UserOpType.RM: IconId.ICON_TO_DELETE,
        UserOpType.MV: IconId.ICON_TO_MOVE,
        UserOpType.MV_ONTO: IconId.ICON_TO_MOVE,
        UserOpType.CP: IconId.ICON_TO_ADD,
        UserOpType.CP_ONTO: IconId.ICON_TO_UPDATE,
    }

    @staticmethod
    def has_dst(op_type: UserOpType) -> bool:
        return op_type.has_dst()

    @staticmethod
    def display_label(op_type: UserOpType) -> str:
        return OpTypeMeta._display_label_dict[op_type]

    @staticmethod
    def all_display_labels():
        return OpTypeMeta._display_label_dict.items()

    @staticmethod
    def icon_src_file(op_type: UserOpType) -> IconId:
        return OpTypeMeta._icon_src_file_dict[op_type]

    @staticmethod
    def icon_dst_file(op_type: UserOpType) -> IconId:
        return OpTypeMeta._icon_dst_file_dict[op_type]

    @staticmethod
    def icon_src_dir(op_type: UserOpType) -> IconId:
        return OpTypeMeta._icon_src_dir_dict[op_type]

    @staticmethod
    def icon_dst_dir(op_type: UserOpType) -> IconId:
        return OpTypeMeta._icon_dst_dir_dict[op_type]

    @staticmethod
    def icon_cat_node(op_type: UserOpType) -> IconId:
        return OpTypeMeta._icon_cat_node[op_type]

    @staticmethod
    def get_icon_for(device_uid: UID, node_uid: UID, op: UserOp) -> IconId:
        if op.has_dst() and op.dst_node.device_uid == device_uid and op.dst_node.uid == node_uid:
            if op.dst_node.is_dir():
                return OpTypeMeta._icon_dst_dir_dict[op.op_type]
            else:
                return OpTypeMeta._icon_dst_file_dict[op.op_type]

        assert op.src_node.uid == node_uid
        if op.src_node.is_dir():
            return OpTypeMeta._icon_src_dir_dict[op.op_type]
        else:
            return OpTypeMeta._icon_src_file_dict[op.op_type]


class Batch:
    """
    ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
    CLASS Batch
    ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼
    """

    def __init__(self, batch_uid: UID, user_op_list: List[UserOp]):
        assert batch_uid, 'No batch_uid!'
        assert user_op_list, f'No ops in batch {batch_uid}!'

        self.batch_uid: UID = batch_uid
        self.op_list: List[UserOp] = user_op_list
