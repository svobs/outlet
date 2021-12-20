from enum import IntEnum
import logging
from typing import Dict, List, Optional, Set, Union

from constants import IconId, TreeID
from model.node_identifier import GUID, NodeIdentifier
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
    """AKA "Update".
    Copy content of src node to existing dst node, overwriting the previous contents of dst.
    
    Implmeentation note: unlike its CP op counterpart, the dst node of this operation should stay the same as the node about to be removed
    (which likely has is_live=true). We need to retain the information about the node being replaced. The UI will need to add special logic of
    its own if it wants to display info about the node overwriting it."""

    MV = 6
    """Equivalent to CP followed by RM: copy src node to dst node, then delete src node.
    I would actually get rid of this and replace it with a CP followed with an RM, but most file systems provide an atomic operation for this,
    so let's honor that."""

    MV_ONTO = 7
    """Similar to MV, but replace node at dst with src. Copy content of src node to dst node, overwriting the contents of dst, then delete src.
    
    Implmeentation note: unlike its MV op counterpart, the dst node of this operation should stay the same as the node about to be removed
    (which likely has is_live=true). We need to retain the information about the node being replaced. The UI will need to add special logic of
    its own if it wants to display info about the node overwriting it."""

    def has_dst(self) -> bool:
        return self == UserOpType.CP or self == UserOpType.MV or self == UserOpType.CP_ONTO or self == UserOpType.MV_ONTO


# ENUM UserOpStatus
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼

class UserOpStatus(IntEnum):
    NOT_STARTED = 1
    EXECUTING = 2
    BLOCKED_BY_ERROR = 3  # upstream error in OpGraph is preventing execution
    STOPPED_ON_ERROR = 4

    # all the values below here are completed:
    COMPLETED_OK = 10
    COMPLETED_NO_OP = 11

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

    def __repr__(self):
        return f'UserOpResult(status={self.status.name} ' \
               f'error={self.error} to_upsert={self.nodes_to_upsert} to_remove={self.nodes_to_remove}'


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

    def set_status(self, status: UserOpStatus):
        if not self.result:
            self.result = UserOpResult(status=status)
        else:
            self.result.status = status

    def is_stopped_on_error(self) -> bool:
        status = self.get_status()
        return status == UserOpStatus.STOPPED_ON_ERROR or status == UserOpStatus.BLOCKED_BY_ERROR

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
    def get_icon_for_node(is_dir: bool, is_dst: bool, op: UserOp) -> IconId:
        if op.get_status() == UserOpStatus.STOPPED_ON_ERROR:
            if is_dir:
                return IconId.ICON_DIR_ERROR
            else:
                return IconId.ICON_FILE_ERROR
        elif op.get_status() == UserOpStatus.BLOCKED_BY_ERROR:
            if is_dir:
                return IconId.ICON_DIR_WARNING
            else:
                return IconId.ICON_FILE_WARNING
        else:
            if is_dir:
                if is_dst:
                    return OpTypeMeta._icon_dst_dir_dict[op.op_type]
                else:
                    return OpTypeMeta._icon_src_dir_dict[op.op_type]
            else:
                if is_dst:
                    return OpTypeMeta._icon_dst_file_dict[op.op_type]
                else:
                    return OpTypeMeta._icon_src_file_dict[op.op_type]


class Batch:
    """
    ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
    CLASS Batch
    ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼
    """

    def __init__(self, batch_uid: UID, op_list: List[UserOp], to_select_in_ui: Optional[Set[GUID]] = None, select_ts: Optional[int] = 0,
                 select_in_tree_id: Optional[TreeID] = None):
        assert batch_uid, 'No batch_uid!'

        self.batch_uid: UID = batch_uid
        self.op_list: List[UserOp] = op_list

        # If provided, will attempt to send a signal back to UI to select the nodes with these identifiers (e.g. for drag & drop).
        # However, these will be ignored if it is determined that the user changed the selection before we are able to get around to them.
        # These are not currently persisted, and may be lost if we go down before we are able to change the selection.
        self.to_select_in_ui: Set[GUID] = to_select_in_ui
        self.select_ts: int = select_ts
        self.select_in_tree_id: TreeID = select_in_tree_id
