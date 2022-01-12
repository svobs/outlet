from enum import IntEnum
import logging
from typing import Dict, List, Optional, Set, Union

from constants import ChangeTreeCategory, IconId, TreeID
from model.node_identifier import GUID
from model.uid import UID
from model.node.node import BaseNode, Node
from util import time_util

logger = logging.getLogger(__name__)


class UserOpType(IntEnum):
    """
    ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
    ENUM UserOpType
    UserOps are agnostic of tree types, but they distinguish between file and dir node operations.
    TODO: rename to TNOType
    ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼
    """
    # --- 1-digit enum = 1 node op ---

    RM = 1
    """Remove src node: file or empty dir"""

    MKDIR = 2
    """Make dir represented by src node."""

    UNLINK = 3
    """Will (a) just remove from parent, for GDrive nodes, or (b) unlink shortcuts/links, if those type"""

    # --- 2-node ops ---

    CP = 10
    """Copy content of src node to dst node, where dst node does not yet exist. Does not work for dirs."""

    CP_ONTO = 11
    """AKA "Update".
    Copy content of src node to existing dst node, overwriting the previous contents of dst. Does not work for dirs.
    
    Implmeentation note: unlike its CP op counterpart, the dst node of this operation should be the node about to be removed
    (with is_live=true). We need to retain the information about the node being replaced. The UI will need to add special logic of
    its own if it wants to display info about the node overwriting it."""

    START_DIR_CP = 12
    """Creates a dir at dst, copying the attributes of the src dir if possible. If not possible, this may be equivalent to MKDIR [dst].
    This operation is distinct from MKDIR both to avoid confusion about intent, and in the hope that some platforms may be able to accomplish
    both the MKDIR and meta copy in a single atomic operation."""

    FINISH_DIR_CP = 13
    """Copies the meta from an existing (src) dir to an existing dst dir.
    This will fail if the dst dir does not exist (the dst dir is assumed to have already been created with START_DIR_CP)."""

    MV = 20
    """Equivalent to CP followed by RM: copy src node to dst node, then delete src node. Does not work for dirs.
    I would actually get rid of this and replace it with a CP followed with an RM, but most file systems provide an atomic operation for this,
    so let's honor that."""

    MV_ONTO = 21
    """Similar to MV, but replace node at dst with src. Copy content of src node to dst node, overwriting the contents of dst, then delete src.
    
    Implmeentation note: unlike its MV op counterpart, the dst node of this operation should stay the same as the node about to be removed
    (which likely has is_live=true). We need to retain the information about the node being replaced. The UI will need to add special logic of
    its own if it wants to display info about the node overwriting it."""

    START_DIR_MV = 22
    """Creates a dir at dst, copying the attributes of the src dir if possible. If not possible, this may just be equivalent to MKDIR [dst].
    This operation is distinct from MKDIR both to avoid confusion about intent, and in the hope that some platforms may be able to accomplish
    both the MKDIR and meta copy in a single atomic operation."""

    FINISH_DIR_MV = 23
    """Copies the meta from an existing (src) dir to an existing dst dir, then deletes src dir, which must be empty.
    This will fail if the dst dir does not exist (the dst dir is assumed to have already been created with START_DIR_CP)."""

    CREATE_LINK = 30
    """Create a link at dst which points to src."""

    def has_dst(self) -> bool:
        return self.value >= 10


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
        """Completed" set DOES NOT include stopped on error"""
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
        return f'UserOpResult(status={self.status.name} error={self.error} to_upsert={self.nodes_to_upsert} to_remove={self.nodes_to_remove}'


class UserOp(BaseNode):
    """
    ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
    CLASS UserOp
    TODO: rename to TreeNodeOperation (TNO)
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

    def is_start_dir_type(self) -> bool:
        return self.op_type == UserOpType.START_DIR_MV or self.op_type == UserOpType.START_DIR_CP

    def is_finish_dir_type(self) -> bool:
        return self.op_type == UserOpType.FINISH_DIR_MV or self.op_type == UserOpType.FINISH_DIR_CP

    def has_dst(self) -> bool:
        return self.op_type.has_dst()

    def __repr__(self):
        if self.dst_node:
            dst = self.dst_node.node_identifier
        else:
            dst = 'None'
        return f'UserOp(uid={self.op_uid} batch={self.batch_uid} type={self.op_type.name} status={self.get_status().name} ' \
               f'src={self.src_node.node_identifier} dst={dst}'


class ChangeTreeCategoryMeta:
    _category_enum_name_dict: Dict[str, ChangeTreeCategory] = {
        ChangeTreeCategory.RM.name: ChangeTreeCategory.RM,
        ChangeTreeCategory.CP.name: ChangeTreeCategory.CP,
        ChangeTreeCategory.CP_ONTO.name: ChangeTreeCategory.CP_ONTO,
        ChangeTreeCategory.MV.name: ChangeTreeCategory.MV,
        ChangeTreeCategory.MV_ONTO.name: ChangeTreeCategory.MV_ONTO
    }

    _op_type_dict: Dict[UserOpType, ChangeTreeCategory] = {
        UserOpType.RM: ChangeTreeCategory.RM,
        UserOpType.MKDIR: ChangeTreeCategory.CP,  # TODO: is this a good idea?
        UserOpType.CP: ChangeTreeCategory.CP,
        UserOpType.START_DIR_CP: ChangeTreeCategory.CP,
        UserOpType.FINISH_DIR_CP: ChangeTreeCategory.CP,
        UserOpType.CP_ONTO: ChangeTreeCategory.CP_ONTO,
        UserOpType.MV: ChangeTreeCategory.MV,
        UserOpType.START_DIR_MV: ChangeTreeCategory.MV,
        UserOpType.FINISH_DIR_MV: ChangeTreeCategory.MV,
        UserOpType.MV_ONTO: ChangeTreeCategory.MV_ONTO,
    }

    # We use these labels for displaying diff previews - thus the 'To Update' name for CP_ONTO.
    # But we also use these for printing nodes to the log
    _display_label_dict: Dict[ChangeTreeCategory, str] = {
        ChangeTreeCategory.RM: 'To Delete',
        ChangeTreeCategory.CP: 'To Add',
        ChangeTreeCategory.CP_ONTO: 'To Update',
        ChangeTreeCategory.MV: 'To Move',
        ChangeTreeCategory.MV_ONTO: 'To Replace'
    }

    _icon_src_file_dict = {
        ChangeTreeCategory.RM: IconId.ICON_FILE_RM,
        ChangeTreeCategory.MV: IconId.ICON_FILE_MV_SRC,
        ChangeTreeCategory.MV_ONTO: IconId.ICON_FILE_MV_SRC,
        ChangeTreeCategory.CP: IconId.ICON_FILE_CP_SRC,
        ChangeTreeCategory.CP_ONTO: IconId.ICON_FILE_UP_SRC
    }
    _icon_dst_file_dict = {
        ChangeTreeCategory.MV: IconId.ICON_FILE_MV_DST,
        ChangeTreeCategory.MV_ONTO: IconId.ICON_FILE_MV_DST,
        ChangeTreeCategory.CP: IconId.ICON_FILE_CP_DST,
        ChangeTreeCategory.CP_ONTO: IconId.ICON_FILE_UP_DST
    }
    _icon_src_dir_dict = {
        ChangeTreeCategory.RM: IconId.ICON_DIR_RM,
        ChangeTreeCategory.MV: IconId.ICON_DIR_MV_SRC,
        ChangeTreeCategory.MV_ONTO: IconId.ICON_DIR_MV_SRC,
        ChangeTreeCategory.CP: IconId.ICON_DIR_CP_SRC,
        ChangeTreeCategory.CP_ONTO: IconId.ICON_DIR_UP_SRC
    }
    _icon_dst_dir_dict = {
        ChangeTreeCategory.MV: IconId.ICON_DIR_MV_DST,
        ChangeTreeCategory.MV_ONTO: IconId.ICON_DIR_MV_DST,
        ChangeTreeCategory.CP: IconId.ICON_DIR_CP_DST,
        ChangeTreeCategory.CP_ONTO: IconId.ICON_DIR_UP_DST
    }
    _icon_cat_node = {
        ChangeTreeCategory.RM: IconId.ICON_TO_DELETE,
        ChangeTreeCategory.MV: IconId.ICON_TO_MOVE,
        ChangeTreeCategory.MV_ONTO: IconId.ICON_TO_MOVE,
        ChangeTreeCategory.CP: IconId.ICON_TO_ADD,
        ChangeTreeCategory.CP_ONTO: IconId.ICON_TO_UPDATE
    }

    @staticmethod
    def category_for_op_type(op_type: UserOpType) -> Optional[ChangeTreeCategory]:
        return ChangeTreeCategoryMeta._op_type_dict.get(op_type)

    @staticmethod
    def category_for_name(cat_enum_name: str) -> Optional[ChangeTreeCategory]:
        return ChangeTreeCategoryMeta._category_enum_name_dict.get(cat_enum_name)

    @staticmethod
    def category_label(category: ChangeTreeCategory) -> str:
        return ChangeTreeCategoryMeta._display_label_dict[category]

    @staticmethod
    def all_display_labels():
        return ChangeTreeCategoryMeta._display_label_dict.items()

    @staticmethod
    def icon_cat_node(category: ChangeTreeCategory) -> IconId:
        return ChangeTreeCategoryMeta._icon_cat_node[category]

    @staticmethod
    def icon_src_file(category: ChangeTreeCategory) -> IconId:
        return ChangeTreeCategoryMeta._icon_src_file_dict[category]

    @staticmethod
    def icon_dst_file(category: ChangeTreeCategory) -> IconId:
        return ChangeTreeCategoryMeta._icon_dst_file_dict[category]

    @staticmethod
    def icon_src_dir(category: ChangeTreeCategory) -> IconId:
        return ChangeTreeCategoryMeta._icon_src_dir_dict[category]

    @staticmethod
    def icon_dst_dir(category: ChangeTreeCategory) -> IconId:
        return ChangeTreeCategoryMeta._icon_dst_dir_dict[category]

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
            category = ChangeTreeCategoryMeta.category_for_op_type(op.op_type)
            return ChangeTreeCategoryMeta.get_icon_for_node_with_category(is_dir, is_dst, category)

    @staticmethod
    def get_icon_for_node_with_category(is_dir: bool, is_dst: bool, category: ChangeTreeCategory) -> IconId:
        if is_dir:
            if is_dst:
                return ChangeTreeCategoryMeta._icon_dst_dir_dict[category]
            else:
                return ChangeTreeCategoryMeta._icon_src_dir_dict[category]
        else:
            if is_dst:
                return ChangeTreeCategoryMeta._icon_dst_file_dict[category]
            else:
                return ChangeTreeCategoryMeta._icon_src_file_dict[category]


class Batch:
    """
    ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
    CLASS Batch
    # TODO: think of better name for this class
    Container for a list of UserOps comprising a single transaction, such as a single drag & drop.
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
