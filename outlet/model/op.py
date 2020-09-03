import time
from enum import IntEnum
from typing import Optional

from treelib import Node

from constants import ICON_ADD_DIR, ICON_ADD_FILE, ICON_GENERIC_DIR, ICON_GENERIC_FILE, ICON_MODIFY_FILE
from index.uid.uid import UID
from model.node.display_node import DisplayNode


# ENUM OpType
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼

class OpType(IntEnum):
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
        return self == OpType.CP or self == OpType.MV or self == OpType.UP


# Class OpRef
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼

class OpRef:
    def __init__(self, action_uid: UID, batch_uid: UID, op_type: OpType, src_uid: UID, dst_uid: UID = None, create_ts: int = None):
        self.action_uid: UID = action_uid
        self.batch_uid: UID = batch_uid
        self.op_type: OpType = op_type
        self.src_uid: UID = src_uid
        self.dst_uid: UID = dst_uid
        self.create_ts: int = create_ts
        if not self.create_ts:
            self.create_ts = int(time.time())

    def __repr__(self):
        return f'OpRef(uid={self.action_uid} type={self.op_type.name} src={self.src_uid} dst={self.dst_uid}'


# Class Op
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼

class Op(Node):
    # TODO: add additional icons
    icon_src_file_dict = {OpType.RM: OpType.RM.name,
                          OpType.MV: ICON_GENERIC_FILE,
                          OpType.UP: ICON_GENERIC_FILE,
                          OpType.CP: ICON_GENERIC_FILE}
    icon_dst_file_dict = {OpType.MV: ICON_ADD_FILE,
                          OpType.UP: ICON_MODIFY_FILE,
                          OpType.CP: ICON_ADD_FILE}
    icon_src_dir_dict = {OpType.MKDIR: ICON_ADD_DIR,
                         OpType.RM: ICON_GENERIC_DIR,
                         OpType.MV: ICON_GENERIC_DIR,
                         OpType.UP: ICON_GENERIC_DIR,
                         OpType.CP: ICON_GENERIC_DIR}
    icon_dst_dir_dict = {OpType.MV: ICON_ADD_DIR,
                         OpType.UP: ICON_GENERIC_DIR,
                         OpType.CP: ICON_ADD_DIR}

    def __init__(self, action_uid: UID, batch_uid: UID, op_type: OpType, src_node: DisplayNode,
                 dst_node: DisplayNode = None, create_ts: int = None):
        assert src_node, 'No src node!'
        Node.__init__(self, identifier=action_uid)
        self.action_uid: UID = action_uid
        self.batch_uid: UID = batch_uid
        self.op_type: OpType = op_type
        self.src_node: DisplayNode = src_node
        self.dst_node: DisplayNode = dst_node
        """If it exists, this is the target. Otherwise the target is the src node"""

        self.create_ts = create_ts
        if not self.create_ts:
            self.create_ts = int(time.time())

        self._completed: bool = False
        """This is only briefly used in a brief interval right after the op has completed, but not yet updated everywhere"""

        self.tag = repr(self)

    def is_completed(self):
        return self._completed

    def set_completed(self):
        self._completed = True

    def has_dst(self) -> bool:
        return self.op_type.has_dst()

    def get_icon_for_node(self, node_uid: UID):
        if self.has_dst() and self.dst_node.uid == node_uid:
            if self.dst_node.is_dir():
                return Op.icon_dst_dir_dict[self.op_type]
            else:
                return Op.icon_dst_file_dict[self.op_type]

        assert self.src_node.uid == node_uid
        if self.src_node.is_dir():
            return Op.icon_src_dir_dict[self.op_type]
        else:
            return Op.icon_src_file_dict[self.op_type]

    def get_planning_node(self) -> Optional[DisplayNode]:
        """Returns the "planning node" (i.e., the node to be created by this transcation), if any"""
        if self.op_type == OpType.MKDIR:
            assert not self.src_node.exists(), f'Expected to not exist: {self.src_node}'
            return self.src_node
        elif self.op_type == OpType.CP:
            assert not self.dst_node.exists(), f'Expected to not exist: {self.dst_node}'
            return self.dst_node
        elif self.op_type == OpType.MV:
            assert not self.dst_node.exists(), f'Expected to not exist: {self.dst_node}'
            return self.dst_node
        return None

    def __repr__(self):
        if self.dst_node:
            dst = self.dst_node.node_identifier
        else:
            dst = 'None'
        return f'Op(uid={self.action_uid} batch={self.batch_uid} type={self.op_type.name} src={self.src_node.node_identifier} ' \
               f'dst={dst}'
