import os
from typing import List, Optional

from constants import IconId, OBJ_TYPE_DIR, TreeType
from error import InvalidOperationError
from model.node.directory_stats import DirectoryStats
from model.node.node import Node
from model.node_identifier import SinglePathNodeIdentifier
from model.uid import UID
from model.user_op import OpTypeMeta, UserOpType


class ContainerNode(Node):
    """
    ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
    CLASS ContainerNode

    Represents a generic display-only directory node which is not backed by a cached object. For use in change trees.
    ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼
    """
    def __init__(self, node_identifier: SinglePathNodeIdentifier):
        assert node_identifier.get_single_path(), f'Bad: {node_identifier}'
        self.dir_stats: Optional[DirectoryStats] = None
        Node.__init__(self, node_identifier)

    def update_from(self, other_node):
        Node.update_from(self, other_node)
        self.dir_stats = other_node.dir_stats

    def get_parent_uids(self) -> List[UID]:
        raise InvalidOperationError

    def has_same_parents(self, other):
        raise InvalidOperationError

    def has_no_parents(self):
        raise InvalidOperationError

    def remove_parent(self, parent_uid_to_remove: UID):
        raise InvalidOperationError

    def add_parent(self, parent_uid: UID):
        raise InvalidOperationError

    @classmethod
    def get_obj_type(cls):
        return OBJ_TYPE_DIR

    def get_size_bytes(self):
        if self.dir_stats:
            return self.dir_stats.get_size_bytes()
        return None

    def get_etc(self):
        if self.dir_stats:
            return self.dir_stats.get_etc()
        return None

    @classmethod
    def is_dir(cls):
        return True

    def is_parent_of(self, potential_child_node: Node) -> bool:
        raise InvalidOperationError('is_parent_of')

    def get_default_icon(self) -> IconId:
        return IconId.ICON_GENERIC_DIR

    @property
    def name(self):
        assert self.node_identifier.get_single_path(), f'For {type(self)}, uid={self.uid}'
        return os.path.basename(self.node_identifier.get_single_path())

    @staticmethod
    def is_container_node() -> bool:
        return True

    @property
    def sync_ts(self):
        return None

    def __eq__(self, other):
        if not isinstance(other, ContainerNode):
            return False

        return other.node_identifier == other.node_identifier

    def __ne__(self, other):
        return not self.__eq__(other)

    def __repr__(self):
        return f'ContainerNode({self.node_identifier})'


class CategoryNode(ContainerNode):
    """
    ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
    CLASS CategoryNode

    Represents a category in the tree (however it can possibly be treated as the root dir)
    ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼
    """

    def __init__(self, node_identifier: SinglePathNodeIdentifier, op_type: UserOpType):
        super().__init__(node_identifier=node_identifier)
        self.op_type = UserOpType(op_type)

    def __repr__(self):
        return f'CategoryNode(type={self.op_type.name}, node_id="{self.node_identifier}")'

    def __eq__(self, other):
        if not isinstance(other, ContainerNode):
            return False

        return other.node_identifier == other.node_identifier and other.name == self.name

    def get_tag(self) -> str:
        return self.name

    @property
    def name(self):
        return OpTypeMeta.display_label(self.op_type)

    def get_default_icon(self) -> IconId:
        return OpTypeMeta.icon_cat_node(op_type=self.op_type)


class RootTypeNode(ContainerNode):
    """
    ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
    CLASS RootTypeNode

    Represents device root (GDrive, local FS, etc.) in a larger tree
    ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼
    """
    def __init__(self, node_identifier: SinglePathNodeIdentifier):
        super().__init__(node_identifier=node_identifier)

    @property
    def name(self):
        if self.node_identifier.tree_type == TreeType.LOCAL_DISK:
            return 'Local Disk'
        elif self.node_identifier.tree_type == TreeType.GDRIVE:
            return 'Google Drive'
        elif self.node_identifier.tree_type == TreeType.MIXED:
            return 'Super Root'
        return 'Unknown'

    def __repr__(self):
        return f'RootTypeNode({self.node_identifier})'

    def get_tag(self) -> str:
        return self.name

    def get_default_icon(self) -> IconId:
        if self.node_identifier.tree_type == TreeType.LOCAL_DISK:
            return IconId.ICON_LOCAL_DISK_LINUX
        elif self.node_identifier.tree_type == TreeType.GDRIVE:
            return IconId.ICON_GDRIVE
        return IconId.ICON_GENERIC_DIR
