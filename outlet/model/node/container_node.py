import os
from typing import List

from error import InvalidOperationError
from model.node.trait import HasChildStats
from model.uid import UID
from model.user_op import UserOpType
from constants import IconId, OBJ_TYPE_DIR, TREE_TYPE_GDRIVE, TREE_TYPE_LOCAL_DISK
from model.node.node import Node
from model.node_identifier import SinglePathNodeIdentifier


class ContainerNode(HasChildStats, Node):
    """
    ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
    CLASS ContainerNode

    Represents a generic display-only directory node which is not backed by a cached object.
    ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼
    """
    def __init__(self, node_identifier: SinglePathNodeIdentifier, nid: UID = None):
        assert node_identifier.get_single_path(), f'Bad: {node_identifier}'
        Node.__init__(self, node_identifier, nid=nid)
        HasChildStats.__init__(self)

    def update_from(self, other_node):
        HasChildStats.update_from(self, other_node)
        Node.update_from(self, other_node)

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

    @classmethod
    def is_file(cls):
        return False

    @classmethod
    def is_dir(cls):
        return True

    @classmethod
    def is_display_only(cls):
        return True

    def is_parent_of(self, potential_child_node: Node) -> bool:
        raise InvalidOperationError('is_parent_of')

    def get_default_icon(self) -> IconId:
        return IconId.ICON_GENERIC_DIR

    @property
    def name(self):
        assert self.node_identifier.get_single_path(), f'For {type(self)}, uid={self.uid}'
        return os.path.basename(self.node_identifier.get_single_path())

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
        return f'ContainerNode(nid="{self.identifier}" node_id="{self.node_identifier}" {self.get_summary()})'


class CategoryNode(ContainerNode):
    """
    ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
    CLASS CategoryNode

    Represents a category in the tree (however it can possibly be treated as the root dir)
    ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼
    """
    display_names = {
        UserOpType.CP: 'To Add',
        UserOpType.RM: 'To Delete',
        UserOpType.UP: 'To Update',
        UserOpType.MV: 'To Move',
    }

    def __init__(self, node_identifier: SinglePathNodeIdentifier, op_type: UserOpType, nid: UID = None):
        super().__init__(node_identifier=node_identifier, nid=nid)
        self.op_type = op_type

    def __repr__(self):
        return f'CategoryNode(nid="{self.identifier}" type={self.op_type.name}, node_id="{self.node_identifier}")'

    def __eq__(self, other):
        if not isinstance(other, ContainerNode):
            return False

        return other.node_identifier == other.node_identifier and other.name == self.name

    def get_tag(self) -> str:
        return self.name

    @property
    def name(self):
        return CategoryNode.display_names[self.op_type]

    def get_default_icon(self) -> IconId:
        # FIXME: allow custom icon for Category Tree nodes ("To Add", "To Delete", etc)
        return IconId.ICON_GENERIC_DIR


class RootTypeNode(ContainerNode):
    """
    ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
    CLASS RootTypeNode

    Represents a type of root in the tree (GDrive, local FS, etc.)
    ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼
    """
    def __init__(self, node_identifier: SinglePathNodeIdentifier, nid: UID = None):
        super().__init__(node_identifier=node_identifier, nid=nid)

    @property
    def name(self):
        if self.node_identifier.tree_type == TREE_TYPE_LOCAL_DISK:
            return 'Local Disk'
        elif self.node_identifier.tree_type == TREE_TYPE_GDRIVE:
            return 'Google Drive'
        return 'Unknown'

    def __repr__(self):
        return f'RootTypeNode(nid="{self.identifier}" node_id="{self.node_identifier}")'

    def get_tag(self) -> str:
        return self.name

    def get_default_icon(self) -> IconId:
        if self.node_identifier.tree_type == TREE_TYPE_LOCAL_DISK:
            return IconId.ICON_LOCAL_DISK_LINUX
        elif self.node_identifier.tree_type == TREE_TYPE_GDRIVE:
            return IconId.ICON_GDRIVE
        return IconId.ICON_GENERIC_DIR

