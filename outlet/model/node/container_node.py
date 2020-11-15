import os

from error import InvalidOperationError
from model.user_op import UserOpType
from constants import ICON_GDRIVE, ICON_GENERIC_DIR, ICON_LOCAL_DISK_LINUX, OBJ_TYPE_DIR, TREE_TYPE_GDRIVE, TREE_TYPE_LOCAL_DISK
from model.node.node import Node, HasChildStats
from model.node_identifier import SinglePathNodeIdentifier


# CLASS ContainerNode
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼

class ContainerNode(HasChildStats, Node):
    """
    Represents a generic display-only directory node which is not backed by a cached object.
    """

    def __init__(self, node_identifier: SinglePathNodeIdentifier, nid: str = None):
        assert node_identifier.get_single_path(), f'Bad: {node_identifier}'
        Node.__init__(self, node_identifier, nid=nid)
        HasChildStats.__init__(self)

    def update_from(self, other_node):
        HasChildStats.update_from(self, other_node)
        Node.update_from(self, other_node)

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

    def get_icon(self):
        return ICON_GENERIC_DIR

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


# CLASS CategoryNode
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼

class CategoryNode(ContainerNode):
    """
    Represents a category in the tree (however it can possibly be treated as the root dir)
    """
    display_names = {
        UserOpType.CP: 'To Add',
        UserOpType.RM: 'To Delete',
        UserOpType.UP: 'To Update',
        UserOpType.MV: 'To Move',
    }

    def __init__(self, node_identifier: SinglePathNodeIdentifier, op_type: UserOpType, nid: str = None):
        super().__init__(node_identifier=node_identifier, nid=nid)
        self.op_type = op_type

    def __repr__(self):
        return f'CategoryNode(nid="{self.identifier}" type={self.op_type.name}, node_id="{self.node_identifier}")'

    def __eq__(self, other):
        if not isinstance(other, ContainerNode):
            return False

        return other.node_identifier == other.node_identifier and other.name == self.name

    @property
    def name(self):
        return CategoryNode.display_names[self.op_type]

    def get_icon(self):
        # FIXME: allow custom icon for Category Tree nodes ("To Add", "To Delete", etc)
        return ICON_GENERIC_DIR


# CLASS RootTypeNode
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼

class RootTypeNode(ContainerNode):
    """
    Represents a type of root in the tree (GDrive, local FS, etc.)
    """

    def __init__(self, node_identifier: SinglePathNodeIdentifier, nid: str = None):
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

    def get_icon(self):
        if self.node_identifier.tree_type == TREE_TYPE_LOCAL_DISK:
            return ICON_LOCAL_DISK_LINUX
        elif self.node_identifier.tree_type == TREE_TYPE_GDRIVE:
            return ICON_GDRIVE
        return ICON_GENERIC_DIR

