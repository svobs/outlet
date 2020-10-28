import os

from error import InvalidOperationError
from model.op import OpType
from constants import ICON_GDRIVE, ICON_GENERIC_DIR, ICON_LOCAL_DISK_LINUX, OBJ_TYPE_DIR, TREE_TYPE_GDRIVE, TREE_TYPE_LOCAL_DISK
from model.node.node import Node, HasChildList
from model.node_identifier import SinglePathNodeIdentifier


# CLASS ContainerNode
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼

class ContainerNode(HasChildList, Node):
    """
    Represents a generic directory (i.e. not a LocalFileNode or domain object)
    """

    def __init__(self, node_identifier: SinglePathNodeIdentifier):
        assert node_identifier.get_single_path(), f'Bad: {node_identifier}'
        Node.__init__(self, node_identifier)
        HasChildList.__init__(self)

    def update_from(self, other_node):
        HasChildList.update_from(self, other_node)
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

        return other.node_identifier == other.node_identifier and other.name == self.name and other.trashed == self.trashed

    def __ne__(self, other):
        return not self.__eq__(other)

    def __repr__(self):
        return f'ContainerNode({self.node_identifier} {self.get_summary()})'


# CLASS CategoryNode
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼

class CategoryNode(ContainerNode):
    """
    Represents a category in the tree (however it can possibly be treated as the root dir)
    """
    display_names = {
        OpType.CP: 'To Add',
        OpType.RM: 'To Delete',
        OpType.UP: 'To Update',
        OpType.MV: 'To Move',
    }

    def __init__(self, node_identifier: SinglePathNodeIdentifier, op_type: OpType):
        super().__init__(node_identifier=node_identifier)
        self.op_type = op_type

    def __repr__(self):
        return f'CategoryNode(type={self.op_type.name}, identifier={self.node_identifier})'

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

    def __init__(self, node_identifier: SinglePathNodeIdentifier):
        super().__init__(node_identifier=node_identifier)

    @property
    def name(self):
        if self.node_identifier.tree_type == TREE_TYPE_LOCAL_DISK:
            return 'Local Disk'
        elif self.node_identifier.tree_type == TREE_TYPE_GDRIVE:
            return 'Google Drive'
        return 'Unknown'

    def __repr__(self):
        return f'RootTypeNode({self.name})'

    def get_icon(self):
        if self.node_identifier.tree_type == TREE_TYPE_LOCAL_DISK:
            return ICON_LOCAL_DISK_LINUX
        elif self.node_identifier.tree_type == TREE_TYPE_GDRIVE:
            return ICON_GDRIVE
        return ICON_GENERIC_DIR

