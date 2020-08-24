import os

from model.op import OpType
from constants import ICON_ADD_DIR, ICON_GDRIVE, ICON_GENERIC_DIR, ICON_LOCAL_DISK, OBJ_TYPE_DIR, TREE_TYPE_GDRIVE, TREE_TYPE_LOCAL_DISK
from model.node.display_node import DisplayNode, HasChildren
from model.node_identifier import NodeIdentifier


# CLASS ContainerNode
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼

class ContainerNode(HasChildren, DisplayNode):
    """
    Represents a generic directory (i.e. not an LocalFileNode or domain object)
    """

    def __init__(self, node_identifier: NodeIdentifier):
        DisplayNode.__init__(self, node_identifier)
        HasChildren.__init__(self)

    @classmethod
    def get_obj_type(cls):
        return OBJ_TYPE_DIR

    @classmethod
    def is_file(cls):
        return False

    @classmethod
    def is_dir(cls):
        return True

    def get_tree_type(self) -> int:
        return self.node_identifier.tree_type

    def is_parent(self, potential_child_node: DisplayNode) -> bool:
        # This is not currently possible to determine without access to the containing tree
        # TODO: extend from HasParentList to add this support
        # TODO: custom exception class, 'InvalidOperationError'
        raise RuntimeError('Not allowed!')

    def get_icon(self):
        # FIXME: allow custom icon for Category Tree nodes ("To Add", "To Delete", etc)
        return ICON_GENERIC_DIR

    @property
    def name(self):
        assert self.node_identifier.full_path, f'For {type(self)}, uid={self.uid}'
        return os.path.basename(self.node_identifier.full_path)

    def __eq__(self, other):
        if not isinstance(other, ContainerNode):
            return False

        return other.uid == self.uid and self.node_identifier.tree_type == other.node_identifier.tree_type and self.full_path == other.full_path \
            and other.name == self.name and other.trashed == self.trashed and self.get_size_bytes() == other.get_size_bytes()

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

    def __init__(self, node_identifier: NodeIdentifier, change_type: OpType):
        super().__init__(node_identifier=node_identifier)
        self.op_type = change_type

    def __repr__(self):
        return f'CategoryNode(type={self.op_type.name}, identifier={self.node_identifier})'

    @property
    def name(self):
        return CategoryNode.display_names[self.op_type]

    def get_icon(self):
        return ICON_GENERIC_DIR


# CLASS RootTypeNode
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼

class RootTypeNode(ContainerNode):
    """
    Represents a type of root in the tree (GDrive, local FS, etc.)
    """

    def __init__(self, node_identifier: NodeIdentifier):
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
            return ICON_LOCAL_DISK
        elif self.node_identifier.tree_type == TREE_TYPE_GDRIVE:
            return ICON_GDRIVE
        return ICON_GENERIC_DIR

