import os

from model.change_action import ChangeType
from constants import ICON_GDRIVE, ICON_GENERIC_DIR, ICON_LOCAL_DISK, TREE_TYPE_GDRIVE, TREE_TYPE_LOCAL_DISK
from model.display_node import DisplayNode, HasChildren
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
    def is_file(cls):
        return False

    @classmethod
    def is_dir(cls):
        return True

    def get_size_bytes(self):
        return self._size_bytes

    def get_icon(self):
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
        ChangeType.CP: 'To Add',
        ChangeType.RM: 'To Delete',
        ChangeType.UP: 'To Update',
        ChangeType.MV: 'To Move',
    }

    def __init__(self, node_identifier: NodeIdentifier, change_type: ChangeType):
        super().__init__(node_identifier=node_identifier)
        self.change_type = change_type

    def __repr__(self):
        return f'CategoryNode(type={self.change_type.name}, identifier={self.node_identifier})'

    @property
    def name(self):
        return CategoryNode.display_names[self.change_type]

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

