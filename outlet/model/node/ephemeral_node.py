from abc import ABC, abstractmethod

from constants import NULL_UID, TREE_TYPE_NA
from model.node.display_node import DisplayNode
from model.node_identifier import LogicalNodeIdentifier

# ⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛

# CLASS EphemeralNode
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼


class EphemeralNode(DisplayNode, ABC):
    """Does not have an identifier - should not be inserted into a treelib.Tree!"""

    def __init__(self):
        super().__init__(LogicalNodeIdentifier(full_path=None, uid=NULL_UID, tree_type=TREE_TYPE_NA))

    def __repr__(self):
        return self.name

    @property
    @abstractmethod
    def name(self):
        return 'EphemeralNode'

    def get_icon(self):
        return None

    @property
    def full_path(self):
        raise RuntimeError

    @property
    def uid(self):
        raise RuntimeError(f'Cannot call uid() for {self}')

    @classmethod
    def is_ephemereal(cls) -> bool:
        return True

    @classmethod
    def is_file(cls):
        return False

    @classmethod
    def is_dir(cls):
        return False

    def get_etc(self):
        return None

    def get_size_bytes(self):
        return None


# CLASS LoadingNode
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼

class LoadingNode(EphemeralNode):
    """
    For use in lazy loading: Temporary node to put as the only child of a directory node,
    which will be deleted and replaced with real data if the node is expanded
    """

    def __init__(self):
        super().__init__()

    @property
    def name(self):
        return 'LoadingNode'


# CLASS EmptyNode
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼

class EmptyNode(EphemeralNode):
    """
    Represents the contents of a directory which is known to be empty
    """

    def __init__(self):
        super().__init__()

    @property
    def name(self):
        return 'EmptyNode'
