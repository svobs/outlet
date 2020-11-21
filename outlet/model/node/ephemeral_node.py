from abc import ABC, abstractmethod

from error import InvalidOperationError
from model.node.node import Node
from model.node_identifier import NullNodeIdentifier

# ⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛

# CLASS EphemeralNode
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼


class EphemeralNode(Node, ABC):
    def __init__(self):
        super().__init__(NullNodeIdentifier())

    def __repr__(self):
        return self.name

    def is_parent_of(self, potential_child_node):
        return False

    @property
    @abstractmethod
    def name(self):
        return 'EphemeralNode'

    def get_icon(self):
        return None

    def get_single_path(self):
        raise InvalidOperationError('get_single_path()')

    def get_path_list(self):
        raise InvalidOperationError('get_path_list()')

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

    @classmethod
    def get_obj_type(cls):
        raise RuntimeError('Invalid operation for EphemeralNode!')

    @property
    def sync_ts(self):
        return None

    def update_from(self, other_node):
        Node.update_from(self, other_node)


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
