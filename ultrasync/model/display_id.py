
from abc import ABC, abstractmethod

from constants import OBJ_TYPE_DISPLAY_ONLY, OBJ_TYPE_LOCAL_DISK


# ⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛


class DisplayId(ABC):
    """
    Represents a unique identifier that can be used across trees and tree types to identify a node.
    Still a work in progress and may change greatly.
    """
    def __init__(self, id_string: str):
        self.id_string = id_string

    @property
    @abstractmethod
    def tree_type(self) -> int:
        return OBJ_TYPE_LOCAL_DISK

    def __repr__(self):
        return f'{self.tree_type}:{self.id_string}'


class LogicalNodeDisplayId(DisplayId):
    def __init__(self, id_string):
        """Object has a path, but does not represent a physical item"""
        super().__init__(id_string=id_string)

    @property
    def tree_type(self) -> int:
        return OBJ_TYPE_DISPLAY_ONLY

