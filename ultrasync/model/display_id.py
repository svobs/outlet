
from abc import ABC, abstractmethod

from constants import OBJ_TYPE_DISPLAY_ONLY, OBJ_TYPE_GDRIVE, OBJ_TYPE_LOCAL_DISK

# ⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛
from model.category import Category


def ensure_category(val):
    if type(val) == str:
        return Category(int(val))
    elif type(val) == int:
        return Category(val)
    return val


class Identifier(ABC):
    """
    Represents a unique identifier that can be used across trees and tree types to identify a node.
    Still a work in progress and may change greatly.
    """
    def __init__(self, full_path: str, category: Category):
        if full_path is not None and full_path.find('/') < 0:
            print('Something bad, better stop here')
        assert full_path is None or full_path.find('/') >= 0, f'full_path does not look like a path: {full_path}'
        self.full_path: str = full_path
        self.category: Category = ensure_category(category)

    @property
    @abstractmethod
    def tree_type(self) -> int:
        return OBJ_TYPE_DISPLAY_ONLY

    @property
    def uid(self) -> str:
        return self.full_path

    def __repr__(self):
        # should never be displayed
        return f'ID:XX:{self.category.name}:{self.full_path}'

    def __eq__(self, other):
        return self.full_path == other.full_path and self.uid == other.uid and self.tree_type == other.tree_type

    def __ne__(self, other):
        return not self.__eq__(other)


class LogicalNodeIdentifier(Identifier):
    def __init__(self, full_path: str, category: Category):
        """Object has a path, but does not represent a physical item"""
        super().__init__(full_path, category)

    @property
    def tree_type(self) -> int:
        return OBJ_TYPE_DISPLAY_ONLY

    def __repr__(self):
        return f'ID:--:{self.full_path}'


"""
◤━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━◥
    CLASS GDriveIdentifier
◣━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━◢
"""


class GDriveIdentifier(Identifier):
    def __init__(self, uid: str, full_path: str, category: Category = Category.NA):
        super().__init__(full_path, category)
        self._uid: str = uid

    @property
    def tree_type(self) -> int:
        return OBJ_TYPE_GDRIVE

    @property
    def uid(self) -> str:
        return self._uid

    def __repr__(self):
        return f'ID:GD:{self.full_path}:{self._uid}'


"""
◤━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━◥
    CLASS LocalFsIdentifier
◣━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━◢
"""


class LocalFsIdentifier(Identifier):
    def __init__(self, full_path, category: Category = Category.NA):
        super().__init__(full_path, category)

    @property
    def tree_type(self) -> int:
        return OBJ_TYPE_LOCAL_DISK

    def __repr__(self):
        return f'ID:FS:{self.full_path}'


def for_values(tree_type: int, full_path: str, uid: str = None, category=Category.NA):
    if tree_type == OBJ_TYPE_LOCAL_DISK:
        return LocalFsIdentifier(full_path=full_path, category=category)
    elif tree_type == OBJ_TYPE_GDRIVE:
        return GDriveIdentifier(uid=uid, full_path=full_path, category=category)
    else:
        return LogicalNodeIdentifier(full_path=full_path, category=category)
