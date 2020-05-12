from abc import ABC, abstractmethod
from typing import List, Union

from constants import GDRIVE_PATH_PREFIX, OBJ_TYPE_DISPLAY_ONLY, OBJ_TYPE_GDRIVE, OBJ_TYPE_LOCAL_DISK, OBJ_TYPE_MIXED, ROOT

# ⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛
from model.category import Category


def ensure_category(val):
    if type(val) == str:
        return Category(int(val))
    elif type(val) == int:
        return Category(val)
    return val


"""
◤━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━◥
    ABSTRACT CLASS Identifier
◣━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━◢
"""


class Identifier(ABC):
    """
    Represents a unique identifier that can be used across trees and tree types to identify a node.
    Still a work in progress and may change greatly.
    """

    def __init__(self, uid: str, full_path: str, category: Category):
        assert full_path is None or full_path.find('/') >= 0, f'full_path does not look like a path: {full_path}'
        self.uid: str = uid
        self.full_path: str = full_path
        self.category: Category = ensure_category(category)

    @property
    @abstractmethod
    def tree_type(self) -> int:
        return OBJ_TYPE_DISPLAY_ONLY

    def __repr__(self):
        # should never be displayed
        return f'∣✪✪∣{self.category.value}⚡{self.full_path}∤{self.uid}∣'

    def __eq__(self, other):
        if isinstance(other, str):
            return other == ROOT and self.uid == ROOT
        return self.full_path == other.full_path and self.uid == other.uid and self.tree_type == other.tree_type

    def __ne__(self, other):
        return not self.__eq__(other)


"""
◤━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━◥
    CLASS LogicalNodeIdentifier
◣━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━◢
"""


class LogicalNodeIdentifier(Identifier):
    def __init__(self, uid: str, full_path: str, category: Category, tree_type=OBJ_TYPE_DISPLAY_ONLY):
        """Object has a path, but does not represent a physical item"""
        super().__init__(uid, full_path, category)
        self._tree_type = tree_type

    @property
    def tree_type(self) -> int:
        return self._tree_type

    def __repr__(self):
        return f'∣--∣{self.category.value}⚡{self.full_path}∤{self.uid}∣'


"""
◤━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━◥
    CLASS GDriveIdentifier
◣━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━◢
"""


class GDriveIdentifier(Identifier):
    def __init__(self, uid: str, full_path: str, category: Category = Category.NA):
        super().__init__(uid, full_path, category)

    @property
    def tree_type(self) -> int:
        return OBJ_TYPE_GDRIVE

    def __repr__(self):
        if self.uid == self.full_path:
            uid_disp = '≡'
        else:
            uid_disp = self.uid
        return f'∣GD∣{self.category.value}⚡{self.full_path}∤{uid_disp}∣'


"""
◤━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━◥
    CLASS LocalFsIdentifier
◣━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━◢
"""


class LocalFsIdentifier(Identifier):
    def __init__(self, full_path, uid=None, category: Category = Category.NA):
        if not uid:
            uid = full_path
        super().__init__(uid, full_path, category)

    @property
    def tree_type(self) -> int:
        return OBJ_TYPE_LOCAL_DISK

    def __repr__(self):
        if self.uid == self.full_path:
            uid_disp = '≡'
        else:
            uid_disp = self.uid
        return f'∣FS∣{self.category.value}⚡{self.full_path}∤{uid_disp}∣'


def for_values(tree_type: int = None,
               full_path: str = None,
               uid: str = None,
               category: Category = Category.NA):
    if not tree_type:
        if full_path:
            if full_path.startswith(GDRIVE_PATH_PREFIX):
                gdrive_path = full_path[len(GDRIVE_PATH_PREFIX):]
                if gdrive_path != '/' and gdrive_path.endswith('/'):
                    gdrive_path = gdrive_path[:-1]
                if not gdrive_path:
                    # can happen if the use enters "gdrive:/"
                    return GDriveIdentifier(uid=uid, full_path='/', category=category)
                return GDriveIdentifier(uid=uid, full_path=gdrive_path, category=category)
            else:
                return LocalFsIdentifier(uid=uid, full_path=full_path, category=category)
        else:
            raise RuntimeError('no tree_type and no full_path supplied')
    elif tree_type == OBJ_TYPE_LOCAL_DISK:
        return LocalFsIdentifier(uid=uid, full_path=full_path, category=category)
    elif tree_type == OBJ_TYPE_GDRIVE:
        return GDriveIdentifier(uid=uid, full_path=full_path, category=category)
    elif tree_type == OBJ_TYPE_MIXED or tree_type == OBJ_TYPE_DISPLAY_ONLY:
        return LogicalNodeIdentifier(full_path=full_path, category=category)
    else:
        raise RuntimeError('bad')
