import logging
import os
from abc import ABC, abstractmethod
from typing import Optional

import format_util
from constants import ICON_GDRIVE, ICON_GENERIC_DIR, ICON_GENERIC_FILE, ICON_LOCAL_DISK, OBJ_TYPE_GDRIVE, OBJ_TYPE_LOCAL_DISK
from model.category import Category
from model.display_id import Identifier

logger = logging.getLogger(__name__)


def ensure_int(val):
    if type(val) == str:
        return int(val)
    return val


# ⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛


class DisplayNode(ABC):
    """Base class for nodes which are meant to be displayed in a UI tree"""

    def __init__(self, identifier: Optional[Identifier]):
        self.identifier = identifier

    @classmethod
    @abstractmethod
    def is_dir(cls):
        return False

    @property
    def name(self):
        assert type(self.identifier.full_path) == str, f'Not a string: {self.identifier.full_path} (this={self})'
        return os.path.basename(self.identifier.full_path)

    @property
    def size_bytes(self):
        return None

    @property
    def full_path(self):
        return self.identifier.full_path

    @property
    def category(self):
        return self.identifier.category

    @property
    def uid(self) -> int:
        return self.identifier.uid

    def get_relative_path(self, parent_tree):
        return parent_tree.get_relative_path_for_item(self)

    @classmethod
    @abstractmethod
    def has_path(cls):
        """If true, this node represents a physical path. If false, it is just a logical node"""
        return False

    def get_icon(self):
        return ICON_GENERIC_FILE


"""
⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛
The following are model objects for use in the hidden 'data' column in the TreeStore, for when a domain object doesn't quite make sense.
⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆"""


class DirNode(DisplayNode):
    """
    Represents a generic directory (i.e. not an FMeta or domain object)
    """

    def __init__(self, identifier):
        super().__init__(identifier)
        self.file_count = 0
        self._size_bytes = 0

    def add_meta_metrics(self, fmeta):
        self.file_count += 1
        if fmeta.size_bytes:
            self._size_bytes += fmeta.size_bytes

    def get_icon(self):
        return ICON_GENERIC_DIR

    @property
    def name(self):
        if type(self.identifier.full_path) == list:
            return os.path.basename(self.identifier.full_path[0])
        return os.path.basename(self.identifier.full_path)

    @property
    def size_bytes(self):
        return self._size_bytes

    @classmethod
    def has_path(cls):
        return True

    @classmethod
    def is_dir(cls):
        return True

    def get_summary(self):
        if not self._size_bytes and not self.file_count:
            return 'None'
        size = format_util.humanfriendlier_size(self._size_bytes)
        return f'{size} in {self.file_count:n} files'

    def __repr__(self):
        return f'DirNode({self.identifier} cat={self.category} {self.get_summary()})'


# ⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛


class CategoryNode(DirNode):
    """
    Represents a category in the tree (however it can possibly be treated as the root dir)
    """
    _display_names = {Category.Nada: 'NA',
                      Category.Ignored: 'Ignored',
                      Category.Added: 'To Add',
                      Category.Deleted: 'To Delete',
                      Category.Updated: 'To Update',
                      Category.Moved: 'To Move',
                      }

    def __init__(self, identifier):
        super().__init__(identifier=identifier)

    def __repr__(self):
        return f'CategoryNode({self.category.name})'

    @property
    def name(self):
        return CategoryNode._display_names[self.category.value]

    def get_icon(self):
        return ICON_GENERIC_DIR


class RootTypeNode(DirNode):
    """
    Represents a type of root in the tree (GDrive, local FS, etc.)
    """

    def __init__(self, identifier):
        super().__init__(identifier=identifier)

    def __repr__(self):
        return f'RootTypeNode({self.name})'

    def get_icon(self):
        if self.identifier.tree_type == OBJ_TYPE_LOCAL_DISK:
            return ICON_LOCAL_DISK
        elif self.identifier.tree_type == OBJ_TYPE_GDRIVE:
            return ICON_GDRIVE
        return ICON_GENERIC_DIR


# ⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛


class EphemeralNode(DisplayNode, ABC):
    def __init__(self):
        super().__init__(None)

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
        raise RuntimeError

    @property
    def category(self):
        raise RuntimeError

    @classmethod
    def is_dir(cls):
        return False

    @classmethod
    def has_path(cls):
        return False


# ⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛


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


# ⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛


class EmptyNode(EphemeralNode):
    """
    Represents the contents of a directory which is known to be empty
    """

    def __init__(self):
        super().__init__()

    @property
    def name(self):
        return 'EmptyNode'
