from abc import ABC, abstractmethod
import os

import humanfriendly
import logging

from constants import CACHE_TYPE_LOCAL_DISK
from model.category import Category

logger = logging.getLogger(__name__)


def ensure_int(val):
    if type(val) == str:
        return int(val)
    return val


def ensure_category(val):
    if type(val) == str:
        return Category(int(val))
    elif type(val) == int:
        return Category(val)
    return val

# ⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛


class DisplayId(ABC):
    def __init__(self, category=None, full_path=None, id_string=None):
        self.category = category
        self.full_path = full_path
        self.id_string = id_string

    @property
    def tree_type(self) -> int:
        return CACHE_TYPE_LOCAL_DISK

# ⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛


class DisplayNode(ABC):
    """Base class for nodes which are meant to be displayed in a UI tree"""
    def __init__(self, category):
        self.category = ensure_category(category)

    @classmethod
    @abstractmethod
    def is_dir(cls):
        return False

    @abstractmethod
    def get_name(self):
        return None

    @property
    def size_bytes(self):
        return None

    @property
    @abstractmethod
    def display_id(self) -> DisplayId:
        return None

    @classmethod
    @abstractmethod
    def has_path(cls):
        """If true, this node represents a physical path. If false, it is just a logical node"""
        return False


"""
⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛
The following are model objects for use in the hidden 'data' column in the TreeStore, for when a domain object doesn't quite make sense.
⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆"""


class DirNode(DisplayNode):
    """
    Represents a generic directory (i.e. not an FMeta or domain object)
    """
    def __init__(self, full_path, category):
        super().__init__(category)
        self.full_path = full_path
        self.file_count = 0
        self._size_bytes = 0

    def add_meta(self, fmeta):
        if fmeta.category != self.category:
            logger.error(f'BAD CATEGORY: expected={self.category} found={fmeta.category} path={fmeta.full_path}')
        assert fmeta.category == self.category
        self.file_count += 1
        self._size_bytes += fmeta.size_bytes

    @property
    def display_id(self):
        return DisplayId(self.category, self.full_path)

    @property
    def size_bytes(self):
        return self._size_bytes

    @classmethod
    def has_path(cls):
        return True

    @classmethod
    def is_dir(cls):
        return True

    def get_name(self):
        return os.path.split(self.full_path)[1]

    def get_summary(self):
        size = humanfriendly.format_size(self._size_bytes)
        return f'{size} in {self.file_count} files'

    def __repr__(self):
        return f'DirNode(full_path="{self.full_path}" {self.get_summary()})'

# ⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛


class CategoryNode(DirNode):
    """
    Represents a category in the tree (however it can possibly be treated as the root dir)
    """
    def __init__(self, full_path, category):
        super().__init__(full_path, category)

    def __repr__(self):
        return f'CategoryNode({self.category.name})'

    def get_name(self):
        return self.category.name

    @property
    def display_id(self):
        return DisplayId(self.category, self.full_path)

# ⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛


class LoadingNode(DisplayNode):
    """
    For use in lazy loading: Temporary node to put as the only child of a directory node,
    which will be deleted and replaced with real data if the node is expanded
    """

    def __init__(self):
        super().__init__(Category.NA)

    @classmethod
    def __repr__(cls):
        return 'LoadingNode'

    def get_name(self):
        return 'LoadingNode'

    @classmethod
    def is_dir(cls):
        return False

    @classmethod
    def has_path(cls):
        return False

    @property
    def display_id(self):
        return None

# ⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛


class EmptyNode(DisplayNode):
    """
    Represents the contents of a directory which is known to be empty
    """
    def __init__(self):
        super().__init__(Category.NA)

    @classmethod
    def __repr__(cls):
        return 'EmptyNode'

    @classmethod
    def get_name(cls):
        return 'EmptyNode'

    @classmethod
    def is_dir(cls):
        return False

    @classmethod
    def has_path(cls):
        return False

    @property
    def display_id(self):
        return None
