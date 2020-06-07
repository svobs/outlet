import copy
import logging
import os
from abc import ABC, abstractmethod
from typing import List, Optional

from treelib import Node

import format_util
from constants import ICON_GDRIVE, ICON_GENERIC_DIR, ICON_GENERIC_FILE, ICON_LOCAL_DISK, TREE_TYPE_GDRIVE, TREE_TYPE_LOCAL_DISK, TREE_TYPE_NA
from index.uid_generator import NULL_UID, UID
from model.category import Category
from model.node_identifier import LogicalNodeIdentifier, NodeIdentifier

logger = logging.getLogger(__name__)


# ⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛


class DisplayNode(Node, ABC):
    """Base class for nodes which are meant to be displayed in a UI tree"""

    def __init__(self, node_identifier: NodeIdentifier):
        # Look at this next line, It is very important.
        Node.__init__(self, identifier=node_identifier.uid)
        self.node_identifier = node_identifier

        self.set_tag()

    def set_tag(self):
        self.tag = f'{self.node_identifier}: "{self.identifier}"'

    @classmethod
    def is_file(cls):
        return False

    @classmethod
    @abstractmethod
    def is_dir(cls):
        return False

    @classmethod
    def is_ephemereal(cls) -> bool:
        return False

    @classmethod
    def is_just_fluff(cls) -> bool:
        return True

    @property
    def name(self):
        assert type(self.node_identifier.full_path) == str, f'Not a string: {self.node_identifier.full_path} (this={self})'
        return os.path.basename(self.node_identifier.full_path)

    @property
    def etc(self):
        return None

    @property
    def md5(self):
        return None

    @property
    def size_bytes(self):
        return None

    @property
    def modify_ts(self):
        return None

    @property
    def change_ts(self):
        return None

    @property
    def full_path(self):
        return self.node_identifier.full_path

    @property
    def parent_uids(self) -> List[UID]:
        return []

    @property
    def category(self):
        return self.node_identifier.category

    @property
    def uid(self) -> UID:
        return self.node_identifier.uid

    @uid.setter
    def uid(self, uid: UID):
        self.node_identifier.uid = uid
        self.identifier = uid
        self.set_tag()

    def get_relative_path(self, parent_tree):
        return parent_tree.get_relative_path_for_item(self)

    @classmethod
    @abstractmethod
    def has_path(cls):
        """If true, this node represents a physical path. If false, it is just a logical node"""
        return False

    def get_icon(self):
        return ICON_GENERIC_FILE


# ABSTRACT CLASS DisplayNodeWithParents
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼

class DisplayNodeWithParents(DisplayNode, ABC):
    def __init__(self, node_identifier: NodeIdentifier, parent_uids: Optional[List[UID]] = None):
        super().__init__(node_identifier)
        self._parent_uids: Optional[List[UID]] = parent_uids

    @property
    def parent_uids(self) -> List[UID]:
        if self._parent_uids:
            if isinstance(self._parent_uids, list):
                return self._parent_uids
            elif isinstance(self._parent_uids, UID):
                return [self._parent_uids]
            assert False
        return []

    @parent_uids.setter
    def parent_uids(self, parent_uids):
        """Can be a list of GoogFolders' UIDs, or a single UID, or None"""
        if not parent_uids:
            self._parent_uids = None
        elif isinstance(parent_uids, list):
            if len(parent_uids) == 1:
                assert isinstance(parent_uids[0], UID), f'Found instead: {parent_uids[0]}, type={type(parent_uids[0])}'
                self._parent_uids = parent_uids[0]
            else:
                self._parent_uids = parent_uids
        else:
            self._parent_uids = parent_uids


"""
⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛
The following are model objects for use in the hidden 'data' column in the TreeStore, for when a domain object doesn't quite make sense.
⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆"""


class DirNode(DisplayNode):
    """
    Represents a generic directory (i.e. not an FMeta or domain object)
    """

    def __init__(self, node_identifier: NodeIdentifier):
        super().__init__(node_identifier)
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
        if type(self.node_identifier.full_path) == list:
            return os.path.basename(self.node_identifier.full_path[0])
        assert self.node_identifier.full_path, f'For {type(self)}, uid={self.uid}'
        return os.path.basename(self.node_identifier.full_path)

    @property
    def etc(self):
        return f'{self.file_count} items'

    @property
    def size_bytes(self):
        return self._size_bytes

    @classmethod
    def is_ephemereal(cls) -> bool:
        return False

    @classmethod
    def is_just_fluff(cls) -> bool:
        return True

    @classmethod
    def is_file(cls):
        return False

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
        return f'DirNode({self.node_identifier} cat={self.category} {self.get_summary()})'

    def clone(self):
        return DirNode(self.node_identifier)


# ⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛


class CategoryNode(DirNode):
    """
    Represents a category in the tree (however it can possibly be treated as the root dir)
    """
    display_names = {Category.Nada: 'NA',
                      Category.Ignored: 'Ignored',
                      Category.Added: 'To Add',
                      Category.Deleted: 'To Delete',
                      Category.Updated: 'To Update',
                      Category.Moved: 'To Move',
                      }

    def __init__(self, node_identifier: NodeIdentifier):
        super().__init__(node_identifier=node_identifier)

    def __repr__(self):
        return f'CategoryNode(cat={self.category.name}, identifier={self.node_identifier})'

    @property
    def name(self):
        return CategoryNode.display_names[self.category.value]

    def get_icon(self):
        return ICON_GENERIC_DIR


class RootTypeNode(DirNode):
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


# ⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛


class EphemeralNode(DisplayNode, ABC):
    """Does not have an identifier - should not be inserted into a treelib.Tree!"""
    def __init__(self):
        super().__init__(LogicalNodeIdentifier(full_path=None, uid=NULL_UID, tree_type=TREE_TYPE_NA, category=Category.Nada))

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
    def is_ephemereal(cls) -> bool:
        return True

    @classmethod
    def is_just_fluff(cls) -> bool:
        return True

    @classmethod
    def is_file(cls):
        return False

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
