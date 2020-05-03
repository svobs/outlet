import os
from abc import ABC, abstractmethod
from typing import Optional

from model.category import Category
from model.display_id import DisplayId
from model.display_node import DisplayNode


class PlanningNode(DisplayNode, ABC):
    """
    Planning nodes represent work which has not yet been done, such as copying a file.
    They can be thought of as 'ghosts of a possible future'. As such, they should not be
    cached like other objects.

    Note also that this node should think of itself as 'living' in its destination tree - thus
    properties like 'full_path' return the destination path, not the source.
    """

    def __init__(self, category=Category.NA):
        super().__init__(category)

    @abstractmethod
    def get_icon(self):
        return None


class FMetaDecorator(PlanningNode, ABC):
    def __init__(self, original_node, dest_path, category=Category.NA):
        super().__init__(category)
        self.original = original_node
        self.dest_path = dest_path
        self._parent = None
        """Only used in Goog trees currently"""

    @property
    def original_full_path(self):
        return self.original.full_path

    @property
    def full_path(self):
        return self.dest_path

    def get_name(self):
        return os.path.basename(self.dest_path)

    @property
    def name(self):
        # Needed for GOOG
        return self.get_name()

    @property
    def md5(self):
        return self.original.md5

    @property
    def sha256(self):
        return self.original.sha256

    @property
    def size_bytes(self):
        return self.original.size_bytes

    @property
    def sync_ts(self):
        return self.original.sync_ts

    @property
    def modify_ts(self):
        return self.original.modify_ts

    @property
    def change_ts(self):
        return self.original.change_ts

    @property
    def id(self):
        return str(self.display_id)

    @property
    def display_id(self) -> Optional[DisplayId]:
        # Piggyback on the underlying class, whether FMeta, GoogNode...
        disp_id = self.original.display_id
        disp_id.id_string = self.dest_path
        disp_id.category = self.category
        return disp_id

    def get_relative_path(self, parent_tree):
        return parent_tree.get_relative_path_of(self.full_path)

    @classmethod
    def has_path(cls):
        return True

    @classmethod
    def is_dir(cls):
        return False

    @classmethod
    def is_ignored(cls):
        return False

    def get_icon(self):
        return self.category.name

    @property
    def parents(self):
        if not self._parent:
            return []
        if isinstance(self._parent, list):
            return self._parent
        return [self._parent]

    @parents.setter
    def parents(self, parents):
        """Can be a list of GoogFolders, or a single instance, or None"""
        if not parents:
            self._parent = None
        elif isinstance(parents, list):
            if len(parents) == 0:
                self._parent = None
            elif len(parents) == 1:
                self._parent = parents[0]
            else:
                self._parent = parents
        else:
            self._parent = parents


class FileToAdd(FMetaDecorator):
    # NOTE: this decorates its enclosed FMeta, EXCEPT for pathname stuff!
    def __init__(self, original, dest_path):
        super().__init__(original, dest_path, Category.ADDED)

    def __repr__(self):
        return f'FileToAdd(original_path={self.original_full_path} dest_path={self.dest_path})'


class FileToMove(FMetaDecorator):
    # NOTE: this decorates its enclosed FMeta, EXCEPT for pathname stuff!
    def __init__(self, original, dest_path):
        super().__init__(original, dest_path, Category.MOVED)

    def __repr__(self):
        return f'FileToMove(original_path={self.original_full_path} dest_path={self.dest_path})'
