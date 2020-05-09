import os
from abc import ABC, abstractmethod
from typing import List, Optional

from model.display_id import Identifier
from model.display_node import DisplayNode


# ABSTRACT CLASS PlanningNode
# ⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟

class PlanningNode(DisplayNode, ABC):
    """
    Planning nodes represent work which has not yet been done, such as copying a file.
    They can be thought of as 'ghosts of a possible future'. As such, they should not be
    cached like other objects.

    Note also that this node should think of itself as 'living' in its destination tree - thus
    properties like 'full_path' return the destination path, not the source.
    """
    def __init__(self, identifier: Optional[Identifier]):
        super().__init__(identifier)


# ABSTRACT CLASS FileDecoratorNode
# ⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟

class FileDecoratorNode(PlanningNode, ABC):
    def __init__(self, identifier, orig_path, original_node):
        super().__init__(identifier)
        self.original_node = original_node
        # GoogNodes do not have a path built-in, so we must store them preemptively
        self._orig_full_path = orig_path
        self._parent_ids = None
        """Only used in Goog trees currently"""

    @property
    def original_full_path(self):
        return self._orig_full_path

    @property
    def dest_path(self):
        return self.identifier.full_path

    @property
    def full_path(self):
        return self.dest_path

    @property
    def name(self):
        return os.path.basename(self.dest_path)

    @property
    def md5(self):
        return self.original_node.md5

    @property
    def sha256(self):
        return self.original_node.sha256

    @property
    def size_bytes(self):
        return self.original_node.size_bytes

    @property
    def sync_ts(self):
        return self.original_node.sync_ts

    @property
    def modify_ts(self):
        return self.original_node.modify_ts

    @property
    def change_ts(self):
        return self.original_node.change_ts

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
    def parent_ids(self) -> List[str]:
        if not self._parent_ids:
            return []
        if isinstance(self._parent_ids, list):
            return self._parent_ids
        return [self._parent_ids]

    @parent_ids.setter
    def parent_ids(self, parent_ids):
        """Can be a list of GoogFolders, or a single instance, or None"""
        if not parent_ids:
            self._parent_ids = None
        elif isinstance(parent_ids, list):
            if len(parent_ids) == 1:
                assert isinstance(parent_ids[0], str)
                self._parent_ids = parent_ids[0]
            else:
                self._parent_ids = parent_ids
        else:
            self._parent_ids = parent_ids


# CLASS FileToAdd
# ⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟

class FileToAdd(FileDecoratorNode):
    # NOTE: this decorates its enclosed FMeta, EXCEPT for pathname stuff!
    def __init__(self, identifier, orig_path, original_node):
        super().__init__(identifier, orig_path, original_node)

    def __repr__(self):
        return f'FileToAdd(original_path={self.original_full_path} dest_path={self.dest_path} md5={self.original_node.md5})'


# CLASS FileToMove
# ⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟

class FileToMove(FileDecoratorNode):
    # NOTE: this decorates its enclosed FMeta, EXCEPT for pathname stuff!
    def __init__(self, identifier, orig_path, original_node):
        super().__init__(identifier, orig_path, original_node)

    def __repr__(self):
        return f'FileToMove(original_path={self.original_full_path} dest_path={self.dest_path} md5={self.original_node.md5})'
