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
    """Decorates a previously existing DisplayNode."""
    def __init__(self, identifier: Identifier, src_node: DisplayNode):
        super().__init__(identifier)

        self.src_node: DisplayNode = src_node
        """The original node (e.g., for a FileToAdd, this would be the "source node"""

        self._parent_ids: Optional[List[str]] = None
        """Only used in Goog trees currently"""

    @property
    def original_full_path(self):
        return self.src_node.full_path

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
        return self.src_node.md5

    @property
    def sha256(self):
        return self.src_node.sha256

    @property
    def size_bytes(self):
        return self.src_node.size_bytes

    @property
    def sync_ts(self):
        return self.src_node.sync_ts

    @property
    def modify_ts(self):
        return self.src_node.modify_ts

    @property
    def change_ts(self):
        return self.src_node.change_ts

    @classmethod
    def has_path(cls):
        return True

    @classmethod
    def is_file(cls):
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
    def parent_ids(self) -> List[int]:
        # TODO: have UIDs use their own type which extends int
        if self._parent_ids:
            if isinstance(self._parent_ids, list):
                return self._parent_ids
            elif isinstance(self._parent_ids, int):
                return [self._parent_ids]
            assert False
        return []

    @parent_ids.setter
    def parent_ids(self, parent_ids):
        """Can be a list of GoogFolders, or a single instance, or None"""
        if not parent_ids:
            self._parent_ids = None
        elif isinstance(parent_ids, list):
            if len(parent_ids) == 1:
                assert isinstance(parent_ids[0], int), f'Found instead: {parent_ids[0]}, type={type(parent_ids[0])}'
                self._parent_ids = parent_ids[0]
            else:
                self._parent_ids = parent_ids
        else:
            self._parent_ids = parent_ids


# CLASS FileToAdd
# ⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟

class FileToAdd(FileDecoratorNode):
    """Decorates a previously existing DisplayNode ('src_node'). This node's identifier stores the
     full path of the destination and the type of the destination tree."""
    def __init__(self, identifier: Identifier, src_node: DisplayNode):
        super().__init__(identifier, src_node)

    def __repr__(self):
        return f'FileToAdd(identifier={self.identifier} src_node={self.src_node})'


# CLASS FileToMove
# ⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟

class FileToMove(FileDecoratorNode):
    """See notes for FileToAdd"""
    def __init__(self, identifier: Identifier, src_node: DisplayNode):
        super().__init__(identifier, src_node)

    def __repr__(self):
        return f'FileToMove(identifier={self.identifier} src_node={self.src_node})'


# CLASS FileToUpdate
# ⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟

class FileToUpdate(FileDecoratorNode):
    """Decorates a previously existing DisplayNode ('src_node'). This node's identifier stores the
     full path of the destination and the type of the destination tree."""
    def __init__(self, identifier: Identifier, src_node: DisplayNode, dst_node: DisplayNode):
        super().__init__(identifier, src_node)
        self.dst_node = dst_node
        """The node to overwrite"""

    def __repr__(self):
        return f'FileToUpdate(identifier={self.identifier} src_node={self.src_node} dst_node={self.dst_node})'
