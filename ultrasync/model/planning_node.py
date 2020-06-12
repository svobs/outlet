import os
from abc import ABC

import format_util
from constants import ICON_ADD_DIR
from model.display_node import DisplayNode, DisplayNodeWithParents
from model.node_identifier import NodeIdentifier


# ABSTRACT CLASS PlanningNode
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼

class PlanningNode(DisplayNodeWithParents, ABC):
    """
    Planning nodes represent work which has not yet been done, such as copying a file.
    They can be thought of as 'ghosts of a possible future'. As such, they should not be
    cached like other objects.

    Note also that this node should think of itself as 'living' in its destination tree - thus
    properties like 'full_path' return the destination path, not the source.
    """
    def __init__(self, node_identifier: NodeIdentifier):
        super().__init__(node_identifier)

    def is_just_fluff(self) -> bool:
        return False


# ABSTRACT CLASS FileDecoratorNode
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼

class FileDecoratorNode(PlanningNode, ABC):
    """Decorates a previously existing DisplayNode."""
    def __init__(self, node_identifier: NodeIdentifier, src_node: DisplayNode):
        super().__init__(node_identifier)

        self.src_node: DisplayNode = src_node
        """The original node (e.g., for a FileToAdd, this would be the "source node"""

    @property
    def original_full_path(self):
        return self.src_node.full_path

    @property
    def dest_path(self):
        return self.node_identifier.full_path

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
    

# CLASS FileToAdd
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼

class FileToAdd(FileDecoratorNode):
    """Decorates a previously existing DisplayNode ('src_node'). This node's node_identifier stores the
     full path of the destination and the type of the destination tree."""
    def __init__(self, node_identifier: NodeIdentifier, src_node: DisplayNode):
        super().__init__(node_identifier, src_node)

    def __repr__(self):
        return f'FileToAdd(node_identifier={self.node_identifier} parent_uids={self.parent_uids} src_node={self.src_node})'


# CLASS FileToMove
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼

class FileToMove(FileDecoratorNode):
    """See notes for FileToAdd"""
    def __init__(self, node_identifier: NodeIdentifier, src_node: DisplayNode):
        super().__init__(node_identifier, src_node)

    def __repr__(self):
        return f'FileToMove(node_identifier={self.node_identifier} parent_uids={self.parent_uids} src_node={self.src_node})'


# CLASS FileToUpdate
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼

class FileToUpdate(FileDecoratorNode):
    """Decorates a previously existing DisplayNode ('src_node'). This node's node_identifier stores the
     full path of the destination and the type of the destination tree."""
    def __init__(self, node_identifier: NodeIdentifier, src_node: DisplayNode, dst_node: DisplayNode):
        assert node_identifier == dst_node.node_identifier
        super().__init__(node_identifier, src_node)
        self.dst_node = dst_node
        """The node to overwrite"""

    def __repr__(self):
        return f'FileToUpdate(node_identifier={self.node_identifier} parent_uids={self.parent_uids} src_node={self.src_node} dst_node={self.dst_node})'


# CLASS LocalDirToAdd
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼


class LocalDirToAdd(PlanningNode):
    def __init__(self, node_identifier: NodeIdentifier):
        super().__init__(node_identifier)

        self.file_count = 0
        self.dir_count = 0
        self._size_bytes = 0

    def zero_out_stats(self):
        self._size_bytes = 0
        self.file_count = 0
        self.dir_count = 0

    def add_meta_metrics(self, child_node):
        if child_node.is_dir():
            self.dir_count += child_node.dir_count + 1
            self.file_count += child_node.file_count
        else:
            self.file_count += 1

        if child_node.size_bytes:
            self._size_bytes += child_node.size_bytes

    def get_summary(self):
        if not self._size_bytes and not self.file_count:
            return '0 items'
        size = format_util.humanfriendlier_size(self._size_bytes)
        return f'{size} in {self.file_count:n} files and {self.dir_count:n} dirs'

    @property
    def size_bytes(self):
        return self._size_bytes

    @property
    def etc(self):
        return f'{self.file_count:n} files'

    def get_icon(self):
        return ICON_ADD_DIR

    @classmethod
    def is_file(cls):
        return False

    @classmethod
    def is_dir(cls):
        return True

    @classmethod
    def has_path(cls):
        return True

    def __repr__(self):
        return f'LocalDirToAdd(node_identifier={self.node_identifier} parent_uids={self.parent_uids})'

    def to_tuple(self):
        pass
