from abc import ABC, abstractmethod
from typing import Any, List, Optional, Union

from model.node_identifier import NodeIdentifier
from model.display_node import DisplayNode


# ABSTRACT CLASS SubtreeSnapshot
# ⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟

class SubtreeSnapshot(ABC):
    def __init__(self, root_identifier: NodeIdentifier):
        super().__init__()
        self.node_identifier: NodeIdentifier = root_identifier

    # From the root node_identifier
    # ⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟

    @property
    def tree_type(self) -> int:
        return self.node_identifier.tree_type

    @property
    def root_path(self):
        return self.node_identifier.full_path

    @property
    def uid(self):
        return self.node_identifier.uid

    def in_this_subtree(self, path: str):
        if isinstance(path, list):
            for p in path:
                # i.e. any
                if p.startswith(self.root_path):
                    return True
            return False

        return path.startswith(self.root_path)

    # Factory methods
    # ⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟

    @abstractmethod
    def get_new_uid(self):
        pass

    @classmethod
    @abstractmethod
    def create_empty_subtree(cls, subtree_root_identifier: NodeIdentifier):
        """Return a new empty subtree with the given root and which is of the same type of this tree"""
        return

    @classmethod
    @abstractmethod
    def create_identifier(cls, full_path, uid, category) -> NodeIdentifier:
        """Create a new node_identifier of the type matching this tree"""
        pass

    # Getters & search
    # ⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟

    @abstractmethod
    def get_all(self) -> List[DisplayNode]:
        """Returns the complete set of all unique items from this subtree."""
        return []

    @abstractmethod
    def get_item_for_identifier(self, identifer: NodeIdentifier) -> Optional[DisplayNode]:
        return None

    @abstractmethod
    def get_ignored_items(self):
        return []

    @abstractmethod
    def get_parent_for_item(self, item) -> Optional[DisplayNode]:
        pass

    @abstractmethod
    def get_ancestor_chain(self, item) -> List[NodeIdentifier]:
        pass

    @abstractmethod
    def get_full_path_for_item(self, item) -> str:
        pass

    @abstractmethod
    def get_relative_path_for_item(self, item):
        pass

    @abstractmethod
    def get_for_path(self, path: str, include_ignored=False) -> List[DisplayNode]:
        pass

    @abstractmethod
    def get_md5_dict(self):
        pass

    # Setter
    # ⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟

    @abstractmethod
    def add_item(self, item):
        pass

    # Stats
    # ⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟

    @abstractmethod
    def get_summary(self):
        pass
