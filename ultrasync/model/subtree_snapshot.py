from abc import ABC, abstractmethod
from typing import Any, List, Optional, Union

from model.display_id import Identifier
from model.display_node import DisplayNode


# ABSTRACT CLASS SubtreeSnapshot
# ⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟

class SubtreeSnapshot(ABC):
    def __init__(self, root_identifier: Identifier):
        super().__init__()
        self.identifier: Identifier = root_identifier

    # From the root identifier
    # ⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟

    @property
    def tree_type(self) -> int:
        return self.identifier.tree_type

    @property
    def root_path(self):
        return self.identifier.full_path

    @property
    def uid(self):
        return self.identifier.uid

    # Factory methods
    # ⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟

    @classmethod
    @abstractmethod
    def create_empty_subtree(cls, subtree_root_identifier: Identifier):
        """Return a new empty subtree with the given root and which is of the same type of this tree"""
        return

    @classmethod
    @abstractmethod
    def create_identifier(cls, full_path, category) -> Identifier:
        """Create a new identifier of the type matching this tree"""
        pass

    # Getters & search
    # ⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟

    @abstractmethod
    def get_all(self) -> List[DisplayNode]:
        """Returns the complete set of all unique items from this subtree."""
        return []

    @abstractmethod
    def get_ignored_items(self):
        return []

    @abstractmethod
    def get_ancestor_chain(self, item) -> List[Identifier]:
        pass

    @abstractmethod
    def get_full_path_for_item(self, item) -> str:
        pass

    @abstractmethod
    def get_relative_path_for_item(self, item):
        pass

    @abstractmethod
    def get_for_path(self, path: str, include_ignored=False, only_this_md5=None) -> Optional[DisplayNode]:
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
