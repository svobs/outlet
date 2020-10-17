from abc import ABC, abstractmethod
from typing import List, Optional

from model.uid import UID
from model.display_tree.display_tree import DisplayTree
from model.node.display_node import DisplayNode
from model.node_identifier import NodeIdentifier


# ABSTRACT CLASS MasterCache
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼

class MasterCache(ABC):
    # Getters / Loaders
    # ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼

    @abstractmethod
    def get_display_tree(self, subtree_root: NodeIdentifier, tree_id: str) -> DisplayTree:
        pass

    @abstractmethod
    def get_node_for_uid(self, uid: UID) -> Optional[DisplayNode]:
        pass

    @abstractmethod
    def get_children(self, node: DisplayNode) -> List[DisplayNode]:
        pass

    @abstractmethod
    def get_parent_for_node(self, node: DisplayNode, required_subtree_path: str = None) -> Optional[DisplayNode]:
        pass

    @abstractmethod
    def refresh_subtree(self, subtree_root_node: DisplayNode, tree_id: str):
        pass

    # Mutators
    # ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼

    @abstractmethod
    def upsert_single_node(self, node: DisplayNode):
        pass

    @abstractmethod
    def remove_single_node(self, node: DisplayNode, to_trash: bool):
        pass

    @abstractmethod
    def remove_subtree(self, subtree_root: DisplayNode, to_trash: bool):
        pass

    @abstractmethod
    def refresh_subtree_stats(self, subtree_root_node: DisplayNode, tree_id: str):
        pass

    # UID <-> DomainID mapping
    # ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼

    @abstractmethod
    def get_node_for_domain_id(self, domain_id: str) -> DisplayNode:
        pass

    @abstractmethod
    def get_uid_for_domain_id(self, domain_id: str, uid_suggestion: Optional[UID] = None) -> UID:
        pass
