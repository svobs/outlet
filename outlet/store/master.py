from abc import ABC, abstractmethod
from typing import List, Optional

from model.has_get_children import HasGetChildren
from model.uid import UID
from model.display_tree.display_tree import DisplayTree
from model.node.node import Node
from model.node_identifier import NodeIdentifier


# ABSTRACT CLASS MasterStore
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
from util.has_lifecycle import HasLifecycle


class MasterStore(HasLifecycle, HasGetChildren, ABC):
    def __init__(self):
        HasLifecycle.__init__(self)

    # Getters / Loaders
    # ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼

    @abstractmethod
    def get_display_tree(self, subtree_root: NodeIdentifier, tree_id: str) -> DisplayTree:
        pass

    @abstractmethod
    def get_node_for_uid(self, uid: UID) -> Optional[Node]:
        pass

    @abstractmethod
    def get_children(self, node: Node) -> List[Node]:
        pass

    @abstractmethod
    def get_parent_list_for_node(self, node: Node) -> List[Node]:
        pass

    @abstractmethod
    def refresh_subtree(self, subtree_root_node: Node, tree_id: str):
        pass

    # Mutators
    # ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼

    @abstractmethod
    def upsert_single_node(self, node: Node):
        pass

    @abstractmethod
    def remove_single_node(self, node: Node, to_trash: bool):
        pass

    @abstractmethod
    def remove_subtree(self, subtree_root: Node, to_trash: bool):
        pass

    @abstractmethod
    def refresh_subtree_stats(self, subtree_root_node: Node, tree_id: str):
        pass

    # UID <-> DomainID mapping
    # ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼

    @abstractmethod
    def get_node_for_domain_id(self, domain_id: str) -> Node:
        pass

    @abstractmethod
    def get_uid_for_domain_id(self, domain_id: str, uid_suggestion: Optional[UID] = None) -> UID:
        pass

    @abstractmethod
    def get_node_list_for_path_list(self, path_list: List[str]) -> List[Node]:
        pass
