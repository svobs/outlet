from abc import ABC, abstractmethod
from typing import Dict, List, Optional, Tuple

from backend.display_tree.filter_state import FilterState
from constants import TreeID
from model.device import Device
from model.node.directory_stats import DirectoryStats
from model.uid import UID
from model.node.node import Node, SPIDNodePair
from model.node_identifier import NodeIdentifier, SinglePathNodeIdentifier
from util.has_lifecycle import HasLifecycle


class TreeStore(HasLifecycle, ABC):
    """
    ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
    ABSTRACT CLASS TreeStore
    ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼
    """
    def __init__(self, device: Device):
        HasLifecycle.__init__(self)
        self.device: Device = device

    # Getters / Loaders
    # ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼

    @property
    def device_uid(self) -> UID:
        return self.device.uid

    @abstractmethod
    def load_subtree(self, subtree_root: NodeIdentifier, tree_id: TreeID):
        pass

    @abstractmethod
    def get_node_for_uid(self, uid: UID) -> Optional[Node]:
        pass

    @abstractmethod
    def get_child_list_for_spid(self, parent_spid: SinglePathNodeIdentifier, filter_state: FilterState) -> List[SPIDNodePair]:
        pass

    @abstractmethod
    def get_parent_list_for_node(self, node: Node) -> List[Node]:
        pass

    @abstractmethod
    def refresh_subtree(self, subtree_root: NodeIdentifier, tree_id: TreeID):
        pass

    # Mutators
    # ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼

    @abstractmethod
    def upsert_single_node(self, node: Node):
        pass

    @abstractmethod
    def update_single_node(self, node: Node):
        pass

    @abstractmethod
    def remove_single_node(self, node: Node, to_trash: bool):
        pass

    @abstractmethod
    def remove_subtree(self, subtree_root: Node, to_trash: bool):
        pass

    @abstractmethod
    def generate_dir_stats(self, subtree_root_node: Node, tree_id: TreeID) -> Dict[UID, DirectoryStats]:
        pass

    # UID <-> DomainID mapping
    # ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼

    @abstractmethod
    def get_node_for_domain_id(self, domain_id: str) -> Node:
        pass

    @abstractmethod
    def get_uid_for_domain_id(self, domain_id: str, uid_suggestion: Optional[UID] = None) -> UID:
        pass

    @abstractmethod
    def get_node_list_for_path_list(self, path_list: List[str]) -> List[Node]:
        pass

    @abstractmethod
    def get_all_files_and_dirs_for_subtree(self, subtree_root: NodeIdentifier) -> Tuple[List[Node], List[Node]]:
        """Returns a tuple of [Files, Dirs]"""
        pass

    @abstractmethod
    def show_tree(self, subtree_root: NodeIdentifier) -> str:
        pass

