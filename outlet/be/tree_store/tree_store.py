from abc import ABC, abstractmethod
from typing import Dict, List, Optional, Tuple

from be.disp_tree.filter_state import FilterState
from constants import TreeID
from model.device import Device
from model.node.dir_stats import DirStats
from model.uid import UID
from model.node.node import TNode, SPIDNodePair
from model.node_identifier import NodeIdentifier, SinglePathNodeIdentifier
from util.has_lifecycle import HasLifecycle
from util.task_runner import Task


class TreeStore(HasLifecycle, ABC):
    """
    ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
    ABSTRACT CLASS TreeStore
    ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼
    """
    def __init__(self, device: Device):
        HasLifecycle.__init__(self)
        self.device: Device = device

    # Store meta
    # ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼

    @property
    def device_uid(self) -> UID:
        return self.device.uid

    @abstractmethod
    def is_gdrive(self) -> bool:
        pass

    # Getters / Loaders
    # ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼

    @abstractmethod
    def load_subtree(self, this_task: Task, subtree_root: NodeIdentifier, tree_id: TreeID):
        """Loads an entire subtree from cache, possibly including other long-running operations to bring it up-to-date.
        This will always be run inside a Task (as denoted in the args)"""
        pass

    @abstractmethod
    def is_cache_loaded_for(self, subtree_root: NodeIdentifier) -> bool:
        pass

    @abstractmethod
    def get_node_for_uid(self, uid: UID) -> Optional[TNode]:
        """throws CacheNotLoadedError if appropriate cache not loaded"""
        pass

    @abstractmethod
    def read_node_for_uid(self, node_uid: UID) -> Optional[TNode]:
        """This actually reads directly from the disk cache if needed"""
        pass

    @abstractmethod
    def get_child_list_for_spid(self, parent_spid: SinglePathNodeIdentifier, filter_state: FilterState) -> List[SPIDNodePair]:
        pass

    @abstractmethod
    def to_sn(self, node: TNode, single_path: Optional[str]) -> SPIDNodePair:
        pass

    @abstractmethod
    def get_parent_for_sn(self, sn: SPIDNodePair) -> Optional[SPIDNodePair]:
        pass

    @abstractmethod
    def get_parent_list_for_node(self, node: TNode) -> List[TNode]:
        pass

    @abstractmethod
    def get_node_list_for_path_list(self, path_list: List[str]) -> List[TNode]:
        """Gets any nodes associated for the list of paths.
        Checks (1) the in-memory cache first, and if that's a miss, checks (2) the disk cache. If both of those miss, checks (3) the live source.
        For GDrive stores, we cannot guarantee that a single path will have only one node, or a single node will have only one path."""
        pass

    @abstractmethod
    def get_subtree_bfs_node_list(self, subtree_root: NodeIdentifier) -> List[TNode]:
        pass

    @abstractmethod
    def get_subtree_bfs_sn_list(self, subtree_root_spid: SinglePathNodeIdentifier) -> List[SPIDNodePair]:
        pass

    @abstractmethod
    def get_all_files_and_dirs_for_subtree(self, subtree_root: NodeIdentifier) -> Tuple[List[TNode], List[TNode]]:
        """Returns a tuple of [Files, Dirs]"""
        pass

    @abstractmethod
    def get_all_files_with_content(self, content_uid: UID, cache_info_list: List) -> List[TNode]:
        pass

    # Mutators
    # ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼

    @abstractmethod
    def upsert_single_node(self, node: TNode) -> TNode:
        """Note: node returned will be the same node as from the cache, which may not match the input node
         but should be considered to be more up-to-date. The caller should update its data with this node."""
        pass

    @abstractmethod
    def update_single_node(self, node: TNode) -> TNode:
        """Note: node returned will be the same node as from the cache, which may not match the input node
         but should be considered to be more up-to-date. The caller should update its data with this node."""
        pass

    @abstractmethod
    def remove_single_node(self, node: TNode, to_trash: bool) -> Optional[TNode]:
        """If to_trash==true, this may actually be an update, in which case the updated node is returned."""
        pass

    @abstractmethod
    def remove_subtree(self, subtree_root: TNode, to_trash: bool):
        pass

    @abstractmethod
    def generate_dir_stats(self, subtree_root_node: TNode, tree_id: TreeID) -> Dict[UID, DirStats]:
        pass

    @abstractmethod
    def populate_filter(self, filter_state: FilterState):
        pass

    @abstractmethod
    def refresh_subtree(self, this_task: Task, subtree_root: NodeIdentifier, tree_id: TreeID):
        pass

    @abstractmethod
    def submit_batch_of_changes(self, subtree_root: NodeIdentifier,  upsert_node_list: List[TNode] = None,
                                remove_node_list: List[TNode] = None):
        pass

    # UID <-> DomainID mapping
    # ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼

    @abstractmethod
    def get_node_for_domain_id(self, domain_id: str) -> TNode:
        pass

    @abstractmethod
    def get_uid_for_domain_id(self, domain_id: str, uid_suggestion: Optional[UID] = None) -> UID:
        pass

    # Etc
    # ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼

    @abstractmethod
    def show_tree(self, subtree_root: NodeIdentifier) -> str:
        pass
