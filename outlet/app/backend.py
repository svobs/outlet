
from abc import ABC, abstractmethod
from typing import List, Optional, Union

from model.node.node import Node
from model.node_identifier import NodeIdentifier
from model.uid import UID


# INTERFACE OutletBackend
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼

class OutletBackend(ABC):
    @abstractmethod
    def read_single_node_from_disk_for_path(self, full_path: str, tree_type: int) -> Node:
        pass

    @abstractmethod
    def get_node_for_uid(self, uid: UID, tree_type: int = None) -> Optional[Node]:
        pass

    @abstractmethod
    def get_node_for_local_path(self, full_path: str) -> Optional[Node]:
        pass

    @abstractmethod
    def next_uid(self) -> UID:
        pass

    @abstractmethod
    def get_uid_for_local_path(self, full_path: str, uid_suggestion: Optional[UID] = None, override_load_check: bool = False) -> UID:
        pass

