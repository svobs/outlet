
from abc import ABC, abstractmethod
from typing import List, Union

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
    def build_identifier(self, tree_type: int = None, path_list: Union[str, List[str]] = None, uid: UID = None,
                         must_be_single_path: bool = False) -> NodeIdentifier:
        pass


