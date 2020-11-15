from abc import ABC, abstractmethod
from typing import List

from model.node.node import Node


# ABSTRACT CLASS HasGetChildren
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼

class HasGetChildren(ABC):

    @abstractmethod
    def get_children(self, node: Node, filter_criteria = None) -> List[Node]:
        pass
