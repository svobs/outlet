import logging
from abc import ABC, abstractmethod
from collections import deque
from typing import Callable, Deque, Iterable, List, Optional, Union

from model.node.display_node import DisplayNode

logger = logging.getLogger(__name__)


# ABSTRACT CLASS DisplayTree
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼

class DisplayTree(ABC):
    def __init__(self, root_node: DisplayNode):
        super().__init__()
        assert isinstance(root_node, DisplayNode)
        self.root_node: DisplayNode = root_node

        self._stats_loaded = False

    # From the root node_identifier
    # ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼

    @property
    def node_identifier(self):
        return self.root_node.node_identifier

    @property
    def tree_type(self) -> int:
        return self.root_node.node_identifier.tree_type

    @property
    def root_path(self):
        return self.root_node.node_identifier.full_path

    @property
    def root_uid(self):
        return self.uid

    @property
    def uid(self):
        return self.root_node.node_identifier.uid

    def print_tree_contents_debug(self):
        logger.debug('print_tree_contents_debug() not implemented for this tree')

    def in_this_subtree(self, full_path: Union[str, List[str]]):
        if not full_path:
            raise RuntimeError('in_this_subtree(): full_path not provided!')

        if isinstance(full_path, list):
            for p in full_path:
                # i.e. if any paths start with
                if p.startswith(self.root_path):
                    return True
            return False

        return full_path.startswith(self.root_path)

    # Getters & search
    # ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼

    @abstractmethod
    def get_children_for_root(self) -> Iterable[DisplayNode]:
        pass

    @abstractmethod
    def get_children(self, parent: DisplayNode) -> Iterable[DisplayNode]:
        pass

    @abstractmethod
    def get_parent_for_node(self, node) -> Optional[DisplayNode]:
        pass

    @abstractmethod
    def get_full_path_for_node(self, node) -> str:
        pass

    @abstractmethod
    def get_relative_path_for_node(self, node):
        pass

    @abstractmethod
    def get_for_path(self, path: str, include_ignored=False) -> List[DisplayNode]:
        pass

    @abstractmethod
    def get_md5_dict(self):
        pass

    def get_ancestors(self, node: DisplayNode, stop_before_func: Callable[[DisplayNode], bool] = None) -> Deque[DisplayNode]:
        ancestors: Deque[DisplayNode] = deque()

        # Walk up the source tree, adding ancestors as we go, until we reach either a node which has already
        # been added to this tree, or the root of the source tree
        ancestor = node
        while ancestor:
            if stop_before_func is not None and stop_before_func(ancestor):
                return ancestors
            ancestor = self.get_parent_for_node(ancestor)
            if ancestor:
                if ancestor.uid == self.uid:
                    # do not include source tree's root node:
                    return ancestors
                ancestors.appendleft(ancestor)

        return ancestors

    # Stats
    # ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼

    @abstractmethod
    def get_summary(self):
        pass

    @abstractmethod
    def refresh_stats(self, tree_id: str):
        pass
