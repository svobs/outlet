import logging
from abc import ABC, abstractmethod
from collections import deque
from typing import Callable, Deque, Iterable, List, Optional, Union

from model.node.node import Node
from model.node_identifier import SinglePathNodeIdentifier
from util import file_util

logger = logging.getLogger(__name__)


# ABSTRACT CLASS DisplayTree
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼

class DisplayTree(ABC):
    def __init__(self, root_node: Node):
        super().__init__()
        assert isinstance(root_node, Node)
        self.root_node: Node = root_node

        self._stats_loaded = False

    # From the root node_identifier
    # ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼

    @property
    def node_identifier(self) -> SinglePathNodeIdentifier:
        """Override this if root node's identifier is not SinglePathNodeIdentifier"""
        assert isinstance(self.root_node.node_identifier, SinglePathNodeIdentifier)
        return self.root_node.node_identifier

    @property
    def root_path(self):
        """Override this if root node's identifier is not SinglePathNodeIdentifier"""
        assert isinstance(self.root_node.node_identifier, SinglePathNodeIdentifier)
        return self.root_node.node_identifier.get_single_path()

    @property
    def tree_type(self) -> int:
        return self.root_node.node_identifier.tree_type

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
            for path in full_path:
                # i.e. if any paths start with
                if path.startswith(self.root_path):
                    return True
            return False

        return full_path.startswith(self.root_path)

    # Getters & search
    # ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼

    @abstractmethod
    def get_children_for_root(self) -> Iterable[Node]:
        pass

    @abstractmethod
    def get_children(self, parent: Node) -> Iterable[Node]:
        pass

    @abstractmethod
    def get_parent_for_node(self, node) -> Optional[Node]:
        pass

    def get_relative_path_list_for_node(self, node: Node) -> List[str]:
        relative_path_list: List[str] = []
        for full_path in node.get_path_list():
            if full_path.startswith(self.root_path):
                relative_path_list.append(file_util.strip_root(full_path, self.root_path))
        return relative_path_list

    @abstractmethod
    def get_node_list_for_path_list(self, path_list: List[str]) -> List[Node]:
        pass

    @abstractmethod
    def get_md5_dict(self):
        pass

    def get_ancestors(self, node: Node, stop_before_func: Callable[[Node], bool] = None) -> Deque[Node]:
        ancestors: Deque[Node] = deque()

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
