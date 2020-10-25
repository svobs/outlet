import logging
from abc import ABC, abstractmethod
from typing import Deque, Iterable, List, Optional, Union

from model.node.node import Node
from model.node_identifier import SinglePathNodeIdentifier
from model.uid import UID
from util import file_util

logger = logging.getLogger(__name__)


# ABSTRACT CLASS DisplayTree
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼

class DisplayTree(ABC):
    def __init__(self, app, tree_id: str, root_identifier: SinglePathNodeIdentifier):
        self.app = app
        self.tree_id: str = tree_id

        assert isinstance(root_identifier, SinglePathNodeIdentifier), f'Expected SinglePathNodeIdentifier but got {type(root_identifier)}'
        self.root_identifier: SinglePathNodeIdentifier = root_identifier
        """This is needed to clarify the (albeit very rare) case where the root node resolves to multiple paths.
        Our display tree can only have one path."""

    # From the root node_identifier
    # ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼

    def get_root_node(self):
        return self.app.cacheman.get_node_for_uid(self.root_identifier.uid)

    @property
    def node_identifier(self) -> SinglePathNodeIdentifier:
        """Override this if root node's identifier is not SinglePathNodeIdentifier"""
        return self.root_identifier

    @property
    def root_path(self) -> str:
        """Override this if root node's identifier is not SinglePathNodeIdentifier"""
        return self.root_identifier.get_single_path()

    @property
    def tree_type(self) -> int:
        return self.root_identifier.tree_type

    @property
    def uid(self):
        return self.root_identifier.uid

    @property
    def root_uid(self) -> UID:
        return self.uid

    def print_tree_contents_debug(self):
        logger.debug('print_tree_contents_debug() not implemented for this tree')

    def is_path_in_subtree(self, path_list: Union[str, List[str]]):
        if not path_list:
            raise RuntimeError('is_path_in_subtree(): full_path not provided!')

        if isinstance(path_list, list):
            for path in path_list:
                # i.e. if any paths start with
                if path.startswith(self.root_path):
                    return True
            return False

        return path_list.startswith(self.root_path)

    # Getters & search
    # ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼

    @abstractmethod
    def get_children_for_root(self) -> Iterable[Node]:
        pass

    @abstractmethod
    def get_children(self, parent: Node) -> Iterable[Node]:
        pass

    @abstractmethod
    def get_single_parent_for_node(self, node) -> Optional[Node]:
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

    def get_ancestor_list(self, single_path_node_identifier: SinglePathNodeIdentifier) -> Deque[Node]:
        return self.app.cacheman.get_ancestor_list_for_single_path_identifier(single_path_node_identifier, stop_at_path=self.root_path)

    # Stats
    # ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼

    @abstractmethod
    def get_summary(self):
        pass

    @abstractmethod
    def refresh_stats(self, tree_id: str):
        pass
