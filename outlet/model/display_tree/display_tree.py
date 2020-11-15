import collections
import logging
import os
from abc import ABC, abstractmethod
from typing import Callable, Deque, Iterable, List, Optional, Union

from model.node.node import Node, SPIDNodePair
from model.node_identifier import SinglePathNodeIdentifier
from model.uid import UID
from ui.tree.filter_criteria import FilterCriteria

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

        # See refresh_stats() for the following
        self._stats_loaded = False

    # From the root node_identifier
    # ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼

    def get_root_sn(self) -> SPIDNodePair:
        return SPIDNodePair(self.root_identifier, self.get_root_node())

    def get_root_node(self):
        return self.app.cacheman.get_node_for_uid(self.root_identifier.uid)

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

    # Getters & search
    # ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼

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

    @abstractmethod
    def get_children_for_root(self, filter_criteria: FilterCriteria = None) -> Iterable[Node]:
        pass

    @abstractmethod
    def get_children(self, parent: Node, filter_criteria: FilterCriteria = None) -> Iterable[Node]:
        pass

    @staticmethod
    def _make_child_sn_list(child_node_list: Iterable[Node], parent_path: str) -> Iterable[SPIDNodePair]:
        child_sn_list: List[SPIDNodePair] = []
        for child_node in child_node_list:
            if child_node.node_identifier.is_spid():
                # no need to do extra work!
                child_sn = SPIDNodePair(child_node.node_identifier, child_node)
            else:
                child_path = os.path.join(parent_path, child_node.name)
                if child_path not in child_node.get_path_list():
                    # this means we're not following the rules
                    raise RuntimeError(f'Could not find derived path ("{child_path}") in path list ({child_node.get_path_list()}) of child!')
                child_sn = SPIDNodePair(SinglePathNodeIdentifier(child_node.uid, child_path, child_node.get_tree_type()), child_node)
            child_sn_list.append(child_sn)
        return child_sn_list

    def get_child_sn_list_for_root(self) -> Iterable[SPIDNodePair]:
        child_node_list: Iterable[Node] = self.get_children_for_root()
        return self._make_child_sn_list(child_node_list, self.root_path)

    def get_child_sn_list(self, parent: SPIDNodePair) -> Iterable[SPIDNodePair]:
        child_node_list: Iterable[Node] = self.get_children(parent.node)
        return self._make_child_sn_list(child_node_list, parent.spid.get_single_path())

    @abstractmethod
    def get_node_list_for_path_list(self, path_list: List[str]) -> List[Node]:
        pass

    def get_ancestor_list(self, spid: SinglePathNodeIdentifier) -> Deque[Node]:
        return self.app.cacheman.get_ancestor_list_for_single_path_identifier(spid, stop_at_path=self.root_path)

    @staticmethod
    def _build_child_spid(child_node: Node, parent_path: str):
        return SinglePathNodeIdentifier(child_node.uid, os.path.join(parent_path, child_node.name), tree_type=child_node.get_tree_type())

    def visit_each_sn_for_subtree(self, on_file_found: Callable[[SPIDNodePair], None], subtree_root: SPIDNodePair = None):
        if not subtree_root:
            subtree_root = self.get_root_sn()

        queue: Deque[SPIDNodePair] = collections.deque()
        queue.append(subtree_root)

        while len(queue) > 0:
            sn: SPIDNodePair = queue.popleft()
            if sn.node.is_live():  # avoid pending op nodes
                if sn.node.is_dir():
                    child_list = self.get_children(sn.node)
                    if child_list:
                        for child in child_list:
                            if child.node_identifier.is_spid():
                                child_spid = child.node_identifier
                            else:
                                child_spid = DisplayTree._build_child_spid(child, sn.spid.get_single_path())
                            assert child_spid.get_single_path() in child.get_path_list(), \
                                f'Child path "{child_spid.get_single_path()}" does not correspond to actual node: {child}'
                            queue.append(SPIDNodePair(child_spid, child))
                else:
                    on_file_found(sn)

    # Stats
    # ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼

    @abstractmethod
    def get_summary(self):
        pass

    @abstractmethod
    def refresh_stats(self, tree_id: str):
        pass

    def print_tree_contents_debug(self):
        logger.debug('print_tree_contents_debug() not implemented for this tree')
