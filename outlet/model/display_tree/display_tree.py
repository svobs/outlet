import logging
import os
from typing import Deque, Iterable, List, Optional, Union

from model.has_get_children import HasGetChildren
from model.node.node import Node, SPIDNodePair
from model.node_identifier import SinglePathNodeIdentifier
from model.uid import UID
from store.cache_manager import DisplayTreeUiState
from ui.tree.filter_criteria import FilterCriteria

logger = logging.getLogger(__name__)


# ABSTRACT CLASS DisplayTree
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼

class DisplayTree(HasGetChildren):
    def __init__(self, backend, state: DisplayTreeUiState):
        self.backend = backend
        self.state = state

    # From the root node_identifier
    # ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼

    @property
    def tree_id(self):
        return self.state.tree_id

    def get_root_sn(self) -> SPIDNodePair:
        return self.state.root_sn

    def get_root_identifier(self) -> SinglePathNodeIdentifier:
        return self.state.root_sn.spid

    def get_root_node(self):
        return self.state.root_sn.node

    def is_needs_manual_load(self):
        return self.state.needs_manual_load

    def set_needs_manual_load(self, needs_manual_load: bool):
        self.state.needs_manual_load = needs_manual_load

    @property
    def root_path(self) -> str:
        """Override this if root node's identifier is not SinglePathNodeIdentifier"""
        return self.state.root_sn.spid.get_single_path()

    @property
    def tree_type(self) -> int:
        return self.state.root_sn.spid.tree_type

    @property
    def uid(self):
        return self.state.root_sn.spid.uid

    @property
    def root_uid(self) -> UID:
        return self.uid

    # More root meta
    # ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼
    def is_root_exists(self) -> bool:
        return self.state.root_exists

    def set_root_exists(self, exists: bool):
        self.state.root_exists = exists

    def get_offending_path(self) -> Optional[str]:
        """Only present in *some* cases where root_exists() == False"""
        return self.state.offending_path

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

    def get_children_for_root(self, filter_criteria: FilterCriteria = None) -> Iterable[Node]:
        return self.get_children(self.get_root_node(), filter_criteria)

    def get_children(self, parent: Node, filter_criteria: FilterCriteria = None) -> Iterable[Node]:
        return self.backend.cacheman.get_children(parent, filter_criteria)

    def get_ancestor_list(self, spid: SinglePathNodeIdentifier) -> Deque[Node]:
        """TODO: Add to gRPC API"""
        return self.backend.cacheman.get_ancestor_list_for_single_path_identifier(spid, stop_at_path=self.root_path)

    def get_child_sn_list_for_root(self) -> Iterable[SPIDNodePair]:
        child_node_list: Iterable[Node] = self.get_children_for_root()
        return self._make_child_sn_list(child_node_list, self.root_path)

    def get_child_sn_list(self, parent: SPIDNodePair) -> Iterable[SPIDNodePair]:
        child_node_list: Iterable[Node] = self.get_children(parent.node)
        return self._make_child_sn_list(child_node_list, parent.spid.get_single_path())

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

    # Stats
    # ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼

    def print_tree_contents_debug(self):
        logger.debug(f'[{self.tree_id}] Contents of tree with root "{self.get_root_sn().spid}": \n' +
                     self.backend.cacheman.show_tree(self.get_root_sn().spid))

    def __repr__(self):
        return f'DisplayTree(tree_id="{self.tree_id}" root="{self.get_root_sn()}"])'
