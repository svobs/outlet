import logging
import os
from typing import Deque, Iterable, List, Union

from model.has_get_children import HasGetChildren
from model.node.node import Node, SPIDNodePair
from model.node_identifier import SinglePathNodeIdentifier
from model.uid import UID
from ui.tree.filter_criteria import FilterCriteria

logger = logging.getLogger(__name__)


# ABSTRACT CLASS DisplayTree
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼

class DisplayTree(HasGetChildren):
    def __init__(self, app, tree_id: str, root_identifier: SinglePathNodeIdentifier):
        self.app = app
        self.tree_id: str = tree_id

        assert isinstance(root_identifier, SinglePathNodeIdentifier), f'Expected SinglePathNodeIdentifier but got {type(root_identifier)}'
        self.root_identifier: SinglePathNodeIdentifier = root_identifier
        """This is needed to clarify the (albeit very rare) case where the root node resolves to multiple paths.
        Our display tree can only have one path."""

    # From the root node_identifier
    # ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼

    def get_root_sn(self) -> SPIDNodePair:
        return SPIDNodePair(self.root_identifier, self.get_root_node())

    def get_root_node(self):
        """FIXME: Add this to member var instead"""
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

    def get_children_for_root(self, filter_criteria: FilterCriteria = None) -> Iterable[Node]:
        return self.get_children(self.get_root_node(), filter_criteria)

    def get_children(self, parent: Node, filter_criteria: FilterCriteria = None) -> Iterable[Node]:
        return self.app.cacheman.get_children(parent, filter_criteria)

    def get_ancestor_list(self, spid: SinglePathNodeIdentifier) -> Deque[Node]:
        """TODO: Add to gRPC API"""
        return self.app.cacheman.get_ancestor_list_for_single_path_identifier(spid, stop_at_path=self.root_path)

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
        logger.debug(f'[{self.tree_id}] Contents of tree with root "{self.root_identifier}": \n' +
                     self.app.cacheman.show_tree(self.root_identifier))

    def __repr__(self):
        return f'DisplayTree(tree_id="{self.tree_id}" root="{self.root_identifier}"])'
