import logging
import threading
from typing import Iterable, List, Optional

from model.node.node import Node, SPIDNodePair
from model.node_identifier import SinglePathNodeIdentifier
from model.display_tree.display_tree import DisplayTree
from ui.tree.filter_criteria import FilterCriteria

logger = logging.getLogger(__name__)


# ABSTRACT CLASS LazyLoadDisplayTreeDecorator
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼

class LazyLoadDisplayTreeDecorator(DisplayTree):
    """Wraps a DisplayTree. Can optionally wrap a root identifier instead, in which case it only loads the tree when
     it is requested (either via the "tree" attribute or via one of the get_children() methods"""
    # TODO: consider pruning this so that we just cache the tree. Can probably just delete the class and put functionality into controller

    def __init__(self, controller, root_identifier: SinglePathNodeIdentifier = None, tree: DisplayTree = None):
        assert root_identifier or tree, f'Neither root_identifier nor tree provided!'
        self.con = controller
        super().__init__(self.con.app, self.con.tree_id, root_identifier)

        self._loaded: bool = False
        self._lock: threading.Lock = threading.Lock()
        if tree:
            self._tree: Optional[DisplayTree] = tree
            self._root_identifier: SinglePathNodeIdentifier = self._tree.root_identifier
            self._loaded = True
        else:
            self._tree: Optional[DisplayTree] = None
            self._root_identifier: SinglePathNodeIdentifier = root_identifier

    def _ensure_is_loaded(self):
        """Performs a SYNCHRONOUS load if needed"""
        if not self._loaded:
            with self._lock:
                if not self._loaded:
                    # This will also start live monitoring if configured:
                    logger.debug(f'[{self.con.tree_id}] Tree was requested. Loading: {self._root_identifier}')
                    self._tree = self.con.cacheman.load_subtree(self._root_identifier, self.con.tree_id)
                    self._root_identifier = self._tree.root_identifier
                    self._loaded = True
                    logger.debug(f'[{self.con.tree_id}] Tree was loaded successfully.')

    def get_root_identifier(self) -> SinglePathNodeIdentifier:
        return self._root_identifier

    def get_tree(self) -> DisplayTree:
        self._ensure_is_loaded()
        return self._tree

    @property
    def tree(self) -> DisplayTree:
        self._ensure_is_loaded()
        return self._tree

    def get_children_for_root(self, filter_criteria: FilterCriteria = None) -> Iterable[Node]:
        return self.get_tree().get_children_for_root(filter_criteria)

    def get_children(self, node: Node, filter_criteria: FilterCriteria = None) -> Iterable[Node]:
        """Return the children for the given parent_uid.
        The children of the given node can look very different depending on value of 'tree_display_mode'"""
        return self.get_tree().get_children(node, filter_criteria)

    def get_child_sn_list_for_root(self) -> Iterable[SPIDNodePair]:
        return self.get_tree().get_child_sn_list_for_root()

    def get_child_sn_list(self, parent: SPIDNodePair) -> Iterable[SPIDNodePair]:
        return self.get_tree().get_child_sn_list(parent)

    def get_node_list_for_path_list(self, path_list: List[str]) -> List[Node]:
        return self.get_tree().get_node_list_for_path_list(path_list)

    def get_summary(self):
        return self.get_tree().get_summary()

    def refresh_stats(self, tree_id: str):
        return self.get_tree().refresh_stats(tree_id)
