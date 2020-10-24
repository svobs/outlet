import logging
import threading
from typing import Iterable

from model.node.node import Node
from model.node_identifier import SinglePathNodeIdentifier
from model.display_tree.display_tree import DisplayTree

logger = logging.getLogger(__name__)


# ABSTRACT CLASS LazyLoadDisplayTreeDecorator
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼

class LazyLoadDisplayTreeDecorator:
    """Wraps a DisplayTree. Can optionally wrap a root identifier instead, in which case it only loads the tree when
     it is requested (either via the "tree" attribute or via one of the get_children() methods"""
    def __init__(self, controller, root: SinglePathNodeIdentifier = None, tree: DisplayTree = None):
        self.con = controller
        self._loaded: bool = False
        self._root: SinglePathNodeIdentifier = root
        self._lock: threading.Lock = threading.Lock()
        if tree:
            self._tree: DisplayTree = tree
            self._loaded = True
        else:
            self._tree = None

    def _ensure_is_loaded(self):
        """Performs a SYNCHRONOUS load if needed"""
        if not self._loaded:
            with self._lock:
                if not self._loaded:
                    # This will also start live monitoring if configured:
                    logger.debug(f'[{self.con.tree_id}] Tree was requested. Loading: {self._root}')
                    self._tree = self.con.cacheman.load_subtree(self._root, self.con.tree_id)
                    self._loaded = True
                    logger.debug(f'[{self.con.tree_id}] Tree was loaded successfully.')

    def get_root_identifier(self) -> SinglePathNodeIdentifier:
        if self._tree:
            return self._tree.node_identifier
        return self._root

    def get_tree(self) -> DisplayTree:
        return self.tree

    @property
    def tree(self) -> DisplayTree:
        self._ensure_is_loaded()
        return self._tree

    def get_children_for_root(self) -> Iterable[Node]:
        return self.tree.get_children_for_root()

    def get_children(self, node: Node) -> Iterable[Node]:
        """Return the children for the given parent_uid.
        The children of the given node can look very different depending on value of 'tree_display_mode'"""
        return self.tree.get_children(node)
