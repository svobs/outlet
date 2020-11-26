import logging
import threading
from typing import Optional

from model.display_tree.display_tree import DisplayTree
from model.node_identifier import SinglePathNodeIdentifier

logger = logging.getLogger(__name__)


# ABSTRACT CLASS DisplayTreeLazyLoader
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼

class DisplayTreeLazyLoader:
    """Wraps a DisplayTree. Can optionally wrap a root identifier instead, in which case it only loads the tree when
     it is requested (either via the "tree" attribute or via one of the get_children() methods"""

    def __init__(self, controller, root_identifier: SinglePathNodeIdentifier = None, tree: DisplayTree = None):
        assert root_identifier or tree, f'Neither root_identifier nor tree provided!'
        self.con = controller

        self._loaded: bool = False
        self._lock: threading.Lock = threading.Lock()
        if tree:
            self._tree: Optional[DisplayTree] = tree
            self._root_identifier: SinglePathNodeIdentifier = self._tree.get_root_identifier()
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
                    # TODO: replace this with GRPC call
                    self._tree = self.con.cacheman.create_display_tree(self._root_identifier, self.con.tree_id)
                    self._root_identifier = self._tree.get_root_identifier()
                    self._loaded = True
                    logger.debug(f'[{self.con.tree_id}] Tree was loaded successfully.')

    def get_root_identifier(self) -> SinglePathNodeIdentifier:
        return self._root_identifier

    def get_tree(self) -> DisplayTree:
        self._ensure_is_loaded()
        return self._tree
