import logging
from abc import ABC, abstractmethod
from typing import List, Optional

from model.display_id import Identifier
from model.display_node import DisplayNode
from model.subtree_snapshot import SubtreeSnapshot

logger = logging.getLogger(__name__)


# ABSTRACT CLASS DisplayTreeBuilder
# ⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟

class DisplayTreeBuilder(ABC):
    """Abstract base class. Subclasses can implement different strategies for how to group and organize the underlying data tree"""
    def __init__(self, controller, root: Identifier = None, tree: SubtreeSnapshot = None):
        self.con = controller
        self._loaded = False
        self._root = root
        if tree:
            self._tree = tree
            self._loaded = True
        else:
            self._tree = None

    def _ensure_is_loaded(self):
        if not self._loaded:
            logger.debug(f'Tree was requested. Loading: {self._root}')
            self._tree = self.con.cache_manager.load_subtree(self._root, self.con.tree_id)
            self._loaded = True

    def get_root_identifier(self) -> Identifier:
        if self._tree:
            return self._tree.identifier
        return self._root

    def get_tree(self):
        return self.tree

    @property
    def tree(self):
        self._ensure_is_loaded()
        return self._tree

    @abstractmethod
    def get_children_for_root(self) -> Optional[List[DisplayNode]]:
        pass

    @abstractmethod
    def get_children(self, parent_identifier: Identifier) -> Optional[List[DisplayNode]]:
        """Return the children for the given parent_id.
        The children of the given node can look very different depending on value of 'tree_display_mode'"""
        return None
