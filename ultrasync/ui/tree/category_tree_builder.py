import logging
from typing import Dict, List, Optional

import treelib

from model.category import Category
from model.display_id import Identifier
from model.display_node import CategoryNode, DisplayNode
from model.subtree_snapshot import SubtreeSnapshot
from ui.tree.display_tree_builder import DisplayTreeBuilder

logger = logging.getLogger(__name__)


class CategoryTreeBuilder(DisplayTreeBuilder):
    def __init__(self, controller, root: Identifier = None, tree: SubtreeSnapshot = None):
        super().__init__(controller=controller, root=root, tree=tree)

        self._category_trees: Dict[Category, treelib.Tree] = {}
        """Each entry is lazy-loaded"""

    def get_children_for_root(self) -> Optional[List[DisplayNode]]:
        return self.tree.get_children_for_root()

    def get_children(self, parent_identifier: Identifier) -> Optional[List[DisplayNode]]:
        return self.tree.get_children(parent_identifier)
