import logging
from typing import List, Optional

from model.display_node import DisplayNode
from model.node_identifier import NodeIdentifier
from ui.tree.category_display_tree import CategoryDisplayTree
from ui.tree.display_tree_builder import DisplayTreeBuilder

logger = logging.getLogger(__name__)


class CategoryTreeBuilder(DisplayTreeBuilder):
    """This class doesn't really do anything: everything is loaded via the tree"""
    def __init__(self, controller, root: NodeIdentifier = None, tree: CategoryDisplayTree = None):
        super().__init__(controller=controller, root=root, tree=tree)
        assert tree is None or isinstance(tree, CategoryDisplayTree), f'For {tree}'
        logger.debug('CategoryTreeBuilder init')

    def get_children_for_root(self) -> Optional[List[DisplayNode]]:
        return self.tree.get_children_for_root()

    def get_children(self, parent_identifier: NodeIdentifier) -> Optional[List[DisplayNode]]:
        return self.tree.get_children(parent_identifier)
