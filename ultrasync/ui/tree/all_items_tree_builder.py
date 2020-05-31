import logging
from typing import Iterable

from model.display_node import DisplayNode
from model.gdrive_subtree import GDriveSubtree
from model.gdrive_whole_tree import GDriveWholeTree
from model.node_identifier import NodeIdentifier
from model.subtree_snapshot import SubtreeSnapshot
from ui.tree.display_tree_builder import DisplayTreeBuilder

logger = logging.getLogger(__name__)

SUPER_DEBUG = False


# CLASS AllItemsGDriveTreeBuilder
# ⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟

class AllItemsGDriveTreeBuilder(DisplayTreeBuilder):
    """Works with either a GDriveWholeTree or a GDriveSubtree"""
    def __init__(self, controller, root: NodeIdentifier = None, tree: SubtreeSnapshot = None):
        super().__init__(controller=controller, root=root, tree=tree)
        assert tree is None or isinstance(tree, GDriveWholeTree) or isinstance(tree, GDriveSubtree), f'For {tree}'
        logger.debug('AllItemsGDriveTreeBuilder init')

    def get_children_for_root(self) -> Iterable[DisplayNode]:
        return self.tree.get_children_for_root()

    def get_children(self, parent_identifier: NodeIdentifier) -> Iterable[DisplayNode]:
        return self.tree.get_children(parent_identifier=parent_identifier)


# CLASS AllItemsLocalFsTreeBuilder
# ⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟

class AllItemsLocalFsTreeBuilder(DisplayTreeBuilder):
    def __init__(self, controller, root: NodeIdentifier = None, tree: SubtreeSnapshot = None):
        super().__init__(controller=controller, root=root, tree=tree)

    def get_children_for_root(self) -> Iterable[DisplayNode]:
        return self.get_children(parent_identifier=self.tree.node_identifier)

    def get_children(self, parent_identifier: NodeIdentifier) -> Iterable[DisplayNode]:
        return self.tree.get_children(parent_identifier)
