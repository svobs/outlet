import logging
from typing import List, Optional

from model.display_id import GDriveIdentifier, Identifier
from model.display_node import DisplayNode
from model.gdrive_tree import GDriveSubtree, GDriveTree, GDriveWholeTree
from model.subtree_snapshot import SubtreeSnapshot
from ui.tree.display_tree_builder import DisplayTreeBuilder

logger = logging.getLogger(__name__)


# CLASS AllItemsTreeBuilder
# ⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟

# FIXME: currently only GDrive is supported - not local files
class AllItemsTreeBuilder(DisplayTreeBuilder):
    def __init__(self, controller, root: Identifier = None, tree: SubtreeSnapshot = None):
        super().__init__(controller=controller, root=root, tree=tree)

    def get_children_for_root(self) -> Optional[List[DisplayNode]]:
        if isinstance(self.tree, GDriveWholeTree):
            # Whole tree? -> return the root nodes
            return self.tree.roots
        else:
            # Subtree? -> return the subtree root
            assert isinstance(self.tree, GDriveSubtree)
            parent_id = self.tree.root_id
            return self.tree.get_children(parent_id)

    def get_children(self, parent_identifier: Identifier) -> Optional[List[DisplayNode]]:
        assert isinstance(self.tree, GDriveTree)
        return self.tree.get_children(parent_id=parent_identifier)
