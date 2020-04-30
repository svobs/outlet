import logging
from typing import Dict

from model.display_id import DisplayId
from model.fmeta import LocalFsDisplayId
from ui.tree import category_tree_builder

logger = logging.getLogger(__name__)


# ⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛
# CLASS LocalDiskSubtreeMS
# ⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟

import treelib
from stopwatch import Stopwatch

from model.category import Category
from model.display_node import CategoryNode
from ui.tree.meta_store import BaseMetaStore


class LocalDiskSubtreeMS(BaseMetaStore):
    """Meta store for a subtree on disk
    """
    def __init__(self, tree_id, config, fmeta_tree):
        super().__init__(tree_id, config)

        self._fmeta_tree = fmeta_tree
        """The source tree"""

        self._category_trees: Dict[Category, treelib.Tree] = {}
        """Each entry is lazy-loaded"""

        self._root_level_nodes = []

    @classmethod
    def is_lazy(cls):
        return True

    def get_root_path(self):
        return self._fmeta_tree.root_path

    def get_whole_tree(self):
        # TODO: rename this to prevent confusion. This is not a displayable tree
        return self._fmeta_tree

    # Must return files AND directories
    def get_children(self, parent_id: LocalFsDisplayId):
        if not parent_id:
            # Root level.
            if not self._root_level_nodes:
                self._root_level_nodes = []
                for category in [Category.Added, Category.Deleted, Category.Moved,
                                 Category.Updated, Category.Ignored]:
                    self._root_level_nodes.append(CategoryNode(self._fmeta_tree.root_path, category))
            return self._root_level_nodes

        if parent_id.category == Category.NA:
            raise RuntimeError('Not implemented!')

        children = []
        category_tree: treelib.Tree = self._category_trees.get(parent_id.category, None)
        if not category_tree:
            category_stopwatch = Stopwatch()
            category_tree = category_tree_builder.build_category_tree(self._fmeta_tree, CategoryNode(self._fmeta_tree.root_path, parent_id.category))
            self._category_trees[parent_id.category] = category_tree
            logger.info(f'Tree constructed for "{parent_id.category.name}" in: {category_stopwatch}')

        try:
            for child in category_tree.children(parent_id.id_string):
                children.append(child.data)
        except Exception as err:
            logger.debug(f'CategoryTree for "{self.get_root_path()}": ' + category_tree.show(stdout=False))
            raise

        return children
