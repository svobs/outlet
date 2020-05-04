import logging
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional

import treelib

from constants import TreeDisplayMode
from model.category import Category
from model.display_id import Identifier
from model.display_node import CategoryNode, DisplayNode
from model.subtree_snapshot import SubtreeSnapshot
from stopwatch_sec import Stopwatch
from ui.tree import category_tree_builder

logger = logging.getLogger(__name__)


class BaseMetaStore(ABC):
    def __init__(self, tree_id: str, config):
        self.tree_id = tree_id
        self.config = config

    @abstractmethod
    def get_root_identifier(self) -> Identifier:
        pass

    @abstractmethod
    def get_model(self) -> SubtreeSnapshot:
        pass

    @abstractmethod
    def is_lazy(self):
        return True

    @abstractmethod
    def get_full_path_for_item(self, item) -> str:
        raise NotImplementedError


class DummyMS(BaseMetaStore):
    """Just a placeholder with no actual data, to be replaced once data is available"""

    def __init__(self, tree_id: str, config, root: Identifier):
        super().__init__(tree_id=tree_id, config=config)
        self._root_identifier = root

    def get_root_identifier(self) -> Identifier:
        return self._root_identifier

    def get_model(self):
        return None

    @classmethod
    def is_lazy(cls):
        return False

    def get_full_path_for_item(self, item) -> str:
        raise NotImplementedError


class LazyMetaStore(BaseMetaStore, ABC):
    def __init__(self, tree_id: str, config):
        super().__init__(tree_id, config)

        self._category_trees: Dict[Category, treelib.Tree] = {}
        """Each entry is lazy-loaded"""

    @classmethod
    def is_lazy(cls):
        return True

    @abstractmethod
    def get_children_for_root(self, tree_display_mode: TreeDisplayMode) -> Optional[List[DisplayNode]]:
        pass

    @abstractmethod
    def get_children(self, parent_id: Identifier, tree_display_mode: TreeDisplayMode) -> Optional[List[DisplayNode]]:
        """Return the children for the given parent_id.
        The children of the given node can look very different depending on value of 'tree_display_mode'"""
        return None

    def _get_change_category_roots(self):
        # Root level: categories. For use by subclasses
        root_level_nodes = []
        for category in [Category.Added, Category.Deleted, Category.Moved,
                         Category.Updated, Category.Ignored]:
            category_node = CategoryNode(self.get_root_identifier().uid, category)
            root_level_nodes.append(category_node)
        return root_level_nodes

    def _get_category_children(self, parent_id: Identifier):
        children = []
        category_tree: treelib.Tree = self._category_trees.get(parent_id.category, None)
        if not category_tree:
            category_stopwatch = Stopwatch()
            category_node = CategoryNode(self.get_root_identifier().uid, parent_id.category)
            category_tree = category_tree_builder.build_category_tree(self.get_model(), category_node)
            self._category_trees[parent_id.category] = category_tree
            logger.info(f'Tree constructed for "{parent_id.category.name}" (size {len(category_tree)}) in: {category_stopwatch}')

        try:
            # Need to get relative path for item:

            for child in category_tree.children(parent_id.uid):
                children.append(child.data)
        except Exception:
            logger.debug(f'CategoryTree for "{self.get_root_identifier()}": ' + category_tree.show(stdout=False))
            raise

        return children
