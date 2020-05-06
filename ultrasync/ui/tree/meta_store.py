import logging
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional

import treelib

import file_util
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
    def get_children(self, parent_identifier: Identifier, tree_display_mode: TreeDisplayMode) -> Optional[List[DisplayNode]]:
        """Return the children for the given parent_id.
        The children of the given node can look very different depending on value of 'tree_display_mode'"""
        return None

    def _get_change_category_roots(self):
        # Root level: categories. For use by subclasses
        root_level_nodes = []
        for category in [Category.Added, Category.Deleted, Category.Moved,
                         Category.Updated, Category.Ignored]:
            category_node = CategoryNode(self.get_root_identifier().full_path, category)
            root_level_nodes.append(category_node)
        return root_level_nodes

    def _get_category_children(self, parent_identifier: Identifier):
        """Gets and returns the children for the given parent_identifier, assuming we are displaying category trees.
        If a category tree for the category of the given identifier has not been constructed yet, it will be constructed
        and cached before being returned."""
        children = []
        category_tree: treelib.Tree = self._category_trees.get(parent_identifier.category, None)
        if not category_tree:
            category_stopwatch = Stopwatch()
            category_node = CategoryNode(self.get_root_identifier().full_path, parent_identifier.category)
            category_tree = category_tree_builder.build_category_tree(self.get_model(), category_node)
            self._category_trees[parent_identifier.category] = category_tree
            logger.debug(f'Tree constructed for "{parent_identifier.category.name}" (size {len(category_tree)}) in: {category_stopwatch}')

        try:
            # Need to get relative path for item:
            relative_path = file_util.strip_root(parent_identifier.full_path, self.get_root_identifier().full_path)

            for child in category_tree.children(relative_path):
                children.append(child.data)
        except Exception:
            logger.debug(f'CategoryTree for "{self.get_root_identifier()}": ' + category_tree.show(stdout=False))
            raise

        return children
