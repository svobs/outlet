import logging
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional

import treelib

import file_util
from constants import OBJ_TYPE_GDRIVE, OBJ_TYPE_LOCAL_DISK, TreeDisplayMode
from model.category import Category
from model.display_id import DisplayId
from model.display_node import CategoryNode, DisplayNode
from model.subtree_snapshot import SubtreeSnapshot
from stopwatch_sec import Stopwatch
from ui.tree import category_tree_builder

logger = logging.getLogger(__name__)


class BaseMetaStore(ABC):
    def __init__(self, tree_id, config):
        self.tree_id = tree_id
        self.config = config

    @abstractmethod
    def get_root_path(self) -> str:
        return None

    @abstractmethod
    def get_whole_tree(self) -> SubtreeSnapshot:
        return None

    @abstractmethod
    def is_lazy(self):
        return True

    @abstractmethod
    def get_tree_type(self):
        return None

    @abstractmethod
    def get_path_for_item(self, item) -> str:
        raise NotImplementedError


class DummyMS(BaseMetaStore):
    """Just a placeholder with no actual data, to be replaced once data is available"""

    def __init__(self, tree_id, config, root_path: str, tree_type: int):
        super().__init__(tree_id=tree_id, config=config)
        self._root_path = root_path
        self._tree_type = tree_type

    def get_root_path(self):
        return self._root_path

    def get_whole_tree(self):
        return None

    @classmethod
    def is_lazy(cls):
        return False

    def get_tree_type(self):
        return self._tree_type

    def get_path_for_item(self, item) -> str:
        raise NotImplementedError


class LazyMetaStore(BaseMetaStore, ABC):
    def __init__(self, tree_id, config):
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
    def get_children(self, parent_id: DisplayId, tree_display_mode: TreeDisplayMode) -> Optional[List[DisplayNode]]:
        """Return the children for the given parent_id.
        The children of the given node can look very different depending on value of 'tree_display_mode'"""
        return None

    def _get_change_category_roots(self):
        # Root level: categories. For use by subclasses
        root_level_nodes = []
        for category in [Category.Added, Category.Deleted, Category.Moved,
                         Category.Updated, Category.Ignored]:
            root_level_nodes.append(CategoryNode(self.get_root_path(), category))
        return root_level_nodes

    def _get_category_children(self, parent_id: DisplayId):
        children = []
        category_tree: treelib.Tree = self._category_trees.get(parent_id.category, None)
        if not category_tree:
            category_stopwatch = Stopwatch()
            category_tree = category_tree_builder.build_category_tree(self.get_whole_tree(), CategoryNode(self.get_root_path(), parent_id.category))
            self._category_trees[parent_id.category] = category_tree
            logger.info(f'Tree constructed for "{parent_id.category.name}" (size {len(category_tree)}) in: {category_stopwatch}')

        try:
            # Need to get relative path for item:

            relative_path = file_util.strip_root(parent_id.id_string, self.get_root_path())
            if relative_path.startswith('/'):
                logger.debug(f"FUCK! '{parent_id.id_string}' /  '{self.get_root_path()}'")
            for child in category_tree.children(relative_path):
                children.append(child.data)
        except Exception:
            logger.debug(f'CategoryTree for "{self.get_root_path()}": ' + category_tree.show(stdout=False))
            raise

        return children
