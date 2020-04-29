import logging
from abc import ABC, abstractmethod

from model.category import Category
from model.display_node import CategoryNode
from ui.tree import category_tree_builder

logger = logging.getLogger(__name__)


class BaseMetaStore(ABC):
    def __init__(self, tree_id, config):
        self.tree_id = tree_id
        self.config = config

    @abstractmethod
    def get_root_path(self):
        return None

    @abstractmethod
    def get_whole_tree(self):
        return None

    @abstractmethod
    def is_lazy(self):
        return True


class StaticWholeTreeMS(BaseMetaStore):
    """Read-only, not persisted"""
    def __init__(self, tree_id, config, tree):
        super().__init__(tree_id=tree_id, config=config)
        self._fmeta_tree = tree

    def get_root_path(self):
        return self._fmeta_tree.root_path

    def get_whole_tree(self):
        return self._fmeta_tree

    def get_category_trees(self):
        change_trees = []
        for category in [Category.Added,
                         Category.Deleted,
                         Category.Moved,
                         Category.Updated,
                         Category.Ignored]:
            # Build fake tree for category:
            change_tree = category_tree_builder.build_category_tree(self._fmeta_tree, CategoryNode(self.get_root_path(), category))
            change_trees.append(change_tree)
        return change_trees

    @classmethod
    def is_lazy(cls):
        return False


class DummyMS(BaseMetaStore):
    """Just a placeholder with no actual data, to be replaced once data is available"""
    def __init__(self, tree_id, config, root_path):
        super().__init__(tree_id=tree_id, config=config)
        self._root_path = root_path

    def get_root_path(self):
        return self._root_path

    def get_whole_tree(self):
        return None

    @classmethod
    def is_lazy(cls):
        return False
