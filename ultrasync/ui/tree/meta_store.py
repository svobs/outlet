import logging
from abc import ABC, abstractmethod

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
