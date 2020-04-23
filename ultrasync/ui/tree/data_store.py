import logging
from abc import ABC, abstractmethod

logger = logging.getLogger(__name__)


class BaseStore(ABC):
    def __init__(self, tree_id, config):
        self.tree_id = tree_id
        self.config = config

    @abstractmethod
    def get_root_path(self):
        return None

    @abstractmethod
    def get_whole_tree(self):
        return None


class StaticWholeTreeStore(BaseStore):
    """Read-only, not persisted"""
    def __init__(self, tree_id, config, tree):
        super().__init__(tree_id=tree_id, config=config)
        self._tree = tree

    def get_root_path(self):
        return self._tree.root_path

    def get_whole_tree(self):
        return self._tree


class DisplayStrategy(ABC):
    def __init__(self, controller=None):
        self.con = controller

    @abstractmethod
    def populate_root(self):
        """Draws from the undelying data store as needed, to populate the display store."""
        pass

    @abstractmethod
    def init(self):
        """Do post-wiring stuff like connect listeners."""
        pass
