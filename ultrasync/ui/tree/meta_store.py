import logging
from abc import ABC, abstractmethod

from constants import OBJ_TYPE_GDRIVE, OBJ_TYPE_LOCAL_DISK

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

    @abstractmethod
    def get_tree_type(self):
        return None


class DummyMS(BaseMetaStore):
    """Just a placeholder with no actual data, to be replaced once data is available"""
    def __init__(self, tree_id, config, root_path: str):
        super().__init__(tree_id=tree_id, config=config)
        self._root_path = root_path

    def get_root_path(self):
        return self._root_path

    def get_whole_tree(self):
        return None

    @classmethod
    def is_lazy(cls):
        return False

    def get_tree_type(self):
        # Not the greatest logic here...
        if self._root_path.find('/') >= 0:
            return OBJ_TYPE_LOCAL_DISK
        else:
            return OBJ_TYPE_GDRIVE
