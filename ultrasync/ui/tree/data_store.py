import logging


logger = logging.getLogger(__name__)


# TODO: make abstract base class
class BaseStore:
    def __init__(self, tree_id, config):
        self.tree_id = tree_id
        self.config = config

    def get_root_path(self):
        return None

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
