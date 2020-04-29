import logging
from ui.tree.meta_store import BaseMetaStore


logger = logging.getLogger(__name__)


# TODO: need more metadata about each dir: need to know if we have *all* the children
class GDriveDataStore(BaseMetaStore):
    @classmethod
    def is_lazy(cls):
        return True

    def __init__(self, tree_id, config, gdrive_meta, root_path):
        super().__init__(tree_id=tree_id, config=config)
        self._gdrive_meta = gdrive_meta
        self._root_path = root_path

    def get_root_path(self):
        return self._root_path

    def get_whole_tree(self):
        return self._gdrive_meta

    def get_children(self, parent_id):
        if parent_id is None:
            return self._gdrive_meta.roots

        return self._gdrive_meta.get_children(parent_id=parent_id)


