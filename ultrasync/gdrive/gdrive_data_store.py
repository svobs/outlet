from fmeta import fmeta_tree_cache
from fmeta.fmeta import Category
from fmeta.fmeta_tree_loader import FMetaTreeLoader, TreeMetaScanner
from ui import actions
import logging
from pydispatch import dispatcher
from ui.tree.data_store import BaseStore


logger = logging.getLogger(__name__)


class GDriveDataStore(BaseStore):
    def __init__(self, tree_id, config, gdrive_meta):
        super().__init__(tree_id=tree_id, config=config)
        self._gdrive_meta = gdrive_meta
        self._root_path = '/'  # TODO

    def get_root_path(self):
        return self._root_path

    def get_whole_tree(self):
        return self._gdrive_meta

    def get_children(self, parent_id):
        if parent_id is None:
            return self._gdrive_meta.roots

        return self._gdrive_meta.get_children(parent_id=parent_id)


