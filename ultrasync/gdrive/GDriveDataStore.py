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
        self._root_path = '/' # TODO

    def get_root_path(self):
        return self._root_path

    def get_whole_tree(self):
        return self._gdrive_meta

    def get_children(self, drive_path):
        if drive_path == '/':
            return self._gdrive_meta.roots
        # TODO: may want to rethink lookup by path - lookup by ID may be wiser
        parent_node = self._gdrive_meta.path_dict.get(drive_path, None)
        if not parent_node:
            raise RuntimeError(f'No match for path: "{drive_path}"')

        if not parent_node.is_dir():
            raise RuntimeError(f'Node is not a child (id={parent_node.id}, path="{drive_path}")')

        return self._gdrive_meta.get_children(parent_node.id)


