import logging

from constants import OBJ_TYPE_GDRIVE
from model.display_id import DisplayId
from ui.tree.meta_store import BaseMetaStore


logger = logging.getLogger(__name__)


# CLASS GDriveMS
# ⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟


class GDriveMS(BaseMetaStore):

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
        elif isinstance(parent_id, DisplayId):
            parent_id = parent_id.id_string

        return self._gdrive_meta.get_children(parent_id=parent_id)

    @classmethod
    def is_lazy(cls):
        return True

    @classmethod
    def get_tree_type(cls):
        return OBJ_TYPE_GDRIVE

