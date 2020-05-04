import logging

from constants import OBJ_TYPE_GDRIVE, ROOT
from model.display_id import DisplayId
from model.gdrive_tree import GDriveSubtree, GDriveWholeTree
from ui.tree.meta_store import BaseMetaStore


logger = logging.getLogger(__name__)


# CLASS GDriveMS
# ⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟


class GDriveMS(BaseMetaStore):

    def __init__(self, tree_id, config, gdrive_meta, root_path):
        super().__init__(tree_id=tree_id, config=config)
        self._gdrive_meta: GDriveSubtree = gdrive_meta
        self._root_id = root_path

    def get_root_path(self):
        return self._root_id

    def get_whole_tree(self):
        return self._gdrive_meta

    def get_children(self, parent_id):
        if parent_id is None or parent_id == ROOT:
            if isinstance(self._gdrive_meta, GDriveWholeTree):
                # Whole tree? -> return the root nodes
                return self._gdrive_meta.roots
            else:
                # Subtree? -> return the subtree root
                assert isinstance(self._gdrive_meta, GDriveSubtree)
                parent_id = self._gdrive_meta.root_id
        elif isinstance(parent_id, DisplayId):
            parent_id = parent_id.id_string

        return self._gdrive_meta.get_children(parent_id=parent_id)

    @classmethod
    def is_lazy(cls):
        return True

    @classmethod
    def get_tree_type(cls):
        return OBJ_TYPE_GDRIVE

    def get_path_for_item(self, item) -> str:
        return self._gdrive_meta.get_path_for_item(item)

