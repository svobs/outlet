import logging
from typing import Any, List, Optional

from constants import OBJ_TYPE_GDRIVE, ROOT, TreeDisplayMode
from model.category import Category
from model.display_id import GDriveIdentifier, Identifier
from model.display_node import DisplayNode
from model.gdrive_tree import GDriveSubtree, GDriveWholeTree
from ui.tree.meta_store import BaseMetaStore, LazyMetaStore

logger = logging.getLogger(__name__)


# CLASS GDriveMS
# ⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟


class GDriveMS(LazyMetaStore):

    def get_root_identifier(self) -> Identifier:
        return self.root_identifier

    def __init__(self, tree_id, config, gdrive_meta, root_identifier: GDriveIdentifier):
        super().__init__(tree_id=tree_id, config=config)
        self._gdrive_meta: GDriveSubtree = gdrive_meta
        self.root_identifier: GDriveIdentifier = root_identifier

    def get_whole_tree(self):
        return self._gdrive_meta

    def get_children_for_root(self, tree_display_mode: TreeDisplayMode) -> Optional[List[DisplayNode]]:
        if tree_display_mode == TreeDisplayMode.CHANGES_ONE_TREE_PER_CATEGORY:
            return self._get_change_category_roots()
        elif tree_display_mode == TreeDisplayMode.ONE_TREE_ALL_ITEMS:
            if isinstance(self._gdrive_meta, GDriveWholeTree):
                # Whole tree? -> return the root nodes
                return self._gdrive_meta.roots
            else:
                # Subtree? -> return the subtree root
                assert isinstance(self._gdrive_meta, GDriveSubtree)
                parent_id = self._gdrive_meta.root_id
        else:
            raise NotImplementedError(f'Not supported: {tree_display_mode}')

    def get_children(self, parent_id: Identifier, tree_display_mode: TreeDisplayMode):
        if parent_id is None or parent_id.full_path == ROOT:
            raise RuntimeError(f'get_children() called for empty parent!')

        if tree_display_mode == TreeDisplayMode.ONE_TREE_ALL_ITEMS:
            return self._gdrive_meta.get_children(parent_id=parent_id)
        elif tree_display_mode == TreeDisplayMode.CHANGES_ONE_TREE_PER_CATEGORY:
            if parent_id.category == Category.NA:
                raise NotImplementedError('Not implemented!')

            return self._get_category_children(parent_id=parent_id)
        else:
            raise NotImplementedError(f'Nope: {tree_display_mode}')

    def get_full_path_for_item(self, item) -> str:
        return self._gdrive_meta.get_full_path_for_item(item)
