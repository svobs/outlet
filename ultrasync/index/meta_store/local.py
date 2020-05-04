import logging
from typing import List, Optional

from constants import OBJ_TYPE_LOCAL_DISK, ROOT, TreeDisplayMode
from model.fmeta import LocalFsDisplayId
from model.fmeta_tree import FMetaTree

logger = logging.getLogger(__name__)

# ⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛
# CLASS LocalDiskSubtreeMS
# ⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟

from model.category import Category
from model.display_node import DisplayNode
from ui.tree.meta_store import LazyMetaStore


# TODO: rename file to local_lazy
class LocalDiskSubtreeMS(LazyMetaStore):
    """Meta store for a subtree on disk
    """

    def __init__(self, tree_id, config, fmeta_tree):
        super().__init__(tree_id, config)

        self._fmeta_tree: FMetaTree = fmeta_tree
        """The source tree"""

    def get_root_path(self):
        return self._fmeta_tree.root_path

    def get_whole_tree(self):
        # TODO: rename this to prevent confusion. This is not a displayable tree
        return self._fmeta_tree

    def get_children_for_root(self, tree_display_mode: TreeDisplayMode) -> Optional[List[DisplayNode]]:
        if tree_display_mode == TreeDisplayMode.CHANGES_ONE_TREE_PER_CATEGORY:
            return self._get_change_category_roots()
        else:
            raise NotImplementedError(f'Not supported: {tree_display_mode}')

    # Must return files AND directories
    def get_children(self, parent_id: LocalFsDisplayId, tree_display_mode: TreeDisplayMode):
        if parent_id is None or parent_id == ROOT:
            raise RuntimeError(f'get_children() called for empty parent!')

        if tree_display_mode == TreeDisplayMode.CHANGES_ONE_TREE_PER_CATEGORY:
            if parent_id.category == Category.NA:
                raise NotImplementedError('Not implemented!')

            return self._get_category_children(parent_id=parent_id)
        else:
            raise NotImplementedError(f'Cannot do this: {tree_display_mode}')

    @classmethod
    def get_tree_type(cls):
        return OBJ_TYPE_LOCAL_DISK

    def get_path_for_item(self, item) -> str:
        return self._fmeta_tree.get_path_for_item(item)
