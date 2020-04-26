from cache import fmeta_tree_cache
from fmeta.fmeta_tree_loader import FMetaTreeLoader
from ui import actions
import logging
from pydispatch import dispatcher
from ui.tree.meta_store import BaseMetaStore


logger = logging.getLogger(__name__)


class BulkLoadFMetaStore(BaseMetaStore):
    def __init__(self, tree_id, config, root_path):
        super().__init__(tree_id=tree_id, config=config)
        self.cache = fmeta_tree_cache.from_config(config=self.config, tree_id=self.tree_id)
        self._root_path = root_path
        self._fmeta_tree = None

        dispatcher.connect(signal=actions.ROOT_PATH_UPDATED, receiver=self._on_root_path_updated, sender=tree_id)

    def _on_root_path_updated(self, sender, new_root):
        if self.get_root_path() != new_root:
            # Root changed. Invalidate the current tree contents
            self._fmeta_tree = None
            self._root_path = new_root

    def get_root_path(self):
        return self._root_path

    def get_whole_tree(self):
        #  -> loads from primary/cache. Fire 'progress' when loading. root_path_panel
        if self._fmeta_tree is None:
            tree_loader = FMetaTreeLoader(tree_root_path=self._root_path, cache=self.cache, tree_id=self.tree_id)
            self._fmeta_tree = tree_loader.get_current_tree()
        return self._fmeta_tree
