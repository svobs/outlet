from fmeta import fmeta_tree_cache
from fmeta.fmeta import Category
from fmeta.fmeta_tree_loader import FMetaTreeLoader, TreeMetaScanner
from ui import actions
import logging
from pydispatch import dispatcher
from ui.tree.data_store import BaseStore


logger = logging.getLogger(__name__)


class PersistentFMetaStore(BaseStore):
    def __init__(self, tree_id, config):
        super().__init__(tree_id=tree_id, config=config)
        self.cache = fmeta_tree_cache.from_config(config=self.config, tree_id=self.tree_id)
        self._root_path = self.config.get(self._root_path_config_entry())
        self._fmeta_tree = None

        dispatcher.connect(signal=actions.ROOT_PATH_UPDATED, receiver=self._on_root_path_updated, sender=tree_id)

    def _on_root_path_updated(self, sender, new_root_path):
        if self.get_root_path() != new_root_path:
            # Root changed. Invalidate the current tree contents
            self._fmeta_tree = None
            self.config.write(transient_path=self._root_path_config_entry(), value=new_root_path)
            self._root_path = new_root_path

    def _root_path_config_entry(self):
        return f'transient.{self.tree_id}.root_path'

    def get_root_path(self):
        return self._root_path

    def get_whole_tree(self):
        #  -> loads from primary/cache. Fire 'progress' when loading. root_path_panel
        if self._fmeta_tree is None:
            tree_loader = FMetaTreeLoader(tree_root_path=self._root_path, cache=self.cache, tree_id=self.tree_id)
            self._fmeta_tree = tree_loader.get_current_tree()
        return self._fmeta_tree
