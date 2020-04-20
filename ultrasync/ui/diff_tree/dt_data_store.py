from fmeta import fmeta_tree_cache
from fmeta.fmeta import Category
from fmeta.fmeta_tree_loader import FMetaTreeLoader, TreeMetaScanner
from ui import actions
import logging
from pydispatch import dispatcher


logger = logging.getLogger(__name__)


class SimpleDataStore:
    def __init__(self, tree_id, fmeta_tree):
        self.tree_id = tree_id
        self._fmeta_tree = fmeta_tree
        self.editable = False

    def get_root_path(self):
        return self._fmeta_tree.root_path

    def set_root_path(self, new_root_path):
        if self._fmeta_tree.root_path != new_root_path:
            raise RuntimeError('Root path cannot be changed for this tree!')

    def get_fmeta_tree(self):
        return self._fmeta_tree

    def is_category_node_expanded(self, category):
        if category == Category.Ignored:
            return False
        return True

    def set_category_node_expanded_state(self, category, is_expanded):
        pass


class DtConfigFileStore(SimpleDataStore):
    def __init__(self, config, tree_id, editable):
        super().__init__(tree_id=tree_id, fmeta_tree=None)
        self.config = config
        self.tree_id = tree_id
        """If false, hide checkboxes and tree root change button"""
        self.editable = editable
        self.cache = fmeta_tree_cache.from_config(config=self.config, tree_id=self.tree_id)
        self._root_path = self.config.get(self._root_path_config_entry())

        dispatcher.connect(signal=actions.ROOT_PATH_UPDATED, receiver=self._on_root_path_updated, sender=tree_id)

    def _on_root_path_updated(self, sender, new_root):
        self.set_root_path(new_root)

    def _root_path_config_entry(self):
        return f'transient.{self.tree_id}.root_path'

    def get_root_path(self):
        return self._root_path

    def set_root_path(self, new_root_path):
        if self.get_root_path() != new_root_path:
            # Root changed. Invalidate the current tree contents
            self._fmeta_tree = None
            self.config.write(transient_path=self._root_path_config_entry(), value=new_root_path)
            self._root_path = new_root_path

    def get_fmeta_tree(self):
        #  -> loads from primary/cache. Fire 'progress' when loading. root_path_panel
        if self._fmeta_tree is None:
            tree_loader = FMetaTreeLoader(tree_root_path=self._root_path, cache=self.cache, tree_id=self.tree_id)
            self._fmeta_tree = tree_loader.get_current_tree()
        return self._fmeta_tree

    def is_category_node_expanded(self, category):
        cfg_path = f'transient.{self.tree_id}.expanded_state.{category.name}'
        return self.config.get(cfg_path, True)

    def set_category_node_expanded_state(self, category, is_expanded):
        cfg_path = f'transient.{self.tree_id}.expanded_state.{category.name}'
        self.config.write(cfg_path, is_expanded)
