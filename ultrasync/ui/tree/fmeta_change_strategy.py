"""
Populates the DiffTreePanel's model from a given FMetaTree.
"""
import logging
import os

import gi
import treelib

import file_util
from constants import TreeDisplayMode
from ui.tree.category_tree_builder import CategoryTreeBuilder

gi.require_version("Gtk", "3.0")
from gi.repository import GLib

from fmeta.fmeta_tree_scanner import TreeMetaScanner

from ui import actions
from ui.tree.lazy_display_strategy import LazyDisplayStrategy


logger = logging.getLogger(__name__)


class FMetaChangeTreeStrategy(LazyDisplayStrategy):
    def __init__(self, config, controller=None):
        super().__init__(config, controller)

    def init(self):
        super().init()

    def _append_full_category_tree_to_model(self, category_tree: treelib.Tree):
        def append_recursively(parent_iter, parent_uid, node: treelib.Node):
            # Do a DFS of the change tree and populate the UI tree along the way
            if node.data.is_dir():
                parent_iter = self._append_dir_node(parent_iter=parent_iter, parent_uid=parent_uid, node_data=node.data)

                child_relative_path = file_util.strip_root(node.data.full_path, self.con.tree_builder.get_root_identifier().full_path)

                for child in category_tree.children(child_relative_path):
                    append_recursively(parent_iter, parent_uid, child)
            else:
                self._append_file_node(parent_iter, parent_uid, node.data)

        if category_tree.size(1) > 0:
            # logger.debug(f'Appending category: {category.name}')
            root_node: treelib.Node = category_tree.get_node(category_tree.root)
            append_recursively(None, None, root_node)

    def populate_root(self):
        logger.debug(f'Repopulating tree "{self.con.treeview_meta.tree_id}"')
        assert not self.con.treeview_meta.lazy_load

        # All at once
        assert isinstance(self.con.tree_builder, CategoryTreeBuilder)
        category_trees = self.con.tree_builder.get_category_trees_static()

        def update_ui():
            # Wipe out existing items:
            self.con.display_store.clear_model()

            for category_tree in category_trees:
                self._append_full_category_tree_to_model(category_tree)

            # Restore user prefs for expanded nodes:
            self._set_expand_states_from_config()
            logger.debug(f'Done repopulating diff tree "{self.con.treeview_meta.tree_id}"')

        GLib.idle_add(update_ui)

        # Show tree summary:
        actions.set_status(sender=self.con.treeview_meta.tree_id,
                           status_msg=self.con.get_tree().get_summary())
