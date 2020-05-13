from typing import List
import logging

from stopwatch import Stopwatch

from constants import OBJ_TYPE_DISPLAY_ONLY, OBJ_TYPE_GDRIVE, OBJ_TYPE_LOCAL_DISK, TreeDisplayMode
from model.display_id import Identifier
from ui.tree import tree_factory_templates
from ui.tree.all_items_tree_builder import AllItemsGDriveTreeBuilder, AllItemsLocalFsTreeBuilder
from ui.tree.category_tree_builder import CategoryTreeBuilder
from model.display_node import DisplayNode
from model.subtree_snapshot import SubtreeSnapshot
from ui.dialog.base_dialog import BaseDialog


import gi

from ui.tree.display_store import DisplayStore

gi.require_version("Gtk", "3.0")
from gi.repository import GLib


logger = logging.getLogger(__name__)


# CLASS TreePanelController
# ⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟


class TreePanelController:
    """
    This class is mostly just a place to hold references in memory of all the disparate components
    required to make a tree panel. Hopefully I will think of ways to refine it more in the future.
    """
    def __init__(self, parent_win, display_store, treeview_meta):
        self.parent_win: BaseDialog = parent_win
        self.tree_builder = None
        self.display_store = display_store
        self.treeview_meta = treeview_meta
        self.tree_view = None
        self.root_dir_panel = None
        self.display_strategy = None
        self.status_bar = None
        self.content_box = None
        self.action_handlers = None

    def init(self):
        """Should be called after all controller components have been wired together"""
        self.treeview_meta.init()
        self.display_strategy.init()
        self.action_handlers.init()

    def load(self):
        self.display_strategy.populate_root()

    def reload(self, new_root=None, new_tree=None, tree_display_mode: TreeDisplayMode = None):
        """Invalidate whatever cache the tree_builder built up, and re-populate the display tree"""
        if new_root:
            logger.debug(f'reload() with new root: {new_root}')
            self.set_tree(root=new_root, tree_display_mode=tree_display_mode)
        elif new_tree:
            logger.debug(f'reload() with new tree')
            self.set_tree(tree=new_tree, tree_display_mode=tree_display_mode)
        else:
            logger.debug(f'reload() with same tree')
            tree = self.tree_builder.get_tree()
            self.set_tree(tree=tree, tree_display_mode=tree_display_mode)
        self.load()

    def rebuild_treeview_with_checkboxes(self, new_root=None, new_tree=None, tree_display_mode: TreeDisplayMode = None):
        """Tear out half the guts here and swap it out, to add checkboxes..."""
        def rebuild_treeview():
            if not self.treeview_meta.has_checkboxes:
                logger.info('Rebuilding treeview!')
                self.action_handlers.disconnect_gtk_listeners()
                self.treeview_meta = self.treeview_meta.but_with_checkboxes()
                self.display_store = DisplayStore(self.treeview_meta)

                new_treeview = tree_factory_templates.build_treeview(self.display_store)
                tree_factory_templates.replace_widget(self.tree_view, new_treeview)
                self.tree_view = new_treeview
                self.action_handlers.init()

            self.reload(new_root=new_root, new_tree=new_tree, tree_display_mode=tree_display_mode)

        GLib.idle_add(rebuild_treeview)

    def get_single_selection(self):
        """Assumes that only one node can be selected at a given time"""
        selection = self.tree_view.get_selection()
        model, tree_paths = selection.get_selected_rows()
        if len(tree_paths) == 1:
            return self.display_store.get_node_data(tree_paths)
        elif len(tree_paths) == 0:
            return None
        else:
            raise Exception(f'Selection has more rows than expected: count={len(tree_paths)}')

    def get_multiple_selection(self) -> List[DisplayNode]:
        """Returns a list of the selected items (empty if none)"""
        selection = self.tree_view.get_selection()
        model, tree_paths = selection.get_selected_rows()
        items = []
        for tree_path in tree_paths:
            item = self.display_store.get_node_data(tree_path)
            items.append(item)
        return items

    def get_checked_rows_as_list(self) -> List[DisplayNode]:
        timer = Stopwatch()
        checked_rows: List[DisplayNode] = self.display_strategy.get_checked_rows_as_list()
        logger.debug(f'{timer} Retreived {len(checked_rows)} checked rows')

        return checked_rows

    def get_tree(self):
        return self.tree_builder.get_tree()

    def set_tree(self, root: Identifier = None, tree: SubtreeSnapshot = None, tree_display_mode: TreeDisplayMode = None):
        # Clear old display (if any)
        GLib.idle_add(self.display_store.clear_model)

        if root:
            tree_type = root.tree_type
        elif tree:
            tree_type = tree.tree_type
        else:
            raise RuntimeError('"root" and "tree" are both empty!')

        if tree_display_mode:
            self.treeview_meta.tree_display_mode = tree_display_mode

        if self.tree_display_mode == TreeDisplayMode.CHANGES_ONE_TREE_PER_CATEGORY:
            tree_builder = CategoryTreeBuilder(controller=self, root=root, tree=tree)
        elif self.tree_display_mode == TreeDisplayMode.ONE_TREE_ALL_ITEMS:
            if tree_type == OBJ_TYPE_GDRIVE:
                tree_builder = AllItemsGDriveTreeBuilder(controller=self, root=root, tree=tree)
            elif tree_type == OBJ_TYPE_LOCAL_DISK:
                tree_builder = AllItemsLocalFsTreeBuilder(controller=self, root=root, tree=tree)
            else:
                raise RuntimeError(f'Unrecognized tree type: {tree_type}')
        else:
            raise RuntimeError(f'Unrecognized value for tree_display_mode: "{self.treeview_meta.tree_display_mode}"')

        self.tree_builder = tree_builder

    # CONVENIENCE METHODS
    # ⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟

    @property
    def cache_manager(self):
        return self.parent_win.application.cache_manager

    @property
    def task_runner(self):
        return self.parent_win.application.task_runner

    @property
    def config(self):
        """Convenience method. Retreives the tree_id from the parent_win"""
        return self.parent_win.config

    @property
    def tree_id(self):
        """Convenience method. Retreives the tree_id from the metastore"""
        return self.treeview_meta.tree_id

    def get_root_identifier(self):
        return self.tree_builder.get_root_identifier()

    @property
    def tree_display_mode(self):
        return self.treeview_meta.tree_display_mode

