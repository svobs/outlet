import logging
from typing import List, Tuple

from pydispatch import dispatcher

from constants import TreeDisplayMode
from model.node.display_node import DisplayNode
from model.node_identifier import NodeIdentifier
from model.display_tree.display_tree import DisplayTree
from util.stopwatch_sec import Stopwatch
from ui import actions
from ui.dialog.base_dialog import BaseDialog
from ui.tree import tree_factory_templates
from ui.tree.display_store import DisplayStore

from ui.tree.display_tree_decorator import LazyLoadDisplayTreeDecorator

import gi
gi.require_version("Gtk", "3.0")
from gi.repository import GLib, Gtk

logger = logging.getLogger(__name__)


# CLASS TreePanelController
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼


class TreePanelController:
    """
    This class is mostly just a place to hold references in memory of all the disparate components
    required to make a tree panel. Hopefully I will think of ways to refine it more in the future.
    """
    def __init__(self, parent_win, display_store, treeview_meta):
        self.parent_win: BaseDialog = parent_win
        self.app = parent_win.application
        self.lazy_tree = None
        self.display_store = display_store
        self.treeview_meta = treeview_meta
        self.tree_id = treeview_meta.tree_id
        """Cached in controller, in case treeview_meta goes away"""
        self.tree_view = None
        self.root_dir_panel = None
        self.display_mutator = None
        self.status_bar = None
        self.content_box = None
        self.user_input_listeners = None
        self.tree_actions = None

    def init(self):
        self._set_column_visibilities()

        """Should be called after all controller components have been wired together"""
        self.treeview_meta.init()
        self.display_mutator.init()
        self.user_input_listeners.init()
        self.tree_actions.init()

        self.app.cache_manager.register_tree_controller(self)

    def destroy(self):
        self.app.cache_manager.unregister_tree_controller(self)

        self.user_input_listeners.disconnect_gtk_listeners()
        self.user_input_listeners = None
        self.tree_actions = None
        self.treeview_meta = None
        del self.display_mutator  # really kill those listeners

        self.lazy_tree = None
        self.display_store = None
        self.root_dir_panel = None
        self.tree_view = None

    def _set_column_visibilities(self):
        # the columns stored in TreeViewMeta are 1
        self.tree_view.get_column(self.treeview_meta.col_num_modify_ts_view).set_visible(self.treeview_meta.show_modify_ts_col)
        self.tree_view.get_column(self.treeview_meta.col_num_change_ts_view).set_visible(self.treeview_meta.show_change_ts_col)
        self.tree_view.get_column(self.treeview_meta.col_num_etc_view).set_visible(self.treeview_meta.show_etc_col)

    def reload(self, new_root=None, new_tree=None, tree_display_mode: TreeDisplayMode = None,
               show_checkboxes: bool = False, hide_checkboxes: bool = False):
        """Invalidate whatever cache the lazy_tree built up, and re-populate the display tree"""
        def _reload():

            checkboxes_visible = self.treeview_meta.has_checkboxes
            if (show_checkboxes and not checkboxes_visible) or (hide_checkboxes and checkboxes_visible):
                # Change in checkbox visibility means tearing out half the guts here and swapping them out...
                logger.info(f'[{self.tree_id}] Rebuilding treeview!')
                checkboxes_visible = not checkboxes_visible
                self.user_input_listeners.disconnect_gtk_listeners()
                self.treeview_meta = self.treeview_meta.but_with_checkboxes(checkboxes_visible)
                self.display_store = DisplayStore(self.treeview_meta)

                assets = self.parent_win.application.assets
                new_treeview = tree_factory_templates.build_treeview(self.display_store, assets)
                tree_factory_templates.replace_widget(self.tree_view, new_treeview)
                self.tree_view = new_treeview
                self.user_input_listeners.init()
                self.treeview_meta.init()
                self._set_column_visibilities()

            if new_root:
                logger.info(f'[{self.tree_id}] reload() with new root: {new_root}')
                self.set_tree(root=new_root, tree_display_mode=tree_display_mode)
            elif new_tree:
                logger.info(f'[{self.tree_id}] reload() with new tree: {new_tree}')
                self.set_tree(tree=new_tree, tree_display_mode=tree_display_mode)
            else:
                logger.info(f'[{self.tree_id}] reload() with same tree')
                tree = self.lazy_tree.get_tree()
                self.set_tree(tree=tree, tree_display_mode=tree_display_mode)

            # Back to the non-UI thread with you!
            dispatcher.send(signal=actions.LOAD_UI_TREE, sender=self.tree_id)

        GLib.idle_add(_reload)

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

    def get_multiple_selection_and_paths(self) -> Tuple[List[DisplayNode], List[Gtk.TreePath]]:
        """Returns a list of the selected items (empty if none)"""
        selection = self.tree_view.get_selection()
        model, tree_paths = selection.get_selected_rows()
        items = []
        for tree_path in tree_paths:
            item = self.display_store.get_node_data(tree_path)
            items.append(item)
        return items, tree_paths

    def get_checked_rows_as_list(self) -> List[DisplayNode]:
        timer = Stopwatch()
        checked_rows: List[DisplayNode] = self.display_mutator.get_checked_rows_as_list()
        logger.debug(f'{timer} Retreived {len(checked_rows)} checked rows')

        return checked_rows

    def get_tree(self) -> DisplayTree:
        return self.lazy_tree.get_tree()

    def set_tree(self, root: NodeIdentifier = None, tree: DisplayTree = None, tree_display_mode: TreeDisplayMode = None):
        # Clear old display (if any)
        GLib.idle_add(self.display_store.clear_model)

        if not root and not tree:
            raise RuntimeError('"root" and "tree" are both empty!')

        if tree_display_mode:
            logger.debug(f'Setting TreeDisplayMode={tree_display_mode.name} for root={root}, tree={tree}')
            self.treeview_meta.tree_display_mode = tree_display_mode

        self.lazy_tree = LazyLoadDisplayTreeDecorator(controller=self, root=root, tree=tree)

    # CONVENIENCE METHODS
    # ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼

    @property
    def cache_manager(self):
        return self.parent_win.application.cache_manager

    @property
    def config(self):
        """Convenience method. Retreives the tree_id from the parent_win"""
        return self.parent_win.config

    def get_root_identifier(self):
        return self.lazy_tree.get_root_identifier()

    @property
    def tree_display_mode(self):
        return self.treeview_meta.tree_display_mode
