import logging
from typing import Any, Callable, List, Tuple, Union

import gi
from pydispatch import dispatcher

from constants import GDRIVE_PATH_PREFIX, SUPER_DEBUG, TREE_TYPE_GDRIVE, TreeDisplayMode
from diff.change_maker import SPIDNodePair
from model.display_tree.display_tree import DisplayTree
from model.node.node import Node
from model.node_identifier import SinglePathNodeIdentifier
from ui import actions
from ui.dialog.base_dialog import BaseDialog
from ui.tree import tree_factory_templates
from ui.tree.display_store import DisplayStore
from ui.tree.display_tree_decorator import LazyLoadDisplayTreeDecorator
from util.stopwatch_sec import Stopwatch

gi.require_version("Gtk", "3.0")
from gi.repository import GLib, Gtk
from gi.repository.Gtk import TreeIter, TreePath

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
        self.app = parent_win.app
        self._lazy_tree = None
        self.display_store = display_store
        self.treeview_meta = treeview_meta
        self.tree_id: str = treeview_meta.tree_id
        """Cached in controller, in case treeview_meta goes away"""

        self.tree_view = None
        self.root_dir_panel = None
        self.filter_panel = None
        self.display_mutator = None
        self.status_bar = None
        self.content_box = None

        self.tree_ui_listeners = None
        self.tree_actions = None

    def init(self):
        logger.debug(f'[{self.tree_id}] Controller init')
        self._set_column_visibilities()

        """Should be called after all controller components have been wired together"""
        self.treeview_meta.init()
        self.display_mutator.start()
        self.tree_actions.start()
        # Need to start TreeUiListeners AFTER TreeActions... Need a better solution
        self.tree_ui_listeners.init()

        self.app.cacheman.register_tree_controller(self)

    def destroy(self):
        logger.debug(f'[{self.tree_id}] Destroying controller')
        if self.app.cacheman:
            self.app.cacheman.unregister_tree_controller(self)

        if self.tree_ui_listeners:
            self.tree_ui_listeners.disconnect_gtk_listeners()
        self.tree_ui_listeners = None
        self.tree_actions = None
        self.treeview_meta.destroy()

        self.display_mutator.shutdown()

        self.root_dir_panel = None

    def _set_column_visibilities(self):
        # the columns stored in TreeViewMeta are 1
        self.tree_view.get_column(self.treeview_meta.col_num_modify_ts_view).set_visible(self.treeview_meta.show_modify_ts_col)
        self.tree_view.get_column(self.treeview_meta.col_num_change_ts_view).set_visible(self.treeview_meta.show_change_ts_col)
        self.tree_view.get_column(self.treeview_meta.col_num_etc_view).set_visible(self.treeview_meta.show_etc_col)

    def reload(self, new_root: SinglePathNodeIdentifier = None, new_tree=None, tree_display_mode: TreeDisplayMode = None,
               show_checkboxes: bool = False, hide_checkboxes: bool = False):
        """Invalidate whatever cache the _lazy_tree built up, and re-populate the display tree"""
        def _reload():

            checkboxes_visible = self.treeview_meta.has_checkboxes
            if (show_checkboxes and not checkboxes_visible) or (hide_checkboxes and checkboxes_visible):
                # Change in checkbox visibility means tearing out half the guts here and swapping them out...
                logger.info(f'[{self.tree_id}] Rebuilding treeview!')
                checkboxes_visible = not checkboxes_visible
                self.tree_ui_listeners.disconnect_gtk_listeners()
                self.treeview_meta = self.treeview_meta.but_with_checkboxes(checkboxes_visible)
                self.display_store = DisplayStore(self.treeview_meta)

                assets = self.parent_win.app.assets
                new_treeview = tree_factory_templates.build_treeview(self.display_store, assets)
                tree_factory_templates.replace_widget(self.tree_view, new_treeview)
                self.tree_view = new_treeview
                self.tree_ui_listeners.init()
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
                tree = self._lazy_tree.get_tree()
                self.set_tree(tree=tree, tree_display_mode=tree_display_mode)

            # Back to the non-UI thread with you!
            dispatcher.send(signal=actions.LOAD_UI_TREE, sender=self.tree_id)

        GLib.idle_add(_reload)

    def build_spid_from_tree_path(self, tree_path: Gtk.TreePath) -> SinglePathNodeIdentifier:
        node = self.display_store.get_node_data(tree_path)
        single_path = self.derive_single_path_from_tree_path(tree_path)
        return SinglePathNodeIdentifier(uid=node.uid, path_list=single_path, tree_type=node.get_tree_type())

    def build_sn_from_tree_path(self, tree_path: Union[TreeIter, TreePath]) -> SPIDNodePair:
        node = self.display_store.get_node_data(tree_path)
        single_path = self.derive_single_path_from_tree_path(tree_path)
        spid = SinglePathNodeIdentifier(uid=node.uid, path_list=single_path, tree_type=node.get_tree_type())
        return SPIDNodePair(spid, node)

    def derive_single_path_from_tree_path(self, tree_path: Gtk.TreePath, include_gdrive_prefix: bool = False) -> str:
        """Travels up the display tree and constructs a single path for the given node.
        Some background: while a node can have several paths associated with it, each display tree node can only be associated
        with a single path - but that information is implicit in the tree structure itself and must be reconstructed from it."""
        tree_path = self.display_store.ensure_tree_path(tree_path)

        # don't mess up the caller; make a copy before modifying:
        tree_path_copy = tree_path.copy()

        node = self.display_store.get_node_data(tree_path_copy)
        is_gdrive: bool = node.get_tree_type() == TREE_TYPE_GDRIVE

        single_path = ''

        while True:
            node = self.display_store.get_node_data(tree_path_copy)
            single_path = f'/{node.name}{single_path}'
            # Go up the tree, one level per loop,
            # with each node updating itself based on its immediate children
            tree_path_copy.up()
            if tree_path_copy.get_depth() < 1:
                # Stop at root
                break

        base_path = self.get_tree().root_identifier.get_single_path()
        if base_path != '/':
            single_path = f'{base_path}{single_path}'

        if is_gdrive and include_gdrive_prefix:
            single_path = f'{GDRIVE_PATH_PREFIX}{single_path}'
        logger.debug(f'derive_single_path_from_tree_path(): derived path: {single_path}')
        return single_path

    def _execute_on_current_single_selection(self, action_func: Callable[[Gtk.TreePath], Any]):
        """Assumes that only one node can be selected at a given time"""
        selection = self.tree_view.get_selection()
        model, tree_path_list = selection.get_selected_rows()
        if len(tree_path_list) == 1:
            tree_path = tree_path_list[0]
            return action_func(tree_path)
        elif len(tree_path_list) == 0:
            return None
        else:
            raise Exception(f'Selection has more rows than expected: count={len(tree_path_list)}')

    def get_single_selection_display_identifier(self) -> SinglePathNodeIdentifier:
        return self._execute_on_current_single_selection(self.build_spid_from_tree_path)

    def get_single_selection(self) -> Node:
        return self._execute_on_current_single_selection(lambda tp: self.display_store.get_node_data(tp))

    def get_multiple_selection(self) -> List[Node]:
        """Returns a list of the selected items (empty if none)"""
        selection = self.tree_view.get_selection()
        model, tree_paths = selection.get_selected_rows()
        items = []
        for tree_path in tree_paths:
            item = self.display_store.get_node_data(tree_path)
            items.append(item)
        return items

    def get_multiple_selection_sn_list(self) -> List[SPIDNodePair]:
        """Returns a list of the selected items (empty if none)"""
        selection = self.tree_view.get_selection()
        model, tree_paths = selection.get_selected_rows()
        sn_list = []
        for tree_path in tree_paths:
            sn_list.append(self.build_sn_from_tree_path(tree_path))
        return sn_list

    def get_multiple_selection_and_paths(self) -> Tuple[List[Node], List[Gtk.TreePath]]:
        """Returns a list of the selected items (empty if none)"""
        selection = self.tree_view.get_selection()
        model, tree_paths = selection.get_selected_rows()
        items = []
        for tree_path in tree_paths:
            item = self.display_store.get_node_data(tree_path)
            items.append(item)
        return items, tree_paths

    def get_checked_rows_as_list(self) -> List[SPIDNodePair]:
        timer = Stopwatch()
        checked_rows: List[SPIDNodePair] = self.display_mutator.get_checked_rows_as_list()
        if SUPER_DEBUG:
            more = ': ' + ', '.join([str(sn.spid.uid) for sn in checked_rows])
        else:
            more = ''
        logger.debug(f'[{self.tree_id}] {timer} Retreived {len(checked_rows)} checked rows{more}')

        return checked_rows

    def get_tree(self) -> DisplayTree:
        return self._lazy_tree.get_tree()

    def set_tree(self, root: SinglePathNodeIdentifier = None, tree: DisplayTree = None, tree_display_mode: TreeDisplayMode = None):
        # Clear old display (if any)
        GLib.idle_add(self.display_store.clear_model)

        if not root and not tree:
            raise RuntimeError('"root" and "tree" are both empty!')

        if tree_display_mode:
            logger.debug(f'[{self.tree_id}] Setting TreeDisplayMode={tree_display_mode.name} for root={root}, tree={tree}')
            self.treeview_meta.tree_display_mode = tree_display_mode

        self._lazy_tree = LazyLoadDisplayTreeDecorator(controller=self, root_identifier=root, tree=tree)

    # CONVENIENCE METHODS
    # ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼

    @property
    def cacheman(self):
        return self.parent_win.app.cacheman

    @property
    def config(self):
        """Convenience method. Retreives the tree_id from the parent_win"""
        return self.parent_win.config

    def get_root_identifier(self) -> SinglePathNodeIdentifier:
        return self._lazy_tree.get_root_identifier()

    @property
    def tree_display_mode(self):
        return self.treeview_meta.tree_display_mode

