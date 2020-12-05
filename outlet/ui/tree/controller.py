import logging
from typing import List

from pydispatch import dispatcher

from constants import SUPER_DEBUG, TreeDisplayMode
from diff.change_maker import SPIDNodePair
from model.display_tree.display_tree import DisplayTree
from model.node_identifier import SinglePathNodeIdentifier
from ui import actions
from ui.dialog.base_dialog import BaseDialog
from ui.tree import tree_factory_templates
from ui.tree.display_store import DisplayStore
from util.has_lifecycle import HasLifecycle
from util.stopwatch_sec import Stopwatch

import gi
gi.require_version("Gtk", "3.0")
from gi.repository import GLib

logger = logging.getLogger(__name__)


# CLASS TreePanelController
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼

class TreePanelController(HasLifecycle):
    """
    This class is mostly just a place to hold references in memory of all the disparate components
    required to make a tree panel. Hopefully I will think of ways to refine it more in the future.
    """
    def __init__(self, parent_win, treeview_meta):
        HasLifecycle.__init__(self)
        self.parent_win: BaseDialog = parent_win
        self.app = parent_win.app
        self._display_tree = None
        self.display_store = None
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

    def start(self):
        HasLifecycle.start(self)
        logger.debug(f'[{self.tree_id}] Controller init start')

        """Should be called after all controller components have been wired together"""
        self.treeview_meta.start()
        self.display_mutator.start()
        self.tree_actions.start()
        # Need to start TreeUiListeners AFTER TreeActions... Need a better solution
        self.tree_ui_listeners.start()

        self.app.register_tree_controller(self)

        self._set_column_visibilities()

        logger.info(f'[{self.tree_id}] Controller init done')

    def shutdown(self):
        HasLifecycle.shutdown(self)
        logger.debug(f'[{self.tree_id}] Shutting down controller')

        # This should be received by both frontend and backend
        dispatcher.send(signal=actions.DEREGISTER_DISPLAY_TREE, sender=self.tree_id)

        if self.tree_ui_listeners:
            self.tree_ui_listeners.shutdown()
            self.tree_ui_listeners = None
        if self.tree_actions:
            self.tree_actions.shutdown()
            self.tree_actions = None
        if self.treeview_meta:
            self.treeview_meta.shutdown()
        if self.display_mutator:
            self.display_mutator.shutdown()

        self.root_dir_panel = None

    def _set_column_visibilities(self):
        # the columns stored in TreeViewMeta are 1
        self.tree_view.get_column(self.treeview_meta.col_num_modify_ts_view).set_visible(self.treeview_meta.show_modify_ts_col)
        self.tree_view.get_column(self.treeview_meta.col_num_change_ts_view).set_visible(self.treeview_meta.show_change_ts_col)
        self.tree_view.get_column(self.treeview_meta.col_num_etc_view).set_visible(self.treeview_meta.show_etc_col)

    def reload(self, new_tree=None, tree_display_mode: TreeDisplayMode = None,
               show_checkboxes: bool = False, hide_checkboxes: bool = False):
        """Invalidate whatever cache the ._display_tree built up, and re-populate the display tree"""
        def _reload():

            checkboxes_visible = self.treeview_meta.has_checkboxes
            if (show_checkboxes and not checkboxes_visible) or (hide_checkboxes and checkboxes_visible):
                # Change in checkbox visibility means tearing out half the guts here and swapping them out...
                logger.info(f'[{self.tree_id}] Rebuilding treeview!')
                checkboxes_visible = not checkboxes_visible
                self.tree_ui_listeners.disconnect_gtk_listeners()
                self.treeview_meta = self.treeview_meta.but_with_checkboxes(checkboxes_visible)
                self.display_store = DisplayStore(self)

                assets = self.app.assets
                new_treeview = tree_factory_templates.build_treeview(self.display_store, assets)
                tree_factory_templates.replace_widget(self.tree_view, new_treeview)
                self.tree_view = new_treeview
                self.tree_ui_listeners.start()
                self.treeview_meta.start()
                self._set_column_visibilities()

            if new_tree:
                logger.info(f'[{self.tree_id}] reload() with new tree: {new_tree}')
                self.set_tree(display_tree=new_tree, tree_display_mode=tree_display_mode)
            else:
                logger.info(f'[{self.tree_id}] reload() with same tree')
                self.set_tree(display_tree=self._display_tree, tree_display_mode=tree_display_mode)

            # Send signal to backend to load the subtree. When it's ready, it will notify us
            self.app.backend.start_subtree_load(self.tree_id)

        GLib.idle_add(_reload)

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
        return self._display_tree

    def set_tree(self, display_tree: DisplayTree, tree_display_mode: TreeDisplayMode = None):
        # Clear old GTK3 displayed nodes (if any)
        self.display_store.clear_model_on_ui_thread()

        if tree_display_mode:
            logger.debug(f'[{self.tree_id}] Setting TreeDisplayMode={tree_display_mode.name} for display_tree={display_tree}')
            self.treeview_meta.tree_display_mode = tree_display_mode

        self._display_tree = display_tree

    # CONVENIENCE METHODS
    # ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼

    @property
    def config(self):
        """Convenience method. Retreives the tree_id from the app"""
        return self.app.config

    def get_root_identifier(self) -> SinglePathNodeIdentifier:
        return self._display_tree.get_root_identifier()

    @property
    def tree_display_mode(self):
        return self.treeview_meta.tree_display_mode

