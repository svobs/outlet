import logging
from typing import List
from pydispatch import dispatcher

from constants import SUPER_DEBUG
from model.display_tree.display_tree import DisplayTree
from model.node.node import SPIDNodePair
from model.node_identifier import GUID, SinglePathNodeIdentifier
from ui.gtk.dialog.base_dialog import BaseDialog
from signal_constants import Signal
from ui.gtk.tree import tree_factory_templates
from ui.gtk.tree.display_mutator import DisplayMutator
from ui.gtk.tree.display_store import DisplayStore
from ui.gtk.tree.tree_actions import TreeActions
from ui.gtk.tree.ui_listeners import TreeUiListeners
from util.has_lifecycle import HasLifecycle
from util.stopwatch_sec import Stopwatch

import gi
gi.require_version("Gtk", "3.0")
from gi.repository import GLib

logger = logging.getLogger(__name__)


class TreePanelController(HasLifecycle):
    """
    ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
    CLASS TreePanelController

    This class is mostly just a place to hold references in memory of all the disparate components
    required to make a tree panel. Hopefully I will think of ways to refine it more in the future.
    ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼
    """

    def __init__(self, parent_win, display_tree, treeview_meta):
        HasLifecycle.__init__(self)
        self.parent_win: BaseDialog = parent_win
        self.app = parent_win.app

        self._display_tree: DisplayTree = display_tree

        self.display_mutator = DisplayMutator(controller=self)
        self.treeview_meta = treeview_meta
        self.display_store = DisplayStore(self, treeview_meta)
        """Cached in controller, in case treeview_meta goes away"""

        # UI components
        self.tree_view = None
        self.root_dir_panel = None
        self.filter_panel = None
        self.status_bar = None
        self.status_bar_container = None
        self.content_box = None

        self.tree_ui_listeners = TreeUiListeners(controller=self)
        self.tree_actions = TreeActions(controller=self)

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
        dispatcher.send(signal=Signal.DEREGISTER_DISPLAY_TREE, sender=self.tree_id)

        if self.root_dir_panel:
            self.root_dir_panel.shutdown()
            self.root_dir_panel = None
        if self.tree_ui_listeners:
            self.tree_ui_listeners.shutdown()
            self.tree_ui_listeners = None
        if self.tree_actions:
            self.tree_actions.shutdown()
            self.tree_actions = None
        if self.display_mutator:
            self.display_mutator.shutdown()
        if self.treeview_meta:
            self.treeview_meta.shutdown()
        self.tree_view = None

    @property
    def tree_id(self) -> str:
        return self.treeview_meta.tree_id

    def _set_column_visibilities(self):
        # the columns stored in TreeViewMeta are 1
        self.tree_view.get_column(self.treeview_meta.col_num_modify_ts_view).set_visible(self.treeview_meta.show_modify_ts_col)
        self.tree_view.get_column(self.treeview_meta.col_num_change_ts_view).set_visible(self.treeview_meta.show_change_ts_col)
        self.tree_view.get_column(self.treeview_meta.col_num_etc_view).set_visible(self.treeview_meta.show_etc_col)

    def reload(self, new_tree: DisplayTree = None):
        """Invalidate whatever cache the ._display_tree built up, and re-populate the display tree"""

        def _reload():

            # 1. TEAR DOWN:
            # Change in checkbox visibility means tearing out half the guts here and swapping them out...
            old_tree_id = self.tree_id
            logger.info(f'[{old_tree_id}] Rebuilding treeview!')
            self.tree_ui_listeners.disconnect_gtk_listeners()
            self.tree_actions.shutdown()

            if new_tree:
                logger.info(f'[{old_tree_id}] reload() with new tree: {new_tree}')

                if new_tree.state.tree_display_mode != self.tree_display_mode:
                    logger.info(f'[{old_tree_id}] Looks like we are changing tree display mode. Clearing selection.')
                    # Changing to/from ChangeTree.
                    # Selection can almost certainly not be retained, and will probably cause errors. Just unselect everything for now:
                    selection = self.tree_view.get_selection()
                    selection.unselect_all()

                self.set_tree(display_tree=new_tree)
                new_tree_id = new_tree.tree_id
                has_checkboxes = new_tree.state.has_checkboxes
            else:
                logger.info(f'[{self.tree_id}] reload() with same tree')
                self.set_tree(display_tree=self._display_tree)
                new_tree_id = self._display_tree.tree_id
                has_checkboxes = self.treeview_meta.has_checkboxes

            # 2. REBUILD:
            if old_tree_id != new_tree_id:
                logger.debug(f'Changing tree_id from "{old_tree_id}" to "{new_tree_id}"')
            self.treeview_meta = self.treeview_meta.but_with_checkboxes(has_checkboxes, new_tree_id)
            self.display_store = DisplayStore(self, self.treeview_meta)

            new_treeview = tree_factory_templates.build_treeview(self.display_store, self.app.assets)
            tree_factory_templates.replace_widget(self.tree_view, new_treeview)
            self.tree_view = new_treeview
            self.tree_ui_listeners.connect_gtk_listeners()
            self.treeview_meta.start()
            self._set_column_visibilities()
            self.tree_actions = TreeActions(controller=self)

            assert self.treeview_meta.tree_id == self.get_tree().tree_id, f'tree_id from treeview_meta ({self.treeview_meta.tree_id})' \
                                                                          f' does not match tree ({self.get_tree().tree_id})'

            # Send signal to backend to load the subtree. When it's ready, it will notify us via the LOAD_SUBTREE_DONE signal
            self.backend.start_subtree_load(self.tree_id)

        GLib.idle_add(_reload)

    def generate_checked_row_list(self) -> List[GUID]:
        timer = Stopwatch()
        checked_sn_list: List[SPIDNodePair] = self.display_mutator.generate_checked_row_list()
        checked_guid_list: List[GUID] = [sn.spid.guid for sn in checked_sn_list]
        if SUPER_DEBUG:
            more = ': ' + ', '.join(checked_guid_list)
        else:
            more = ''
        logger.debug(f'[{self.tree_id}] {timer} Retreived {len(checked_sn_list)} checked items{more}')

        return checked_guid_list

    @property
    def backend(self):
        return self.app.backend

    def get_tree(self) -> DisplayTree:
        return self._display_tree

    def set_tree(self, display_tree: DisplayTree):
        # Clear old GTK3 displayed nodes (if any)
        self.display_store.clear_model_on_ui_thread()
        self.treeview_meta.tree_display_mode = display_tree.state.tree_display_mode

        self._display_tree = display_tree

    # CONVENIENCE METHODS
    # ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼

    def get_root_spid(self) -> SinglePathNodeIdentifier:
        return self._display_tree.get_root_spid()

    @property
    def tree_display_mode(self):
        return self.treeview_meta.tree_display_mode
