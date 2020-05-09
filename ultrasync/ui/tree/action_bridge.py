

import logging
from typing import List, Optional

from pydispatch import dispatcher

import ui.actions as actions
from model.display_id import Identifier
from model.display_node import DisplayNode
from model.fmeta import FMeta

import gi
gi.require_version("Gtk", "3.0")
from gi.repository import GLib, Gdk, Gtk

logger = logging.getLogger(__name__)

# CLASS TreeActionBridge
# ⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟


class TreeActionBridge:
    def __init__(self, config, controller):
        self.con = controller
        self.ui_enabled = True
        self.connected_eids = []

    def init(self):
        actions.connect(actions.TOGGLE_UI_ENABLEMENT, self._on_enable_ui_toggled)

        targeted_signals = []
        general_signals = [actions.TOGGLE_UI_ENABLEMENT]

        if self.con.cache_manager.reload_tree_on_root_path_update:
            dispatcher.connect(signal=actions.ROOT_PATH_UPDATED, receiver=self._on_root_path_updated, sender=self.con.tree_id)
            targeted_signals.append(actions.ROOT_PATH_UPDATED)

            if self.con.cache_manager.load_all_caches_on_startup:
                # IMPORTANT: Need both options to be enabled for this tree to be loaded automatically!
                actions.connect(signal=actions.LOAD_ALL_CACHES_DONE, handler=self._after_all_caches_loaded)
                general_signals.append(actions.LOAD_ALL_CACHES_DONE)

        # Status bar
        actions.connect(signal=actions.SET_STATUS, handler=self._on_set_status, sender=self.con.tree_id)
        targeted_signals.append(actions.SET_STATUS)

        logger.debug(f'Listening for signals: Any={general_signals}, "{self.con.tree_id}"={targeted_signals}')

        # TreeView
        eid = self.con.tree_view.connect("row-activated", self._on_row_activated, self.con.tree_id)
        self.connected_eids.append(eid)
        eid = self.con.tree_view.connect('button-press-event', self._on_tree_button_press, self.con.tree_id)
        self.connected_eids.append(eid)
        eid = self.con.tree_view.connect('key-press-event', self._on_key_press, self.con.tree_id)
        self.connected_eids.append(eid)
        eid = self.con.tree_view.connect('row-expanded', self._on_toggle_gtk_row_expanded_state, True)
        self.connected_eids.append(eid)
        eid = self.con.tree_view.connect('row-collapsed', self._on_toggle_gtk_row_expanded_state, False)
        self.connected_eids.append(eid)
        # select.connect("changed", self._on_tree_selection_changed)

    def disconnect_gtk_listeners(self):
        for eid in self.connected_eids:
            self.con.tree_view.disconnect(eid)

    # LISTENERS begin
    # ⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟

    def _on_root_path_updated(self, sender, new_root: Identifier):
        logger.debug(f'Received signal: "{actions.ROOT_PATH_UPDATED}"')

        # Reload subtree and refresh display
        if self.con.config.get('cache.load_cache_when_tree_root_selected'):
            logger.debug(f'Got new root. Reloading subtree for: {new_root}')
            # Loads from disk if necessary:
            self.con.reload(new_root)
        else:
            # Just wipe out the old root
            self.con.set_tree(root=new_root)

    def _after_all_caches_loaded(self, sender):
        logger.debug(f'Received signal: "{actions.LOAD_ALL_CACHES_DONE}"')

        try:
            # Reload subtree and refresh display
            self.con.load()
        except RuntimeError as err:
            # TODO: custom exceptions
            logger.warning(f'Failed to load cache for "{self.con.tree_id}": {repr(err)}')

    # Remember, use member functions instead of lambdas, because PyDispatcher will remove refs
    def _on_set_status(self, sender, status_msg):
        GLib.idle_add(lambda: self.con.status_bar.set_label(status_msg))

    def _on_enable_ui_toggled(self, sender, enable):
        # Enable/disable listeners:
        self.ui_enabled = enable

    def _on_tree_selection_changed(self, selection):
        model, treeiter = selection.get_selected_rows()
        if treeiter is not None and len(treeiter) == 1:
            meta = self.con.display_store.get_node_data(treeiter)
            if isinstance(meta, FMeta):
                logger.info(f'User selected cat="{meta.category.name}" md5="{meta.md5}" path="{meta.full_path}"')
            else:
                logger.info(f'User selected {self.con.display_store.get_node_name(treeiter)}')
        return self.on_selection_changed(treeiter)

    def _on_row_activated(self, tree_view, tree_path, col, tree_id):
        if not self.ui_enabled:
            logger.debug('Ignoring row activation - UI is disabled')
            # Allow it to propagate down the chain:
            return False
        selection = tree_view.get_selection()
        model, treeiter = selection.get_selected_rows()
        if not treeiter:
            logger.error('Row somehow activated with no selection!')
            return False
        else:
            logger.debug(f'User activated {len(treeiter)} rows')

        if len(treeiter) == 1:
            if self.on_single_row_activated(tree_view=tree_view, tree_iter=treeiter, tree_path=tree_path):
                return True
        else:
            if self.on_multiple_rows_activated(tree_view=tree_view, tree_iter=treeiter):
                return True
        return False

    def _on_toggle_gtk_row_expanded_state(self, tree_view, parent_iter, tree_path, is_expanded):
        parent_data = self.con.display_store.get_node_data(parent_iter)
        logger.debug(f'[{self.con.tree_id}] Sending signal "{actions.NODE_EXPANSION_TOGGLED}" with is_expanded={is_expanded} for node: {parent_data}')
        if not parent_data.is_dir():
            raise RuntimeError(f'Node is not a directory: {type(parent_data)}; node_data')

        dispatcher.send(signal=actions.NODE_EXPANSION_TOGGLED, sender=self.con.tree_id, parent_iter=parent_iter,
                        node_data=parent_data, is_expanded=is_expanded, expand_all=False)

        return True

    def _on_key_press(self, tree_view, event, tree_id):
        """Fired when a key is pressed"""
        if not self.ui_enabled:
            logger.debug('Ignoring key press - UI is disabled')
            return False

        # Note: if the key sequence matches a Gnome keyboard shortcut, it will grab part
        # of the sequence and we will never get notified
        mods = []
        if (event.state & Gdk.ModifierType.CONTROL_MASK) == Gdk.ModifierType.CONTROL_MASK:
            mods.append('Ctrl')
        if (event.state & Gdk.ModifierType.SHIFT_MASK) == Gdk.ModifierType.SHIFT_MASK:
            mods.append('Shift')
        if (event.state & Gdk.ModifierType.META_MASK) == Gdk.ModifierType.META_MASK:
            mods.append('Meta')
        if (event.state & Gdk.ModifierType.SUPER_MASK) == Gdk.ModifierType.SUPER_MASK:
            mods.append('Super')
        if (event.state & Gdk.ModifierType.MOD1_MASK) == Gdk.ModifierType.MOD1_MASK:
            mods.append('Alt')
        logger.debug(f'Key pressed: {Gdk.keyval_name(event.keyval)} ({event.keyval}), mods: {" ".join(mods)}')

        if event.keyval == Gdk.KEY_Delete:
            logger.debug('DELETE key detected!')
            # Get the TreeView selected row(s)
            selection = tree_view.get_selection()
            model, paths = selection.get_selected_rows()
            if self.on_delete_key_pressed(selected_tree_paths=paths):
                return True
        return False

    def _on_tree_button_press(self, tree_view, event, tree_id):
        """Used for displaying context menu on right click"""
        if not self.ui_enabled:
            logger.debug('Ignoring button press - UI is disabled')
            return False

        if event.button == 3:  # right click
            tree_path, col, cell_x, cell_y = tree_view.get_path_at_pos(int(event.x), int(event.y))
            node_data = self.con.display_store.get_node_data(tree_path)
            logger.debug(f'User right-clicked on {node_data}')

            if self.on_row_right_clicked(event=event, tree_path=tree_path, node_data=node_data):
                # Suppress selection event:
                return True
        return False

    # ⮝⮝⮝⮝⮝⮝⮝⮝⮝⮝⮝⮝⮝⮝⮝⮝⮝⮝⮝⮝⮝⮝⮝⮝⮝⮝⮝⮝⮝⮝⮝⮝⮝⮝⮝⮝⮝⮝⮝⮝⮝⮝⮝
    # LISTENERS end

    # ACTIONS begin
    # ⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟

    # To be optionally overridden:
    def on_selection_changed(self, treeiter):
        return False

    def on_single_row_activated(self, tree_view, tree_iter, tree_path):
        return False

    def on_multiple_rows_activated(self, tree_view, tree_iter):
        return False

    def on_delete_key_pressed(self, selected_tree_paths):
        return False

    def on_row_right_clicked(self, event, tree_path, node_data: DisplayNode):
        id_clicked = node_data.uid
        selected_items: List[DisplayNode] = self.con.get_multiple_selection()

        if len(selected_items) > 1:
            # Multiple selected items:
            for item in selected_items:
                if item.uid == id_clicked:
                    # User right-clicked on selection -> apply context menu to all selected items:
                    context_menu = self.build_context_menu_multiple(selected_items)
                    if context_menu:
                        context_menu.popup_at_pointer(event)
                        # Suppress selection event
                        return True
                    else:
                        return False

        # Singular item, or singular selection (equivalent logic). Display context menu:
        context_menu = self.build_context_menu(tree_path, node_data)
        if context_menu:
            context_menu.popup_at_pointer(event)
            return True

        return False

    # ⮝⮝⮝⮝⮝⮝⮝⮝⮝⮝⮝⮝⮝⮝⮝⮝⮝⮝⮝⮝⮝⮝⮝⮝⮝⮝⮝⮝⮝⮝⮝⮝⮝⮝⮝⮝⮝⮝⮝⮝⮝⮝⮝
    # ACTIONS end

    def build_context_menu_multiple(self, selected_items: List[DisplayNode]) -> Optional[Gtk.Menu]:
        """Create context menu for multiple selected items"""
        return None

    def build_context_menu(self, tree_path: Gtk.TreePath, node_data: DisplayNode) -> Optional[Gtk.Menu]:
        """Create context menu for single item, or singular selection (equivalent logic)"""
        return None
