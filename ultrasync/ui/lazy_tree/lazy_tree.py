

import logging

from pydispatch import dispatcher

import ui.actions as actions
from fmeta.fmeta import FMeta, Category

import gi
gi.require_version("Gtk", "3.0")
from gi.repository import GLib, Gtk, Gdk

logger = logging.getLogger(__name__)


class TreeViewListenerAdapter:
    def __init__(self, tree_id, tree_view, status_bar, display_store, selection_mode):
        self.tree_id = tree_id
        self.treeview = tree_view
        self.status_bar = status_bar
        self.display_store = display_store

        select = self.treeview.get_selection()
        select.set_mode(selection_mode)

        self._add_listeners()

    # --- LISTENERS ---

    def _add_listeners(self):
        actions.connect(actions.TOGGLE_UI_ENABLEMENT, self._on_enable_ui_toggled)

        actions.connect(actions.SET_STATUS, self._on_set_status, self.tree_id)

        self.treeview.connect("row-activated", self._on_row_activated)
        self.treeview.connect('button-press-event', self._on_tree_button_press)
        self.treeview.connect('key-press-event', self._on_key_press)
        self.treeview.connect('row-expanded', self._on_toggle_row_expanded_state, True)
        self.treeview.connect('row-collapsed', self._on_toggle_row_expanded_state, False)

        # select.connect("changed", self._on_tree_selection_changed)

    # Remember, use member functions instead of lambdas, because PyDispatcher will remove refs
    def _on_set_status(self, sender, status_msg):
        GLib.idle_add(lambda: self.status_bar.set_label(status_msg))

    def _on_enable_ui_toggled(self, sender, enable):
        # TODO! Disable listeners
        pass

    def _on_tree_selection_changed(self, selection):
        model, treeiter = selection.get_selected_rows()
        if treeiter is not None and len(treeiter) == 1:
            meta = self.display_store.get_node_data(treeiter)
            if isinstance(meta, FMeta):
                logger.debug(f'User selected cat="{meta.category.name}" sig="{meta.signature}" path="{meta.file_path}" prev_path="{meta.prev_path}"')
            else:
                logger.debug(f'User selected {self.display_store.get_node_name(treeiter)}')

    def _on_row_activated(self, tree_view, path, col):
        selection = tree_view.get_selection()
        model, treeiter = selection.get_selected_rows()
        if not treeiter:
            logger.error('Row somehow activated with no selection!')
            return

        if len(treeiter) == 1:
            dispatcher.send(signal=actions.SINGLE_ROW_ACTIVATED, sender=self.tree_id, tree_iter=treeiter)
        else:
            dispatcher.send(signal=actions.MULTIPLE_ROWS_ACTIVATED, sender=self.tree_id, tree_iter=treeiter)

    def _on_toggle_row_expanded_state(self, tree_view, parent_iter, tree_path, is_expanded):
        node_data = self.display_store.get_node_data(parent_iter)
        logger.debug(f'Toggling expanded state to {is_expanded} for node: {node_data}')
        if not node_data.is_dir():
            raise RuntimeError(f'Node is not a directory: {type(node_data)}; node_data')

        dispatcher.send(signal=actions.NODE_EXPANSION_TOGGLED, sender=self.tree_id, parent_iter=parent_iter,
                        node_data=node_data, is_expanded=is_expanded)

        return True

    def _on_key_press(self, widget, event, user_data=None):
        """Fired when a key is pressed"""

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
        logger.debug(f'Key pressed, mods: {Gdk.keyval_name(event.keyval)} ({event.keyval}), {" ".join(mods)}')

        if event.keyval == Gdk.KEY_Delete:
            logger.debug('DELETE key detected!')
            # Get the TreeView selected row(s)
            selection = self.treeview.get_selection()
            model, paths = selection.get_selected_rows()
            dispatcher.send(signal=actions.DELETE_KEY_PRESSED, sender=self.tree_id, tree_paths=paths)
            return True
        else:
            return False

    def _on_tree_button_press(self, tree_view, event):
        """Used for displaying context menu on right click"""
        if event.button == 3:  # right click
            tree_path, col, cell_x, cell_y = tree_view.get_path_at_pos(int(event.x), int(event.y))
            node_data = self.display_store.get_node_data(tree_path)
            logger.debug(f'User right-clicked on {node_data}')
            dispatcher.send(signal=actions.ROW_RIGHT_CLICKED, sender=self.tree_id, tree_path=tree_path, node_data=node_data)
            # Suppress selection event:
            return True
        return False

    # --- END of LISTENERS ---
