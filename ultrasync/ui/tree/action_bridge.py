

import logging

from pydispatch import dispatcher

import ui.actions as actions
from model.fmeta import FMeta

import gi
gi.require_version("Gtk", "3.0")
from gi.repository import GLib, Gdk

logger = logging.getLogger(__name__)

# CLASS TreeActionBridge
# ⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟


class TreeActionBridge:
    def __init__(self, controller):
        self.con = controller
        self.ui_enabled = True

    def init(self):
        actions.connect(actions.TOGGLE_UI_ENABLEMENT, self._on_enable_ui_toggled)

        # Status bar
        logger.debug(f'Status bar will listen for signals from sender: {self.con.tree_id}')
        actions.connect(signal=actions.SET_STATUS, handler=self._on_set_status, sender=self.con.tree_id)

        # TreeView
        self.con.tree_view.connect("row-activated", self._on_row_activated, self.con.tree_id)
        self.con.tree_view.connect('button-press-event', self._on_tree_button_press, self.con.tree_id)
        self.con.tree_view.connect('key-press-event', self._on_key_press, self.con.tree_id)
        self.con.tree_view.connect('row-expanded', self._on_toggle_row_expanded_state, True)
        self.con.tree_view.connect('row-collapsed', self._on_toggle_row_expanded_state, False)
        # select.connect("changed", self._on_tree_selection_changed)

    # LISTENERS
    # ⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟

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
                logger.info(f'User selected cat="{meta.category.name}" md5="{meta.md5}" path="{meta.full_path}" prev_path="{meta.prev_path}"')
            else:
                logger.info(f'User selected {self.con.display_store.get_node_name(treeiter)}')
        return self.on_selection_changed(treeiter)

    def _on_row_activated(self, tree_view, tree_path, col, tree_id):
        if not self.ui_enabled:
            logger.debug('Ignoring row activation - UI is disabled')
            return True
        selection = tree_view.get_selection()
        model, treeiter = selection.get_selected_rows()
        if not treeiter:
            logger.error('Row somehow activated with no selection!')
            return
        else:
            logger.debug(f'User activated {len(treeiter)} rows')

        if len(treeiter) == 1:
            if self.on_single_row_activated(tree_view=tree_view, tree_iter=treeiter, tree_path=tree_path):
                return True
        else:
            if self.on_multiple_rows_activated(tree_view=tree_view, tree_iter=treeiter):
                return True
        return False

    def _on_toggle_row_expanded_state(self, tree_view, parent_iter, tree_path, is_expanded):
        if not self.ui_enabled:
            logger.debug('Ignoring row expansion toggle - UI is disabled')
            return True
        node_data = self.con.display_store.get_node_data(parent_iter)
        logger.debug(f'[{self.con.tree_id}] Sending signal "{actions.NODE_EXPANSION_TOGGLED}" with is_expanded={is_expanded} for node: {node_data}')
        if not node_data.is_dir():
            raise RuntimeError(f'Node is not a directory: {type(node_data)}; node_data')

        dispatcher.send(signal=actions.NODE_EXPANSION_TOGGLED, sender=self.con.tree_id, parent_iter=parent_iter,
                        node_data=node_data, is_expanded=is_expanded)

        return True

    def _on_key_press(self, tree_view, event, tree_id):
        """Fired when a key is pressed"""
        if not self.ui_enabled:
            logger.debug('Ignoring key press - UI is disabled')
            return True

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
            selection = tree_view.get_selection()
            model, paths = selection.get_selected_rows()
            if self.on_delete_key_pressed(selected_tree_paths=paths):
                return True
        return False

    def _on_tree_button_press(self, tree_view, event, tree_id):
        """Used for displaying context menu on right click"""
        if not self.ui_enabled:
            logger.debug('Ignoring button press - UI is disabled')
            return True

        if event.button == 3:  # right click
            tree_path, col, cell_x, cell_y = tree_view.get_path_at_pos(int(event.x), int(event.y))
            node_data = self.con.display_store.get_node_data(tree_path)
            logger.debug(f'User right-clicked on {node_data}')

            if self.on_row_right_clicked(event=event, tree_path=tree_path, node_data=node_data):
                # Suppress selection event:
                return True
        return False

    # ▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲
    # END LISTENERS

    # To be optionally overridden:
    def on_selection_changed(self, treeiter):
        return False

    def on_single_row_activated(self, tree_view, tree_iter, tree_path):
        return False

    def on_multiple_rows_activated(self, tree_view, tree_iter):
        return False

    def on_delete_key_pressed(self, selected_tree_paths):
        return False

    def on_row_right_clicked(self, event, tree_path, node_data):
        return False

