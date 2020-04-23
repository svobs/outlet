import logging
from ui.tree.action_bridge import TreeActionBridge

import gi
gi.require_version("Gtk", "3.0")
from gi.repository import GLib, Gtk, Gdk

logger = logging.getLogger(__name__)


class GDriveActionHandlers(TreeActionBridge):
    def __init__(self, controller=None):
        super().__init__(controller)

    def init(self):
        super().init()

    # --- LISTENERS ---

    def on_single_row_activated(self, tree_view, tree_iter, tree_path):
        """Fired when an item is double-clicked or when an item is selected and Enter is pressed"""
        if tree_view.row_expanded(tree_path):
            tree_view.collapse_row(tree_path)
        else:
            tree_view.expand_row(path=tree_path, open_all=False)
        return True

    # --- END of LISTENERS ---

    # --- ACTIONS ---

    # --- END ACTIONS ---
