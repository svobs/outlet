import logging
import os
from typing import Optional

import file_util
from constants import GDRIVE_PATH_PREFIX
from gdrive.client import GDriveClient
from model.display_node import CategoryNode, DisplayNode
from model.goog_node import GoogFile, GoogNode
from model.planning_node import FileDecoratorNode
from ui.tree.action_bridge import TreeActionBridge

import gi
gi.require_version("Gtk", "3.0")
from gi.repository import GLib, Gtk, Gdk, GObject

logger = logging.getLogger(__name__)

# CLASS GDriveActionHandlers
# ⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟


class GDriveActionHandlers(TreeActionBridge):
    def __init__(self, config, controller=None):
        super().__init__(config, controller)

    def init(self):
        super().init()

    # LISTENERS begin
    # ⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟

    def on_single_row_activated(self, tree_view, tree_iter, tree_path):
        """Fired when an item is double-clicked or when an item is selected and Enter is pressed"""
        if tree_view.row_expanded(tree_path):
            tree_view.collapse_row(tree_path)
        else:
            tree_view.expand_row(path=tree_path, open_all=False)
        return True

    # ⮝⮝⮝⮝⮝⮝⮝⮝⮝⮝⮝⮝⮝⮝⮝⮝⮝⮝⮝⮝⮝⮝⮝⮝⮝⮝⮝⮝⮝⮝⮝⮝⮝⮝⮝⮝⮝⮝⮝⮝⮝⮝⮝
    # LISTENERS end

    # ACTIONS begin
    # ⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟
    def download_file(self, node: GoogFile):
        gdrive_client = GDriveClient(self.con.config)
        cache_dir_path = file_util.get_resource_path(self.con.config.get('cache.cache_dir_path'))
        dest_file = os.path.join(cache_dir_path, node.name)
        try:
            gdrive_client.download_file(node.uid, dest_file)
        except Exception as err:
            self.con.parent_win.show_error_msg('Download failed', repr(err))
            raise

    def delete_dir_tree(self, subtree_root: str, tree_path: Gtk.TreePath):
        # TODO
        pass

    def delete_single_file(self, file_path: str, tree_path: Gtk.TreePath):
        # TODO
        pass

    def expand_all(self, tree_path):
        # TODO
        pass

    # ⮝⮝⮝⮝⮝⮝⮝⮝⮝⮝⮝⮝⮝⮝⮝⮝⮝⮝⮝⮝⮝⮝⮝⮝⮝⮝⮝⮝⮝⮝⮝⮝⮝⮝⮝⮝⮝⮝⮝⮝⮝⮝⮝
    # ACTIONS end

    def build_context_menu(self, tree_path: Gtk.TreePath, node_data: DisplayNode) -> Optional[Gtk.Menu]:
        """Dynamic context menu (right-click on tree item)"""

        if not node_data.has_path():
            # 'Loading' node, 'Empty' node, etc.
            return

        menu = Gtk.Menu()

        if isinstance(node_data, FileDecoratorNode):
            file_exists = False
            full_path = node_data.original_full_path
            if isinstance(node_data.original_node, GoogNode):
                full_path_display = GDRIVE_PATH_PREFIX + full_path
            else:
                full_path_display = full_path
        else:
            full_path = self.con.get_tree().get_full_path_for_item(node_data)
            if isinstance(node_data, GoogNode):
                full_path_display = GDRIVE_PATH_PREFIX + full_path
            else:
                full_path_display = full_path
            file_exists = True
        file_name = os.path.basename(full_path)
        is_dir = node_data.is_dir()

        is_category_node = type(node_data) == CategoryNode

        item = Gtk.MenuItem(label='')
        label = item.get_child()
        full_path_display = GObject.markup_escape_text(full_path_display)
        label.set_markup(f'<i>{full_path_display}</i>')
        item.set_sensitive(False)
        menu.append(item)

        if file_exists and not is_dir:
            item = Gtk.MenuItem(label=f'Download from Google Drive')
            item.connect('activate', lambda menu_item, node: self.download_file(node), node_data)
            menu.append(item)

        item = Gtk.SeparatorMenuItem()
        menu.append(item)

        if not file_exists:
            item = Gtk.MenuItem(label='')
            label = item.get_child()
            label.set_markup(f'<i>Path not found</i>')
            item.set_sensitive(False)
            menu.append(item)

        if is_dir:
            item = Gtk.MenuItem(label=f'Expand all')
            item.connect('activate', lambda menu_item: self.expand_all(tree_path))
            menu.append(item)

            if not is_category_node and file_exists:
                item = Gtk.MenuItem(label=f'Delete tree "{file_name}" from Google Drive')
                item.connect('activate', lambda menu_item, abs_p: self.delete_dir_tree(abs_p, tree_path), full_path)
                menu.append(item)
        elif file_exists:
            item = Gtk.MenuItem(label=f'Delete "{file_name}" from Google Drive')
            item.connect('activate', lambda menu_item, abs_p: self.delete_single_file(abs_p, tree_path), full_path)
            menu.append(item)

        menu.show_all()
        return menu
