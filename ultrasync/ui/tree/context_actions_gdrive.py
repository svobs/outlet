import logging
import os
from typing import List, Optional

import gi
import treelib

import file_util
from constants import GDRIVE_PATH_PREFIX
from gdrive.client import GDriveClient
from model.display_node import CategoryNode, DisplayNode, EphemeralNode
from model.goog_node import GoogFile, GoogNode
from model.planning_node import FileDecoratorNode

gi.require_version("Gtk", "3.0")
from gi.repository import Gtk, GObject

logger = logging.getLogger(__name__)

DATE_REGEX = r'^[\d]{4}(\-[\d]{2})?(-[\d]{2})?'

# CLASS ContextActionsGDrive
# ⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟


class ContextActionsGDrive:
    def __init__(self, controller):
        self.con = controller
        self.download_dir = self.con.config.get('download_dir', None)

    def build_context_menu(self, tree_path: Gtk.TreePath, node_data: DisplayNode) -> Optional[Gtk.Menu]:
        """Dynamic context menu (right-click on tree item)"""

        if not node_data.has_path():
            # 'Loading' node, 'Empty' node, etc.
            return

        menu = Gtk.Menu()

        # Derive variables:
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
            item.connect('activate', self.download_file, node_data)
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
            item.connect('activate', self.expand_all, tree_path)
            menu.append(item)

            if not is_category_node and file_exists:
                item = Gtk.MenuItem(label=f'Delete tree "{file_name}" from Google Drive')
                item.connect('activate', self.delete_dir_tree, full_path)
                menu.append(item)
        elif file_exists:
            item = Gtk.MenuItem(label=f'Delete "{file_name}" from Google Drive')
            item.connect('activate', lambda menu_item, abs_p: self.delete_single_file(abs_p, tree_path), full_path)
            menu.append(item)

        menu.show_all()
        return menu

    # ACTIONS begin
    # ⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟

    def download_file(self, menu_item, node: GoogFile):
        gdrive_client = GDriveClient(self.con.config)
        cache_dir_path = file_util.get_resource_path(self.con.config.get('cache.cache_dir_path'))
        dest_file = os.path.join(cache_dir_path, node.name)
        try:
            gdrive_client.download_file(node.goog_id, dest_file)
        except Exception as err:
            self.con.parent_win.show_error_msg('Download failed', repr(err))
            raise

    def delete_dir_tree(self, menu_item, subtree_root: str, tree_path: Gtk.TreePath):
        # TODO
        pass

    def delete_single_file(self, menu_item, file_path: str, tree_path: Gtk.TreePath):
        # TODO
        pass

    def expand_all(self, menu_item, tree_path):
        def expand_me(tree_iter):
            path = self.con.display_store.model.get_path(tree_iter)
            self.con.display_strategy.expand_subtree(path)

        if not self.con.tree_view.row_expanded(tree_path):
            self.con.display_strategy.expand_subtree(tree_path)
        else:
            self.con.display_store.do_for_descendants(tree_path=tree_path, action_func=expand_me)

    # ⮝⮝⮝⮝⮝⮝⮝⮝⮝⮝⮝⮝⮝⮝⮝⮝⮝⮝⮝⮝⮝⮝⮝⮝⮝⮝⮝⮝⮝⮝⮝⮝⮝⮝⮝⮝⮝⮝⮝⮝⮝⮝⮝
    # ACTIONS end
