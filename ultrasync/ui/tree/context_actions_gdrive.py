import logging
import os
import re
import subprocess
from typing import Optional

import gi
from pydispatch import dispatcher

import file_util
from constants import TREE_TYPE_GDRIVE, TREE_TYPE_LOCAL_DISK
from gdrive.client import GDriveClient
from model.display_node import CategoryNode, DisplayNode
from model.goog_node import GoogFile
from model.planning_node import FileDecoratorNode
from ui import actions
from ui.tree.context_actions_localdisk import ContextActions

gi.require_version("Gtk", "3.0")
from gi.repository import Gtk

logger = logging.getLogger(__name__)

DATE_REGEX = r'^[\d]{4}(\-[\d]{2})?(-[\d]{2})?'
OPEN = 1
SHOW = 2

# CLASS ContextActionsGDrive
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼


class ContextActionsGDrive:
    def __init__(self, controller):
        self.con = controller
        self.download_dir = file_util.get_resource_path(self.con.config.get('download_dir'))
        self.post_download_action = OPEN

    def _build_menu_items_for_single_node(self, menu, tree_path, node: DisplayNode):
        full_path = node.full_path
        is_category_node = type(node) == CategoryNode
        file_exists = ContextActions.file_exists(node)
        is_dir = node.is_dir()
        file_name = os.path.basename(full_path)
        is_gdrive = node.node_identifier.tree_type == TREE_TYPE_GDRIVE

        if file_exists:
            item = Gtk.MenuItem(label='Show in Nautilus')
            item.connect('activate', lambda menu_item, f: self.show_in_nautilus(f), full_path)
            menu.append(item)
        else:
            item = Gtk.MenuItem(label='')
            label = item.get_child()
            label.set_markup(f'<i>Path not found</i>')
            item.set_sensitive(False)
            menu.append(item)

        if node.node_identifier.tree_type == TREE_TYPE_GDRIVE and file_exists and not is_dir:
            item = Gtk.MenuItem(label=f'Download from Google Drive')
            item.connect('activate', self._download_file_from_gdrive, node)
            menu.append(item)

        if is_dir:
            item = Gtk.MenuItem(label=f'Go into "{file_name}"')
            item.connect('activate', self._set_as_tree_root, node)
            menu.append(item)

            if node.node_identifier.tree_type == TREE_TYPE_LOCAL_DISK:
                match = re.match(DATE_REGEX, file_name)
                if match:
                    item = Gtk.MenuItem(label=f'Use EXIFTool on dir')
                    item.connect('activate', lambda menu_item: ContextActions.call_exiftool(full_path, self.con.parent_win))
                    menu.append(item)

            if not is_category_node and file_exists:
                label = f'Delete tree "{file_name}"'
                if is_gdrive:
                    label += ' from Google Drive'
                item = Gtk.MenuItem(label=label)
                item.connect('activate', lambda menu_item, abs_p: self.delete_dir_tree(abs_p, tree_path), full_path)
                menu.append(item)
        elif file_exists:
            label = f'Delete "{file_name}"'
            if is_gdrive:
                label += ' from Google Drive'
            item = Gtk.MenuItem(label=label)
            item.connect('activate', lambda menu_item, abs_p: self.delete_single_file(abs_p, tree_path), full_path)
            menu.append(item)

    def build_context_menu(self, tree_path: Gtk.TreePath, node: DisplayNode) -> Optional[Gtk.Menu]:
        """Dynamic context menu (right-click on tree item)"""

        if node.is_ephemereal():
            # 'Loading' node, 'Empty' node, etc.
            return
        is_dir = os.path.isdir(node.full_path)

        menu = Gtk.Menu()

        if isinstance(node, FileDecoratorNode):
            # Source:
            item = ContextActions.build_full_path_display_item(menu, 'Src: ', node.src_node)
            src_submenu = Gtk.Menu()
            item.set_submenu(src_submenu)

            item = Gtk.SeparatorMenuItem()
            menu.append(item)

            self._build_menu_items_for_single_node(src_submenu, tree_path, node.src_node)

            # Destination:
            item = ContextActions.build_full_path_display_item(menu, 'Dst: ', node)
            dst_submenu = Gtk.Menu()
            item.set_submenu(dst_submenu)

            item = Gtk.SeparatorMenuItem()
            menu.append(item)

            self._build_menu_items_for_single_node(dst_submenu, tree_path, node)
        else:
            # Single item
            item = ContextActions.build_full_path_display_item(menu, '', node)
            # gray it out
            item.set_sensitive(False)

            item = Gtk.SeparatorMenuItem()
            menu.append(item)

            self._build_menu_items_for_single_node(menu, tree_path, node)

        # item = Gtk.MenuItem(label='Test Remove Node')
        # item.connect('activate', lambda menu_item, n: dispatcher.send(signal=actions.NODE_REMOVED, sender=ID_GLOBAL_CACHE, node=n), node_data)
        # menu.append(item)

        if is_dir:
            item = Gtk.MenuItem(label=f'Expand all')
            item.connect('activate', self.expand_all, tree_path)
            menu.append(item)

        menu.show_all()
        return menu

    # ACTIONS begin
    # ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼

    def _set_as_tree_root(self, menu_item, node):
        dispatcher.send(signal=actions.ROOT_PATH_UPDATED, sender=self.con.tree_id, new_root=node.node_identifier)

    def _download_file_from_gdrive(self, menu_item, node: GoogFile):
        gdrive_client = GDriveClient(self.con.config)

        os.makedirs(name=self.download_dir, exist_ok=True)
        dest_file = os.path.join(self.download_dir, node.name)
        try:
            gdrive_client.download_file(node.goog_id, dest_file)
            if self.post_download_action == OPEN:
                self.call_xdg_open(dest_file)
            elif self.post_download_action == SHOW:
                self.show_in_nautilus(dest_file)
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
        self.con.display_strategy.expand_all(tree_path)

    def call_xdg_open(self, file_path):
        # TODO: put this in a shared file
        if os.path.exists(file_path):
            logger.info(f'Calling xdg-open for: {file_path}')
            subprocess.check_call(["xdg-open", file_path])
        else:
            self.con.parent_win.show_error_msg(f'Cannot open file', f'File not found: {file_path}')

    def show_in_nautilus(self, file_path):
        # TODO: put this in a shared file
        if os.path.exists(file_path):
            logger.info(f'Opening in Nautilus: {file_path}')
            subprocess.check_call(["nautilus", "--browser", file_path])
        else:
            self.con.parent_win.show_error_msg('Cannot open file in Nautilus', f'File not found: {file_path}')

    # ▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲
    # ACTIONS end
