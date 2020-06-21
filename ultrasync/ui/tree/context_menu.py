import logging
import os
import re
from typing import List, Optional

import gi
from pydispatch import dispatcher

from constants import GDRIVE_PATH_PREFIX, TREE_TYPE_GDRIVE, TREE_TYPE_LOCAL_DISK
from model.display_node import CategoryNode, DisplayNode
from model.goog_node import GoogNode
from ui import actions
from ui.tree.tree_actions import DATE_REGEX

gi.require_version("Gtk", "3.0")
from gi.repository import Gtk, GObject

logger = logging.getLogger(__name__)


# CLASS TreeContextMenu
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼

class TreeContextMenu:
    def __init__(self, controller):
        self.con = controller

    def build_context_menu_multiple(self, selected_items: List[DisplayNode], selected_tree_paths: List[Gtk.TreePath]) -> Optional[Gtk.Menu]:
        menu = Gtk.Menu()

        # Show number of items selected
        item = Gtk.MenuItem(label='')
        label = item.get_child()
        display = GObject.markup_escape_text(f'{len(selected_items)} items selected')
        label.set_markup(f'<i>{display}</i>')
        item.set_sensitive(False)
        menu.append(item)

        if self.con.treeview_meta.has_checkboxes:
            tree_paths: List[Gtk.TreePath] = []
            for item, path in zip(selected_items, selected_tree_paths):
                # maybe I'm nitpicking here
                if not item.is_ephemereal():
                    tree_paths.append(path)

            item = Gtk.MenuItem(label=f'Check All')
            item.connect('activate', self.send_signal, actions.SET_ROWS_CHECKED, {'tree_paths': tree_paths})
            menu.append(item)

            item = Gtk.MenuItem(label=f'Uncheck All')
            item.connect('activate', self.send_signal, actions.SET_ROWS_UNCHECKED, {'tree_paths': tree_paths})
            menu.append(item)

        is_localdisk = len(selected_items) > 0 and selected_items[0].node_identifier.tree_type == TREE_TYPE_LOCAL_DISK
        if is_localdisk:
            item = Gtk.MenuItem(label=f'Use EXIFTool on dirs')
            item.connect('activate', self.send_signal, actions.CALL_EXIFTOOL_LIST, {'node_list': selected_items})
            menu.append(item)

        items_to_delete_local: List[DisplayNode] = []
        items_to_delete_gdrive: List[DisplayNode] = []
        for selected_item in selected_items:
            if selected_item.node_identifier.tree_type == TREE_TYPE_LOCAL_DISK and TreeContextMenu.file_exists(selected_item):
                items_to_delete_local.append(selected_item)
            elif selected_item.node_identifier.tree_type == TREE_TYPE_GDRIVE and TreeContextMenu.file_exists(selected_item):
                items_to_delete_gdrive.append(selected_item)

        if len(items_to_delete_local) > 0:
            item = Gtk.MenuItem(label=f'Delete {len(items_to_delete_local)} items from local disk')
            item.connect('activate', self.send_signal, actions.DELETE_SUBTREE, {'node_list': items_to_delete_local})
            menu.append(item)

        if len(items_to_delete_gdrive) > 0:
            item = Gtk.MenuItem(label=f'Delete {len(items_to_delete_gdrive)} items from Google Drive')
            item.connect('activate', self.send_signal, actions.DELETE_SUBTREE, {'node_list': items_to_delete_gdrive})
            menu.append(item)

        menu.show_all()
        return menu

    def _build_menu_items_for_single_node(self, menu, tree_path, node: DisplayNode):
        full_path = node.full_path
        is_category_node = type(node) == CategoryNode
        file_exists = TreeContextMenu.file_exists(node)
        is_dir = node.is_dir()
        file_name = os.path.basename(full_path)
        is_gdrive = node.node_identifier.tree_type == TREE_TYPE_GDRIVE

        if file_exists and node.node_identifier.tree_type == TREE_TYPE_LOCAL_DISK:
            item = Gtk.MenuItem(label='Show in Nautilus')
            item.connect('activate', self.send_signal, actions.SHOW_IN_NAUTILUS, {'full_path': full_path})
            menu.append(item)

        if file_exists and not is_dir:
            if is_gdrive:
                item = Gtk.MenuItem(label=f'Download from Google Drive')
                item.connect('activate', self.send_signal, actions.DOWNLOAD_FROM_GDRIVE, {'node': node})
                menu.append(item)
            elif node.node_identifier.tree_type == TREE_TYPE_LOCAL_DISK:
                item = Gtk.MenuItem(label=f'Open with default application')
                item.connect('activate', self.send_signal, actions.CALL_XDG_OPEN, {'node': node})
                menu.append(item)

        if not file_exists:
            item = Gtk.MenuItem(label='')
            label = item.get_child()
            label.set_markup(f'<i>Path not found</i>')
            item.set_sensitive(False)
            menu.append(item)

        if is_dir:
            item = Gtk.MenuItem(label=f'Go into "{file_name}"')
            item.connect('activate', self.send_signal, actions.ROOT_PATH_UPDATED, {'new_root' : node.node_identifier})
            menu.append(item)

        if file_exists and is_dir and node.node_identifier.tree_type == TREE_TYPE_LOCAL_DISK:
            match = re.match(DATE_REGEX, file_name)
            if match:
                item = Gtk.MenuItem(label=f'Use EXIFTool on dir')
                item.connect('activate', self.send_signal, actions.CALL_EXIFTOOL, {'full_path': full_path})
                menu.append(item)

        if file_exists and is_dir and not is_category_node:
            label = f'Delete tree "{file_name}"'
            if is_gdrive:
                label += ' from Google Drive'
            item = Gtk.MenuItem(label=label)
            node = self.con.display_store.get_node_data(tree_path)
            item.connect('activate', self.send_signal, actions.DELETE_SUBTREE, {'node': node})
            menu.append(item)

        if file_exists and not is_dir:
            label = f'Delete "{file_name}"'
            if is_gdrive:
                label += ' from Google Drive'
            item = Gtk.MenuItem(label=label)
            item.connect('activate', self.send_signal, actions.DELETE_SINGLE_FILE, {'node': node})
            menu.append(item)

    def build_context_menu(self, tree_path: Gtk.TreePath, node: DisplayNode) -> Optional[Gtk.Menu]:
        """Dynamic context menu (right-click on tree item)"""

        if node.is_ephemereal():
            # 'Loading' node, 'Empty' node, etc.
            return
        is_dir = os.path.isdir(node.full_path)

        menu = Gtk.Menu()

        if False:  # FIXME isinstance(node, FileDecoratorNode):
            # Source:
            item = TreeContextMenu.build_full_path_display_item(menu, 'Src: ', node.src_node)
            if TreeContextMenu.file_exists(node.src_node):
                src_submenu = Gtk.Menu()
                item.set_submenu(src_submenu)
                self._build_menu_items_for_single_node(src_submenu, tree_path, node.src_node)
            else:
                item.set_sensitive(False)

            item = Gtk.SeparatorMenuItem()
            menu.append(item)

            # Destination:
            item = TreeContextMenu.build_full_path_display_item(menu, 'Dst: ', node)
            if TreeContextMenu.file_exists(node):
                dst_submenu = Gtk.Menu()
                item.set_submenu(dst_submenu)
                self._build_menu_items_for_single_node(dst_submenu, tree_path, node)
            else:
                item.set_sensitive(False)

            item = Gtk.SeparatorMenuItem()
            menu.append(item)
        else:
            # Single item
            item = TreeContextMenu.build_full_path_display_item(menu, '', node)
            # gray it out
            item.set_sensitive(False)

            item = Gtk.SeparatorMenuItem()
            menu.append(item)

            self._build_menu_items_for_single_node(menu, tree_path, node)

        if is_dir:
            item = Gtk.MenuItem(label=f'Expand all')
            item.connect('activate', self.send_signal, actions.EXPAND_ALL, {'tree_path': tree_path})
            menu.append(item)

        menu.show_all()
        return menu

    @staticmethod
    def file_exists(node: DisplayNode):
        if node.node_identifier.tree_type == TREE_TYPE_LOCAL_DISK:
            return os.path.exists(node.full_path)
        elif node.node_identifier.tree_type == TREE_TYPE_GDRIVE:
            try:
                # We assume that the node exists in GDrive if a Google ID has been assigned
                return node.goog_id and True
            except AttributeError:
                # Shouldn't happen...
                return False

    @staticmethod
    def build_full_path_display_item(menu: Gtk.Menu, preamble: str, node: DisplayNode) -> Gtk.MenuItem:
        if isinstance(node, GoogNode):
            full_path_display = GDRIVE_PATH_PREFIX + node.full_path
        else:
            full_path_display = node.full_path

        item = Gtk.MenuItem(label='')
        label = item.get_child()
        full_path_display = GObject.markup_escape_text(full_path_display)
        label.set_markup(f'<i>{preamble}{full_path_display}</i>')
        menu.append(item)
        return item

    def send_signal(self, menu_item, signal: str, kwargs):
        dispatcher.send(signal=signal, sender=self.con.tree_id, **kwargs)
