import logging
import re
from typing import List, Optional

from pydispatch import dispatcher

from constants import GDRIVE_PATH_PREFIX, SUPER_DEBUG, TREE_TYPE_GDRIVE, TREE_TYPE_LOCAL_DISK
from model.node.container_node import CategoryNode
from model.node.node import Node
from model.user_op import UserOp
from ui import actions
from ui.tree.tree_actions import DATE_REGEX

import gi
gi.require_version("Gtk", "3.0")
from gi.repository import GLib, Gtk

logger = logging.getLogger(__name__)


# CLASS TreeContextMenu
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼

class TreeContextMenu:
    def __init__(self, controller):
        self.con = controller

    def build_context_menu_multiple(self, selected_items: List[Node], selected_tree_paths: List[Gtk.TreePath]) -> Optional[Gtk.Menu]:
        menu = Gtk.Menu()

        # Show number of items selected
        item = Gtk.MenuItem(label='')
        label = item.get_child()
        display = GLib.markup_escape_text(f'{len(selected_items)} items selected')
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

        items_to_delete_local: List[Node] = []
        items_to_delete_gdrive: List[Node] = []
        for selected_item in selected_items:
            if selected_item.node_identifier.tree_type == TREE_TYPE_LOCAL_DISK and selected_item.is_live():
                items_to_delete_local.append(selected_item)
            elif selected_item.node_identifier.tree_type == TREE_TYPE_GDRIVE and selected_item.is_live():
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

    def _build_menu_items_for_single_node(self, menu, tree_path, node: Node, single_path: str):
        is_category_node = type(node) == CategoryNode
        file_exists = node.is_live()
        is_dir = node.is_dir()
        is_gdrive = node.node_identifier.tree_type == TREE_TYPE_GDRIVE

        # MenuItem: 'Show in Nautilus'
        if file_exists and node.node_identifier.tree_type == TREE_TYPE_LOCAL_DISK:
            item = Gtk.MenuItem(label='Show in Nautilus')
            item.connect('activate', self.send_signal, actions.SHOW_IN_NAUTILUS, {'full_path': node.get_single_path()})
            menu.append(item)

        # MenuItem: 'Download from Google Drive' [GDrive] OR 'Open with default app' [Local]
        if file_exists and not is_dir:
            if is_gdrive:
                item = Gtk.MenuItem(label=f'Download from Google Drive')
                item.connect('activate', self.send_signal, actions.DOWNLOAD_FROM_GDRIVE, {'node': node})
                menu.append(item)
            elif node.node_identifier.tree_type == TREE_TYPE_LOCAL_DISK:
                item = Gtk.MenuItem(label=f'Open with default app')
                item.connect('activate', self.send_signal, actions.CALL_XDG_OPEN, {'node': node})
                menu.append(item)

        # Label: Does not exist
        if not file_exists:
            # FIXME: this is not entirely correct when examining logical nodes which represent real paths
            item = Gtk.MenuItem(label='')
            label = item.get_child()
            label.set_markup(f'<i>Does not exist</i>')
            item.set_sensitive(False)
            menu.append(item)

        # MenuItem: 'Go into {dir}'
        if file_exists and is_dir:
            item = Gtk.MenuItem(label=f'Go into "{node.name}"')
            # FIXME: need to resolve this to single path. Will break for some GDrive nodes!
            item.connect('activate', self.send_signal, actions.ROOT_PATH_UPDATED, {'new_root': node.node_identifier})
            menu.append(item)

        # MenuItem: 'Use EXIFTool on dir'
        if file_exists and is_dir and node.node_identifier.tree_type == TREE_TYPE_LOCAL_DISK:
            match = re.match(DATE_REGEX, node.name)
            if match:
                item = Gtk.MenuItem(label=f'Use EXIFTool on dir')
                item.connect('activate', self.send_signal, actions.CALL_EXIFTOOL, {'full_path': single_path})
                menu.append(item)

        # MenuItem: 'Delete tree'
        if file_exists and is_dir and not is_category_node:
            label = f'Delete tree "{node.name}"'
            if is_gdrive:
                label += ' from Google Drive'
            item = Gtk.MenuItem(label=label)
            node = self.con.display_store.get_node_data(tree_path)
            item.connect('activate', self.send_signal, actions.DELETE_SUBTREE, {'node': node})
            menu.append(item)

        # MenuItem: 'Delete'
        if file_exists and not is_dir:
            label = f'Delete "{node.name}"'
            if is_gdrive:
                label += ' from Google Drive'
            item = Gtk.MenuItem(label=label)
            item.connect('activate', self.send_signal, actions.DELETE_SINGLE_FILE, {'node': node})
            menu.append(item)

    def build_context_menu(self, tree_path: Gtk.TreePath, node: Node) -> Optional[Gtk.Menu]:
        """Dynamic context menu (right-click on tree item) for the given 'node' at 'tree_path'"""

        if node.is_ephemereal():
            # 'Loading' node, 'Empty' node, etc.
            return None

        menu = Gtk.Menu()

        if node.node_identifier.tree_type == TREE_TYPE_GDRIVE:
            single_path = self.con.display_store.derive_single_path_from_tree_path(tree_path, include_gdrive_prefix=False)
        else:
            assert node.node_identifier.tree_type == TREE_TYPE_LOCAL_DISK
            single_path = node.get_single_path()

        op: Optional[UserOp] = self.con.app.cacheman.get_last_pending_op_for_node(node.uid)
        if op and not op.is_completed() and op.has_dst():
            if SUPER_DEBUG:
                logger.debug(f'Building context menu for op: {op}')
            logger.warning('TODO: test this!')
            # Split into separate entries for src and dst.

            # (1/2) Source:
            if op.src_node.uid == node.uid:
                # src node
                src_path = single_path
            else:
                src_path = op.src_node.get_path_list()[0]
            item = TreeContextMenu.build_full_path_display_item(menu, 'Src: ', op.src_node, src_path)
            if op.src_node.is_live():
                src_submenu = Gtk.Menu()
                item.set_submenu(src_submenu)
                self._build_menu_items_for_single_node(src_submenu, tree_path, op.src_node, src_path)
            else:
                item.set_sensitive(False)

            # MenuItem: ---
            item = Gtk.SeparatorMenuItem()
            menu.append(item)

            # (2/2) Destination:
            if op.dst_node.uid == node.uid:
                # src node
                dst_path = single_path
            else:
                dst_path = op.src_node.get_path_list()[0]
            item = TreeContextMenu.build_full_path_display_item(menu, 'Dst: ', op.dst_node, dst_path)
            if op.dst_node.is_live():
                dst_submenu = Gtk.Menu()
                item.set_submenu(dst_submenu)
                self._build_menu_items_for_single_node(dst_submenu, tree_path, op.dst_node, dst_path)
            else:
                item.set_sensitive(False)

            item = Gtk.SeparatorMenuItem()
            menu.append(item)
        else:
            # Single item
            item = TreeContextMenu.build_full_path_display_item(menu, '', node, single_path)
            # gray it out
            item.set_sensitive(False)

            # MenuItem: ---
            item = Gtk.SeparatorMenuItem()
            menu.append(item)

            self._build_menu_items_for_single_node(menu, tree_path, node, single_path)

        if node.is_dir():
            item = Gtk.MenuItem(label=f'Expand all')
            item.connect('activate', self.send_signal, actions.EXPAND_ALL, {'tree_path': tree_path})
            menu.append(item)

        if node.is_live():
            # MenuItem: ---
            item = Gtk.SeparatorMenuItem()
            menu.append(item)

            # MenuItem: Refresh
            item = Gtk.MenuItem(label='Refresh')
            item.connect('activate', self.send_signal, actions.REFRESH_SUBTREE, {'node': node})
            menu.append(item)

        menu.show_all()
        return menu

    @staticmethod
    def build_full_path_display_item(menu: Gtk.Menu, preamble: str, node: Node, single_path: str) -> Gtk.MenuItem:
        path_display = single_path
        if node.get_tree_type() == TREE_TYPE_GDRIVE:
            path_display = GDRIVE_PATH_PREFIX + path_display

        item = Gtk.MenuItem(label='')
        label = item.get_child()
        full_path_display = GLib.markup_escape_text(path_display)
        label.set_markup(f'<i>{preamble}{full_path_display}</i>')
        menu.append(item)
        return item

    def send_signal(self, menu_item, signal: str, kwargs):
        dispatcher.send(signal=signal, sender=self.con.tree_id, **kwargs)
