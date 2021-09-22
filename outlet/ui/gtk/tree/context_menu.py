import logging
import re
from typing import Dict, List, Optional

import gi
from pydispatch import dispatcher

from constants import DATE_REGEX, SUPER_DEBUG_ENABLED, TreeType
from model.device import Device
from model.node.node import Node, SPIDNodePair
from model.uid import UID
from model.user_op import UserOp
from signal_constants import Signal

gi.require_version("Gtk", "3.0")
from gi.repository import GLib, Gtk

logger = logging.getLogger(__name__)


class TreeContextMenu:
    """
    ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
    CLASS TreeContextMenu
    ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼
    """

    def __init__(self, controller):
        self.con = controller

    # TODO: does this account for logical nodes?
    def build_context_menu_multiple(self, selected_sn_list: List[SPIDNodePair], selected_tree_paths: List[Gtk.TreePath]) -> Optional[Gtk.Menu]:
        menu = Gtk.Menu()

        # Show number of items selected
        item = Gtk.MenuItem(label='')
        label = item.get_child()
        display = GLib.markup_escape_text(f'{len(selected_sn_list)} items selected')
        label.set_markup(f'<i>{display}</i>')
        item.set_sensitive(False)
        menu.append(item)

        if self.con.treeview_meta.has_checkboxes:
            tree_paths: List[Gtk.TreePath] = []
            for sn, path in zip(selected_sn_list, selected_tree_paths):
                # maybe I'm nitpicking here
                if not sn.node.is_ephemereal():
                    tree_paths.append(path)

            item = Gtk.MenuItem(label=f'Check All')
            item.connect('activate', self.send_signal, Signal.SET_ROWS_CHECKED, {'tree_paths': tree_paths})
            menu.append(item)

            item = Gtk.MenuItem(label=f'Uncheck All')
            item.connect('activate', self.send_signal, Signal.SET_ROWS_UNCHECKED, {'tree_paths': tree_paths})
            menu.append(item)

        is_localdisk = len(selected_sn_list) > 0 and selected_sn_list[0].spid.tree_type == TreeType.LOCAL_DISK
        if is_localdisk:
            item = Gtk.MenuItem(label=f'Use EXIFTool on Dirs')
            item.connect('activate', self.send_signal, Signal.CALL_EXIFTOOL_LIST, {'sn_list': selected_sn_list})
            menu.append(item)

        # Show an option to delete nodes (sort nodes by subtree; display option for each subtree found)
        items_to_delete_by_device_uid: Dict[UID, List[Node]] = {}
        for selected_sn in selected_sn_list:
            if selected_sn.node.is_live():
                node_list_for_device = items_to_delete_by_device_uid.get(selected_sn.spid.device_uid, None)
                if node_list_for_device is None:
                    node_list_for_device = []
                    items_to_delete_by_device_uid[selected_sn.spid.device_uid] = node_list_for_device
                node_list_for_device.append(selected_sn.node)

        device_list: List[Device] = self.con.backend.get_device_list()
        for device_uid, node_list in items_to_delete_by_device_uid.items():
            device = None
            for some_device in device_list:
                if some_device.uid == device_uid:
                    device = some_device
                    break

            item = Gtk.MenuItem(label=f'Delete {len(node_list)} Items from {device.friendly_name}')
            item.connect('activate', self.send_signal, Signal.DELETE_SUBTREE, {'node_list': node_list})
            menu.append(item)

        menu.show_all()
        return menu

    def _build_menu_items_for_single_node(self, menu, tree_path, node: Node, single_path: str):
        if node.is_container_node():
            return

        is_gdrive = node.node_identifier.tree_type == TreeType.GDRIVE

        # MenuItem: 'Show in Nautilus'
        if node.is_live() and node.node_identifier.tree_type == TreeType.LOCAL_DISK:
            item = Gtk.MenuItem(label='Show in Nautilus')
            item.connect('activate', self.send_signal, Signal.SHOW_IN_NAUTILUS, {'full_path': single_path})
            menu.append(item)

        # MenuItem: 'Download from Google Drive' [GDrive] OR 'Open with default app' [Local]
        if node.is_live() and not node.is_dir():
            if is_gdrive:
                item = Gtk.MenuItem(label=f'Download from Google Drive')
                item.connect('activate', self.send_signal, Signal.DOWNLOAD_FROM_GDRIVE, {'node': node})
                menu.append(item)
            elif node.node_identifier.tree_type == TreeType.LOCAL_DISK:
                item = Gtk.MenuItem(label=f'Open with Default App')
                item.connect('activate', self.send_signal, Signal.CALL_XDG_OPEN, {'full_path': single_path})
                menu.append(item)

        # Label: Does not exist
        if not node.is_live():
            # FIXME: this is not entirely correct when examining logical nodes which represent real paths
            item = Gtk.MenuItem(label='')
            label = item.get_child()
            label.set_markup(f'<i>Does not exist</i>')
            item.set_sensitive(False)
            menu.append(item)

        # MenuItem: 'Go Into {dir}'
        if node.is_live() and node.is_dir() and self.con.treeview_meta.can_change_root:
            item = Gtk.MenuItem(label=f'Go Into "{node.name}"')

            def go_into(menu_item):
                spid = self.con.app.backend.node_identifier_factory.for_values(uid=node.uid, device_uid=node.device_uid, tree_type=node.tree_type,
                                                                               path_list=single_path, must_be_single_path=True)
                self.con.app.backend.create_display_tree_from_spid(self.con.tree_id, spid)

            item.connect('activate', go_into)
            menu.append(item)

        # MenuItem: 'Use EXIFTool on dir'
        if node.is_live() and node.is_dir() and node.node_identifier.tree_type == TreeType.LOCAL_DISK:
            match = re.match(DATE_REGEX, node.name)
            if match:
                item = Gtk.MenuItem(label=f'Use EXIFTool on Dir')
                item.connect('activate', self.send_signal, Signal.CALL_EXIFTOOL, {'full_path': single_path})
                menu.append(item)

        # MenuItem: 'Delete tree'
        if node.is_live() and node.is_dir() and not node.is_display_only():
            label = f'Delete Tree "{node.name}"'
            if is_gdrive:
                label += ' from Google Drive'
            item = Gtk.MenuItem(label=label)
            item.connect('activate', self.send_signal, Signal.DELETE_SUBTREE, {'node_list': [node]})
            menu.append(item)

        # MenuItem: 'Delete'
        if node.is_live() and not node.is_dir():
            label = f'Delete "{node.name}"'
            if is_gdrive:
                label += ' from Google Drive'
            item = Gtk.MenuItem(label=label)
            item.connect('activate', self.send_signal, Signal.DELETE_SINGLE_FILE, {'node': node})
            menu.append(item)

    def build_context_menu_single(self, tree_path: Gtk.TreePath, sn: SPIDNodePair) -> Optional[Gtk.Menu]:
        """Dynamic context menu (right-click on tree item) for the given 'node' at 'tree_path'"""

        if sn.node.is_ephemereal():
            # 'Loading' node, 'Empty' node, etc.
            return None

        menu = Gtk.Menu()

        single_path = sn.spid.get_single_path()

        op: Optional[UserOp] = self.con.app.backend.get_last_pending_op(sn.node.uid)
        if op and op.has_dst():
            if SUPER_DEBUG_ENABLED:
                logger.debug(f'Building context menu for op: {op}')
            logger.warning('TODO: test this!')  # FIXME: test and then remove this msg
            # Split into separate entries for src and dst.

            # (1/2) Source:
            if op.src_node.uid == sn.node.uid:
                # src node
                src_path = single_path
            else:
                src_path = op.src_node.get_path_list()[0]
            item = TreeContextMenu.build_full_path_display_item('Src: ', op.src_node, src_path)
            if op.src_node.is_live():
                src_submenu = Gtk.Menu()
                item.set_submenu(src_submenu)
                # FIXME: add BE call to retrieve SPIDNodePair from node_uid + path
                self._build_menu_items_for_single_node(src_submenu, tree_path, op.src_node, src_path)
            else:
                item.set_sensitive(False)
            menu.append(item)

            # MenuItem: ---
            menu.append(Gtk.SeparatorMenuItem())

            # (2/2) Destination:
            if op.dst_node.uid == sn.node.uid:
                # src node
                dst_path = single_path
            else:
                dst_path = op.src_node.get_path_list()[0]
            item = TreeContextMenu.build_full_path_display_item('Dst: ', op.dst_node, dst_path)
            if op.dst_node.is_live():
                dst_submenu = Gtk.Menu()
                item.set_submenu(dst_submenu)
                self._build_menu_items_for_single_node(dst_submenu, tree_path, op.dst_node, dst_path)
            else:
                item.set_sensitive(False)
            menu.append(item)

            # MenuItem: ---
            menu.append(Gtk.SeparatorMenuItem())
        else:
            # Single item
            item = TreeContextMenu.build_full_path_display_item('', sn.node, single_path)
            # gray it out
            item.set_sensitive(False)
            menu.append(item)

            # MenuItem: ---
            menu.append(Gtk.SeparatorMenuItem())

            self._build_menu_items_for_single_node(menu, tree_path, sn.node, single_path)

        if sn.node.is_dir():
            item = Gtk.MenuItem(label=f'Expand All')
            item.connect('activate', self.send_signal, Signal.EXPAND_ALL, {'tree_path': tree_path})
            menu.append(item)

        if sn.node.is_live():
            # MenuItem: ---
            menu.append(Gtk.SeparatorMenuItem())

            # MenuItem: Refresh
            item = Gtk.MenuItem(label='Refresh')
            item.connect('activate', self.refresh_subtree, sn.node.node_identifier)
            menu.append(item)

        menu.show_all()
        return menu

    @staticmethod
    def build_full_path_display_item(preamble: str, node: Node, single_path: str) -> Gtk.MenuItem:
        item = Gtk.MenuItem(label='')
        label = item.get_child()
        full_path_display = GLib.markup_escape_text(single_path)
        label.set_markup(f'<i>{preamble}{full_path_display}</i>')
        return item

    def send_signal(self, menu_item, signal: Signal, kwargs: dict):
        logger.debug(f'[{self.con.tree_id}] Sending signal: {signal.name} with kwargs: {kwargs}')
        dispatcher.send(signal=signal, sender=self.con.tree_id, **kwargs)

    def refresh_subtree(self, menu_item, node_identifier):
        self.con.backend.enqueue_refresh_subtree_task(node_identifier, tree_id=self.con.tree_id)
