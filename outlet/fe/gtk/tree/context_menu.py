import logging
from typing import Callable, Dict, List, Optional

from pydispatch import dispatcher

from constants import ActionID, MenuItemType
from model.context_menu import ContextMenuItem
from model.node.node import SPIDNodePair
from signal_constants import Signal

import gi
gi.require_version("Gtk", "3.0")
from gi.repository import GLib, Gtk

logger = logging.getLogger(__name__)


class TreeContextMenu:
    """
    ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
    CLASS TreeContextMenu

    Most of the client-run actions for GTK3 can be found in: ui.gtk.tree.tree_actions.TreeActions
    ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼
    """

    def __init__(self, controller):
        self.con = controller

        self._action_handler_dict: Dict[ActionID, Callable[[Optional[List[SPIDNodePair]], Optional[List[Gtk.TreePath]]], None]] = {
            ActionID.REFRESH: lambda sn_list, tree_path_list:
                self.con.backend.enqueue_refresh_subtree_task(sn_list[0].node.node_identifier, tree_id=self.con.tree_id),

            ActionID.EXPAND_ALL: lambda sn_list, tree_path_list:
                dispatcher.send(signal=Signal.EXPAND_ALL, sender=self.con.tree_id, tree_path=tree_path_list[0]),

            ActionID.GO_INTO_DIR: lambda sn_list, tree_path_list:
                self.con.app.backend.create_display_tree_from_spid(self.con.tree_id, sn_list[0].spid),

            ActionID.SHOW_IN_FILE_EXPLORER: lambda sn_list, tree_path_list:
                dispatcher.send(signal=Signal.SHOW_IN_NAUTILUS, sender=self.con.tree_id, full_path=sn_list[0].spid.get_single_path()),

            ActionID.OPEN_WITH_DEFAULT_APP: lambda sn_list, tree_path_list:
                dispatcher.send(signal=Signal.CALL_XDG_OPEN, sender=self.con.tree_id, full_path=sn_list[0].spid.get_single_path()),

            ActionID.DELETE_SINGLE_FILE: lambda sn_list, tree_path_list:
                dispatcher.send(signal=Signal.DELETE_SINGLE_FILE, sender=self.con.tree_id, node=sn_list[0].node),

            ActionID.DELETE_SUBTREE: lambda sn_list, tree_path_list:
                dispatcher.send(signal=Signal.DELETE_SUBTREE, sender=self.con.tree_id, node_list=[sn.node for sn in sn_list]),

            ActionID.DELETE_SUBTREE_FOR_SINGLE_DEVICE: lambda sn_list, tree_path_list:
                dispatcher.send(signal=Signal.DELETE_SUBTREE, sender=self.con.tree_id, node_list=[sn.node for sn in sn_list]),

            ActionID.DOWNLOAD_FROM_GDRIVE: lambda sn_list, tree_path_list:
                dispatcher.send(signal=Signal.DOWNLOAD_FROM_GDRIVE, sender=self.con.tree_id, node=sn_list[0].node),

            ActionID.SET_ROWS_CHECKED: lambda sn_list, tree_path_list:
                dispatcher.send(signal=Signal.SET_ROWS_CHECKED, sender=self.con.tree_id, tree_paths=tree_path_list),

            ActionID.SET_ROWS_UNCHECKED: lambda sn_list, tree_path_list:
                dispatcher.send(signal=Signal.SET_ROWS_UNCHECKED, sender=self.con.tree_id, tree_paths=tree_path_list),

            ActionID.CALL_EXIFTOOL:lambda sn_list, tree_path_list:
                dispatcher.send(signal=Signal.CALL_EXIFTOOL_LIST, sender=self.con.tree_id, sn_list=sn_list),
        }

    def build_context_menu(self, menu_item_list: List[ContextMenuItem], sn_list: List[SPIDNodePair], tree_path_list: List[Gtk.TreePath]) \
            -> Optional[Gtk.Menu]:

        menu = Gtk.Menu()

        for menu_item_meta in menu_item_list:
            item = self._build_menu_item(menu_item_meta, sn_list, tree_path_list)
            menu.append(item)

        menu.show_all()
        return menu

    def _build_menu_item(self, menu_item_meta: ContextMenuItem, sn_list: List[SPIDNodePair], tree_path_list: List[Gtk.TreePath]) -> Gtk.MenuItem:

        if menu_item_meta.item_type == MenuItemType.NORMAL:
            item = Gtk.MenuItem(label=menu_item_meta.title)

        elif menu_item_meta.item_type == MenuItemType.SEPARATOR:
            item = Gtk.SeparatorMenuItem()
        elif menu_item_meta.item_type == MenuItemType.ITALIC_DISABLED:
            item = Gtk.MenuItem(label='')
            label = item.get_child()
            display = GLib.markup_escape_text(menu_item_meta.title)
            label.set_markup(f'<i>{display}</i>')
            item.set_sensitive(False)
        elif menu_item_meta.item_type == MenuItemType.DISABLED:
            item = Gtk.MenuItem(label=menu_item_meta.title)
            item.set_sensitive(False)
        else:
            raise RuntimeError(f'Unrecognized menu item type: {menu_item_meta.item_type}')

        if menu_item_meta.action_id != ActionID.NO_ACTION:
            if menu_item_meta.target_guid_list:
                filtered_sn_list = list(filter(lambda sn: sn.spid.guid in menu_item_meta.target_guid_list, sn_list))
            else:
                filtered_sn_list = sn_list
            item.connect('activate', self._do_action, menu_item_meta.action_id, {'sn_list': filtered_sn_list, 'tree_path_list': tree_path_list})

        if menu_item_meta.submenu_item_list:
            submenu = Gtk.Menu()
            item.set_submenu(submenu)
            for submenu_menu_item_meta in menu_item_meta.submenu_item_list:
                submenu_item = self._build_menu_item(submenu_menu_item_meta, sn_list, tree_path_list)
                submenu.append(submenu_item)

        return item

    def _do_action(self, menu_item, action_id: ActionID, kwargs: dict):
        handler = self._action_handler_dict.get(action_id)
        if not handler:
            raise RuntimeError(f'Could not find handler for action_id: {action_id}')

        logger.debug(f'[{self.con.tree_id}] Handling action: {action_id.name}')

        sn_list = kwargs['sn_list']
        tree_path_list = kwargs['tree_path_list']

        handler(sn_list, tree_path_list)
