import logging
import re
from typing import Dict, List, Optional

from constants import ActionID, DATE_REGEX, MenuItemType, TreeID, TreeType
from logging_constants import SUPER_DEBUG_ENABLED
from model.context_menu import ContextMenuItem
from model.device import Device
from model.node.node import Node, SPIDNodePair
from model.node_identifier import GUID
from model.uid import UID
from model.user_op import UserOp
from util.has_lifecycle import HasLifecycle

logger = logging.getLogger(__name__)


class ContextMenuBuilder(HasLifecycle):
    def __init__(self, backend, action_manager):
        HasLifecycle.__init__(self)
        self.backend = backend
        self.action_manager = action_manager

    def start(self):
        logger.debug(f'[ContextMenuManager] Startup started')
        HasLifecycle.start(self)
        logger.debug(f'[ContextMenuManager] Startup done')

    def shutdown(self):
        logger.debug(f'[ContextMenuManager] Shutdown started')
        HasLifecycle.shutdown(self)
        logger.debug(f'[ContextMenuManager] Shutdown done')

    def build_context_menu(self, tree_id: TreeID, guid_list: List[GUID]) -> List[ContextMenuItem]:
        sn_list: List[SPIDNodePair] = self.backend.cacheman.get_sn_list_for_guid_list(guid_list, tree_id=tree_id)

        if len(sn_list) == 1:
            return self._build_context_menu_for_single_sn(sn_list[0], tree_id)
        else:
            return self._build_context_menu_for_sn_list(sn_list, guid_list, tree_id)

    def _build_submenu_for_op_node(self, op: UserOp, op_node: Node, op_node_label: str, clicked_sn: SPIDNodePair, tree_id: TreeID) \
            -> ContextMenuItem:
        if op_node.device_uid == clicked_sn.node.device_uid and op_node.uid == clicked_sn.node.uid:
            target_sn = clicked_sn
        else:
            path = op_node.get_path_list()[0]  # this will only be multiple for GDrive, and logically all paths are relevant
            target_sn = self.backend.cacheman.get_sn_for(device_uid=op_node.device_uid, node_uid=op_node.uid, full_path=path)

        if op.src_node.is_live():
            item_type = MenuItemType.NORMAL
            submenu_item_list = self._build_menu_items_for_single_node(target_sn, tree_id)
        else:
            item_type = MenuItemType.ITALIC_DISABLED
            submenu_item_list = []
        title = f'{op_node_label}: {target_sn.spid.get_single_path()}'
        item = ContextMenuItem(item_type=item_type, title=title, action_id=ActionID.NO_ACTION)
        item.submenu_item_list = submenu_item_list
        return item

    def _build_context_menu_for_single_sn(self, sn: SPIDNodePair, tree_id: TreeID) -> List[ContextMenuItem]:
        """Dynamic context menu (right-click on tree item) for the given SPIDNodePair"""

        menu_item_list = []
        if sn.node.is_ephemereal():
            # 'Loading' node, 'Empty' node, etc.
            return menu_item_list

        single_path = sn.spid.get_single_path()

        op: Optional[UserOp] = self.backend.cacheman.get_last_pending_op_for_node(device_uid=sn.node.device_uid, node_uid=sn.node.uid)
        if op and op.has_dst():
            if SUPER_DEBUG_ENABLED:
                logger.debug(f'Building context menu for op: {op}')
            logger.warning('TODO: test this!')  # FIXME: test and then remove this msg
            # Split into separate entries for src and dst.

            # (1/2) Source:
            item = self._build_submenu_for_op_node(op, op.src_node, 'Src', sn, tree_id)
            menu_item_list.append(item)

            # MenuItem: ---
            menu_item_list.append(ContextMenuItem.separator())

            # (2/2) Destination:
            item = self._build_submenu_for_op_node(op, op.dst_node, 'Dst', sn, tree_id)
            menu_item_list.append(item)

            # MenuItem: ---
            menu_item_list.append(ContextMenuItem.separator())

        else:
            # Single item
            item = ContextMenuItem(item_type=MenuItemType.ITALIC_DISABLED, title=single_path, action_id=ActionID.NO_ACTION)
            menu_item_list.append(item)

            # MenuItem: ---
            menu_item_list.append(ContextMenuItem.separator())

            for item in self._build_menu_items_for_single_node(sn, tree_id):
                menu_item_list.append(item)

        if sn.node.is_dir():
            item = ContextMenuItem(item_type=MenuItemType.NORMAL, title='Expand All', action_id=ActionID.EXPAND_ALL)
            item.target_guid_list = [sn.spid.guid]
            menu_item_list.append(item)

        if sn.node.is_live():
            # MenuItem: ---
            menu_item_list.append(ContextMenuItem.separator())

            # MenuItem: Refresh
            item = ContextMenuItem(item_type=MenuItemType.NORMAL, title='Refresh', action_id=ActionID.REFRESH)
            item.target_guid_list = [sn.spid.guid]
            menu_item_list.append(item)

        return menu_item_list

    def _build_menu_items_for_single_node(self, sn: SPIDNodePair, tree_id: TreeID) -> List[ContextMenuItem]:
        if sn.node.is_container_node():
            return []

        tree_type = self.backend.cacheman.get_tree_type_for_device_uid(sn.spid.device_uid)
        menu_item_list = []

        # MenuItem: 'Show in Nautilus'
        if sn.node.is_live() and tree_type == TreeType.LOCAL_DISK:
            item = ContextMenuItem(item_type=MenuItemType.NORMAL, title='Show in File Explorer', action_id=ActionID.SHOW_IN_FILE_EXPLORER)
            item.target_guid_list = [sn.spid.guid]
            menu_item_list.append(item)

        # MenuItem: 'Download from Google Drive' [GDrive] OR 'Open with default app' [Local]
        if sn.node.is_live() and not sn.node.is_dir():
            if tree_type == TreeType.GDRIVE:
                item = ContextMenuItem(item_type=MenuItemType.NORMAL, title='Download from Google Drive', action_id=ActionID.DOWNLOAD_FROM_GDRIVE)
                item.target_guid_list = [sn.spid.guid]
                menu_item_list.append(item)
            elif tree_type == TreeType.LOCAL_DISK:
                item = ContextMenuItem(item_type=MenuItemType.NORMAL, title='Open with Default App', action_id=ActionID.OPEN_WITH_DEFAULT_APP)
                item.target_guid_list = [sn.spid.guid]
                menu_item_list.append(item)

        # Label: Does not exist
        if not sn.node.is_live():
            # We have already filtered out container nodes, so this only leaves us with planning nodes
            item = ContextMenuItem(item_type=MenuItemType.ITALIC_DISABLED, title='Does not exist', action_id=ActionID.NO_ACTION)
            menu_item_list.append(item)

        tree_meta = self.backend.cacheman.get_active_display_tree_meta(tree_id)
        if not tree_meta:
            raise RuntimeError(f'No DisplayTreeMeta found for tree_id "{tree_id}"')

        if sn.node.is_live():
            if sn.node.is_dir():
                # MenuItem: 'Go Into {dir}'
                if tree_meta.can_change_root():
                    item = ContextMenuItem(item_type=MenuItemType.NORMAL, title=f'Go Into "{sn.node.name}"', action_id=ActionID.GO_INTO_DIR)
                    item.target_guid_list = [sn.spid.guid]
                    menu_item_list.append(item)

                # MenuItem: 'Use EXIFTool on dir'
                if tree_type == TreeType.LOCAL_DISK:
                    match = re.match(DATE_REGEX, sn.node.name)
                    if match:
                        item = ContextMenuItem(item_type=MenuItemType.NORMAL, title=f'Use EXIFTool on Dir"', action_id=ActionID.CALL_EXIFTOOL)
                        item.target_guid_list = [sn.spid.guid]
                        menu_item_list.append(item)

                # MenuItem: 'Delete tree'
                title = f'Delete Tree "{sn.node.name}"'
                if tree_type == TreeType.GDRIVE:
                    title += ' from Google Drive'
                item = ContextMenuItem(item_type=MenuItemType.NORMAL, title=title, action_id=ActionID.DELETE_SUBTREE)
                item.target_guid_list = [sn.spid.guid]
                menu_item_list.append(item)

            else:  # not dir
                # MenuItem: 'Delete'
                title = f'Delete "{sn.node.name}"'
                if tree_type == TreeType.GDRIVE:
                    title += ' from Google Drive'
                item = ContextMenuItem(item_type=MenuItemType.NORMAL, title=title, action_id=ActionID.DELETE_SINGLE_FILE)
                item.target_guid_list = [sn.spid.guid]
                menu_item_list.append(item)

        self._add_custom_action_list([sn.node], [sn.spid.guid], menu_item_list)

        return menu_item_list

    def _build_context_menu_for_sn_list(self, selected_sn_list: List[SPIDNodePair], selected_guid_list: List[GUID], tree_id: TreeID) \
            -> List[ContextMenuItem]:
        menu_item_list = []

        # Show number of items selected
        item = ContextMenuItem(item_type=MenuItemType.ITALIC_DISABLED, title=f'{len(selected_sn_list)} items selected', action_id=ActionID.NO_ACTION)
        menu_item_list.append(item)

        tree_meta = self.backend.cacheman.get_active_display_tree_meta(tree_id)
        if not tree_meta:
            raise RuntimeError(f'No DisplayTreeMeta found for tree_id "{tree_id}"')

        if tree_meta.has_checkboxes():
            item = ContextMenuItem(item_type=MenuItemType.NORMAL, title=f'Check All"', action_id=ActionID.SET_ROWS_CHECKED)
            item.target_guid_list = selected_guid_list
            menu_item_list.append(item)

            item = ContextMenuItem(item_type=MenuItemType.NORMAL, title=f'Uncheck All"', action_id=ActionID.SET_ROWS_UNCHECKED)
            item.target_guid_list = selected_guid_list
            menu_item_list.append(item)

        # KLUDGE! Prob will remove this anyway:
        is_localdisk = len(selected_sn_list) > 0 and selected_sn_list[0].node.tree_type == TreeType.LOCAL_DISK
        if is_localdisk:
            item = ContextMenuItem(item_type=MenuItemType.NORMAL, title=f'Use EXIFTool on Dirs"', action_id=ActionID.CALL_EXIFTOOL)
            item.target_guid_list = selected_guid_list
            menu_item_list.append(item)

        # Show an option to delete nodes (sort nodes by subtree; display option for each subtree found)
        guid_list_to_delete_by_device_uid: Dict[UID, List[GUID]] = {}
        for selected_sn in selected_sn_list:
            if selected_sn.node.is_live():
                guid_list_for_device = guid_list_to_delete_by_device_uid.get(selected_sn.spid.device_uid, None)
                if guid_list_for_device is None:
                    guid_list_for_device = []
                    guid_list_to_delete_by_device_uid[selected_sn.spid.device_uid] = guid_list_for_device
                guid_list_for_device.append(selected_sn.spid.guid)

        device_list: List[Device] = self.backend.get_device_list()
        for device_uid, guid_list in guid_list_to_delete_by_device_uid.items():
            device = None
            for some_device in device_list:
                if some_device.uid == device_uid:
                    device = some_device
                    break

            title = f'Delete {len(guid_list)} Items from {device.friendly_name}'
            item = ContextMenuItem(item_type=MenuItemType.NORMAL, title=title, action_id=ActionID.DELETE_SUBTREE_FOR_SINGLE_DEVICE)
            item.target_guid_list = guid_list
            menu_item_list.append(item)

        self._add_custom_action_list([sn.node for sn in selected_sn_list], selected_guid_list, menu_item_list)

        return menu_item_list

    # Adds custom actions, if applicable:
    def _add_custom_action_list(self, node_list: List[Node], guid_list: List[GUID], menu_item_list):
        for custom_action in self.action_manager.get_custom_action_list():
            try:
                if custom_action.is_enabled_for(node_list):
                    title = custom_action.get_label(node_list)
                    item = ContextMenuItem(item_type=MenuItemType.NORMAL, title=title, action_id=custom_action.action_id)
                    item.target_guid_list = guid_list
                    menu_item_list.append(item)
            except RuntimeError:
                logger.exception(f'Failed to evaluate enablement of custom action: {custom_action}')