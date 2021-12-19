import logging
import re
from typing import Any, Callable, Dict, List, Optional, TypeVar

from constants import ActionID, DATE_REGEX, MAX_ROWS_PER_ACTIVATION, MenuItemType, SUPER_DEBUG_ENABLED, TreeID, TreeType
from model.context_menu import ContextMenuItem
from model.device import Device
from model.display_tree.tree_action import TreeAction
from model.node.node import Node, SPIDNodePair
from model.node_identifier import GUID
from model.uid import UID
from model.user_op import UserOp
from signal_constants import ID_CONTEXT_MENU_MANAGER, Signal
from util.has_lifecycle import HasLifecycle
from pydispatch import dispatcher

logger = logging.getLogger(__name__)


class ContextMenuManager(HasLifecycle):
    def __init__(self, backend):
        HasLifecycle.__init__(self)
        self.backend = backend

        self._action_handler_dict: Dict[ActionID, Callable[[TreeAction], None]] = {
            ActionID.DELETE_SUBTREE_FOR_SINGLE_DEVICE: self._delete_subtree_for_single_device,
            ActionID.DELETE_SUBTREE: self._delete_subtree_for_single_device,
            ActionID.DELETE_SINGLE_FILE: self._delete_single_file,
            ActionID.ACTIVATE: self._activate
        }

    def start(self):
        logger.debug(f'[ContextMenuManager] Startup started')
        HasLifecycle.start(self)
        logger.debug(f'[ContextMenuManager] Startup done')

    def shutdown(self):
        logger.debug(f'[ContextMenuManager] Shutdown started')
        HasLifecycle.shutdown(self)
        logger.debug(f'[ContextMenuManager] Shutdown done')

    def get_context_menu(self, tree_id: TreeID, guid_list: List[GUID]) -> List[ContextMenuItem]:
        sn_list: List[SPIDNodePair] = self.backend.cacheman.get_sn_list_for_guid_list(guid_list, tree_id=tree_id)

        if len(sn_list) == 1:
            return self.build_context_menu_for_single_sn(sn_list[0], tree_id)
        else:
            return self.build_context_menu_for_sn_list(sn_list, tree_id)

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

    def build_context_menu_for_single_sn(self, sn: SPIDNodePair, tree_id: TreeID) -> List[ContextMenuItem]:
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
            menu_item_list.append(item)

        if sn.node.is_live():
            # MenuItem: ---
            menu_item_list.append(ContextMenuItem.separator())

            # MenuItem: Refresh
            item = ContextMenuItem(item_type=MenuItemType.NORMAL, title='Refresh', action_id=ActionID.REFRESH)
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
            menu_item_list.append(item)

        # MenuItem: 'Download from Google Drive' [GDrive] OR 'Open with default app' [Local]
        if sn.node.is_live() and not sn.node.is_dir():
            if tree_type == TreeType.GDRIVE:
                item = ContextMenuItem(item_type=MenuItemType.NORMAL, title='Download from Google Drive', action_id=ActionID.DOWNLOAD_FROM_GDRIVE)
                menu_item_list.append(item)
            elif tree_type == TreeType.LOCAL_DISK:
                item = ContextMenuItem(item_type=MenuItemType.NORMAL, title='Open with Default App', action_id=ActionID.OPEN_WITH_DEFAULT_APP)
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
                    menu_item_list.append(item)

                # MenuItem: 'Use EXIFTool on dir'
                if tree_type == TreeType.LOCAL_DISK:
                    match = re.match(DATE_REGEX, sn.node.name)
                    if match:
                        item = ContextMenuItem(item_type=MenuItemType.NORMAL, title=f'Use EXIFTool on Dir"', action_id=ActionID.CALL_EXIFTOOL)
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

        return menu_item_list

    def build_context_menu_for_sn_list(self, selected_sn_list: List[SPIDNodePair], tree_id: TreeID) -> List[ContextMenuItem]:
        menu_item_list = []

        # Show number of items selected
        item = ContextMenuItem(item_type=MenuItemType.ITALIC_DISABLED, title=f'{len(selected_sn_list)} items selected', action_id=ActionID.NO_ACTION)
        menu_item_list.append(item)

        tree_meta = self.backend.cacheman.get_active_display_tree_meta(tree_id)
        if not tree_meta:
            raise RuntimeError(f'No DisplayTreeMeta found for tree_id "{tree_id}"')

        if tree_meta.has_checkboxes():
            item = ContextMenuItem(item_type=MenuItemType.NORMAL, title=f'Check All"', action_id=ActionID.SET_ROWS_CHECKED)
            menu_item_list.append(item)

            item = ContextMenuItem(item_type=MenuItemType.NORMAL, title=f'Uncheck All"', action_id=ActionID.SET_ROWS_UNCHECKED)
            menu_item_list.append(item)

        # KLUDGE! Prob will remove this anyway:
        is_localdisk = len(selected_sn_list) > 0 and selected_sn_list[0].node.tree_type == TreeType.LOCAL_DISK
        if is_localdisk:
            item = ContextMenuItem(item_type=MenuItemType.NORMAL, title=f'Use EXIFTool on Dirs"', action_id=ActionID.CALL_EXIFTOOL)
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

        return menu_item_list

    def execute_tree_action_list(self, tree_action_list: List[TreeAction]):
        assert tree_action_list, f'tree_action_list is empty!'
        for tree_action in tree_action_list:
            self._execute_tree_action(tree_action)

    def _execute_tree_action(self, tree_action: TreeAction):
        action_handler = self._action_handler_dict.get(tree_action.action_id)
        if not action_handler:
            raise RuntimeError(f'Backend cannot find an action handler for: {tree_action.action_id.name}')

        logger.debug(f'[{tree_action.tree_id}] Calling ActionHandler for action {tree_action.action_id} target_guid_list={tree_action.target_guid_list}')
        action_handler(tree_action)

    def _get_sn_list_for_guid_list(self, guid_list: List[GUID], tree_id: TreeID):
        return [self.backend.cacheman.get_sn_for_guid(guid=guid, tree_id=tree_id) for guid in guid_list]

    def _delete_single_file(self, cxt: TreeAction):
        if not cxt.target_guid_list:
            raise RuntimeError(f'Cannot delete file: no nodes provided!')
        if len(cxt.target_guid_list) > 1:
            raise RuntimeError(f'Cannot call DeleteSingleFile for multiple nodes!')

        return self._delete_subtree_for_single_device(cxt)

    def _delete_subtree_for_single_device(self, cxt: TreeAction):
        if not cxt.target_guid_list:
            raise RuntimeError(f'Cannot delete subtree: no nodes provided!')
        sn_list: List[SPIDNodePair] = self._get_sn_list_for_guid_list(guid_list=cxt.target_guid_list, tree_id=cxt.tree_id)
        device_uid = sn_list[0].spid.device_uid
        node_uid_list = []
        for sn in sn_list:
            if sn.node.device_uid != device_uid:
                logger.error(f'Found unexpected device: {sn.node.device_uid} (expected: {device_uid})')
                raise RuntimeError(f'Invalid: cannot call _delete_subtree_for_single_device() for more than a single device!')
            node_uid_list.append(sn.node.uid)
        self.backend.delete_subtree(device_uid, node_uid_list)

    def _activate(self, cxt: TreeAction):
        """Default 'activation' of node(s) in tree (e.g. double-click or Enter)"""
        if not cxt.target_guid_list:
            raise RuntimeError(f'Cannot do "activation": no nodes provided!')

        if len(cxt.target_guid_list) > MAX_ROWS_PER_ACTIVATION:
            self.backend.report_error(sender=ID_CONTEXT_MENU_MANAGER, msg='Too many rows selected',
                                      secondary_msg=f'You selected {len(cxt.target_guid_list)} items, which is too many for you.\n\n'
                                      f'Try selecting less items first. This message exists for your protection. You child.')

        target_sn_list = self._get_sn_list_for_guid_list(guid_list=cxt.target_guid_list, tree_id=cxt.tree_id)

        dir_count = 0
        file_count = 0

        for sn in target_sn_list:
            if sn.node.is_dir():
                dir_count += 1
            elif sn.node.is_file():
                file_count += 1

        if dir_count == len(target_sn_list):  # All dirs
            # Expand/collapse row:
            if _is_true_for_all_in_list(cxt.target_guid_list, lambda guid: not self.backend.cacheman.is_row_expanded(guid)):
                action = TreeAction(cxt.tree_id, action_id=ActionID.EXPAND_ROWS, target_guid_list=cxt.target_guid_list)
            else:
                # Collapse by default
                action = TreeAction(cxt.tree_id, action_id=ActionID.COLLAPSE_ROWS, target_guid_list=cxt.target_guid_list)

            dispatcher.send(sender=ID_CONTEXT_MENU_MANAGER, signal=Signal.EXECUTE_ACTION, action_list=[action])
            return
        elif file_count == len(target_sn_list):  # All files
            action_list: [TreeAction] = []
            for sn in target_sn_list:
                action = self._activate_file_sn(sn, cxt)
                if action:
                    action_list.append(action)

            dispatcher.send(sender=ID_CONTEXT_MENU_MANAGER, signal=Signal.EXECUTE_ACTION, action_list=action_list)

    def _activate_file_sn(self, sn, cxt: TreeAction):
        """
        Attempt to open it no matter where it is. Includes the last pending operation.
        In the future, we should enhance this so that it will find the most convenient copy anywhere and open that copy.
       """

        if sn.node.is_live():
            return self._create_action_for_file_node(sn.node, cxt.tree_id)

        op: Optional[UserOp] = self.backend.get_last_pending_op(sn.node.uid)
        if op and op.has_dst():
            logger.warning('TODO: test this!')

            if op.src_node.is_live():
                return self._create_action_for_file_node(op.src_node, cxt.tree_id)
            elif op.dst_node.is_live():
                return self._create_action_for_file_node(op.dst_node, cxt.tree_id)
        else:
            logger.debug(f'Aborting activation: node does not exist: {sn.node}')
        return False

    @staticmethod
    def _create_action_for_file_node(node: Node, tree_id: TreeID) -> Optional[TreeAction]:
        if node.tree_type == TreeType.LOCAL_DISK:
            return TreeAction(tree_id, ActionID.OPEN_WITH_DEFAULT_APP, [], [node])
        elif node.tree_type == TreeType.GDRIVE:
            return TreeAction(tree_id, ActionID.DOWNLOAD_FROM_GDRIVE, [], [node])
        return None


T = TypeVar('T')


def _is_true_for_all_in_list(obj_list: List[T], evaluation_func: Callable[[T], bool]) -> bool:
    for obj in obj_list:
        if evaluation_func(obj):
            return True
    return False
