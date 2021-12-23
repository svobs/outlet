import importlib
import logging
import os
from pathlib import Path
from typing import Callable, Dict, List, Optional, TypeVar

from pydispatch import dispatcher

from backend.executor.central import ExecPriority
from constants import ActionID, CONFIG_PY_DIR, CONFIG_PY_MODULE, INIT_FILE, LOGGING_CONSTANTS_FILE, MAX_ROWS_PER_ACTIVATION, TreeID, TreeType
from logging_constants import SUPER_DEBUG_ENABLED
from model.display_tree.tree_action import TreeAction
from model.node.node import Node, SPIDNodePair
from model.node_identifier import GUID
from model.user_op import UserOp
from signal_constants import ID_ACTION_MANAGER, Signal
from util.file_util import get_resource_path
from util.has_lifecycle import HasLifecycle
from util.task_runner import Task

logger = logging.getLogger(__name__)


class CustomMenuAction:
    def __init__(self, action_id, get_label_func, is_enabled_func, run_func):
        self.action_id: int = action_id
        self._get_label_func: Callable = get_label_func
        self._is_enabled_func: Callable = is_enabled_func
        self._run_func: Callable = run_func

    def get_label(self, node_list: List[Node]) -> bool:
        return self._get_label_func(node_list)

    def is_enabled_for(self, node_list: List[Node]) -> bool:
        return self._is_enabled_func(node_list)

    def execute(self, node_list: List[Node]):
        self._run_func(node_list)

    def __repr__(self):
        return f'CustomMenuAction[action_id={self.action_id} label="{self.get_label([])}"]'


class ActionManager(HasLifecycle):
    def __init__(self, backend):
        HasLifecycle.__init__(self)
        self.backend = backend

        self._action_handler_dict: Dict[int, Callable[[TreeAction], None]] = {
            ActionID.DELETE_SUBTREE_FOR_SINGLE_DEVICE: self._delete_subtree_for_single_device,
            ActionID.DELETE_SUBTREE: self._delete_subtree_for_single_device,
            ActionID.DELETE_SINGLE_FILE: self._delete_single_file,
            ActionID.ACTIVATE: self._activate,
            ActionID.REFRESH: self._refresh_subtree
        }

        self._custom_module_dict: Dict = {}
        self._custom_action_dict: Dict[int, CustomMenuAction] = {}

        self.reload_custom_action_handlers_before_invoke: bool = backend.get_config('tree_action.custom.reload_handlers_before_each_invoke')

    def start(self):
        logger.debug(f'[ActionManager] Startup started')
        HasLifecycle.start(self)
        self._load_custom_actions()
        logger.debug(f'[ActionManager] Startup done')

    def shutdown(self):
        logger.debug(f'[ActionManager] Shutdown started')
        HasLifecycle.shutdown(self)
        logger.debug(f'[ActionManager] Shutdown done')

    def _load_custom_actions(self):
        logger.debug(f'Loading custom actions from disk')
        custom_action_dict = {}
        mod_name_list = []
        for file in os.listdir(get_resource_path(CONFIG_PY_DIR)):
            if Path(file).suffix == '.py' and file != LOGGING_CONSTANTS_FILE and file != INIT_FILE:
                mod_name_list.append(Path(file).stem)

        for mod_name in mod_name_list:
            fullname = f'{CONFIG_PY_MODULE}.{mod_name}'
            module = self._custom_module_dict.get(fullname)
            if module:
                module = importlib.reload(module)
            else:
                module = importlib.import_module(f'{CONFIG_PY_MODULE}.{mod_name}')

            self._custom_module_dict[fullname] = module

            try:
                action_id = getattr(module, 'action_id')
                get_label_func = getattr(module, 'get_label')
                run_func = getattr(module, 'run')
                is_enabled_func = getattr(module, 'is_enabled')

                if action_id and get_label_func and run_func and is_enabled_func:
                    if action_id <= ActionID.ACTIVATE:
                        logger.error(f'Error loading custom action: module "{mod_name}" has an invalid action_id ({action_id}) - skipping')
                    else:
                        action = CustomMenuAction(action_id=action_id, get_label_func=get_label_func,
                                                  is_enabled_func=is_enabled_func, run_func=run_func)
                        if action_id in custom_action_dict:
                            raise RuntimeError(f'Whie loading custom action from module "{mod_name}": '
                                               f'there is already a custom action with action_id {action_id}')
                        custom_action_dict[action_id] = action
                        logger.info(f'Loaded custom action from module "{mod_name}": action_id={action_id}')
            except AttributeError as err:
                if SUPER_DEBUG_ENABLED:
                    logger.debug(f'Skipping load of file: {err}')
        self._custom_action_dict = custom_action_dict

    def get_custom_action_list(self) -> List[CustomMenuAction]:
        # TODO: sort by actionID
        return list(self._custom_action_dict.values())

    def execute_tree_action_list(self, tree_action_list: List[TreeAction]):
        assert tree_action_list, f'tree_action_list is empty!'
        for tree_action in tree_action_list:
            self._execute_tree_action(tree_action)

    def _execute_tree_action(self, tree_action: TreeAction):
        action_handler = self._action_handler_dict.get(tree_action.action_id)
        if action_handler:
            logger.debug(f'[{tree_action.tree_id}] Calling handler for action {ActionID(tree_action.action_id).name} '
                         f'target_guid_list={tree_action.target_guid_list}')
            action_handler(tree_action)
        else:
            if self.reload_custom_action_handlers_before_invoke:
                self._load_custom_actions()

            custom_action_handler: Optional[CustomMenuAction] = self._custom_action_dict.get(tree_action.action_id)
            if custom_action_handler:
                self.backend.executor.submit_async_task(Task(ExecPriority.P4_LONG_RUNNING_USER_TASK, self._execute_custom_action,
                                                             custom_action_handler, tree_action))
            else:
                raise RuntimeError(f'Backend cannot find an action handler for action_id {tree_action.action_id}')

    def _execute_custom_action(self, this_task: Task, custom_action_handler, tree_action):
        logger.debug(f'[{tree_action.tree_id}] Calling handler for custom action id={tree_action.action_id} '
                     f'target_guid_list={tree_action.target_guid_list}')
        node_list = self._get_node_list_for_guid_list(tree_action.target_guid_list, tree_action.tree_id)
        try:
            custom_action_handler.execute(node_list)
        except RuntimeError as err:
            self.backend.report_exception(sender=ID_ACTION_MANAGER, msg=f'Error running action', error=err)

    def _get_node_list_for_guid_list(self, guid_list: List[GUID], tree_id: TreeID):
        sn_list = self._get_sn_list_for_guid_list(guid_list, tree_id)
        return [sn.node for sn in sn_list]

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
            self.backend.report_error(sender=ID_ACTION_MANAGER, msg='Too many rows selected',
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
            if _is_true_for_all_in_list(cxt.target_guid_list, lambda guid: not self.backend.cacheman.is_row_expanded(row_guid=guid,
                                                                                                                     tree_id=cxt.tree_id)):
                action = TreeAction(cxt.tree_id, action_id=ActionID.EXPAND_ROWS, target_guid_list=cxt.target_guid_list)
            else:
                # Collapse by default
                action = TreeAction(cxt.tree_id, action_id=ActionID.COLLAPSE_ROWS, target_guid_list=cxt.target_guid_list)

            dispatcher.send(sender=ID_ACTION_MANAGER, signal=Signal.EXECUTE_ACTION, action_list=[action])
            return
        elif file_count == len(target_sn_list):  # All files
            action_list: [TreeAction] = []
            for sn in target_sn_list:
                action = self._activate_file_sn(sn, cxt)
                if action:
                    action_list.append(action)

            dispatcher.send(sender=ID_ACTION_MANAGER, signal=Signal.EXECUTE_ACTION, action_list=action_list)

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

    def _refresh_subtree(self, act: TreeAction):
        target_sn_list = self._get_sn_list_for_guid_list(guid_list=act.target_guid_list, tree_id=act.tree_id)
        node_identifier_list = [sn.node.node_identifier for sn in target_sn_list]
        logger.info(f'Enqueuing task to refresh subtree at {node_identifier_list}')
        for node_identifier in node_identifier_list:
            self.backend.executor.submit_async_task(Task(ExecPriority.P1_USER_LOAD, self._refresh_subtree, node_identifier, act.tree_id))


T = TypeVar('T')


def _is_true_for_all_in_list(obj_list: List[T], evaluation_func: Callable[[T], bool]) -> bool:
    for obj in obj_list:
        if evaluation_func(obj):
            return True
    return False
