from typing import Dict, Iterable, List, Optional, Set

from pydispatch import dispatcher
import logging

from backend.backend_interface import OutletBackend
from backend.diff.task.tree_diff_task import TreeDiffTask
from backend.executor.central import CentralExecutor, ExecPriority
from backend.cache_manager import CacheManager
from backend.icon_store import IconStorePy
from constants import DirConflictPolicy, DragOperation, FileConflictPolicy, IconId, TreeID
from model.device import Device
from model.display_tree.build_struct import DiffResultTreeIds, DisplayTreeRequest, RowsOfInterest
from model.display_tree.display_tree import DisplayTree
from model.display_tree.filter_criteria import FilterCriteria
from model.node.node import Node, SPIDNodePair
from model.node_identifier import GUID, NodeIdentifier, SinglePathNodeIdentifier
from model.uid import UID
from model.user_op import UserOp
from backend.uid.uid_generator import PersistentAtomicIntUidGenerator, UidGenerator
from backend.diff.task.tree_diff_merge_task import TreeDiffMergeTask
from signal_constants import ID_CENTRAL_EXEC, ID_LEFT_DIFF_TREE, ID_LEFT_TREE, ID_RIGHT_DIFF_TREE, ID_RIGHT_TREE, Signal
from util.task_runner import Task

logger = logging.getLogger(__name__)


class BackendIntegrated(OutletBackend):
    """
    ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
    CLASS BackendIntegrated
    ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼
    """

    def __init__(self, app_config):
        OutletBackend.__init__(self)
        self._app_config = app_config
        self.executor: CentralExecutor = CentralExecutor(self)
        self.uid_generator: UidGenerator = PersistentAtomicIntUidGenerator(app_config)
        self.cacheman: CacheManager = CacheManager(self)
        self.icon_cache = IconStorePy(self)

    def start(self):
        logger.debug('Starting backend')

        self.icon_cache.load_all_icons()
        OutletBackend.start(self)

        self.executor.start()

        self.cacheman.start()

    def shutdown(self):
        logger.debug('Shutting down backend')
        OutletBackend.shutdown(self)  # this will disconnect the listener for SHUTDOWN_APP as well

        dispatcher.send(Signal.SHUTDOWN_APP, sender=ID_CENTRAL_EXEC)

        self.cacheman = None
        self.executor = None

    def get_config(self, config_key: str, default_val: Optional[str] = None, required: bool = True) -> Optional[str]:
        return self._app_config.get(config_key, default_val, required)

    def get_config_list(self, config_key_list: List[str]) -> Dict[str, str]:
        response_dict: Dict[str, str] = {}
        for config_key in config_key_list:
            response_dict[config_key] = self._app_config.get(config_key)
        return response_dict

    def put_config(self, config_key: str, config_val: str):
        self._app_config.write(config_key, config_val)

    def put_config_list(self, config_dict: Dict[str, str]):
        for config_key, config_val in config_dict:
            self._app_config.write(config_key, config_val)

    def get_icon(self, icon_id: IconId) -> Optional:
        return self.icon_cache.get_icon(icon_id)

    def get_node_for_uid(self, uid: UID, device_uid: UID) -> Optional[Node]:
        return self.cacheman.get_node_for_uid(uid, device_uid)

    def next_uid(self) -> UID:
        return self.uid_generator.next_uid()

    def get_uid_for_local_path(self, full_path: str, uid_suggestion: Optional[UID] = None) -> UID:
        return self.cacheman.get_uid_for_local_path(full_path, uid_suggestion)

    def get_sn_for(self, node_uid: UID, device_uid: UID, full_path: str) -> Optional[SPIDNodePair]:
        return self.cacheman.get_sn_for(node_uid, device_uid, full_path)

    def request_display_tree(self, request: DisplayTreeRequest) -> Optional[DisplayTree]:
        if not request.tree_id:
            raise RuntimeError(f'Invalid DisplayTree request: tree_id cannot be null for {request}')

        return self.cacheman.request_display_tree(request)

    def start_subtree_load(self, tree_id: TreeID):
        self.cacheman.start_subtree_load(tree_id, send_signals=True)

    def get_op_execution_play_state(self) -> bool:
        return self.executor.enable_op_execution

    def get_device_list(self) -> List[Device]:
        return self.cacheman.get_device_list()

    def get_child_list(self, parent_spid: SinglePathNodeIdentifier, tree_id: TreeID, is_expanding_parent: bool = False, max_results: int = 0) -> \
            Iterable[SPIDNodePair]:
        return self.cacheman.get_child_list(parent_spid, tree_id, is_expanding_parent, max_results)

    def get_ancestor_list(self, spid: SinglePathNodeIdentifier, stop_at_path: Optional[str] = None) -> Iterable[SPIDNodePair]:
        return self.cacheman.get_ancestor_list_for_spid(spid, stop_at_path=stop_at_path)

    def set_selected_rows(self, tree_id: TreeID, selected: Set[GUID]):
        self.cacheman.set_selected_rows(tree_id, selected)

    def remove_expanded_row(self, row_uid: GUID, tree_id: TreeID):
        """AKA collapsing a row on the frontend"""
        self.cacheman.remove_expanded_row(row_uid, tree_id)

    def get_rows_of_interest(self, tree_id: TreeID) -> RowsOfInterest:
        return self.cacheman.get_rows_of_interest(tree_id)

    def drop_dragged_nodes(self, src_tree_id: TreeID, src_guid_list: List[GUID], is_into: bool, dst_tree_id: TreeID, dst_guid: GUID,
                           drag_operation: DragOperation, dir_conflict_policy: DirConflictPolicy, file_conflict_policy: FileConflictPolicy) -> bool:
        return self.cacheman.drop_dragged_nodes(src_tree_id, src_guid_list, is_into, dst_tree_id, dst_guid,
                                                drag_operation, dir_conflict_policy, file_conflict_policy)

    def start_diff_trees(self, tree_id_left: TreeID, tree_id_right: TreeID) -> DiffResultTreeIds:
        """Starts the Diff Trees task async"""
        assert tree_id_left == ID_LEFT_TREE and tree_id_right == ID_RIGHT_TREE, f'Wrong tree IDs: {ID_LEFT_TREE}, {ID_RIGHT_TREE}'
        tree_id_struct: DiffResultTreeIds = DiffResultTreeIds(ID_LEFT_DIFF_TREE, ID_RIGHT_DIFF_TREE)
        # submit with UserOp priority:
        self.executor.submit_async_task(Task(ExecPriority.P7_USER_OP_EXECUTION, TreeDiffTask.do_tree_diff,
                                        self, ID_CENTRAL_EXEC, tree_id_left, tree_id_right, tree_id_struct))
        return tree_id_struct

    def generate_merge_tree(self, tree_id_left: TreeID, tree_id_right: TreeID,
                            selected_change_list_left: List[GUID], selected_change_list_right: List[GUID]):
        task = TreeDiffMergeTask(self)
        task.generate_merge_tree(ID_CENTRAL_EXEC, tree_id_left, tree_id_right, selected_change_list_left, selected_change_list_right)

    def enqueue_refresh_subtree_task(self, node_identifier: NodeIdentifier, tree_id: TreeID):
        self.cacheman.enqueue_refresh_subtree_task(node_identifier, tree_id)

    def get_last_pending_op(self, device_uid: UID, node_uid: UID) -> Optional[UserOp]:
        return self.cacheman.get_last_pending_op_for_node(device_uid, node_uid)

    def download_file_from_gdrive(self, device_uid: UID, node_uid: UID, requestor_id: str):
        self.cacheman.download_file_from_gdrive(device_uid, node_uid, requestor_id)

    def delete_subtree(self, device_uid: UID, node_uid_list: List[UID]):
        self.cacheman.delete_subtree(device_uid, node_uid_list)

    def get_filter_criteria(self, tree_id: TreeID) -> Optional[FilterCriteria]:
        return self.cacheman.get_filter_criteria(tree_id)

    def update_filter_criteria(self, tree_id: TreeID, filter_criteria: FilterCriteria):
        self.cacheman.update_filter_criteria(tree_id, filter_criteria)
