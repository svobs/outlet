import io
import logging
from typing import Dict, Iterable, List, Optional, Set

import grpc
from PIL import Image

from backend.agent.client.signal_receiver_thread import SignalReceiverThread
from backend.agent.client.zeroconf import OutletZeroconfListener
from backend.agent.grpc.conversion import GRPCConverter
from backend.agent.grpc.generated import Outlet_pb2_grpc
from backend.agent.grpc.generated.Outlet_pb2 import ConfigEntry, DeleteSubtree_Request, DownloadFromGDrive_Request, DragDrop_Request, \
    GenerateMergeTree_Request, GetAncestorList_Request, GetChildList_Request, GetConfig_Request, GetConfig_Response, GetDeviceList_Request, \
    GetFilter_Request, GetFilter_Response, GetIcon_Request, GetLastPendingOp_Request, GetLastPendingOp_Response, GetNextUid_Request, \
    GetNodeForUid_Request, GetOpExecPlayState_Request, GetRowsOfInterest_Request, GetSnFor_Request, GetUidForLocalPath_Request, PutConfig_Request, \
    RefreshSubtree_Request, RemoveExpandedRow_Request, RequestDisplayTree_Request, SetSelectedRowSet_Request, SignalMsg, SPIDNodePair, \
    StartDiffTrees_Request, StartDiffTrees_Response, StartSubtreeLoad_Request, TreeContextMenu_Request, UpdateFilter_Request
from backend.backend_interface import OutletBackend
from constants import DirConflictPolicy, DragOperation, ErrorHandlingStrategy, FileConflictPolicy, IconId, TRACE_ENABLED, TreeID
from error import ResultsExceededError
from model.context_menu import ContextMenuItem
from model.device import Device
from model.display_tree.build_struct import DiffResultTreeIds, DisplayTreeRequest, RowsOfInterest
from model.display_tree.display_tree import DisplayTree
from model.display_tree.filter_criteria import FilterCriteria
from model.node.node import Node
from model.node_identifier import GUID, NodeIdentifier, SinglePathNodeIdentifier
from model.uid import UID
from model.user_op import UserOp, UserOpType
from signal_constants import Signal
from util import daemon_util
from util.ensure import ensure_bool, ensure_int
from util.task_runner import Task, TaskRunner

logger = logging.getLogger(__name__)


class BackendGRPCClient(OutletBackend):
    """
    ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
    CLASS BackendGRPCClient

    GTK3 thin client which communicates with the OutletAgent via GRPC.
    ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼
    """
    def __init__(self, cfg):
        OutletBackend.__init__(self)
        self._app_config = cfg
        self.connection_timeout_sec = int(self._app_config.get_config('thin_client.connection_timeout_sec'))

        self._started = False
        self.channel = None
        self.grpc_stub: Optional[Outlet_pb2_grpc.OutletStub] = None
        self._converter = GRPCConverter(self)
        self.signal_thread: SignalReceiverThread = SignalReceiverThread(self, self._converter)

        # TODO: confirm this hasn't broken
        self._fe_task_runner = TaskRunner(max_workers=1)
        """Only needed to generate UIDs which are unique to drag & drop. Looking increasingly kludgey"""

        self._cached_device_list: List[Device] = []

    def start(self):
        if self._started:
            logger.debug('Already started. Ignoring call to start()')
            return

        logger.debug('Starting up BackendGRPCClient')
        OutletBackend.start(self)

        # This is not a gRPC call, but I couldn't find a better place to put it:
        self.connect_dispatch_listener(signal=Signal.ENQUEUE_UI_TASK, receiver=self._on_ui_task_requested)

        self.connect_dispatch_listener(signal=Signal.HANDLE_BATCH_FAILED, receiver=self._on_handle_batch_failed)

        # Some requests are so simple that they can be encapsulated by a single signal:
        self.forward_signal_to_server(signal=Signal.PAUSE_OP_EXECUTION)
        self.forward_signal_to_server(signal=Signal.RESUME_OP_EXECUTION)
        self.forward_signal_to_server(signal=Signal.COMPLETE_MERGE)
        self.forward_signal_to_server(signal=Signal.DEREGISTER_DISPLAY_TREE)
        self.forward_signal_to_server(signal=Signal.EXIT_DIFF_MODE)

        # TODO: make these into cmd line args
        use_fixed_address = ensure_bool(self._app_config.get_config('agent.grpc.use_fixed_address'))
        if use_fixed_address:
            address = self._app_config.get_config('agent.grpc.fixed_address')
            port = ensure_int(self._app_config.get_config('agent.grpc.fixed_port'))
            logger.info(f'Config specifies fixed server address = {address}:{port}')
            self.connect(address, port)
        else:
            zeroconf_timeout_sec = int(self._app_config.get_config('thin_client.zeroconf_discovery_timeout_sec'))

            with OutletZeroconfListener(self) as listener:
                if not listener.wait_for_successful_connect(zeroconf_timeout_sec):
                    raise RuntimeError(f'Timed out looking for server (timeout={zeroconf_timeout_sec}s)!')

    def connect(self, address, port):
        grpc_server_address = f'{address}:{port}'
        self.channel = grpc.insecure_channel(grpc_server_address)
        self.grpc_stub = Outlet_pb2_grpc.OutletStub(self.channel)

        if not self._wait_for_connect():
            raise RuntimeError(f'gRPC failed to connect to server (timeout={self.connection_timeout_sec})')

        self._started = True
        self.signal_thread.start()

    def shutdown(self):
        OutletBackend.shutdown(self)

        if self.channel:
            self.channel.close()
            self.channel = None

        if ensure_bool(self._app_config.get_config('thin_client.kill_server_on_client_shutdown')):
            logger.debug('Configured to kill backend agent: looking for processes to kill...')
            daemon_util.terminate_daemon_if_found()

    def _wait_for_connect(self) -> bool:
        logger.debug(f'Waiting for gRPC to connect to server (timeout_sec={self.connection_timeout_sec})')
        try:
            grpc.channel_ready_future(self.channel).result(timeout=self.connection_timeout_sec)
            logger.info(f'gRPC client connected successfully')
            return True
        except grpc.FutureTimeoutError:
            return False

    def forward_signal_to_server(self, signal: Signal):
        def _forward_signal(sender: str):
            self.grpc_stub.send_signal(SignalMsg(sig_int=signal, sender=sender))

        self.connect_dispatch_listener(signal=signal, receiver=_forward_signal, weak=False)

    def _on_ui_task_requested(self, sender, task_func, *args):
        # FIXME: need to revisit Linux frontend...
        raise RuntimeError("FIXME! This is broken")  # personal note
        task = Task(None, task_func, args)
        self._fe_task_runner.enqueue_task(task)

    def _on_handle_batch_failed(self, sender, batch_uid: UID, error_handling_strategy: ErrorHandlingStrategy):
        signal = SignalMsg(sig_int=Signal.HANDLE_BATCH_FAILED, sender=sender)
        signal.handle_batch_failed.batch_uid = batch_uid
        signal.handle_batch_failed.error_handling_strategy = error_handling_strategy
        self.grpc_stub.send_signal(signal)

    def get_config(self, config_key: str, default_val: Optional[str] = None, required: bool = True) -> Optional[str]:
        """Note: params 'default_val' and 'required' are only enforced locally, not by the server"""
        logger.debug(f'Getting config "{config_key}" (default_val={default_val}, required={required})')
        request = GetConfig_Request()
        request.config_key_list.append(config_key)

        response: GetConfig_Response = self.grpc_stub.get_config(request)
        assert len(response.config_list) <= 1, f'Expected 1 or less entry in response but found {len(response.config_list)} for key "{config_key}"'
        if len(response.config_list) == 1:
            return response.config_list[0].val
        else:
            if required:
                raise RuntimeError(f'Config entry not found but is required: "{config_key}"')
            else:
                return default_val

    def get_config_list(self, config_key_list: List[str]) -> Dict[str, str]:
        request = GetConfig_Request()
        for config_key in config_key_list:
            request.config_key_list.append(config_key)
        response: GetConfig_Response = self.grpc_stub.get_config(request)
        config_dict: Dict[str, str] = {}
        for config in response.config_list:
            config_dict[config.key] = config.val
        return config_dict

    def put_config(self, config_key: str, config_val: str):
        logger.debug(f'Putting config "{config_key}" = "{config_val}"')
        request = PutConfig_Request()
        config = ConfigEntry(key=config_key, val=str(config_val))
        request.config_list.append(config)
        self.grpc_stub.put_config(request)

    def put_config_list(self, config_dict: Dict[str, str]):
        request = PutConfig_Request()
        for config_key, config_val in config_dict:
            config = ConfigEntry(key=config_key, val=config_val)
            request.config_list.append(config)
        self.grpc_stub.put_config(request)

    def get_icon(self, icon_id: IconId) -> Optional:
        request = GetIcon_Request()
        request.icon_id = icon_id
        response = self.grpc_stub.get_icon(request)
        if response.HasField('icon'):
            assert icon_id == response.icon.icon_id
            img_byte_arr = io.BytesIO(response.icon.content)
            return Image.open(img_byte_arr)
        return None

    def get_node_for_uid(self, uid: UID, device_uid: UID) -> Optional[Node]:
        request = GetNodeForUid_Request()
        request.uid = uid
        request.device_uid = device_uid

        grpc_response = self.grpc_stub.get_node_for_uid(request)
        return self._converter.optional_node_from_grpc_container(grpc_response)

    def next_uid(self) -> UID:
        request = GetNextUid_Request()
        grpc_response = self.grpc_stub.get_next_uid(request)
        return UID(grpc_response.uid)

    def get_uid_for_local_path(self, full_path: str, uid_suggestion: Optional[UID] = None) -> UID:
        request = GetUidForLocalPath_Request()
        request.full_path = full_path
        request.uid_suggestion = uid_suggestion
        grpc_response = self.grpc_stub.get_uid_for_local_path(request)
        return UID(grpc_response.uid)

    def get_sn_for(self, node_uid: UID, device_uid: UID, full_path: str) -> Optional[SPIDNodePair]:
        request = GetSnFor_Request()
        request.node_uid = node_uid
        request.device_uid = device_uid
        request.full_path = full_path
        grpc_response = self.grpc_stub.get_sn_for(request)
        return self._converter.sn_from_grpc(grpc_response.sn)

    def request_display_tree(self, request: DisplayTreeRequest) -> Optional[DisplayTree]:
        assert request.tree_id, f'No tree_id in: {request}'
        grpc_req = RequestDisplayTree_Request()
        grpc_req.is_startup = request.is_startup
        if request.tree_id:
            grpc_req.tree_id = request.tree_id
        grpc_req.return_async = request.return_async
        if request.device_uid:
            grpc_req.device_uid = request.device_uid
        if request.user_path:
            grpc_req.user_path = request.user_path
        self._converter.node_identifier_to_grpc(request.spid, grpc_req.spid)
        grpc_req.tree_display_mode = request.tree_display_mode

        response = self.grpc_stub.request_display_tree(grpc_req)

        if response.HasField('display_tree_ui_state'):
            state = self._converter.display_tree_ui_state_from_grpc(response.display_tree_ui_state)
            tree = state.to_display_tree(backend=self)
        else:
            tree = None
        logger.debug(f'Returning tree: {tree}')
        return tree

    def start_subtree_load(self, tree_id: TreeID):
        request = StartSubtreeLoad_Request()
        request.tree_id = tree_id
        self.grpc_stub.start_subtree_load(request)

    def get_op_execution_play_state(self) -> bool:
        response = self.grpc_stub.get_op_exec_play_state(GetOpExecPlayState_Request())
        logger.debug(f'Got op execution state from backend server: is_playing={response.is_enabled}')
        return response.is_enabled

    def get_device_list(self) -> List[Device]:
        if not self._cached_device_list:
            request = GetDeviceList_Request()
            response = self.grpc_stub.get_device_list(request)
            device_list = []
            for grpc_device in response.device_list:
                device_list.append(Device(device_uid=grpc_device.device_uid, long_device_id=grpc_device.long_device_id,
                                          tree_type=grpc_device.tree_type, friendly_name=grpc_device.friendly_name))
            self._cached_device_list = device_list
        return self._cached_device_list

    def get_child_list(self, parent_spid: SinglePathNodeIdentifier, tree_id: TreeID, is_expanding_parent: bool = False, use_filter: bool = False,
                       max_results: int = 0) -> Iterable[SPIDNodePair]:
        if TRACE_ENABLED:
            logger.debug(f'[{tree_id}] Entered get_child_list(): parent_spid={parent_spid}')
        assert tree_id, f'GRPCClient.get_child_list(): No tree_id provided!'
        assert max_results >= 0, f'Bad value for max_results: {max_results}'

        request = GetChildList_Request()
        self._converter.node_identifier_to_grpc(parent_spid, request.parent_spid)
        request.tree_id = tree_id
        request.is_expanding_parent = is_expanding_parent
        request.max_results = max_results

        if use_filter:
            # might want to support this later?
            raise NotImplementedError('use_filter==True not supported via gRPC!')

        response = self.grpc_stub.get_child_list_for_spid(request)

        if response.result_exceeded_count > 0:
            # Convert to exception on this side
            assert max_results > 0, f'Got nonzero result_exceeded_count ({response.result_exceeded_count}) but max_results was 0!'
            raise ResultsExceededError(response.result_exceeded_count)
        return self._converter.sn_list_from_grpc(response.child_list)

    def set_selected_rows(self, tree_id: TreeID, selected: Set[GUID]):
        request = SetSelectedRowSet_Request()
        for guid in selected:
            # Note: gRPC Python uses "append" for repeated scalar fields, and "add" for repeated object fields
            request.selected_row_guid_set.append(guid)
        request.tree_id = tree_id
        self.grpc_stub.set_selected_row_set(request)

    def remove_expanded_row(self, row_uid: GUID, tree_id: TreeID):
        request = RemoveExpandedRow_Request()
        request.node_guid = row_uid
        request.tree_id = tree_id
        self.grpc_stub.remove_expanded_row(request)

    def get_rows_of_interest(self, tree_id: TreeID) -> RowsOfInterest:
        request = GetRowsOfInterest_Request()
        request.tree_id = tree_id
        response = self.grpc_stub.get_rows_of_interest(request)
        rows = RowsOfInterest()
        for guid in response.expanded_row_guid_set:
            rows.expanded.add(guid)
        for guid in response.selected_row_guid_set:
            rows.selected.add(guid)
        return rows

    def get_context_menu(self, tree_id: TreeID, identifier_list: List[NodeIdentifier]) -> List[ContextMenuItem]:
        request = TreeContextMenu_Request()
        request.tree_id = tree_id

        for node_identifier in identifier_list:
            node_identifier_grpc = request.identifier_list.add()
            self._converter.node_identifier_to_grpc(node_identifier, node_identifier_grpc)
        response = self.grpc_stub.get_context_menu(request)
        return self._converter.menu_item_list_from_grpc(response.menu_item_list)

    def get_ancestor_list(self, spid: SinglePathNodeIdentifier, stop_at_path: Optional[str] = None) -> Iterable[SPIDNodePair]:
        request = GetAncestorList_Request()
        if stop_at_path:
            request.stop_at_path = stop_at_path
        self._converter.node_identifier_to_grpc(spid, request.spid)

        response = self.grpc_stub.get_ancestor_list_for_spid(request)
        return self._converter.sn_list_from_grpc(response.ancestor_list)

    def drop_dragged_nodes(self, src_tree_id: TreeID, src_guid_list: List[GUID], is_into: bool, dst_tree_id: TreeID, dst_guid: GUID,
                           drag_operation: DragOperation, dir_conflict_policy: DirConflictPolicy, file_conflict_policy: FileConflictPolicy) -> bool:
        request = DragDrop_Request()
        request.src_tree_id = src_tree_id
        request.dst_tree_id = dst_tree_id
        request.is_into = is_into
        request.dst_guid = dst_guid
        request.drag_operation = drag_operation
        request.dir_conflict_policy = dir_conflict_policy
        request.file_conflict_policy = file_conflict_policy

        for src_guid in src_guid_list:
            request.src_guid_list.append(src_guid)

        response = self.grpc_stub.drop_dragged_nodes(request)
        return response.is_accepted

    def start_diff_trees(self, tree_id_left: TreeID, tree_id_right: TreeID) -> DiffResultTreeIds:
        request = StartDiffTrees_Request()
        request.tree_id_left = tree_id_left
        request.tree_id_right = tree_id_right
        response: StartDiffTrees_Response = self.grpc_stub.start_diff_trees(request)

        return DiffResultTreeIds(response.tree_id_left, response.tree_id_right)

    def generate_merge_tree(self, tree_id_left: TreeID, tree_id_right: TreeID,
                            selected_change_list_left: List[GUID],selected_change_list_right: List[GUID]):
        request = GenerateMergeTree_Request()
        request.tree_id_left = tree_id_left
        request.tree_id_right = tree_id_right

        for guid in selected_change_list_left:
            request.change_list_left.append(guid)

        for guid in selected_change_list_right:
            request.change_list_right.append(guid)

        self.grpc_stub.generate_merge_tree(request)

    def enqueue_refresh_subtree_task(self, node_identifier: NodeIdentifier, tree_id: TreeID):
        request = RefreshSubtree_Request()
        self._converter.node_identifier_to_grpc(node_identifier, request.node_identifier)
        request.tree_id = tree_id
        self.grpc_stub.refresh_subtree(request)

    def get_last_pending_op(self, device_uid: UID, node_uid: UID) -> Optional[UserOp]:
        request = GetLastPendingOp_Request()
        request.device_uid = device_uid
        request.node_uid = node_uid
        response: GetLastPendingOp_Response = self.grpc_stub.get_last_pending_op_for_node(request)
        if not response.HasField('user_op'):
            return None

        src_node = self._converter.node_from_grpc(response.user_op.src_node)
        dst_node = self._converter.node_from_grpc(response.user_op.dst_node)
        op_type = UserOpType(response.user_op.op_type)
        return UserOp(response.user_op.op_uid, response.user_op.batch_uid, op_type, src_node, dst_node, response.user_op.create_ts)

    def download_file_from_gdrive(self, device_uid: UID, node_uid: UID, requestor_id: str):
        request = DownloadFromGDrive_Request(device_uid=device_uid, node_uid=node_uid, requestor_id=requestor_id)
        self.grpc_stub.download_file_from_gdrive(request)

    def delete_subtree(self, device_uid: UID, node_uid_list: List[UID]):
        request = DeleteSubtree_Request()
        request.device_uid = device_uid
        for node_uid in node_uid_list:
            request.node_uid_list.append(node_uid)
        self.grpc_stub.delete_subtree(request)

    def get_filter_criteria(self, tree_id: TreeID) -> Optional[FilterCriteria]:
        request = GetFilter_Request()
        request.tree_id = tree_id
        response: GetFilter_Response = self.grpc_stub.get_filter(request)
        if response.HasField('filter_criteria'):
            return self._converter.filter_criteria_from_grpc(response.filter_criteria)
        else:
            return None

    def update_filter_criteria(self, tree_id: TreeID, filter_criteria: FilterCriteria):
        request = UpdateFilter_Request()
        request.tree_id = tree_id
        self._converter.filter_criteria_to_grpc(filter_criteria, request.filter_criteria)
        self.grpc_stub.update_filter(request)
