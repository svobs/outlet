import logging
from typing import Dict, Iterable, List, Optional

from zeroconf import ServiceBrowser, Zeroconf

from backend.backend_interface import OutletBackend
from backend.daemon.client.signal_receiver_thread import SignalReceiverThread
from backend.daemon.client.zeroconf import OutletZeroconfListener
from backend.daemon.grpc.conversion import GRPCConverter
from backend.daemon.grpc.generated import Outlet_pb2_grpc
from backend.daemon.grpc.generated.Outlet_pb2 import ConfigEntry, DeleteSubtree_Request, DownloadFromGDrive_Request, DragDrop_Request, \
    GenerateMergeTree_Request, \
    GetAncestorList_Request, GetChildList_Request, \
    GetConfig_Request, GetConfig_Response, GetFilter_Request, GetFilter_Response, GetLastPendingOp_Request, \
    GetLastPendingOp_Response, GetNextUid_Request, \
    GetNodeForLocalPath_Request, \
    GetNodeForUid_Request, \
    GetOpExecPlayState_Request, \
    GetUidForLocalPath_Request, \
    PutConfig_Request, RefreshSubtree_Request, RefreshSubtreeStats_Request, RequestDisplayTree_Request, SignalMsg, \
    SPIDNodePair, StartDiffTrees_Request, StartDiffTrees_Response, StartSubtreeLoad_Request, UpdateFilter_Request
from constants import SUPER_DEBUG, ZEROCONF_SERVICE_TYPE
from model.display_tree.build_struct import DiffResultTreeIds, DisplayTreeRequest
from model.display_tree.display_tree import DisplayTree
from model.display_tree.filter_criteria import FilterCriteria
from model.node.node import Node
from model.node_identifier import NodeIdentifier, SinglePathNodeIdentifier
from model.uid import UID
from model.user_op import UserOp, UserOpType
from signal_constants import Signal
from util import daemon_util
from util.ensure import ensure_bool, ensure_int
from util.task_runner import TaskRunner
import grpc

logger = logging.getLogger(__name__)


class BackendGRPCClient(OutletBackend):
    """
    ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
    CLASS BackendGRPCClient

    GTK3 thin client which communicates with the OutletDaemon via GRPC.
    ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼
    """
    def __init__(self, cfg):
        OutletBackend.__init__(self)
        self._config = cfg
        self.connection_timeout_sec = int(self._config.get('thin_client.connection_timeout_sec'))

        self._started = False
        self.channel = None
        self.grpc_stub: Optional[Outlet_pb2_grpc.OutletStub] = None
        self.signal_thread: SignalReceiverThread = SignalReceiverThread(self)

        self._fe_task_runner = TaskRunner()
        """Only needed to generate UIDs which are unique to drag & drop"""

    def start(self):
        if self._started:
            logger.debug('Already started. Ignoring call to start()')
            return

        logger.debug('Starting up BackendGRPCClient')
        OutletBackend.start(self)

        self.connect_dispatch_listener(signal=Signal.ENQUEUE_UI_TASK, receiver=self._on_ui_task_requested)
        self.connect_dispatch_listener(signal=Signal.DOWNLOAD_ALL_GDRIVE_META, receiver=self._on_gdrive_download_meta_requested)

        # Some requests are so simple that they can be encapsulated by a single signal:
        self.connect_dispatch_listener(signal=Signal.PAUSE_OP_EXECUTION, receiver=self._send_pause_op_exec_signal)
        self.connect_dispatch_listener(signal=Signal.RESUME_OP_EXECUTION, receiver=self._send_resume_op_exec_signal)
        self.connect_dispatch_listener(signal=Signal.COMPLETE_MERGE, receiver=self._send_complete_merge_signal)

        # TODO: hmm...looks like a chicken & egg problem here. Ideally we should get the config from the server
        use_fixed_address = ensure_bool(self._config.get('grpc.use_fixed_address'))
        if use_fixed_address:
            address = self._config.get('grpc.fixed_address')
            port = ensure_int(self._config.get('grpc.fixed_port'))
            logger.debug(f'Config specifies fixed server address = {address}:{port}')
            self.connect(address, port)
        else:
            zeroconf_timeout_sec = int(self._config.get('thin_client.zeroconf_discovery_timeout_sec'))
            zeroconf = Zeroconf()
            try:
                listener = OutletZeroconfListener(zeroconf, self)
                ServiceBrowser(zeroconf, ZEROCONF_SERVICE_TYPE, listener)
                if not listener.wait_for_successful_connect(zeroconf_timeout_sec):
                    raise RuntimeError(f'Timed out looking for server (timeout={zeroconf_timeout_sec}s)!')
            finally:
                zeroconf.close()

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

        if ensure_bool(self._config.get('thin_client.kill_server_on_client_shutdown')):
            logger.debug('Configured to kill backend daemon: looking for processes to kill...')
            daemon_util.terminate_daemon_if_found()

    def _wait_for_connect(self) -> bool:
        logger.debug(f'Waiting for gRPC to connect to server (timeout_sec={self.connection_timeout_sec})')
        try:
            grpc.channel_ready_future(self.channel).result(timeout=self.connection_timeout_sec)
            logger.info(f'gRPC client connected successfully')
            return True
        except grpc.FutureTimeoutError:
            return False

    def _send_pause_op_exec_signal(self, sender: str):
        self.grpc_stub.send_signal(SignalMsg(sig_int=Signal.PAUSE_OP_EXECUTION, sender=sender))

    def _send_resume_op_exec_signal(self, sender: str):
        self.grpc_stub.send_signal(SignalMsg(sig_int=Signal.RESUME_OP_EXECUTION, sender=sender))

    def _send_complete_merge_signal(self, sender: str):
        self.grpc_stub.send_signal(SignalMsg(sig_int=Signal.COMPLETE_MERGE, sender=sender))

    def _on_ui_task_requested(self, sender, task_func, *args):
        self._fe_task_runner.enqueue(task_func, *args)

    def _on_gdrive_download_meta_requested(self, sender):
        self.grpc_stub.send_signal(SignalMsg(sig_int=Signal.DOWNLOAD_ALL_GDRIVE_META, sender=sender))

    def get_config(self, config_key: str, default_val: Optional[str] = None) -> Optional[str]:
        logger.debug(f'Getting config "{config_key}"')
        request = GetConfig_Request()
        request.config_key_list.append(config_key)
        response: GetConfig_Response = self.grpc_stub.get_config(request)
        assert len(response.config_list) == 1, f'Expected exactly 1 entry in response but found {len(response.config_list)} for key "{config_key}"'
        config_val = response.config_list[0].val
        if config_val:
            return config_val
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

    def get_node_for_uid(self, uid: UID, tree_type: int = None) -> Optional[Node]:
        request = GetNodeForUid_Request()
        request.uid = uid
        if tree_type:
            request.tree_type = tree_type

        grpc_response = self.grpc_stub.get_node_for_uid(request)
        return GRPCConverter.optional_node_from_grpc_container(grpc_response)

    def get_node_for_local_path(self, full_path: str) -> Optional[Node]:
        request = GetNodeForLocalPath_Request()
        request.full_path = full_path
        grpc_response = self.grpc_stub.get_node_for_local_path(request)
        return GRPCConverter.optional_node_from_grpc_container(grpc_response)

    def next_uid(self) -> UID:
        request = GetNextUid_Request()
        grpc_response = self.grpc_stub.get_next_uid(request)
        return UID(grpc_response.uid)

    def get_uid_for_local_path(self, full_path: str, uid_suggestion: Optional[UID] = None, override_load_check: bool = False) -> UID:
        request = GetUidForLocalPath_Request()
        request.full_path = full_path
        request.uid_suggestion = uid_suggestion
        grpc_response = self.grpc_stub.get_uid_for_local_path(request)
        return UID(grpc_response.uid)

    def request_display_tree(self, request: DisplayTreeRequest) -> Optional[DisplayTree]:
        assert request.tree_id, f'No tree_id in: {request}'
        grpc_req = RequestDisplayTree_Request()
        grpc_req.is_startup = request.is_startup
        if request.tree_id:
            grpc_req.tree_id = request.tree_id
        grpc_req.return_async = request.return_async
        if request.user_path:
            grpc_req.user_path = request.user_path
        GRPCConverter.node_identifier_to_grpc(request.spid, grpc_req.spid)
        grpc_req.tree_display_mode = request.tree_display_mode

        response = self.grpc_stub.request_display_tree_ui_state(grpc_req)

        if response.HasField('display_tree_ui_state'):
            state = GRPCConverter.display_tree_ui_state_from_grpc(response.display_tree_ui_state)
            tree = state.to_display_tree(backend=self)
        else:
            tree = None
        logger.debug(f'Returning tree: {tree}')
        return tree

    def start_subtree_load(self, tree_id: str):
        request = StartSubtreeLoad_Request()
        request.tree_id = tree_id
        self.grpc_stub.start_subtree_load(request)

    def get_op_execution_play_state(self) -> bool:
        response = self.grpc_stub.get_op_exec_play_state(GetOpExecPlayState_Request())
        logger.debug(f'Got op execution state from backend server: is_playing={response.is_enabled}')
        return response.is_enabled

    def get_children(self, parent: Node, tree_id: str) -> Iterable[Node]:
        if SUPER_DEBUG:
            logger.debug(f'[{tree_id}] Entered get_children(): parent={parent}')
        assert tree_id, f'GRPCClient.get_children(): No tree_id provided!'

        request = GetChildList_Request()
        request.tree_id = tree_id
        GRPCConverter.node_to_grpc(parent, request.parent_node)

        response = self.grpc_stub.get_child_list_for_node(request)
        return GRPCConverter.node_list_from_grpc(response.node_list)

    def get_ancestor_list(self, spid: SinglePathNodeIdentifier, stop_at_path: Optional[str] = None) -> Iterable[Node]:
        request = GetAncestorList_Request()
        if stop_at_path:
            request.stop_at_path = stop_at_path
        GRPCConverter.node_identifier_to_grpc(spid, request.spid)

        response = self.grpc_stub.get_ancestor_list_for_spid(request)
        return GRPCConverter.node_list_from_grpc(response.node_list)

    def drop_dragged_nodes(self, src_tree_id: str, src_sn_list: List[SPIDNodePair], is_into: bool, dst_tree_id: str, dst_sn: SPIDNodePair):
        request = DragDrop_Request()
        request.src_tree_id = src_tree_id
        request.dst_tree_id = dst_tree_id
        request.is_into = is_into

        for src_sn in src_sn_list:
            grpc_sn = request.src_sn_list.add()
            GRPCConverter.sn_to_grpc(src_sn, grpc_sn)

        GRPCConverter.sn_to_grpc(dst_sn, request.dst_sn)

        self.grpc_stub.drop_dragged_nodes(request)

    def start_diff_trees(self, tree_id_left: str, tree_id_right: str) -> DiffResultTreeIds:
        request = StartDiffTrees_Request()
        request.tree_id_left = tree_id_left
        request.tree_id_right = tree_id_right
        response: StartDiffTrees_Response = self.grpc_stub.start_diff_trees(request)

        return DiffResultTreeIds(response.tree_id_left, response.tree_id_right)

    def generate_merge_tree(self, tree_id_left: str, tree_id_right: str,
                            selected_changes_left: List[SPIDNodePair], selected_changes_right: List[SPIDNodePair]):
        request = GenerateMergeTree_Request()
        request.tree_id_left = tree_id_left
        request.tree_id_right = tree_id_right

        for src_sn in selected_changes_left:
            grpc_sn = request.change_list_left.add()
            GRPCConverter.sn_to_grpc(src_sn, grpc_sn)

        for src_sn in selected_changes_right:
            grpc_sn = request.change_list_right.add()
            GRPCConverter.sn_to_grpc(src_sn, grpc_sn)

        self.grpc_stub.generate_merge_tree(request)

    def enqueue_refresh_subtree_task(self, node_identifier: NodeIdentifier, tree_id: str):
        request = RefreshSubtree_Request()
        GRPCConverter.node_identifier_to_grpc(node_identifier, request.node_identifier)
        request.tree_id = tree_id
        self.grpc_stub.refresh_subtree(request)

    def enqueue_refresh_subtree_stats_task(self, root_uid: UID, tree_id: str):
        request = RefreshSubtreeStats_Request()
        request.root_uid = root_uid
        request.tree_id = tree_id
        self.grpc_stub.refresh_subtree_stats(request)

    def get_last_pending_op(self, node_uid: UID) -> Optional[UserOp]:
        request = GetLastPendingOp_Request()
        request.node_uid = node_uid
        response: GetLastPendingOp_Response = self.grpc_stub.get_last_pending_op_for_node(request)
        if not response.HasField('user_op'):
            return None

        src_node = GRPCConverter.node_from_grpc(response.user_op.src_node)
        dst_node = GRPCConverter.node_from_grpc(response.user_op.dst_node)
        op_type = UserOpType(response.user_op.op_type)
        return UserOp(response.user_op.op_uid, response.user_op.batch_uid, op_type, src_node, dst_node, response.user_op.create_ts)

    def download_file_from_gdrive(self, node_uid: UID, requestor_id: str):
        request = DownloadFromGDrive_Request(node_uid=node_uid, requestor_id=requestor_id)
        self.grpc_stub.download_file_from_gdrive(request)

    def delete_subtree(self, node_uid_list: List[UID]):
        request = DeleteSubtree_Request()
        for node_uid in node_uid_list:
            request.node_uid_list.append(node_uid)
        self.grpc_stub.delete_subtree(request)

    def get_filter_criteria(self, tree_id: str) -> Optional[FilterCriteria]:
        request = GetFilter_Request()
        request.tree_id = tree_id
        response: GetFilter_Response = self.grpc_stub.get_filter(request)
        if response.HasField('filter_criteria'):
            return GRPCConverter.filter_criteria_from_grpc(response.filter_criteria)
        else:
            return None

    def update_filter_criteria(self, tree_id: str, filter_criteria: FilterCriteria):
        request = UpdateFilter_Request()
        request.tree_id = tree_id
        GRPCConverter.filter_criteria_to_grpc(filter_criteria, request.filter_criteria)
        self.grpc_stub.update_filter(request)
