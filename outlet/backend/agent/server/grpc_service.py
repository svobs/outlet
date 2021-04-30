import io
import logging
import threading

from pydispatch import dispatcher

from collections import deque
from typing import Deque, Dict, Optional, Set

from backend.backend_integrated import BackendIntegrated
from backend.agent.grpc.generated.Outlet_pb2_grpc import OutletServicer
from backend.executor.central import CentralExecutor
from backend.cache_manager import CacheManager
from constants import SUPER_DEBUG
from backend.agent.grpc.conversion import GRPCConverter
from backend.agent.grpc.generated.Outlet_pb2 import ConfigEntry, DeleteSubtree_Request, DirMetaUpdate, DragDrop_Request, DragDrop_Response, Empty, \
    GenerateMergeTree_Request, \
    GetAncestorList_Response, GetChildList_Response, \
    GetConfig_Request, GetConfig_Response, GetDeviceList_Request, GetDeviceList_Response, GetIcon_Request, GetIcon_Response, \
    GetRowsOfInterest_Request, \
    GetRowsOfInterest_Response, \
    GetFilter_Response, \
    GetLastPendingOp_Request, \
    GetLastPendingOp_Response, \
    GetNextUid_Response, \
    GetNodeForUid_Request, \
    GetSnFor_Request, GetSnFor_Response, GetUidForLocalPath_Request, \
    GetUidForLocalPath_Response, PlayState, PutConfig_Request, PutConfig_Response, RemoveExpandedRow_Request, RemoveExpandedRow_Response, \
    RequestDisplayTree_Response, \
    SendSignalResponse, SetSelectedRowSet_Request, SetSelectedRowSet_Response, SignalMsg, \
    SingleNode_Response, \
    StartDiffTrees_Request, StartDiffTrees_Response, StartSubtreeLoad_Request, \
    StartSubtreeLoad_Response, Subscribe_Request, UpdateFilter_Request, UpdateFilter_Response, UserOp
from error import ResultsExceededError
from model.device import Device
from model.display_tree.build_struct import DiffResultTreeIds, DisplayTreeRequest, RowsOfInterest
from model.display_tree.display_tree import DisplayTree, DisplayTreeUiState
from model.node.node import Node, SPIDNodePair
from model.node_identifier import GUID
from model.uid import UID
from backend.uid.uid_generator import UidGenerator
from signal_constants import Signal
from util.has_lifecycle import HasLifecycle

logger = logging.getLogger(__name__)


class OutletGRPCService(OutletServicer, HasLifecycle):
    """
    ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
    CLASS OutletGRPCService

    Backend gRPC Server
    ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼
    """

    def __init__(self, backend):
        HasLifecycle.__init__(self)
        assert isinstance(backend, BackendIntegrated)
        self.backend = backend
        self.uid_generator: UidGenerator = backend.uid_generator
        self.cacheman: CacheManager = backend.cacheman
        self.executor: CentralExecutor = backend.executor

        self._cv_has_signal = threading.Condition()
        self._queue_lock = threading.Lock()
        self._thread_signal_queues: Dict[int, Deque] = {}
        self._shutdown: bool = False

        self._converter = GRPCConverter(self.backend)

    def start(self):
        HasLifecycle.start(self)

        # PyDispatcher signals to be sent across gRPC:

        # complex:
        self.connect_dispatch_listener(signal=Signal.DISPLAY_TREE_CHANGED, receiver=self._on_display_tree_changed_grpcserver)
        self.connect_dispatch_listener(signal=Signal.OP_EXECUTION_PLAY_STATE_CHANGED, receiver=self._on_op_exec_play_state_changed)
        self.connect_dispatch_listener(signal=Signal.TOGGLE_UI_ENABLEMENT, receiver=self._on_ui_enablement_toggled)
        self.connect_dispatch_listener(signal=Signal.ERROR_OCCURRED, receiver=self._on_error_occurred)
        self.connect_dispatch_listener(signal=Signal.SET_STATUS, receiver=self._on_set_status)
        self.connect_dispatch_listener(signal=Signal.DOWNLOAD_FROM_GDRIVE_DONE, receiver=self._on_gdrive_download_done)
        self.connect_dispatch_listener(signal=Signal.GENERATE_MERGE_TREE_DONE, receiver=self._on_generate_merge_tree_done)

        self.connect_dispatch_listener(signal=Signal.NODE_UPSERTED, receiver=self._on_node_upserted)
        self.connect_dispatch_listener(signal=Signal.NODE_REMOVED, receiver=self._on_node_removed)

        self.connect_dispatch_listener(signal=Signal.DEVICE_UPSERTED, receiver=self._on_device_upserted)
        self.connect_dispatch_listener(signal=Signal.REFRESH_SUBTREE_STATS_DONE, receiver=self._on_refresh_stats_done)

        # simple:
        self.forward_signal_to_clients(signal=Signal.LOAD_SUBTREE_STARTED)
        self.forward_signal_to_clients(signal=Signal.LOAD_SUBTREE_DONE)
        self.forward_signal_to_clients(signal=Signal.DIFF_TREES_FAILED)
        self.forward_signal_to_clients(signal=Signal.DIFF_TREES_DONE)
        self.forward_signal_to_clients(signal=Signal.GENERATE_MERGE_TREE_FAILED)
        self.forward_signal_to_clients(signal=Signal.SHUTDOWN_APP)
        self.forward_signal_to_clients(signal=Signal.GENERATE_MERGE_TREE_FAILED)

        logger.debug("OutletGRPCService started")

    def forward_signal_to_clients(self, signal: Signal):
        self.connect_dispatch_listener(signal=signal, receiver=self._send_signal_to_all_clients, weak=False)

    def shutdown(self):
        HasLifecycle.shutdown(self)
        self._shutdown = True

    # Server -> client signaling via always-open gRPC stream
    # ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼

    def _send_signal_to_all_clients(self, signal: Signal, sender: str):
        """Convenience method to create a simple gRPC SignalMsg and then enqueue it to be sent to all connected clients"""
        assert sender, f'Sender is required for signal {signal.name}'
        if SUPER_DEBUG:
            logger.debug(f'Forwarding signal to gRPC clients: {signal} sender={sender}')

        self._send_grpc_signal_to_all_clients(SignalMsg(sig_int=signal, sender=sender))

    def _send_grpc_signal_to_all_clients(self, signal_grpc: SignalMsg):
        """Sends the gRPC signal to all connected clients (well, it actually passively enqueues it to be picked up by each of their threads,
        but the whole process should happen very quickly)"""
        if self._shutdown and signal_grpc != Signal.SHUTDOWN_APP:
            return

        with self._queue_lock:
            if SUPER_DEBUG:
                logger.debug(f'Queuing signal="{Signal(signal_grpc.sig_int).name}" with sender="'
                             f'{signal_grpc.sender}" to {len(self._thread_signal_queues)} connected clients')
            for queue in self._thread_signal_queues.values():
                queue.append(signal_grpc)

        with self._cv_has_signal:
            self._cv_has_signal.notifyAll()

    def subscribe_to_signals(self, request: Subscribe_Request, context):
        """This method should be called by gRPC when it is handling a request. The calling thread will be used to process the stream
        and so will be tied up. NOTE: originally I attempted to put this logic into its own class, but that resulted in bizarre errors."""
        try:
            def on_rpc_done():
                logger.info(f'Client cancelled signal subscription (ThreadID {thread_id})')
                # remove data structs for client:
                with self._queue_lock:
                    del self._thread_signal_queues[thread_id]
                # notify the thread so it will stop waiting
                with self._cv_has_signal:
                    self._cv_has_signal.notifyAll()

            context.add_callback(on_rpc_done)
            thread_id: int = threading.get_ident()
            logger.info(f'Adding a signal subscriber with ThreadID {thread_id}')
            with self._queue_lock:
                signal_queue = self._thread_signal_queues.get(thread_id, None)
                if signal_queue:
                    logger.warning(f'Found an existing gRPC signal queue for ThreadID: {thread_id} Will overwrite')
                self._thread_signal_queues[thread_id] = deque()

            while not self._shutdown:

                while True:  # empty the queue
                    with self._queue_lock:
                        if SUPER_DEBUG:
                            logger.debug(f'Checking signal queue for ThreadID {thread_id}')
                        signal_queue: Optional[Deque] = self._thread_signal_queues.get(thread_id, None)
                        if signal_queue is None:
                            logger.debug(f'Looks like  ThreadID {thread_id} signal subscription ended (queue is not there). Cleaning up our end.')
                            return
                        if len(signal_queue) > 0:
                            signal: Optional[SignalMsg] = signal_queue.popleft()
                        else:
                            break

                    if signal:
                        if SUPER_DEBUG:
                            logger.info(f'[ThreadID:{thread_id}] Sending gRPC signal="{Signal(signal.sig_int).name}" with sender="{signal.sender}"')
                        yield signal

                logger.debug(f'Signal queue for ThreadID {thread_id} emptied. Will wait for more signals')
                with self._cv_has_signal:
                    self._cv_has_signal.wait()

            with self._queue_lock:
                del self._thread_signal_queues[thread_id]
        except RuntimeError:
            logger.exception('Unexpected error processing signal subscription')

    # Short-lived async (non-streaming) client requests
    # ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼

    def send_signal(self, request, context):
        """This is a request from the CLIENT to relay a Signal to the server"""
        sig = Signal(request.sig_int)
        logger.info(f'Relaying signal from gRPC: "{sig.name}" from sender "{request.sender}"')
        dispatcher.send(signal=sig, sender=request.sender)
        return SendSignalResponse()

    def get_config(self, request: GetConfig_Request, context):
        response = GetConfig_Response()
        for config_key in request.config_key_list:
            logger.debug(f'Getting config "{config_key}"')
            config_val = self.backend.get_config(config_key, "")
            config = ConfigEntry(key=config_key, val=str(config_val))
            response.config_list.append(config)

        return response

    def put_config(self, request: PutConfig_Request, context):
        for config in request.config_list:
            logger.debug(f'Putting config "{config.key}" = "{config.val}"')
            self.backend.put_config(config_key=config.key, config_val=config.val)

        return PutConfig_Response()

    @staticmethod
    def _img_to_bytes(image):
        img_byte_arr = io.BytesIO()
        image.save(img_byte_arr, format='PNG')
        img_byte_arr.flush()
        return img_byte_arr.getvalue()

    def get_icon(self, request: GetIcon_Request, context):
        image = self.backend.get_icon(request.icon_id)
        response = GetIcon_Response()

        if image:
            response.icon.icon_id = request.icon_id
            response.icon.content = self._img_to_bytes(image)
            logger.debug(f'Returning requested image with iconId={request.icon_id}')
        else:
            logger.debug(f'Could not find image with requested iconId={request.icon_id}')

        return response

    def get_node_for_uid(self, request: GetNodeForUid_Request, context):
        response = SingleNode_Response()
        node = self.cacheman.get_node_for_uid(request.full_path, request.device_uid)
        if node:
            self._converter.node_to_grpc(node, response.node)
        return response

    def get_next_uid(self, request, context):
        response = GetNextUid_Response()
        response.uid = self.uid_generator.next_uid()
        return response

    def get_uid_for_local_path(self, request: GetUidForLocalPath_Request, context):
        response = GetUidForLocalPath_Response()
        response.uid = self.cacheman.get_uid_for_local_path(request.full_path, request.uid_suggestion)
        return response

    def get_sn_for(self, request: GetSnFor_Request, context):
        response = GetSnFor_Response()

        sn = self.cacheman.get_sn_for(request.node_uid, request.device_uid, request.full_path)
        if sn:
            self._converter.sn_to_grpc(sn, response.sn)

        return response

    def request_display_tree(self, grpc_req, context):
        if grpc_req.HasField('spid'):
            spid = self._converter.node_identifier_from_grpc(grpc_req.spid)
        else:
            spid = None

        request = DisplayTreeRequest(tree_id=grpc_req.tree_id, return_async=grpc_req.return_async, user_path=grpc_req.user_path, spid=spid,
                                     device_uid=grpc_req.device_uid, is_startup=grpc_req.is_startup, tree_display_mode=grpc_req.tree_display_mode)

        display_tree_ui_state: Optional[DisplayTreeUiState] = self.cacheman.request_display_tree(request)

        response = RequestDisplayTree_Response()
        if display_tree_ui_state:
            logger.debug(f'Converting DisplayTreeUiState: {display_tree_ui_state}')
            self._converter.display_tree_ui_state_to_grpc(display_tree_ui_state, response.display_tree_ui_state)
        return response

    def start_subtree_load(self, request: StartSubtreeLoad_Request, context):
        self.backend.start_subtree_load(request.tree_id)
        return StartSubtreeLoad_Response()

    def _on_subtree_load_started(self, sender: str):
        self._send_signal_to_all_clients(Signal.LOAD_SUBTREE_STARTED, sender)

    def _on_subtree_load_done(self, sender: str):
        self._send_signal_to_all_clients(Signal.LOAD_SUBTREE_DONE, sender)

    def _on_diff_failed(self, sender: str):
        self._send_signal_to_all_clients(Signal.DIFF_TREES_FAILED, sender)

    def _on_generate_merge_tree_failed(self, sender: str):
        self._send_signal_to_all_clients(Signal.GENERATE_MERGE_TREE_FAILED, sender)

    def _on_diff_done(self, sender: str):
        self._send_signal_to_all_clients(Signal.DIFF_TREES_DONE, sender)

    def _on_refresh_stats_done(self, sender: str, status_msg: str, dir_stats_dict: Dict, key_is_uid: bool):
        signal = SignalMsg(sig_int=Signal.REFRESH_SUBTREE_STATS_DONE, sender=sender)
        signal.stats_update.status_msg = status_msg

        for key, dir_stats in dir_stats_dict.items():
            dir_meta_grpc: DirMetaUpdate = signal.stats_update.dir_meta_list.add()
            if key_is_uid:
                dir_meta_grpc.uid = key
            else:
                dir_meta_grpc.guid = key
            self._converter.dir_stats_to_grpc(dir_stats, dir_meta_grpc)

        self._send_grpc_signal_to_all_clients(signal)

    def _on_set_status(self, sender: str, status_msg: str):
        signal = SignalMsg(sig_int=Signal.SET_STATUS, sender=sender)
        signal.status_msg.msg = status_msg
        self._send_grpc_signal_to_all_clients(signal)

    def _on_gdrive_download_done(self, sender, filename: str):
        signal = SignalMsg(sig_int=Signal.DOWNLOAD_FROM_GDRIVE_DONE, sender=sender)
        signal.download_msg.filename = filename
        self._send_grpc_signal_to_all_clients(signal)

    def _on_node_upserted(self, sender: str, sn: SPIDNodePair, parent_guid: GUID):
        signal = SignalMsg(sig_int=Signal.NODE_UPSERTED, sender=sender)
        self._converter.sn_to_grpc(sn, signal.sn)
        signal.parent_guid = parent_guid
        self._send_grpc_signal_to_all_clients(signal)

    def _on_node_removed(self, sender: str, sn: SPIDNodePair, parent_guid: GUID):
        signal = SignalMsg(sig_int=Signal.NODE_REMOVED, sender=sender)
        self._converter.sn_to_grpc(sn, signal.sn)
        signal.parent_guid = parent_guid
        self._send_grpc_signal_to_all_clients(signal)

    def _on_device_upserted(self, sender: str, device: Device):
        signal = SignalMsg(sig_int=Signal.DEVICE_UPSERTED, sender=sender)
        self._converter.device_to_grpc(device, signal.device)
        self._send_grpc_signal_to_all_clients(signal)

    def _on_error_occurred(self, sender: str, msg: str, secondary_msg: Optional[str]):
        signal = SignalMsg(sig_int=Signal.ERROR_OCCURRED, sender=sender)
        signal.error_occurred.msg = msg
        if secondary_msg:
            signal.error_occurred.secondary_msg = secondary_msg
        self._send_grpc_signal_to_all_clients(signal)

    def _on_ui_enablement_toggled(self, sender: str, enable: bool):
        signal = SignalMsg(sig_int=Signal.TOGGLE_UI_ENABLEMENT, sender=sender)
        signal.ui_enablement.enable = enable
        self._send_grpc_signal_to_all_clients(signal)

    def _on_display_tree_changed_grpcserver(self, sender: str, tree: DisplayTree):
        signal = SignalMsg(sig_int=Signal.DISPLAY_TREE_CHANGED, sender=sender)
        self._converter.display_tree_ui_state_to_grpc(tree.state, signal.display_tree_ui_state)

        logger.debug(f'Relaying signal across gRPC: "{Signal.DISPLAY_TREE_CHANGED.name}", sender={sender}, tree={tree}')
        self._send_grpc_signal_to_all_clients(signal)

    def _on_generate_merge_tree_done(self, sender: str, tree: DisplayTree):
        signal = SignalMsg(sig_int=Signal.GENERATE_MERGE_TREE_DONE, sender=sender)
        self._converter.display_tree_ui_state_to_grpc(tree.state, signal.display_tree_ui_state)

        logger.debug(f'Relaying signal across gRPC: "{Signal.GENERATE_MERGE_TREE_DONE.name}", sender={sender}, tree={tree}')
        self._send_grpc_signal_to_all_clients(signal)

    def _on_op_exec_play_state_changed(self, sender: str, is_enabled: bool):
        signal = SignalMsg(sig_int=Signal.OP_EXECUTION_PLAY_STATE_CHANGED, sender=sender)
        signal.play_state.is_enabled = is_enabled
        logger.debug(f'Relaying signal across gRPC: "{Signal.OP_EXECUTION_PLAY_STATE_CHANGED.name}", sender={sender}, is_enabled={is_enabled}')
        self._send_grpc_signal_to_all_clients(signal)

    def get_op_exec_play_state(self, request, context):
        response = PlayState()
        response.is_enabled = self.executor.enable_op_execution

        if SUPER_DEBUG:
            logger.debug(f'Relaying op_execution_state.is_enabled = {response.is_enabled}')
        return response

    def get_device_list(self, request: GetDeviceList_Request, context):
        response = GetDeviceList_Response()
        for device in self.cacheman.get_device_list():
            grpc_device = response.device_list.add()
            self._converter.device_to_grpc(device, grpc_device)

        return response

    def get_child_list_for_spid(self, request, context):
        max_results = request.max_results

        response = GetChildList_Response()
        try:
            parent_spid = self._converter.node_identifier_from_grpc(request.parent_spid)
            child_list = self.cacheman.get_child_list(parent_spid, request.tree_id, max_results)
            self._converter.sn_list_to_grpc(child_list, response.child_list)
            logger.debug(f'[{request.tree_id}] Relaying {len(child_list)} children for {parent_spid}')
        except ResultsExceededError as err:
            response.result_exceeded_count = err.actual_count
            logger.debug(f'[{request.tree_id}] Too many children ({response.result_exceeded_count}) for {request.parent_spid}'
                         f' (max was {max_results})')

        return response

    def get_ancestor_list_for_spid(self, request, context):
        spid = self._converter.node_identifier_from_grpc(request.spid)
        ancestor_list: Deque[SPIDNodePair] = self.cacheman.get_ancestor_list_for_spid(spid=spid, stop_at_path=request.stop_at_path)
        response = GetAncestorList_Response()
        self._converter.sn_list_to_grpc(ancestor_list, response.ancestor_list)
        logger.debug(f'Relaying {len(ancestor_list)} ancestors for: {spid}')
        return response

    def drop_dragged_nodes(self, request: DragDrop_Request, context):
        src_sn_list = []
        for src_sn in request.src_sn_list:
            src_sn_list.append(self._converter.sn_from_grpc(src_sn))
        dst_sn = self._converter.sn_from_grpc(request.dst_sn)

        self.cacheman.drop_dragged_nodes(request.src_tree_id, src_sn_list, request.is_into, request.dst_tree_id, dst_sn)

        return DragDrop_Response()

    def generate_merge_tree(self, request: GenerateMergeTree_Request, context):
        selected_changes_left = []
        for src_sn in request.change_list_left:
            selected_changes_left.append(self._converter.sn_from_grpc(src_sn))

        selected_changes_right = []
        for src_sn in request.change_list_right:
            selected_changes_right.append(self._converter.sn_from_grpc(src_sn))

        self.backend.generate_merge_tree(request.tree_id_left, request.tree_id_right, selected_changes_left, selected_changes_right)
        return Empty()

    def start_diff_trees(self, request: StartDiffTrees_Request, context):
        tree_id_struct: DiffResultTreeIds = self.backend.start_diff_trees(request.tree_id_left, request.tree_id_right)
        return StartDiffTrees_Response(tree_id_left=tree_id_struct.tree_id_left, tree_id_right=tree_id_struct.tree_id_right)

    def refresh_subtree_stats(self, request, context):
        self.backend.cacheman.enqueue_refresh_subtree_stats_task(request.root_uid, request.tree_id)
        return Empty()

    def refresh_subtree(self, request, context):
        node_identifier = self._converter.node_identifier_from_grpc(request.node_identifier)
        self.backend.cacheman.enqueue_refresh_subtree_task(node_identifier, request.tree_id)
        return Empty()

    def get_last_pending_op_for_node(self, request: GetLastPendingOp_Request, context):
        user_op: Optional[UserOp] = self.backend.cacheman.get_last_pending_op_for_node(UID(request.node_uid))

        response = GetLastPendingOp_Response()
        if user_op:
            response.user_op.op_uid = user_op.op_uid
            response.user_op.batch_uid = user_op.batch_uid
            response.user_op.op_type = user_op.op_type
            response.user_op.create_ts = user_op.create_ts

            self._converter.node_to_grpc(user_op.src_node, response.user_op.src_node)
            self._converter.node_to_grpc(user_op.dst_node, response.user_op.dst_node)

        return response

    def download_file_from_gdrive(self, request, context):
        self.cacheman.download_file_from_gdrive(request.node_uid, request.requestor_id)
        return Empty()

    def delete_subtree(self, request: DeleteSubtree_Request, context):
        self.cacheman.delete_subtree(request.node_uid_list)
        return Empty()

    def get_filter(self, request, context):
        filter_criteria = self.cacheman.get_filter_criteria(request.tree_id)

        response = GetFilter_Response()
        self._converter.filter_criteria_to_grpc(filter_criteria, response.filter_criteria)

        return response

    def update_filter(self, request: UpdateFilter_Request, context):
        filter_criteria = self._converter.filter_criteria_from_grpc(request.filter_criteria)
        self.cacheman.update_filter_criteria(request.tree_id, filter_criteria)
        return UpdateFilter_Response()

    def set_selected_row_set(self, request: SetSelectedRowSet_Request, context):
        selected: Set[GUID] = set()
        for guid in request.selected_row_guid_set:
            selected.add(guid)
        self.cacheman.set_selected_rows(request.tree_id, selected)

        return SetSelectedRowSet_Response()

    def remove_expanded_row(self, request: RemoveExpandedRow_Request, context):
        self.cacheman.remove_expanded_row(request.row_guid, request.tree_id)
        return RemoveExpandedRow_Response()

    def get_rows_of_interest(self, request: GetRowsOfInterest_Request, context):
        rows: RowsOfInterest = self.cacheman.get_rows_of_interest(tree_id=request.tree_id)
        response = GetRowsOfInterest_Response()
        for guid in rows.expanded:
            response.expanded_row_guid_set.append(guid)
        for guid in rows.selected:
            response.selected_row_guid_set.append(guid)

        return response
