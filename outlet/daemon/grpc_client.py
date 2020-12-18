import logging
import threading
import time
from typing import Iterable, List, Optional, Tuple

from pydispatch import dispatcher

import grpc
from app.backend import DiffResultTreeIds, OutletBackend
from constants import GRPC_CLIENT_REQUEST_MAX_RETRIES, GRPC_CLIENT_SLEEP_ON_FAILURE_SEC, GRPC_SERVER_ADDRESS, SUPER_DEBUG, TreeDisplayMode
from daemon.grpc import Outlet_pb2_grpc
from daemon.grpc.conversion import Converter
from daemon.grpc.Outlet_pb2 import DeleteSubtree_Request, DownloadFromGDrive_Request, DragDrop_Request, GetAncestorList_Request, GetChildList_Request, \
    GetLastPendingOp_Request, \
    GetLastPendingOp_Response, GetNextUid_Request, \
    GetNodeForLocalPath_Request, \
    GetNodeForUid_Request, \
    GetOpExecPlayState_Request, \
    GetUidForLocalPath_Request, \
    RefreshSubtree_Request, RefreshSubtreeStats_Request, RequestDisplayTree_Request, SignalMsg, \
    SPIDNodePair, StartDiffTrees_Request, StartDiffTrees_Response, StartSubtreeLoad_Request, Subscribe_Request
from executor.task_runner import TaskRunner
from model.display_tree.display_tree import DisplayTree
from model.node.node import Node
from model.node_identifier import NodeIdentifier, SinglePathNodeIdentifier
from model.uid import UID
from model.user_op import UserOp, UserOpType
from store.cache_manager import DisplayTreeRequest
from ui.signal import ID_CENTRAL_EXEC, Signal
from model.display_tree.filter_criteria import FilterCriteria
from util.has_lifecycle import HasLifecycle

logger = logging.getLogger(__name__)


class SignalReceiverThread(HasLifecycle, threading.Thread):
    """
    ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
    CLASS SignalReceiverThread

    Listens for notifications which will be sent asynchronously by the backend gRPC server.
    ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼
    """
    def __init__(self, backend):
        HasLifecycle.__init__(self)
        threading.Thread.__init__(self, target=self.run, name='SignalReceiverThread', daemon=True)

        self.backend = backend
        self._shutdown: bool = False

    def start(self):
        HasLifecycle.start(self)
        threading.Thread.start(self)

    def shutdown(self):
        HasLifecycle.shutdown(self)
        # self.dispatcher_thread.shutdown()

        if self._shutdown:
            return

        logger.debug(f'Shutting down {self.name}')
        self._shutdown = True

    @staticmethod
    def _try_repeatedly(request_func):
        retries_remaining = GRPC_CLIENT_REQUEST_MAX_RETRIES
        while True:
            try:
                return request_func()
            except Exception as err:
                logger.exception(f'Request failed: {repr(err)}: sleeping {GRPC_CLIENT_SLEEP_ON_FAILURE_SEC} secs (retries left: {retries_remaining})')
                if retries_remaining == 0:
                    # Fatal error: shutdown the rest of the app
                    logger.error(f'Too many failures: sending shutdown signal')
                    dispatcher.send(signal=Signal.SHUTDOWN_APP, sender=ID_CENTRAL_EXEC)
                    raise

                time.sleep(GRPC_CLIENT_SLEEP_ON_FAILURE_SEC)
                retries_remaining -= 1

    def run(self):
        logger.info(f'Starting {self.name}...')

        while not self._shutdown:
            logger.info('Subscribing to signals from server...')

            while not self._shutdown:
                logger.debug('Subscribing to GRPC signals')
                self._try_repeatedly(lambda: self._receive_server_signals())

            logger.debug('Connection to server ended.')

        logger.debug(f'{self.name} Run loop ended.')

    def _receive_server_signals(self):
        request = Subscribe_Request()
        response_iter = self.backend.grpc_stub.subscribe_to_signals(request)
        logger.debug(f'Subscribed to signals from server')

        while not self._shutdown:
            if not response_iter:
                logger.warning('ResponseIter is None! Killing connection')
                return
            try:
                signal = next(response_iter)  # blocks each time until signal received, or server shutdown
                if signal:
                    logger.debug(f'Got gRPC signal "{Signal(signal.sig_int).name}" from sender "{signal.sender}"')
                    try:
                        self._relay_signal_locally(signal)
                    except RuntimeError:
                        logger.exception('Unexpected error while relaying signal!')
                else:
                    logger.warning('Received None for signal! Killing connection')
                    return
            except StopIteration:
                logger.error('Server disconnected! Bailing...')
                return

    def _relay_signal_locally(self, signal: SignalMsg):
        """Take the signal (received from server) and dispatch it to our UI process"""
        sig = Signal(signal.sig_int)
        kwargs = {}
        # TODO: convert this long conditional list into an action dict
        if signal.sig_int == Signal.DISPLAY_TREE_CHANGED:
            display_tree_ui_state = Converter.display_tree_ui_state_from_grpc(signal.display_tree_ui_state)
            tree: DisplayTree = display_tree_ui_state.to_display_tree(backend=self.backend)
            kwargs['tree'] = tree
        elif signal.sig_int == Signal.OP_EXECUTION_PLAY_STATE_CHANGED:
            kwargs['is_enabled'] = signal.play_state.is_enabled
        elif signal.sig_int == Signal.TOGGLE_UI_ENABLEMENT:
            kwargs['enable'] = signal.ui_enablement.enable
        elif signal.sig_int == Signal.ERROR_OCCURRED:
            kwargs['msg'] = signal.error_occurred.msg
            kwargs['secondary_msg'] = signal.error_occurred.secondary_msg
        elif signal.sig_int == Signal.NODE_UPSERTED:
            kwargs['node'] = Converter.node_from_grpc(signal.node)
        elif signal.sig_int == Signal.NODE_REMOVED:
            kwargs['node'] = Converter.node_from_grpc(signal.node)
        elif signal.sig_int == Signal.NODE_MOVED:
            kwargs['src_node'] = Converter.node_from_grpc(signal.src_dst_node_list.src_node)
            kwargs['dst_node'] = Converter.node_from_grpc(signal.src_dst_node_list.dst_node)
        elif signal.sig_int == Signal.SET_STATUS:
            kwargs['status_msg'] = signal.status_msg.msg
        elif signal.sig_int == Signal.DOWNLOAD_FROM_GDRIVE_DONE:
            kwargs['filename'] = signal.download_msg.filename
        logger.info(f'Relaying locally: signal="{sig.name}" sender="{signal.sender}" args={kwargs}')
        kwargs['signal'] = sig
        kwargs['sender'] = signal.sender
        # IMPORTANT: Do not be tempted to use PyDispatcher's "named" argument for kwargs. It seems to fail in unexpected ways
        dispatcher.send(**kwargs)


class BackendGRPCClient(OutletBackend):
    """
    ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
    CLASS BackendGRPCClient

    GTK3 thin client which communicates with the OutletDaemon via GRPC.
    ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼
    """
    def __init__(self, cfg):
        OutletBackend.__init__(self)
        self.config = cfg

        self.channel = None
        self.grpc_stub: Optional[Outlet_pb2_grpc.OutletStub] = None
        self.signal_thread: SignalReceiverThread = SignalReceiverThread(self)

        self._fe_task_runner = TaskRunner()
        """Only needed to generate UIDs which are unique to drag & drop"""

    def start(self):
        logger.debug('Starting up BackendGRPCClient')
        OutletBackend.start(self)

        self.connect_dispatch_listener(signal=Signal.ENQUEUE_UI_TASK, receiver=self._on_ui_task_requested)

        self.connect_dispatch_listener(signal=Signal.PAUSE_OP_EXECUTION, receiver=self._send_pause_op_exec_signal)
        self.connect_dispatch_listener(signal=Signal.RESUME_OP_EXECUTION, receiver=self._send_resume_op_exec_signal)

        self.channel = grpc.insecure_channel(GRPC_SERVER_ADDRESS)
        self.grpc_stub = Outlet_pb2_grpc.OutletStub(self.channel)

        self.signal_thread.start()

    def shutdown(self):
        OutletBackend.shutdown(self)

        if self.channel:
            self.channel.close()
            self.channel = None
            self.grpc_stub = None

    def _send_pause_op_exec_signal(self, sender: str):
        self.grpc_stub.send_signal(SignalMsg(sig_int=Signal.PAUSE_OP_EXECUTION, sender=sender))

    def _send_resume_op_exec_signal(self, sender: str):
        self.grpc_stub.send_signal(SignalMsg(sig_int=Signal.RESUME_OP_EXECUTION, sender=sender))

    def send_signal_to_server(self, signal: str, sender: str):
        """General-use method for signals with no additional args"""
        self.grpc_stub.send_signal(SignalMsg(sig_int=signal, sender_name=sender))

    def _on_ui_task_requested(self, sender, task_func, *args):
        self._fe_task_runner.enqueue(task_func, *args)

    def get_node_for_uid(self, uid: UID, tree_type: int = None) -> Optional[Node]:
        request = GetNodeForUid_Request()
        request.uid = uid
        if tree_type:
            request.tree_type = tree_type

        grpc_response = self.grpc_stub.get_node_for_uid(request)
        if grpc_response.HasField('node'):
            return grpc_response.node
        return None

    def get_node_for_local_path(self, full_path: str) -> Optional[Node]:
        request = GetNodeForLocalPath_Request()
        request.full_path = full_path
        grpc_response = self.grpc_stub.get_node_for_local_path(request)
        if grpc_response.HasField('node'):
            return grpc_response.node
        return None

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
        Converter.node_identifier_to_grpc(request.spid, grpc_req.spid)
        grpc_req.tree_display_mode = request.tree_display_mode

        response = self.grpc_stub.request_display_tree_ui_state(grpc_req)

        if response.HasField('display_tree_ui_state'):
            state = Converter.display_tree_ui_state_from_grpc(response.display_tree_ui_state)
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

    def get_children(self, parent: Node, tree_id: str, filter_criteria: FilterCriteria = None) -> Iterable[Node]:
        if SUPER_DEBUG:
            logger.debug(f'[{tree_id}] Entered get_children(): parent={parent} filter_criteria={filter_criteria}')

        request = GetChildList_Request()
        if tree_id:
            request.tree_id = tree_id
        Converter.node_to_grpc(parent, request.parent_node)
        if filter_criteria:
            Converter.filter_criteria_to_grpc(filter_criteria, request.filter_criteria)

        response = self.grpc_stub.get_child_list_for_node(request)
        return Converter.node_list_from_grpc(response.node_list)

    def get_ancestor_list(self, spid: SinglePathNodeIdentifier, stop_at_path: Optional[str] = None) -> Iterable[Node]:
        request = GetAncestorList_Request()
        if stop_at_path:
            request.stop_at_path = stop_at_path
        Converter.node_identifier_to_grpc(spid, request.spid)

        response = self.grpc_stub.get_ancestor_list_for_spid(request)
        return Converter.node_list_from_grpc(response.node_list)

    def drop_dragged_nodes(self, src_tree_id: str, src_sn_list: List[SPIDNodePair], is_into: bool, dst_tree_id: str, dst_sn: SPIDNodePair):
        request = DragDrop_Request()
        request.src_tree_id = src_tree_id
        request.dst_tree_id = dst_tree_id
        request.is_into = is_into
        for src_sn in src_sn_list:
            grpc_sn = request.src_sn_list.add()
            Converter.sn_to_grpc(src_sn, grpc_sn)
        Converter.sn_to_grpc(dst_sn, request.dst_sn)

        self.grpc_stub.drop_dragged_nodes(request)

    def start_diff_trees(self, tree_id_left: str, tree_id_right: str) -> DiffResultTreeIds:
        request = StartDiffTrees_Request()
        request.tree_id_left = tree_id_left
        request.tree_id_right = tree_id_right
        response: StartDiffTrees_Response = self.grpc_stub.start_diff_trees(request)

        return DiffResultTreeIds(response.tree_id_left, response.tree_id_right)

    def generate_merge_tree(self, tree_id_left: str, tree_id_right: str,
                            selected_changes_left: List[SPIDNodePair], selected_changes_right: List[SPIDNodePair]):
        # FIXME: 2
        pass

    def enqueue_refresh_subtree_task(self, node_identifier: NodeIdentifier, tree_id: str):
        request = RefreshSubtree_Request()
        Converter.node_identifier_to_grpc(node_identifier, request.node_identifier)
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

        src_node = Converter.node_from_grpc(response.user_op.src_node)
        dst_node = Converter.node_from_grpc(response.user_op.dst_node)
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
