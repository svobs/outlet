import asyncio
import copy
import logging
import threading
import time
from typing import Dict, Iterable, List, Optional, Union

import grpc
from pydispatch import dispatcher

from app.backend import OutletBackend
from constants import GRPC_CLIENT_REQUEST_MAX_RETRIES, GRPC_CLIENT_SLEEP_ON_FAILURE_SEC, GRPC_SERVER_ADDRESS
from daemon.grpc import Outlet_pb2_grpc
from daemon.grpc.conversion import Converter
from daemon.grpc.Outlet_pb2 import DragDrop_Request, GetAncestorList_Request, GetChildList_Request, GetNextUid_Request, GetNodeForLocalPath_Request, \
    GetNodeForUid_Request, \
    GetOpExecPlayState_Request, \
    GetUidForLocalPath_Request, \
    RequestDisplayTree_Request, Signal, \
    SPIDNodePair, StartSubtreeLoad_Request, Subscribe_Request
from executor.task_runner import TaskRunner
from model.display_tree.display_tree import DisplayTree
from model.node.node import Node
from model.node_identifier import NodeIdentifier, SinglePathNodeIdentifier
from model.node_identifier_factory import NodeIdentifierFactory
from model.uid import UID
from store.uid.uid_generator import SimpleUidGenerator
from ui import actions
from ui.tree.filter_criteria import FilterCriteria
from util.has_lifecycle import HasLifecycle

logger = logging.getLogger(__name__)


# CLASS ClientSignalThread
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
class SignalReceiverThread(HasLifecycle, threading.Thread):
    """Listens for notifications which will be sent asynchronously by the backend gRPC server"""
    def __init__(self, backend):
        HasLifecycle.__init__(self)
        threading.Thread.__init__(self, target=self.run, name='SignalReceiverThread', daemon=True)

        # self.dispatcher_thread = DispatcherQueueThread()

        self.backend = backend
        self._shutdown: bool = False

    def start(self):
        HasLifecycle.start(self)
        threading.Thread.start(self)
        # self.dispatcher_thread.start()

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
                logger.debug(f'Error type: {type(err)}')
                logger.error(f'Request failed: {repr(err)}: sleeping {GRPC_CLIENT_SLEEP_ON_FAILURE_SEC} secs (retries remaining: {retries_remaining})')
                if retries_remaining == 0:
                    # Fatal error: shutdown the rest of the app
                    logger.error(f'Too many failures: sending shutdown signal')
                    dispatcher.send(signal=actions.SHUTDOWN_APP, sender=actions.ID_CENTRAL_EXEC)
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
            signal = next(response_iter)  # blocks each time until signal received, or server shutdown
            if signal:
                logger.debug(f'Got gRPC signal "{signal.signal_name}" from sender "{signal.sender_name}"')
                try:
                    self._relay_signal_locally(signal)
                except Exception:
                    logger.exception('Unexpected error while relaying signal!')
            else:
                logger.warning('Received None for signal! Killing connection')
                return

    def _relay_signal_locally(self, signal: Signal):
        """Take the signal (received from server) and dispatch it to our UI process"""
        kwargs = {}
        if signal.signal_name == actions.DISPLAY_TREE_CHANGED:
            display_tree_ui_state = Converter.display_tree_ui_state_from_grpc(signal.display_tree_ui_state)
            tree: DisplayTree = display_tree_ui_state.to_display_tree(backend=self.backend)
            kwargs['tree'] = tree
            sender = str(signal.sender_name)
            # dispatcher.send(signal=actions.DISPLAY_TREE_CHANGED, sender=sender, tree=tree)
            # dispatcher.send(signal=actions.DISPLAY_TREE_CHANGED, sender=actions.ID_RIGHT_TREE, tree=tree)
        elif signal.signal_name == actions.OP_EXECUTION_PLAY_STATE_CHANGED:
            is_enabled: bool = signal.play_state.is_enabled
            kwargs['is_enabled'] = is_enabled
        # elif signal.signal_name == actions.LOAD_SUBTREE_DONE:
        #     dispatcher.send(signal=actions.LOAD_SUBTREE_DONE, sender=actions.ID_RIGHT_TREE)
        #
        # logger.debug(f'Relaying signal locally "{signal.signal_name}" with sender="{signal.sender_name}" kwargs={kwargs}')
        kwargs['signal'] = signal.signal_name
        kwargs['sender'] = signal.sender_name
        logger.info(f'Relaying signal locally: {kwargs}')
        # dispatcher.send(signal=actions.LOAD_SUBTREE_DONE, sender=actions.ID_RIGHT_TREE)
        dispatcher.send(**kwargs)
        # self.dispatcher_thread.enqueue(kwargs)


# CLASS BackendGRPCClient
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼

class BackendGRPCClient(OutletBackend):
    """GTK3 thin client which communicates with the OutletDaemon via GRPC."""

    def __init__(self, cfg):
        OutletBackend.__init__(self)
        self.config = cfg

        self.channel = None
        self.grpc_stub: Optional[Outlet_pb2_grpc.OutletStub] = None
        self.signal_thread: SignalReceiverThread = SignalReceiverThread(self)

        self._task_runner = TaskRunner()
        """Only needed to generate UIDs which are unique to drag & drop"""

    def start(self):
        logger.debug('Starting up BackendGRPCClient')
        OutletBackend.start(self)

        self.connect_dispatch_listener(signal=actions.ENQUEUE_UI_TASK, receiver=self._on_ui_task_requested)

        self.connect_dispatch_listener(signal=actions.PAUSE_OP_EXECUTION, receiver=self._send_pause_op_exec_signal)
        self.connect_dispatch_listener(signal=actions.RESUME_OP_EXECUTION, receiver=self._send_resume_op_exec_signal)

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
        self.grpc_stub.send_signal(Signal(signal_name=actions.PAUSE_OP_EXECUTION, sender_name=sender))

    def _send_resume_op_exec_signal(self, sender: str):
        self.grpc_stub.send_signal(Signal(signal_name=actions.RESUME_OP_EXECUTION, sender_name=sender))

    def send_signal_to_server(self, signal: str, sender: str):
        """General-use method for signals with no additional args"""
        self.grpc_stub.send_signal(Signal(signal_name=signal, sender_name=sender))

    def _on_ui_task_requested(self, sender, task_func, *args):
        self._task_runner.enqueue(task_func, *args)

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

    def request_display_tree(self, tree_id: str, return_async: bool, user_path: str = None, spid: SinglePathNodeIdentifier = None,
                             is_startup: bool = False) -> Optional[DisplayTree]:

        request = RequestDisplayTree_Request()
        request.is_startup = is_startup
        request.tree_id = tree_id
        request.return_async = return_async
        if user_path:
            request.user_path = user_path
        Converter.node_identifier_to_grpc(spid, request.spid)

        response = self.grpc_stub.request_display_tree_ui_state(request)

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

    def get_children(self, parent: Node, filter_criteria: FilterCriteria = None) -> Iterable[Node]:
        request = GetChildList_Request()
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
