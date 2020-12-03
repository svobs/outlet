import logging
import threading

from pydispatch import dispatcher

import outlet.daemon.grpc
from collections import deque
from typing import Deque, Dict, Optional

from daemon.grpc.conversion import NodeConverter
from daemon.grpc.Outlet_pb2 import GetNextUid_Response, GetNodeForLocalPath_Request, GetNodeForUid_Request, GetUidForLocalPath_Request, \
    GetUidForLocalPath_Response, PingResponse, PlayState, RequestDisplayTree_Response, SendSignalResponse, Signal, SingleNode_Response, \
    StartSubtreeLoad_Request, \
    StartSubtreeLoad_Response, Subscribe_Request
from daemon.grpc.Outlet_pb2_grpc import OutletServicer
from executor.central import CentralExecutor
from model.display_tree.display_tree import DisplayTree, DisplayTreeUiState
from store.cache_manager import CacheManager
from store.uid.uid_generator import UidGenerator
from ui import actions
from util.has_lifecycle import HasLifecycle

logger = logging.getLogger(__name__)


# CLASS OutletGRPCService
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼

class OutletGRPCService(OutletServicer, HasLifecycle):
    """Backend gRPC Server"""
    def __init__(self, backend):
        HasLifecycle.__init__(self)
        self.uid_generator: UidGenerator = backend.uid_generator
        self.cacheman: CacheManager = backend.cacheman
        self.executor: CentralExecutor = backend.executor

        self._cv_has_signal = threading.Condition()
        self._queue_lock = threading.Lock()
        self._thread_signal_queues: Dict[int, Deque] = {}
        self._shutdown: bool = False

    def start(self):
        HasLifecycle.start(self)

        self.connect_dispatch_listener(signal=actions.LOAD_SUBTREE_STARTED, receiver=self._on_subtree_load_started)
        self.connect_dispatch_listener(signal=actions.LOAD_SUBTREE_DONE, receiver=self._on_subtree_load_done)
        self.connect_dispatch_listener(signal=actions.DISPLAY_TREE_CHANGED, receiver=self._on_display_tree_changed)
        self.connect_dispatch_listener(signal=actions.OP_EXECUTION_PLAY_STATE_CHANGED, receiver=self._on_op_exec_play_state_changed)

    def shutdown(self):
        HasLifecycle.shutdown(self)
        self._shutdown = True

    def send_signal_to_all(self, signal_grpc: outlet.daemon.grpc.Outlet_pb2.Signal):
        if self._shutdown:
            return

        with self._queue_lock:
            logger.debug(f'Queuing signal="{signal_grpc.signal_name}" with sender="'
                         f'{signal_grpc.sender_name}" to {len(self._thread_signal_queues)} connected clients')
            for queue in self._thread_signal_queues.values():
                queue.append(signal_grpc)

        with self._cv_has_signal:
            self._cv_has_signal.notifyAll()

    def ping(self, request, context):
        logger.info(f'Got ping!')
        response = PingResponse()
        response.timestamp = 1000
        return response

    def get_node_for_uid(self, request: GetNodeForUid_Request, context):
        response = SingleNode_Response()
        node = self.cacheman.get_node_for_uid(request.full_path, request.tree_type)
        if node:
            NodeConverter.node_to_grpc(node, response.node)
        return response

    def get_node_for_local_path(self, request: GetNodeForLocalPath_Request, context):
        response = SingleNode_Response()
        node = self.cacheman.get_node_for_local_path(request.full_path)
        if node:
            NodeConverter.node_to_grpc(node, response.node)
        return response

    def get_next_uid(self, request, context):
        response = GetNextUid_Response()
        response.uid = self.uid_generator.next_uid()
        return response

    def get_uid_for_local_path(self, request: GetUidForLocalPath_Request, context):
        response = GetUidForLocalPath_Response()
        response.uid = self.cacheman.get_uid_for_local_path(request.full_path, request.uid_suggestion)
        return response

    def send_signal(self, request, context):
        logger.info(f'Relaying signal from gRPC: "{request.signal_name}" from sender "{request.sender_name}"')
        dispatcher.send(signal=request.signal_name, sender=request.sender_name)
        return SendSignalResponse()

    def subscribe_to_signals(self, request: Subscribe_Request, context):
        """This method should be called by gRPC when it is handling a request. The calling thread will be used
                to process the stream and so will be tied up."""
        try:
            thread_id: int = threading.get_ident()
            logger.info(f'Adding a subscriber for ThreadID {thread_id}')
            with self._queue_lock:
                signal_queue = self._thread_signal_queues.get(thread_id, None)
                if signal_queue:
                    raise RuntimeError(f'There is already a queue for ThreadID: {thread_id}')
                self._thread_signal_queues[thread_id] = deque()

            while not self._shutdown:
                signal: Optional[Signal] = None
                with self._queue_lock:
                    logger.debug(f'Checking signal queue for ThreadID {thread_id}')
                    signal_queue: Deque = self._thread_signal_queues.get(thread_id, None)
                    if len(signal_queue) > 0:
                        signal = signal_queue.popleft()

                if signal:
                    logger.debug(f'[ThreadID:{thread_id}] Sending gRPC signal="{signal.signal_name}" with sender="{signal.sender_name}"')
                    yield signal

                with self._cv_has_signal:
                    self._cv_has_signal.wait()

            with self._queue_lock:
                del self._thread_signal_queues[thread_id]
        except RuntimeError:
            logger.exception('Unexpected error in add_subscriber()')

    def send_signal_via_grpc(self, signal: str, sender: str):
        self.send_signal_to_all(Signal(signal_name=signal, sender_name=sender))

    def request_display_tree_ui_state(self, request, context):
        if request.HasField('spid'):
            spid = NodeConverter.node_identifier_from_grpc(request.spid)
        else:
            spid = None

        display_tree_ui_state: Optional[DisplayTreeUiState] = self.cacheman.request_display_tree_ui_state(
            tree_id=request.tree_id, user_path=request.user_path, spid=spid, is_startup=request.is_startup)

        response = RequestDisplayTree_Response()
        if display_tree_ui_state:
            logger.debug(f'Converting DisplayTreeUiState: {display_tree_ui_state}')
            NodeConverter.display_tree_ui_state_to_grpc(display_tree_ui_state, response.display_tree_ui_state)
        return response

    def start_subtree_load(self, request: StartSubtreeLoad_Request, context):
        self.cacheman.enqueue_load_subtree_task(request.tree_id)
        return StartSubtreeLoad_Response()

    def _on_subtree_load_started(self, sender: str):
        self.send_signal_via_grpc(actions.LOAD_SUBTREE_STARTED, sender)

    def _on_subtree_load_done(self, sender: str):
        self.send_signal_via_grpc(actions.LOAD_SUBTREE_DONE, sender)

    def _on_display_tree_changed(self, sender: str, tree: DisplayTree):
        signal = Signal()
        signal.signal_name = actions.DISPLAY_TREE_CHANGED
        signal.sender_name = sender
        NodeConverter.display_tree_ui_state_to_grpc(tree.state, signal.display_tree_ui_state)

        self.send_signal_to_all(signal)

    def _on_op_exec_play_state_changed(self, sender: str, is_enabled: bool):
        signal = Signal(signal_name=actions.OP_EXECUTION_PLAY_STATE_CHANGED, sender_name=sender)
        signal.play_state.is_enabled = is_enabled
        self.send_signal_to_all(signal)

    def get_op_exec_play_state(self, request, context):
        response = PlayState()
        response.is_enabled = self.executor.enable_op_execution_thread
        return response
