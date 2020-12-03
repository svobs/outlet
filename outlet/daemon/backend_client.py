import asyncio
import logging
import threading
import time
from typing import List, Optional, Union

import grpc
from pydispatch import dispatcher

from app.backend import OutletBackend
from constants import GRPC_CLIENT_REQUEST_MAX_RETRIES, GRPC_SERVER_ADDRESS
from daemon.grpc import Outlet_pb2_grpc
from daemon.grpc.conversion import NodeConverter
from daemon.grpc.Outlet_pb2 import GetNextUid_Request, GetNodeForLocalPath_Request, GetNodeForUid_Request, GetUidForLocalPath_Request, \
    RequestDisplayTree_Request, Signal, \
    SPIDNodePair, StartSubtreeLoad_Request, Subscribe_Request
from executor.task_runner import TaskRunner
from model.display_tree.display_tree import DisplayTree
from model.node.node import Node
from model.node_identifier import NodeIdentifier, SinglePathNodeIdentifier
from model.node_identifier_factory import NodeIdentifierFactory
from model.uid import UID
from ui import actions
from util.has_lifecycle import HasLifecycle

logger = logging.getLogger(__name__)


# CLASS ClientSignalThread
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
class SignalReceiverThread(HasLifecycle, threading.Thread):
    """Listens for notifications which will be sent asynchronously by the backend gRPC server"""
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
                logger.error(f'Request failed: {repr(err)}: sleeping 3 secs (retries remaining: {retries_remaining})')
                if retries_remaining == 0:
                    # Fatal error: shutdown the rest of the app
                    logger.error(f'Too many failures: sending shutdown signal')
                    dispatcher.send(actions.SHUTDOWN_APP, sender=actions.ID_CENTRAL_EXEC)
                    raise

                time.sleep(0.100)
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
                logger.info(f'Got signal "{signal.signal_name}" from sender "{signal.sender_name}" via gRPC')
                self._dispatch_locally(signal)
            else:
                logger.warning('Received None for signal! Killing connection')
                return

    def _dispatch_locally(self, signal: Signal):
        """Take the signal (received from server) and dispatch it to our UI process"""
        if signal.signal_name == actions.DISPLAY_TREE_CHANGED:
            display_tree_ui_state = NodeConverter.display_tree_ui_state_from_grpc(signal.display_tree_ui_state)
            tree = display_tree_ui_state.to_display_tree(backend=self.backend)
            logger.debug(f'Relaying signal locally "{signal.signal_name}" with sender="{signal.sender_name}" state={display_tree_ui_state}')
            dispatcher.send(signal=signal.signal_name, sender=signal.sender_name, tree=tree)
        else:
            logger.debug(f'Relaying signal locally "{signal.signal_name}" with sender="{signal.sender_name}"')
            dispatcher.send(signal=signal.signal_name, sender=signal.sender_name)


# CLASS BackendGRPCClient
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼

class BackendGRPCClient(OutletBackend, HasLifecycle):
    """GTK3 thin client which communicates with the OutletDaemon via GRPC."""

    def __init__(self, cfg):
        HasLifecycle.__init__(self)
        self.config = cfg

        self.channel = None
        self.grpc_stub: Optional[Outlet_pb2_grpc.OutletStub] = None
        self.signal_thread: SignalReceiverThread = SignalReceiverThread(self)

        self._task_runner = TaskRunner()
        self.node_identifier_factory: NodeIdentifierFactory = NodeIdentifierFactory(self)

    def start(self):
        logger.debug('Starting up BackendGRPCClient')
        HasLifecycle.start(self)

        self.connect_dispatch_listener(signal=actions.ENQUEUE_UI_TASK, receiver=self._on_ui_task_requested)

        self.channel = grpc.insecure_channel(GRPC_SERVER_ADDRESS)
        self.grpc_stub = Outlet_pb2_grpc.OutletStub(self.channel)

        self.signal_thread.start()

    def shutdown(self):
        HasLifecycle.shutdown(self)

        if self.channel:
            self.channel.close()
            self.channel = None
            self.grpc_stub = None

    def _on_ui_task_requested(self, sender, task_func, *args, **kwargs):
        self._task_runner.enqueue(task_func, args)

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

    def request_display_tree(self, tree_id: str, user_path: str = None, spid: SinglePathNodeIdentifier = None, is_startup: bool = False) \
            -> Optional[DisplayTree]:

        request = RequestDisplayTree_Request()
        request.is_startup = is_startup
        request.tree_id = tree_id
        if user_path:
            request.user_path = user_path
        NodeConverter.node_identifier_to_grpc(spid, request.spid)

        response = self.grpc_stub.request_display_tree_ui_state(request)

        if response.HasField('display_tree_ui_state'):
            state = NodeConverter.display_tree_ui_state_from_grpc(response.display_tree_ui_state)
            tree = state.to_display_tree(backend=self)
        else:
            tree = None
        logger.debug(f'Returning tree: {tree}')
        return tree

    def start_subtree_load(self, tree_id: str):
        request = StartSubtreeLoad_Request()
        request.tree_id = tree_id
        self.grpc_stub.start_subtree_load(request)
