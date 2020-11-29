import logging
import threading
from typing import List, Optional, Union

import grpc
from pydispatch import dispatcher

from app.backend import OutletBackend
from constants import GRPC_SERVER_ADDRESS
from daemon.grpc import Outlet_pb2_grpc
from daemon.grpc.conversion import NodeConverter
from daemon.grpc.Outlet_pb2 import GetNextUid_Request, GetNodeForLocalPath_Request, GetNodeForUid_Request, GetUidForLocalPath_Request, Signal
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
        threading.Thread.__init__(self, target=self.run_thread, name='SignalReceiverThread', daemon=True)
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

    def run_thread(self):
        logger.info(f'Starting {self.name}...')

        while not self._shutdown:
            logger.info('Subscribing to signals from server...')
            signal_generator = self.backend.grpc_stub.subscribe_to_signals()

            # This should block until input is received:
            for signal in signal_generator:
                self.dispatch(signal)

            logger.info('Connection to server ended.')

    def dispatch(self, signal: Signal):
        """Take the signal (received from server) and dispatch it to our UI process"""
        if signal.signal_name == actions.DISPLAY_TREE_CHANGED:
            display_tree_ui_state = NodeConverter.display_tree_ui_state_from_grpc(signal.display_tree_ui_state)
            tree = display_tree_ui_state.to_display_tree(backend=self.backend)
            dispatcher.send(signal=signal.signal_name, sender=signal.sender_name, tree=tree)
        else:
            dispatcher.send(signal=signal.signal_name, sender=signal.sender_name)


# CLASS BackendGRPCClient
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼

class BackendGRPCClient(OutletBackend, HasLifecycle):
    """GTK3 thin client which communicates with the OutletDaemon via GRPC."""

    def __init__(self, cfg):
        HasLifecycle.__init__(self)
        self.config = cfg

        self.grpc_stub: Optional[Outlet_pb2_grpc.OutletStub] = None
        self.signal_thread: SignalReceiverThread = SignalReceiverThread(self)

        self._task_runner = TaskRunner()
        self.node_identifier_factory: NodeIdentifierFactory = NodeIdentifierFactory(self)

    def start(self):
        HasLifecycle.start(self)

        self.connect_dispatch_listener(signal=actions.ENQUEUE_UI_TASK, receiver=self._on_ui_task_requested)

        with grpc.insecure_channel(GRPC_SERVER_ADDRESS) as channel:
            self.grpc_stub = Outlet_pb2_grpc.OutletStub(channel)
            logger.info(f'Outlet client connected!')

        self.signal_thread.start()

    def shutdown(self):
        HasLifecycle.shutdown(self)

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

    def get_display_tree(self, tree_id: str, user_path: str = None, spid: SinglePathNodeIdentifier = None, is_startup: bool = False) -> DisplayTree:

        # FIXME: implement in gRPC
        pass

    def start_subtree_load(self, tree_id: str):
        # FIXME: implement in gRPC
        pass
