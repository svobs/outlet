import logging
from typing import List, Optional, Union

import grpc

from app.backend import OutletBackend
from daemon.grpc import Outlet_pb2_grpc
from daemon.grpc.conversion import NodeConverter
from daemon.grpc.Outlet_pb2 import GetNextUid_Request, GetNodeForLocalPath_Request, GetNodeForUid_Request, GetUidForLocalPath_Request, \
    ReadSingleNodeFromDiskRequest
from executor.task_runner import TaskRunner
from model.node.node import Node
from model.node_identifier import NodeIdentifier
from model.node_identifier_factory import NodeIdentifierFactory
from model.uid import UID
from ui import actions
from util.has_lifecycle import HasLifecycle

logger = logging.getLogger(__name__)


# CLASS BackendGRPCClient
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼

class BackendGRPCClient(OutletBackend, HasLifecycle):
    """GTK3 thin client which communicates with the OutletDaemon via GRPC."""

    def __init__(self, cfg):
        HasLifecycle.__init__(self)
        self.config = cfg
        self.grpc_stub = None
        self._task_runner = TaskRunner()
        self.node_identifier_factory: NodeIdentifierFactory = NodeIdentifierFactory(self)

    def start(self):
        HasLifecycle.start(self)

        self.connect_dispatch_listener(signal=actions.ENQUEUE_UI_TASK, receiver=self._on_ui_task_requested)

        channel = grpc.insecure_channel('localhost:50051')
        self.grpc_stub = Outlet_pb2_grpc.OutletStub(channel)
        logger.info(f'Outlet client connected!')

    def shutdown(self):
        HasLifecycle.shutdown(self)

    def _on_ui_task_requested(self, sender, task_func, *args, **kwargs):
        self._task_runner.enqueue(task_func, args)

    def read_single_node_from_disk_for_path(self, full_path: str, tree_type: int) -> Node:
        request = ReadSingleNodeFromDiskRequest()
        request.full_path = full_path
        request.tree_type = tree_type
        grpc_response = self.grpc_stub.read_single_node_from_disk_for_path(request)
        node = NodeConverter.node_from_grpc(grpc_response)
        logger.info(f'Got node: {node}')
        return node

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


