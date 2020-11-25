import logging
from typing import List, Union

import grpc

from app.backend import OutletBackend
from daemon.grpc import Outlet_pb2_grpc
from daemon.grpc.conversion import NodeConverter
from daemon.grpc.Outlet_pb2 import ReadSingleNodeFromDiskRequest
from executor.task_runner import TaskRunner
from model.node.node import Node
from model.node_identifier import NodeIdentifier
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
        node = NodeConverter.optional_node_from_grpc(grpc_response.node)
        logger.info(f'Got node: {node}')
        return node

    def build_identifier(self, tree_type: int = None, path_list: Union[str, List[str]] = None, uid: UID = None,
                         must_be_single_path: bool = False) -> NodeIdentifier:
        pass

