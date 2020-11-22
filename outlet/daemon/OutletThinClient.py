import sys
import time

import grpc
import logging

from app_config import AppConfig
from daemon.grpc import Outlet_pb2_grpc
from daemon.grpc.Outlet_pb2 import PingRequest, ReadSingleNodeFromDiskRequest
from model.node.node import Node
from OutletFrontend import OutletFrontend
import outlet.daemon.grpc.dto.Node_pb2

logger = logging.getLogger(__name__)


def _node_from_grpc(grpc_node: outlet.daemon.grpc.dto.Node_pb2.Node) -> Node:
    logger.info(f'Got: {grpc_node.uid}')
    return None


# CLASS OutletThinClient
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼

class OutletThinClient(OutletFrontend):
    """GTK3 thin client which communicates with the OutletDaemon via GRPC."""
    def __init__(self, cfg):
        OutletFrontend.__init__(self, cfg)
        self.config = cfg
        self.stub = None
        self.backend = self

    def start(self):
        OutletFrontend.start(self)
        channel = grpc.insecure_channel('localhost:50051')
        self.stub = Outlet_pb2_grpc.OutletStub(channel)
        logger.info(f'Outlet client connected!')

    def shutdown(self):
        OutletFrontend.shutdown(self)

    def read_single_node_from_disk_for_path(self, full_path: str, tree_type: int) -> Node:
        request = ReadSingleNodeFromDiskRequest()
        request.full_path = full_path
        request.tree_type = tree_type
        grpc_node = self.stub.read_single_node_from_disk_for_path(request)
        return _node_from_grpc(grpc_node)


def main():
    if sys.version_info[0] < 3:
        raise Exception("Python 3 or a more recent version is required.")

    if len(sys.argv) >= 2:
        cfg = AppConfig(sys.argv[1])
    else:
        cfg = AppConfig()

    thin_client = OutletThinClient(cfg)
    thin_client.start()

    ping_request = PingRequest()
    logger.info(f'Sending ping!')
    ping_response = thin_client.stub.ping(ping_request)
    logger.info(f'Ping received: {ping_response.timestamp}')
    time.sleep(3)


if __name__ == '__main__':
    main()
