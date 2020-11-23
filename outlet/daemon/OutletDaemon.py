import logging
import sys
from concurrent import futures

import grpc
import outlet.daemon.grpc.Node_pb2

from app_config import AppConfig
from daemon.grpc import Outlet_pb2_grpc
from daemon.grpc.Outlet_pb2 import PingResponse, ReadSingleNodeFromDiskRequest
from daemon.grpc.Outlet_pb2_grpc import OutletServicer
from model.node.node import Node
from OutletBackend import OutletBackend

logger = logging.getLogger(__name__)


def _node_to_grpc(node: Node):
    grpc_node = outlet.daemon.grpc.Node_pb2.Node()
    grpc_node.uid = int(node.uid)
    for full_path in node.get_path_list():
        grpc_node.path_list.append(full_path)
    grpc_node.nid = str(node.identifier)
    grpc_node.trashed = node.get_trashed_status()
    grpc_node.is_shared = node.is_shared
    return grpc_node


class OutletGRPCService(OutletServicer):
    def __init__(self, parent):
        self.cacheman = parent.cacheman

    def ping(self, request, context):
        logger.info(f'Got ping!')
        response = PingResponse()
        response.timestamp = 1000
        return response

    def read_single_node_from_disk_for_path(self, request: ReadSingleNodeFromDiskRequest, context):
        node = self.cacheman.read_single_node_from_disk_for_path(request.full_path, request.tree_type)
        logger.info(f'Returning: {node}')
        return _node_to_grpc(node)


# CLASS OutletDaemon
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼

class OutletDaemon(OutletBackend):
    def __init__(self, config):
        self.config = config
        OutletBackend.__init__(self, config, self)
        self.backend = OutletGRPCService(self)

    def start(self):
        OutletBackend.start(self)

    def shutdown(self):
        OutletBackend.shutdown(self)

    def serve(self):
        server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
        Outlet_pb2_grpc.add_OutletServicer_to_server(self.backend, server)
        server.add_insecure_port('[::]:50051')
        logger.info('gRPC server starting...')
        server.start()
        logger.info('gRPC server started!')
        server.wait_for_termination()
        logger.info('gRPC server stopped!')


def main():
    if sys.version_info[0] < 3:
        raise Exception("Python 3 or a more recent version is required.")

    if len(sys.argv) >= 2:
        config = AppConfig(sys.argv[1])
    else:
        config = AppConfig()

    logger.info(f'Creating OutletDameon')
    daemon = OutletDaemon(config)
    daemon.start()

    try:
        daemon.serve()
        sys.exit(0)
    except KeyboardInterrupt:
        logger.info('Caught KeyboardInterrupt. Quitting')
        daemon.shutdown()


if __name__ == '__main__':
    main()
