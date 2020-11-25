import logging
import sys
from concurrent import futures

import grpc

from app.backend_integrated import BackendIntegrated
from app_config import AppConfig
from daemon.grpc import Outlet_pb2_grpc
from daemon.grpc.conversion import NodeConverter
from daemon.grpc.Outlet_pb2 import PingResponse, ReadSingleNodeFromDiskRequest, ReadSingleNodeFromDiskResponse
from daemon.grpc.Outlet_pb2_grpc import OutletServicer

logger = logging.getLogger(__name__)


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
        response = ReadSingleNodeFromDiskResponse()
        NodeConverter.optional_node_to_grpc(node, response.node)
        return response


# CLASS OutletDaemon
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼

class OutletDaemon(BackendIntegrated):
    def __init__(self, config):
        self.config = config
        BackendIntegrated.__init__(self, config)
        self._service = OutletGRPCService(self)

    def start(self):
        BackendIntegrated.start(self)

    def shutdown(self):
        BackendIntegrated.shutdown(self)

    def serve(self):
        server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
        Outlet_pb2_grpc.add_OutletServicer_to_server(self._service, server)
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
