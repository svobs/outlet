import logging
from concurrent import futures

import grpc

from app.backend_integrated import BackendIntegrated
from daemon.grpc import Outlet_pb2_grpc
from daemon.outlet_grpc_service import OutletGRPCService
from model.node_identifier_factory import NodeIdentifierFactory

logger = logging.getLogger(__name__)


# CLASS OutletDaemon
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼

class OutletDaemon(BackendIntegrated):
    def __init__(self, config):
        self.config = config
        BackendIntegrated.__init__(self, config)
        self.node_identifier_factory: NodeIdentifierFactory = NodeIdentifierFactory(self)
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
