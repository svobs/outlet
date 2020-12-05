import logging
from concurrent import futures

import grpc

from app.backend_integrated import BackendIntegrated
from constants import GRPC_SERVER_ADDRESS, GRPC_SERVER_MAX_WORKER_THREADS
from daemon.grpc import Outlet_pb2_grpc
from daemon.grpc_server import OutletGRPCService

logger = logging.getLogger(__name__)


# CLASS OutletDaemon
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼

class OutletDaemon(BackendIntegrated):
    def __init__(self, config):
        self.config = config
        BackendIntegrated.__init__(self, config)

        self._grpc_service = OutletGRPCService(self)
        """Contains the gRPC client code"""

    def start(self):
        self._grpc_service.start()
        BackendIntegrated.start(self)

    def shutdown(self):
        BackendIntegrated.shutdown(self)
        self._grpc_service.shutdown()

    def serve(self):
        server = grpc.server(futures.ThreadPoolExecutor(max_workers=GRPC_SERVER_MAX_WORKER_THREADS))
        Outlet_pb2_grpc.add_OutletServicer_to_server(self._grpc_service, server)
        server.add_insecure_port(GRPC_SERVER_ADDRESS)
        logger.info('gRPC server starting...')
        server.start()
        logger.info('gRPC server started!')
        server.wait_for_termination()  # <- blocks
        logger.info('gRPC server stopped!')
