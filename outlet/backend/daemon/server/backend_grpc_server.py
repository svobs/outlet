import logging
from concurrent import futures

import grpc

from backend.backend_integrated import BackendIntegrated
from backend.daemon.grpc.generated import Outlet_pb2_grpc
from backend.daemon.server.grpc_service import OutletGRPCService
from constants import GRPC_SERVER_MAX_WORKER_THREADS
from util.ensure import ensure_bool, ensure_int

logger = logging.getLogger(__name__)


class OutletDaemon(BackendIntegrated):
    """
    ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
    CLASS OutletDaemon
    ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼
    """
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

        use_fixed_address = ensure_bool(self.config.get('grpc.use_fixed_address'))
        if use_fixed_address:
            port = ensure_int(self.config.get('grpc.fixed_port'))
            logger.debug(f'Config specifies fixed port = {port}')
        else:
            port = 0
        port = server.add_insecure_port(f'[::]:{port}')

        if not use_fixed_address:
            # TODO: zeroconf
            pass

        logger.info(f'gRPC server starting on port {port}...')
        server.start()
        logger.info('gRPC server started!')
        server.wait_for_termination()  # <- blocks
        logger.info('gRPC server stopped!')
