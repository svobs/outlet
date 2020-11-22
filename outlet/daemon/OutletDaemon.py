import logging
import sys
from concurrent import futures

import grpc

from app_config import AppConfig
from daemon.grpc import Outlet_pb2_grpc
from daemon.grpc.Outlet_pb2 import PingResponse
from OutletBackend import OutletBackend

logger = logging.getLogger(__name__)


# CLASS OutletDaemon
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼

class OutletDaemon(OutletBackend):
    def __init__(self, config):
        self.config = config
        OutletBackend.__init__(self, config)

    def start(self):
        OutletBackend.start(self)

    def shutdown(self):
        OutletBackend.shutdown(self)

    def serve(self):
        server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
        Outlet_pb2_grpc.add_OutletServicer_to_server(self, server)
        server.add_insecure_port('[::]:50051')
        logger.info('gRPC server starting...')
        server.start()
        logger.info('gRPC server started!')
        server.wait_for_termination()
        logger.info('gRPC server stopped!')

    def ping(self, request, context):
        logger.info(f'Got ping!')
        response = PingResponse()
        response.timestamp = 1000
        return response


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
