from concurrent import futures
import logging

import grpc

from app_config import AppConfig
from daemon.grpc import Outlet_pb2_grpc
from daemon.grpc.Outlet_pb2 import PingResponse
from daemon.grpc.Outlet_pb2_grpc import OutletServicer
logger = logging.getLogger(__name__)


class MattOutlet(OutletServicer):
    def ping(self, request, context):
        logger.info(f'Got ping!')
        response = PingResponse()
        response.timestamp = 1000
        return response


class OutletDaemon:

    def serve(self):
        server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
        Outlet_pb2_grpc.add_OutletServicer_to_server(MattOutlet(), server)
        server.add_insecure_port('[::]:50051')
        logger.info('gRPC server starting...')
        server.start()
        logger.info('gRPC server started!')
        server.wait_for_termination()
        logger.info('gRPC server stopped!')


def main():
    config = AppConfig()

    logger.info(f'Creating OutletDameon')
    server = OutletDaemon()
    server.serve()


if __name__ == '__main__':
    main()
