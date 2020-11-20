import time

import grpc
import logging

from daemon.grpc import Outlet_pb2_grpc
from daemon.grpc.Outlet_pb2 import PingRequest

logger = logging.getLogger(__name__)


def main():
    channel = grpc.insecure_channel('localhost:50051')
    stub = Outlet_pb2_grpc.OutletStub(channel)
    logger.info(f'Outlet client connected!')
    ping_request = PingRequest()
    logger.info(f'Sending ping!')
    stub.ping(ping_request)
    logger.info(f'Ping received!')
    time.sleep(3)


if __name__ == '__main__':
    main()
