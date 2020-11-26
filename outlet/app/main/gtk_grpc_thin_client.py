import logging
import sys
import time

from app.gtk_frontend import OutletApplication
from app_config import AppConfig
from daemon.backend_client import BackendGRPCClient
from daemon.grpc.Outlet_pb2 import PingRequest

logger = logging.getLogger(__name__)


# GTK3 THIN CLIENT with GRPC COMMUNICATION TO BACKEND SERVER
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼

def main():
    if sys.version_info[0] < 3:
        raise Exception("Python 3 or a more recent version is required.")

    if len(sys.argv) >= 2:
        config = AppConfig(sys.argv[1])
    else:
        config = AppConfig()

    backend = BackendGRPCClient(config)
    app = OutletApplication(config, backend)
    try:
        exit_status = app.run(sys.argv)
        sys.exit(exit_status)
    except KeyboardInterrupt:
        logger.info('Caught KeyboardInterrupt. Quitting')
        app.shutdown()


def test_main():
    if sys.version_info[0] < 3:
        raise Exception("Python 3 or a more recent version is required.")

    if len(sys.argv) >= 2:
        cfg = AppConfig(sys.argv[1])
    else:
        cfg = AppConfig()

    thin_client = BackendGRPCClient(cfg)
    thin_client.start()

    ping_request = PingRequest()
    logger.info(f'Sending ping!')
    ping_response = thin_client.grpc_stub.ping(ping_request)
    logger.info(f'Ping received: {ping_response.timestamp}')
    time.sleep(3)


if __name__ == '__main__':
    main()
