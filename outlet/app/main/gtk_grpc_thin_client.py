import logging
import sys

from app.gtk_frontend import OutletApplication
from app_config import AppConfig
from daemon.grpc_client import BackendGRPCClient

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


if __name__ == '__main__':
    main()
