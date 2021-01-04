import logging
import sys
import util.main_util
from backend.daemon.server.backend_grpc_server import OutletDaemon

logger = logging.getLogger(__name__)


# OUTLET DAEMON: provides gRPC access to its APIs
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼


def main():
    logger.info(f'Creating OutletDameon')

    daemon = OutletDaemon(util.main_util.do_main_boilerplate())
    daemon.start()

    try:
        daemon.serve()
        sys.exit(0)
    except KeyboardInterrupt:
        logger.info('Caught KeyboardInterrupt. Quitting')
        daemon.shutdown()


if __name__ == '__main__':
    main()