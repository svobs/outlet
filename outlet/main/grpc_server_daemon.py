import logging
import sys
import util.main_util
from backend.agent.server.backend_grpc_server import OutletAgent

logger = logging.getLogger(__name__)


# OUTLET DAEMON: provides gRPC access to its APIs
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼


def main():
    logger.info(f'Creating OutletDameon')
    app_config = util.main_util.do_main_boilerplate(executing_script_path=__file__)
    agent = OutletAgent(app_config)

    try:
        agent.start()
        agent.serve()
        sys.exit(0)
    except KeyboardInterrupt:
        logger.info(f'Caught KeyboardInterrupt. Quitting')
        agent.shutdown()
    except RuntimeError:
        logger.exception(f'Fatal error (shutting down)')
        agent.shutdown()


if __name__ == '__main__':
    main()
