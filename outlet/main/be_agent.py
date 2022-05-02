#!/usr/local/bin/python3
from main import bootstrap
import sys
from be.agent.svr.be_grpc_server import OutletAgent
import logging

from util import main_util

logger = logging.getLogger(__name__)


# OUTLET AGENT: runs the backend and provides gRPC access to its APIs
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼


def main():
    bootstrap.configure()
    app_config = main_util.do_main_boilerplate(__file__)
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
