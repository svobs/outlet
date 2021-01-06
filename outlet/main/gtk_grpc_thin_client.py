import logging
import sys

from util import main_util, daemon_util
from ui.gtk.gtk_frontend import OutletApplication
from backend.daemon.client.grpc_client import BackendGRPCClient

logger = logging.getLogger(__name__)


# GTK3 THIN CLIENT with GRPC COMMUNICATION TO BACKEND SERVER
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼

def main():
    if sys.version_info[0] < 3:
        raise Exception("Python 3 or a more recent version is required.")

    config = main_util.do_main_boilerplate(executing_script_path=__file__)

    daemon_util.launch_daemon_if_needed()

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
