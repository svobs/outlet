import logging
import sys

from util import main_util, daemon_util
from ui.gtk.gtk_frontend import OutletApplication
from backend.agent.client.grpc_client import BackendGRPCClient
from util.ensure import ensure_bool

logger = logging.getLogger(__name__)

import gi
gi.require_version("Gtk", "3.0")


# GTK3 THIN CLIENT with GRPC COMMUNICATION TO BACKEND SERVER
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼

def main():
    if sys.version_info[0] < 3:
        raise Exception("Python 3 or a more recent version is required.")

    app_config = main_util.do_main_boilerplate(executing_script_path=__file__)

    if ensure_bool(app_config.get('thin_client.launch_server_on_start')):
        kill_existing = ensure_bool(app_config.get('thin_client.kill_existing_server_on_start'))
        daemon_util.launch_daemon_if_needed(kill_existing=kill_existing)

    backend = BackendGRPCClient(app_config)
    try:
        backend.start()
    except RuntimeError:
        logger.exception(f'Fatal error while starting backend; shutting down')
        backend.shutdown()
        exit(1)

    app = OutletApplication(backend)
    try:
        exit_status = app.run(sys.argv)
        sys.exit(exit_status)
    except KeyboardInterrupt:
        logger.info('Caught KeyboardInterrupt. Quitting')
        app.shutdown()


if __name__ == '__main__':
    main()
