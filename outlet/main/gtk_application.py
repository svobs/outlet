import logging
import sys
from main import bootstrap
import util.main_util
from be.backend_integrated import BackendIntegrated

from fe.gtk.gtk_frontend import OutletApplication

logger = logging.getLogger(__name__)


# GTK3 [THICK] APPLICATION
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼

def main():
    bootstrap.configure()
    app_config = util.main_util.do_main_boilerplate(executing_script_path=__file__)
    backend = BackendIntegrated(app_config)
    app = OutletApplication(backend)
    try:
        exit_status = app.run(sys.argv)
        sys.exit(exit_status)
    except KeyboardInterrupt:
        logger.info('Caught KeyboardInterrupt. Quitting')
        app.shutdown()
    except RuntimeError:
        logger.exception(f'Fatal error; shutting down')
        app.shutdown()


if __name__ == '__main__':
    main()
