import logging
import sys
import util.main_util
from backend.backend_integrated import BackendIntegrated

from ui.gtk.gtk_frontend import OutletApplication

logger = logging.getLogger(__name__)


# GTK3 [THICK] APPLICATION
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼

def main():
    config = util.main_util.do_main_boilerplate(executing_script_path=__file__)
    backend = BackendIntegrated(config)
    app = OutletApplication(config, backend)
    try:
        exit_status = app.run(sys.argv)
        sys.exit(exit_status)
    except KeyboardInterrupt:
        logger.info('Caught KeyboardInterrupt. Quitting')
        app.shutdown()


if __name__ == '__main__':
    main()
