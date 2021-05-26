import logging
import os
import sys

from app_config import AppConfig
from constants import SUPER_DEBUG

logger = logging.getLogger(__name__)


def do_main_boilerplate(executing_script_path: str = None) -> AppConfig:
    if executing_script_path:
        executing_script_name = os.path.basename(executing_script_path)
        if executing_script_name.endswith('.py'):
            executing_script_name = executing_script_name.replace('.py', '')
    else:
        executing_script_name = None

    if sys.version_info[0] < 3:
        raise Exception("Python 3 or a more recent version is required.")

    if len(sys.argv) >= 2:
        app_config = AppConfig(config_file_path=sys.argv[1], executing_script_name=executing_script_name)
    else:
        app_config = AppConfig(executing_script_name=executing_script_name)

    # -- logger is now available --

    if SUPER_DEBUG:
        logger.info('SUPER_DEBUG is enabled')

    logger.debug(f'Working dir is: {os.getcwd()}')
    python_path = os.environ.get('PYTHONPATH', None)
    logger.debug(f'PYTHONPATH is: {python_path}')
    logger.debug(f'Sys path is: {sys.path}')

    return app_config
