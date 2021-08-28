import faulthandler
import logging
import os
import sys

from app_config import AppConfig
from constants import DIFF_DEBUG_ENABLED, IS_LINUX, IS_MACOS, IS_WINDOWS, SUPER_DEBUG_ENABLED, TRACE_ENABLED

logger = logging.getLogger(__name__)


def do_main_boilerplate(executing_script_path: str = None) -> AppConfig:
    # Dump thread states to console if we get a segfault:
    faulthandler.enable()

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

    if TRACE_ENABLED:
        logger.info('TRACE_ENABLED is true')
    elif SUPER_DEBUG_ENABLED:
        logger.info('SUPER_DEBUG_ENABLED is true')

    if DIFF_DEBUG_ENABLED:
        logger.info('DIFF_DEBUG_ENABLED is true')

    logger.debug(f'IS_LINUX={IS_LINUX} IS_MACOS={IS_MACOS} IS_WINDOWS={IS_WINDOWS}')

    logger.debug(f'Working dir is: {os.getcwd()}')
    python_path = os.environ.get('PYTHONPATH', None)
    logger.debug(f'PYTHONPATH is: {python_path}')
    logger.debug(f'Sys path is: {sys.path}')

    return app_config
