import logging
import os

import psutil

PYTHON_EXE = 'python3'
DAEMON_SCRIPT_PATH = 'outletd.py'

logger = logging.getLogger(__name__)


def launch_daemon_if_needed():
    for process in psutil.process_iter():
        if process.cmdline() == [PYTHON_EXE, DAEMON_SCRIPT_PATH]:
            logger.info(f'Found running process ({process.pid}); no action needed')
            return

    logger.info(f'Process not found; launching: "{PYTHON_EXE} {DAEMON_SCRIPT_PATH}"')
    launch_daemon()


def launch_daemon():
    os.system(f'{PYTHON_EXE} {DAEMON_SCRIPT_PATH}')
