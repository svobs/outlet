import logging
import os
import subprocess
import sys

from util import file_util

import psutil

logger = logging.getLogger(__name__)

PYTHON_EXE = sys.executable
DAEMON_SCRIPT_PATH = file_util.get_resource_path('outlet/main/grpc_server_daemon.py')


def launch_daemon_if_needed():
    cmdline = [PYTHON_EXE, DAEMON_SCRIPT_PATH]
    logger.debug(f'Checking to see if daemon is running: checking cmdline for: {cmdline}')
    for process in psutil.process_iter():
        if process.cmdline() == cmdline:
            logger.info(f'Found running process ({process.pid}); no action needed')
            return

    logger.info(f'Process not found; launching: "{PYTHON_EXE} {DAEMON_SCRIPT_PATH}"')
    launch_daemon()


def launch_daemon():
    args = [PYTHON_EXE, DAEMON_SCRIPT_PATH]
    subprocess.Popen(args)

