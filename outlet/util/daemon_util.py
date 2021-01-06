import logging
import os
import platform
import signal
import subprocess
import sys

from util import file_util

import psutil

logger = logging.getLogger(__name__)

PYTHON_EXE = sys.executable
DAEMON_SCRIPT_PATH = file_util.get_resource_path('outlet/main/grpc_server_daemon.py')


def launch_daemon_if_needed(kill_existing=False):
    cmdline = [PYTHON_EXE, DAEMON_SCRIPT_PATH]
    logger.debug(f'Checking to see if daemon is running: checking cmdline for: {cmdline}')
    for process in psutil.process_iter():
        if process.cmdline() == cmdline:
            if kill_existing:
                logger.warning(f'Killing existing process ({process.pid})')
                process.kill()
            else:
                logger.info(f'Found running process ({process.pid}); no action needed')
                return

    logger.info(f'Process not found; launching: "{PYTHON_EXE} {DAEMON_SCRIPT_PATH}"')
    launch_daemon()


def launch_daemon():
    args = [PYTHON_EXE, DAEMON_SCRIPT_PATH]
    if platform.system() == 'Windows':
        logger.debug('OS is Windows!')
        creationflags = subprocess.DETACHED_PROCESS
        subprocess.Popen(args, stdin=subprocess.DEVNULL, stderr=subprocess.DEVNULL, stdout=subprocess.DEVNULL, close_fds=True, creationflags=creationflags)
    else:
        logger.debug('OS is NOT Windows!')

        def preexec():  # Don't forward signals and thus don't quit with the parent
            # Ignore the SIGINT signal by setting the handler to the standard
            # signal handler SIG_IGN.
            signal.signal(signal.SIGINT, signal.SIG_IGN)
        subprocess.Popen(args, stdin=subprocess.DEVNULL, stderr=subprocess.DEVNULL, stdout=subprocess.DEVNULL, close_fds=True, preexec_fn = preexec)

