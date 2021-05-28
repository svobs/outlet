import logging
import platform
import signal
import subprocess
import sys
from typing import Callable

from util import file_util

import psutil

logger = logging.getLogger(__name__)

PYTHON_EXE = sys.executable
AGENT_SCRIPT_PATH = file_util.get_resource_path('outlet/main/grpc_server_agent.py')


def launch_daemon_if_needed(kill_existing=False):
    def on_found_func(process):
        if kill_existing:
            logger.warning(f'Killing existing process ({process.pid})')
            process.kill()
            process.wait(5)  # throws exception if timeout
        else:
            logger.info(f'Found running process ({process.pid}); no action needed')

    proc_found = _do_for_running_process(on_found_func)

    if not proc_found:
        logger.info(f'Process not found; launching: "{PYTHON_EXE} {AGENT_SCRIPT_PATH}"')
        launch_daemon()


def terminate_daemon_if_found():
    def on_found_func(process):
        logger.warning(f'Terminating existing process ({process.pid})')
        process.terminate()
        process.wait(5)  # throws exception if timeout
        logger.warning(f'Process ({process.pid}) terminated')

    _do_for_running_process(on_found_func)


def _do_for_running_process(on_found_func: Callable) -> bool:
    cmdline = [PYTHON_EXE, AGENT_SCRIPT_PATH]
    logger.debug(f'Checking to see if agent is running: checking cmdline for: {cmdline}')
    proc_found: bool = False

    for process in psutil.process_iter():
        if process.cmdline() == cmdline:
            proc_found = True
            on_found_func(process)

    return proc_found


def launch_daemon():
    args = [PYTHON_EXE, AGENT_SCRIPT_PATH]
    if platform.system() == 'Windows':
        logger.debug('OS is Windows!')
        creationflags = subprocess.DETACHED_PROCESS
        subprocess.Popen(args, stdin=subprocess.DEVNULL, stderr=subprocess.DEVNULL, stdout=subprocess.DEVNULL, close_fds=True,
                         creationflags=creationflags)
    else:
        logger.debug('OS is NOT Windows!')

        def preexec():  # Don't forward signals and thus don't quit with the parent
            # Ignore the SIGINT signal by setting the handler to the standard
            # signal handler SIG_IGN.
            signal.signal(signal.SIGINT, signal.SIG_IGN)
        subprocess.Popen(args, stdin=subprocess.DEVNULL, stderr=subprocess.DEVNULL, stdout=subprocess.DEVNULL, close_fds=True, preexec_fn=preexec)
