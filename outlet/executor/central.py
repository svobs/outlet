import threading
import logging

from command.cmd_executor import CommandExecutor
from global_actions import GlobalActions
from task_runner import CentralTaskRunner

logger = logging.getLogger(__name__)


# CLASS CentralExecutor
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼

class CentralExecutor:
    """Half-baked proto-module which will at least let me see all execution in one place"""
    def __init__(self, application):
        self.app = application
        self._command_executor = CommandExecutor(self.app)
        self._global_actions = GlobalActions(self.app)
        self._task_runner = CentralTaskRunner(self.app)
        self.enable_op_execution_thread = application.config.get('executor.enable_op_execution_thread')

        self._op_execution_thread = threading.Thread(target=self._run_op_execution_thread, name='OpExecutionThread', daemon=True)
        """Executes changes as needed in its own thread, which blocks until a change is available."""

    def start(self):
        logger.debug('Central Executor starting')
        self._global_actions.start()

        if self.enable_op_execution_thread:
            self.start_op_execution_thread()
        else:
            logger.warning('OpExecutionThread is disabled!')

    def start_op_execution_thread(self):
        if not self._op_execution_thread.is_alive():
            logger.debug('Starting op execution thread...')
            self._op_execution_thread.start()

    def submit_async_task(self, task_func, *args):
        """Will expand on this later."""
        # TODO: add mechanism to prioritize some tasks over others

        self._task_runner.enqueue(task_func, *args)

    def shutdown(self):
        if self._task_runner:
            self._task_runner.shutdown()
            self._task_runner = None

        # Kill listeners by dereferencing parent:
        if self._global_actions:
            self._global_actions.shutdown()
            self._global_actions = None

        logger.debug('CentralExecutor shut down')

    def _run_op_execution_thread(self):
        """This is a consumer thread for the ChangeManager's dependency tree"""
        logger.info('Starting OpExecutionThread...')

        while True:
            # Should be ok to do simple infinite loop, because get_next_command() will block until work is available.
            # May need to throttle here in the future however if we are seeing hiccups in the UI for large numbers of operations

            command = self.app.cache_manager.get_next_command()
            if not command:
                logger.debug('Got None for next command. Shutting down')
                self.shutdown()
                return

            logger.debug(f'Got a command to execute: {command.__class__.__name__}')

            self.submit_async_task(self._command_executor.execute_command, command)

