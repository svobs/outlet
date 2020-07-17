import threading
import logging

from pydispatch import dispatcher

from cmd.cmd_executor import CommandExecutor
from global_actions import GlobalActions
from task_runner import CentralTaskRunner
from ui import actions

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

        self._change_thread = threading.Thread(target=self._run_change_thread, name='ChangeExecThread', daemon=True)
        """Executes changes as needed in its own thread, which blocks until a change is available.
        TODO: Might be nice to think of a better name than "ChangeExecThread"..."""

    def start(self):
        self._global_actions.init()
        self._change_thread.start()

        # Kick off cache load now that we have a progress bar
        dispatcher.send(actions.LOAD_ALL_CACHES, sender=actions.ID_CENTRAL_EXEC)

    def submit_async_task(self, task_func, *args):
        """Will expand on this later."""
        # TODO: add mechanism to prioritize some tasks over others

        self._task_runner.enqueue(task_func, *args)

    def execute_command_batch(self, command_batch):
        self.submit_async_task(self._command_executor.execute_batch, command_batch)

    def _run_change_thread(self):
        """This is a consumer thread for the ChangeManager's dependency tree"""

        while True:
            # Should be ok to do simple infinite loop, because get_next_command() will block until work is available.
            # May need to throttle here in the future however if we are seeing hiccups in the UI for large numbers of operations

            command = self.app.cache_manager.get_next_command()
            logger.debug(f'Got a command to execute')

            self.submit_async_task(self._command_executor.execute_batch, [command])

