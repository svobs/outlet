from pydispatch import dispatcher

from command.command_executor import CommandExecutor
from global_actions import GlobalActions
from task_runner import CentralTaskRunner
from ui import actions


# CLASS CentralExecutor
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼

class CentralExecutor:
    """Half-baked proto-module which will at least let me see all execution in one place"""
    def __init__(self, application):
        self.app = application
        self._command_executor = CommandExecutor(self.app)
        self._global_actions = GlobalActions(self.app)
        self._task_runner = CentralTaskRunner(self.app)

    def start(self):
        self._global_actions.init()

        # Kick off cache load now that we have a progress bar
        dispatcher.send(actions.LOAD_ALL_CACHES, sender=actions.ID_CENTRAL_EXEC)

    def submit_async_task(self, task_func, *args):
        """Will expand on this later."""
        # TODO: add mechanism to prioritize some tasks over others

        self._task_runner.enqueue(task_func, *args)

    def execute_command_batch(self, command_batch):
        self.submit_async_task(self._command_executor.execute_batch, command_batch)
