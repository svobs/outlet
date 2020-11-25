import threading
import logging

from pydispatch import dispatcher

from command.cmd_executor import CommandExecutor
from executor.task_runner import TaskRunner
from global_actions import GlobalActions
from ui import actions
from util.has_lifecycle import HasLifecycle

logger = logging.getLogger(__name__)


# CLASS CentralExecutor
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼

class CentralExecutor(HasLifecycle):
    """Half-baked proto-module which will at least let me see all execution in one place"""
    def __init__(self, app):
        HasLifecycle.__init__(self)
        self.app = app
        self._shutdown_requested: bool = False
        self._command_executor = CommandExecutor(self.app)
        self._global_actions = GlobalActions(self.app)
        self._task_runner = TaskRunner()
        self.enable_op_execution_thread = app.config.get('executor.enable_op_execution_thread')

        self._op_execution_thread = threading.Thread(target=self._run_op_execution_thread, name='OpExecutionThread', daemon=True)
        """Executes changes as needed in its own thread, which blocks until a change is available."""

        self._cv_can_execute = threading.Condition()

    def start(self):
        logger.debug('Central Executor starting')
        HasLifecycle.start(self)

        self._global_actions.start()

        if self.enable_op_execution_thread:
            self.start_op_execution_thread()
        else:
            logger.warning(f'{self._op_execution_thread.name} is disabled!')

        self.connect_dispatch_listener(signal=actions.PAUSE_OP_EXECUTION, receiver=self._pause_op_execution)
        self.connect_dispatch_listener(signal=actions.RESUME_OP_EXECUTION, receiver=self._start_op_execution)

    def start_op_execution_thread(self):
        if not self._op_execution_thread.is_alive():
            logger.debug(f'Starting {self._op_execution_thread.name}...')
            self._op_execution_thread.start()
        else:
            with self._cv_can_execute:
                # resume thread
                self._cv_can_execute.notifyAll()

    def submit_async_task(self, task_func, *args):
        """Will expand on this later."""
        # TODO: add mechanism to prioritize some tasks over others

        self._task_runner.enqueue(task_func, *args)

    def shutdown(self):
        HasLifecycle.shutdown(self)
        self.app = None
        self._command_executor = None
        self._global_actions = None
        self._task_runner = None

        self._shutdown_requested = True
        with self._cv_can_execute:
            self._cv_can_execute.notifyAll()

        logger.debug('CentralExecutor shut down')

    def _run_op_execution_thread(self):
        """This is a consumer thread for the ChangeManager's dependency tree"""
        while not self._shutdown_requested:
            # Should be ok to do simple infinite loop, because get_next_command() will block until work is available.
            # May need to throttle here in the future however if we are seeing hiccups in the UI for large numbers of operations

            command = self.app.cacheman.get_next_command()
            if not command:
                logger.debug('Got None for next command. Shutting down')
                self.shutdown()
                return

            while not self.enable_op_execution_thread:
                logger.debug(f'Op execution paused; sleeping until notified')
                with self._cv_can_execute:
                    self._cv_can_execute.wait()

                if self._shutdown_requested:
                    return

            logger.debug(f'Got a command to execute: {command.__class__.__name__}')
            self.submit_async_task(self._command_executor.execute_command, command, None, True)

    def _start_op_execution(self, sender):
        logger.debug(f'Received signal "{actions.RESUME_OP_EXECUTION}" from {sender}')
        self.enable_op_execution_thread = True
        self.start_op_execution_thread()
        logger.debug(f'Sending signal "{actions.OP_EXECUTION_PLAY_STATE_CHANGED}" (play_enabled={self.enable_op_execution_thread})')
        dispatcher.send(signal=actions.OP_EXECUTION_PLAY_STATE_CHANGED, sender=actions.ID_CENTRAL_EXEC)

    def _pause_op_execution(self, sender):
        logger.debug(f'Received signal "{actions.PAUSE_OP_EXECUTION}" from {sender}')
        self.enable_op_execution_thread = False
        logger.debug(f'Sending signal "{actions.OP_EXECUTION_PLAY_STATE_CHANGED}" (play_enabled={self.enable_op_execution_thread})')
        dispatcher.send(signal=actions.OP_EXECUTION_PLAY_STATE_CHANGED, sender=actions.ID_CENTRAL_EXEC)
