import threading
import logging

from pydispatch import dispatcher

from backend.diff.task.tree_diff_task import TreeDiffTask
from backend.executor.command.cmd_executor import CommandExecutor
from util.task_runner import TaskRunner
from global_actions import GlobalActions
from model.display_tree.build_struct import DiffResultTreeIds
from signal_constants import ID_CENTRAL_EXEC, ID_LEFT_DIFF_TREE, ID_LEFT_TREE, ID_RIGHT_DIFF_TREE, ID_RIGHT_TREE, Signal
from util.has_lifecycle import HasLifecycle

logger = logging.getLogger(__name__)


class CentralExecutor(HasLifecycle):
    """
    ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
    CLASS CentralExecutor

    Half-baked proto-module which will at least let me see all execution in one place
    ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼
    """
    def __init__(self, backend):
        HasLifecycle.__init__(self)
        self.backend = backend
        self._shutdown_requested: bool = False
        self._command_executor = CommandExecutor(self.backend)
        self._global_actions = GlobalActions(self.backend)
        self._be_task_runner = TaskRunner()
        self.enable_op_execution = backend.get_config('executor.enable_op_execution')
        self._cv_can_execute = threading.Condition()

        self._op_execution_thread = threading.Thread(target=self._run_op_execution_thread, name='OpExecutionThread', daemon=True)
        """Executes changes as needed in its own thread, which blocks until a change is available."""

    def start(self):
        logger.debug('Central Executor starting')
        HasLifecycle.start(self)

        self._global_actions.start()

        if self.enable_op_execution:
            self.start_op_execution_thread()
        else:
            logger.warning(f'{self._op_execution_thread.name} is disabled!')

        self.connect_dispatch_listener(signal=Signal.PAUSE_OP_EXECUTION, receiver=self._pause_op_execution)
        self.connect_dispatch_listener(signal=Signal.RESUME_OP_EXECUTION, receiver=self._start_op_execution)

    def submit_async_task(self, task_func, *args):
        """Will expand on this later."""
        # TODO: add mechanism to prioritize some tasks over others

        self._be_task_runner.enqueue(task_func, *args)

    def shutdown(self):
        HasLifecycle.shutdown(self)
        self.backend = None
        self._command_executor = None
        self._global_actions = None
        self._be_task_runner = None

        self._shutdown_requested = True
        try:
            with self._cv_can_execute:
                self._cv_can_execute.notifyAll()
        except AttributeError:
            pass

        logger.debug('CentralExecutor shut down')

    # Op Execution Thread
    # ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼

    def start_op_execution_thread(self):
        if not self._op_execution_thread.is_alive():
            logger.debug(f'Starting {self._op_execution_thread.name}...')
            self._op_execution_thread.start()
        else:
            with self._cv_can_execute:
                # resume thread
                self._cv_can_execute.notifyAll()

    def _run_op_execution_thread(self):
        """This is a consumer thread for the ChangeManager's dependency tree"""
        while not self._shutdown_requested:
            # Should be ok to do simple infinite loop, because get_next_command() will block until work is available.
            # May need to throttle here in the future however if we are seeing hiccups in the UI for large numbers of operations

            command = None
            try:
                logger.debug(f'{self._op_execution_thread.name}: Getting next command...')
                command = self.backend.cacheman.get_next_command()  # Blocks until received
                if not command:
                    logger.debug(f'{self._op_execution_thread.name}: Got None for next command. Shutting down')
                    self.shutdown()
                    return
            except RuntimeError as e:
                logger.exception(f'BAD: caught exception in {self._op_execution_thread.name}: halting execution')
                self.backend.report_error(sender=ID_CENTRAL_EXEC, msg='Error executing command', secondary_msg=f'{e}')
                self._pause_op_execution(sender=ID_CENTRAL_EXEC)

            # best place to pause is here, after we got unblocked from command (remember: waiting for a command is also blocking behavior)
            while not self.enable_op_execution:
                logger.debug(f'{self._op_execution_thread.name}: paused; sleeping until notified')
                with self._cv_can_execute:
                    self._cv_can_execute.wait()

                if self._shutdown_requested:
                    return

            if command:
                logger.debug(f'{self._op_execution_thread.name}: Got a command to execute: {command.__class__.__name__}')
                self.submit_async_task(self._command_executor.execute_command, command, None, True)

    def _start_op_execution(self, sender):
        logger.debug(f'Received signal "{Signal.RESUME_OP_EXECUTION.name}" from {sender}')
        self.enable_op_execution = True
        self.start_op_execution_thread()
        logger.debug(f'Sending signal "{Signal.OP_EXECUTION_PLAY_STATE_CHANGED.name}" (is_enabled={self.enable_op_execution})')
        dispatcher.send(signal=Signal.OP_EXECUTION_PLAY_STATE_CHANGED, sender=ID_CENTRAL_EXEC, is_enabled=self.enable_op_execution)

    def _pause_op_execution(self, sender):
        logger.debug(f'Received signal "{Signal.PAUSE_OP_EXECUTION.name}" from {sender}')
        self.enable_op_execution = False
        logger.debug(f'Sending signal "{Signal.OP_EXECUTION_PLAY_STATE_CHANGED.name}" (is_enabled={self.enable_op_execution})')
        dispatcher.send(signal=Signal.OP_EXECUTION_PLAY_STATE_CHANGED, sender=ID_CENTRAL_EXEC, is_enabled=self.enable_op_execution)

    # Misc tasks
    # ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼

    def start_tree_diff(self, tree_id_left, tree_id_right) -> DiffResultTreeIds:
        """Starts the Diff Trees task async"""
        assert tree_id_left == ID_LEFT_TREE and tree_id_right == ID_RIGHT_TREE, f'Wrong tree IDs: {ID_LEFT_TREE}, {ID_RIGHT_TREE}'
        tree_id_struct: DiffResultTreeIds = DiffResultTreeIds(ID_LEFT_DIFF_TREE, ID_RIGHT_DIFF_TREE)
        self.submit_async_task(TreeDiffTask.do_tree_diff, self.backend, ID_CENTRAL_EXEC, tree_id_left, tree_id_right, tree_id_struct)
        return tree_id_struct
