from typing import List

from pydispatch import dispatcher
import logging

from constants import SUPER_DEBUG
from util import file_util
from backend.executor.command.cmd_interface import Command, CommandContext, UserOpStatus
from signal_constants import ID_COMMAND_EXECUTOR, Signal

logger = logging.getLogger(__name__)


class CommandExecutor:
    """
    ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
    CLASS CommandExecutor
    ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼
    """
    def __init__(self, backend):
        self.backend = backend
        val = self.backend.get_config('agent.local_disk.staging_dir')
        self.staging_dir: str = file_util.get_resource_path(val)
        logger.debug(f'Staging dir: "{self.staging_dir}"')
        # TODO: optionally clean staging dir at startup

    def execute_command(self, command: Command, context: CommandContext = None, start_stop_progress: bool = False):
        if not command:
            logger.error(f'No command!')
            return

        if start_stop_progress:
            dispatcher.send(signal=Signal.START_PROGRESS, sender=ID_COMMAND_EXECUTOR, total=command.get_total_work())

        try:
            if not context:
                context = CommandContext(self.staging_dir, self.backend)

            if command.get_status() != UserOpStatus.NOT_STARTED:
                logger.info(f'Skipping command: {command} because it has status {command.get_status()}')
            else:
                status_str: str = f'Executing command: {repr(command)}'
                logger.info(status_str)
                if SUPER_DEBUG:
                    logger.debug(f'Command(uid={command.uid}): src={command.op.src_node}) dst={command.op.dst_node}')
                command.op.result = command.execute(context)
                logger.info(f'Command completed with status {command.get_status().name}: {command.get_description()}')
        except Exception as err:
            description = f'Error executing {command.get_description()}'
            logger.exception(description)
            # Save the error inside the command:
            command.set_error_result(err)

            # Report error to the UI:
            detail = f'Command {command.uid} (op {command.op.op_uid}) failed with error: {command.get_error()}'
            self.backend.report_error(ID_COMMAND_EXECUTOR, msg=description, secondary_msg=detail)

        dispatcher.send(signal=Signal.COMMAND_COMPLETE, sender=ID_COMMAND_EXECUTOR, command=command)
        dispatcher.send(signal=Signal.PROGRESS_MADE, sender=ID_COMMAND_EXECUTOR, progress=command.get_total_work())

        if start_stop_progress:
            dispatcher.send(signal=Signal.STOP_PROGRESS, sender=ID_COMMAND_EXECUTOR)
            if context:
                context.shutdown()

    def execute_batch(self, command_batch: List[Command]):
        """deprecated - use execute_command()"""
        total = 0
        count_commands = len(command_batch)

        if count_commands == 0:
            logger.error(f'Command batch is empty!')
            return

        logger.debug(f'Executing command batch size={count_commands}')

        for command in command_batch:
            total += command.get_total_work()

        dispatcher.send(signal=Signal.START_PROGRESS, sender=ID_COMMAND_EXECUTOR, total=total)
        context = None
        try:
            context = CommandContext(self.staging_dir, self.backend)

            for command_num, command in enumerate(command_batch):
                self.execute_command(command, context, False)

        finally:
            dispatcher.send(signal=Signal.STOP_PROGRESS, sender=ID_COMMAND_EXECUTOR)
            if context:
                context.shutdown()

        logger.debug(f'{_get_total_completed(command_batch)} out of {len(command_batch)} completed without error')


def _get_total_completed(command_batch) -> int:
    """Returns the number of commands which executed successfully"""
    total_succeeded: int = 0
    for command in command_batch:
        if command.completed_without_error():
            total_succeeded += 1
    return total_succeeded
