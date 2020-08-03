from typing import List

from pydispatch import dispatcher
import logging

from util import file_util
from cmd.cmd_interface import Command, CommandContext, CommandStatus
from ui import actions

logger = logging.getLogger(__name__)


# CLASS CommandExecutor
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼

class CommandExecutor:
    def __init__(self, application):
        self.application = application
        val = self.application.config.get('staging_dir')
        self.staging_dir = file_util.get_resource_path(val)
        # TODO: clean staging dir at startup

    def execute_command(self, command: Command):
        if not command:
            logger.error(f'No command!')
            return

        needs_gdrive = command.needs_gdrive()

        try:
            context = CommandContext(self.staging_dir, self.application, actions.ID_COMMAND_EXECUTOR, needs_gdrive)

            if command.status() != CommandStatus.NOT_STARTED:
                logger.info(f'Skipping command: {command} because it has status {command.status()}')
            else:
                try:
                    status = f'Executing command {command.get_description()}'
                    dispatcher.send(signal=actions.SET_PROGRESS_TEXT, sender=actions.ID_COMMAND_EXECUTOR, msg=status)
                    logger.info(f'{status}: {repr(command)}')
                    command.result = command.execute(context)
                except Exception as err:
                    logger.exception(f'While executing {command.get_description()}')
                    # Save the error inside the command:
                    command.set_error_result(err)

                dispatcher.send(signal=actions.COMMAND_COMPLETE, sender=actions.ID_COMMAND_EXECUTOR, command=command)
                dispatcher.send(signal=actions.PROGRESS_MADE, sender=actions.ID_COMMAND_EXECUTOR, progress=command.get_total_work())

        finally:
            dispatcher.send(signal=actions.STOP_PROGRESS, sender=actions.ID_COMMAND_EXECUTOR)

        logger.debug(f'{command.get_description()} completed without error')

    def execute_batch(self, command_batch: List[Command]):
        """deprecated - use execute_command()"""
        total = 0
        needs_gdrive = False
        count_commands = len(command_batch)

        if count_commands == 0:
            logger.error(f'Command batch is empty!')
            return

        logger.debug(f'Executing command batch size={count_commands}')

        for command in command_batch:
            total += command.get_total_work()
            if command.needs_gdrive():
                needs_gdrive = True

        dispatcher.send(signal=actions.START_PROGRESS, sender=actions.ID_COMMAND_EXECUTOR, total=total)
        try:
            context = CommandContext(self.staging_dir, self.application, actions.ID_COMMAND_EXECUTOR, needs_gdrive)

            for command_num, command in enumerate(command_batch):
                if command.status() != CommandStatus.NOT_STARTED:
                    logger.info(f'Skipping command: {command} because it has status {command.status()}')
                else:
                    try:
                        status = f'Executing command {(command_num + 1)} of {len(command_batch)}'
                        dispatcher.send(signal=actions.SET_PROGRESS_TEXT, sender=actions.ID_COMMAND_EXECUTOR, msg=status)
                        logger.info(f'{status}: {repr(command)}')
                        command.result = command.execute(context)
                    except Exception as err:
                        logger.exception(f'While executing {command.get_description()}')
                        # Save the error inside the command:
                        command.set_error_result(err)

                    dispatcher.send(signal=actions.COMMAND_COMPLETE, sender=actions.ID_COMMAND_EXECUTOR, command=command)
                    dispatcher.send(signal=actions.PROGRESS_MADE, sender=actions.ID_COMMAND_EXECUTOR, progress=command.get_total_work())

        finally:
            dispatcher.send(signal=actions.STOP_PROGRESS, sender=actions.ID_COMMAND_EXECUTOR)

        logger.debug(f'{_get_total_completed(command_batch)} out of {len(command_batch)} completed without error')


def _get_total_completed(command_batch) -> int:
    """Returns the number of commands which executed successfully"""
    total_succeeded: int = 0
    for command in command_batch:
        if command.completed_without_error():
            total_succeeded += 1
    return total_succeeded
