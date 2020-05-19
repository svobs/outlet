from pydispatch import dispatcher
import logging

import file_util
from command.command import CommandContext, CommandList, CommandStatus
from ui import actions

logger = logging.getLogger(__name__)


class CommandExecutor:
    def __init__(self, application):
        self.application = application
        val = self.application.config.get('staging_dir')
        self.staging_dir = file_util.get_resource_path(val)
        # TODO: clean staging dir at startup

    def enqueue(self, command_list: CommandList):
        self.application.task_runner.enqueue(self._execute_all, command_list)

    def _execute_all(self, command_list: CommandList):
        total = 0
        needs_gdrive = False
        for command in command_list:
            total += command.get_total_work()
            if command.needs_gdrive():
                needs_gdrive = True

        dispatcher.send(signal=actions.START_PROGRESS, sender=actions.ID_COMMAND_EXECUTOR, total=total)
        try:
            context = CommandContext(self.staging_dir, self.application.config, actions.ID_COMMAND_EXECUTOR, needs_gdrive)
            for command_num, command in enumerate(command_list):
                if command.status() != CommandStatus.NOT_STARTED:
                    logger.info(f'Skipping command: {command}')
                else:
                    try:
                        logger.info(f'Executing command {command_num} of {len(command_list)}')
                        command.execute(context)
                    except Exception as err:
                        # If caught here, it indicates a hole in our command logic
                        logger.exception(f'Unexpected exception while running command {command}')
                        command.set_error(err)

                    dispatcher.send(signal=actions.PROGRESS_MADE, sender=actions.ID_COMMAND_EXECUTOR, progress=command.get_total_work())

        finally:
            dispatcher.send(signal=actions.STOP_PROGRESS, sender=actions.ID_COMMAND_EXECUTOR)

        logger.info(f'{command_list.get_total_succeeded()} out of {len(command_list)} succeeded')

        # TODO: ummm...update UI
