from pydispatch import dispatcher
import logging

import file_util
from command.command import Command, CommandContext, CommandPlan, CommandStatus
from ui import actions

logger = logging.getLogger(__name__)


class CommandExecutor:
    def __init__(self, application):
        self.application = application
        val = self.application.config.get('staging_dir')
        self.staging_dir = file_util.get_resource_path(val)
        # TODO: clean staging dir at startup

    def enqueue(self, command_plan: CommandPlan):
        self.application.task_runner.enqueue(self._execute_all, command_plan)

    def _execute_all(self, command_plan: CommandPlan):
        total = 0
        needs_gdrive = False

        logger.debug(f'Executing command plan uid="{command_plan.uid}": ' + command_plan.tree.show(stdout=False))

        for cmd_node in command_plan:
            command: Command = cmd_node.data
            total += command.get_total_work()
            if command.needs_gdrive():
                needs_gdrive = True

        dispatcher.send(signal=actions.START_PROGRESS, sender=actions.ID_COMMAND_EXECUTOR, total=total)
        try:
            context = CommandContext(self.staging_dir, self.application, actions.ID_COMMAND_EXECUTOR, needs_gdrive)

            for command_num, cmd_node in enumerate(command_plan):
                command: Command = cmd_node.data

                if command.status() != CommandStatus.NOT_STARTED:
                    logger.info(f'Skipping command: {command}')
                else:
                    par_node = command_plan.tree.parent(nid=cmd_node.identifier)
                    if par_node and not par_node.data.completed_ok():
                        logger.info(f'Skipping execution of command {cmd_node}: parent did not complete ({par_node.data})')
                    else:
                        try:
                            logger.info(f'Executing command {command_num} of {len(command_plan)}')
                            command.execute(context)
                        except Exception as err:
                            # If caught here, it indicates a hole in our command logic
                            logger.exception(f'Unexpected exception while running command {command}')
                            command.set_error(err)

                    dispatcher.send(signal=actions.PROGRESS_MADE, sender=actions.ID_COMMAND_EXECUTOR, progress=command.get_total_work())

        finally:
            dispatcher.send(signal=actions.STOP_PROGRESS, sender=actions.ID_COMMAND_EXECUTOR)

        logger.info(f'{command_plan.get_total_succeeded()} out of {len(command_plan)} succeeded')

        # TODO: ummm...update UI
