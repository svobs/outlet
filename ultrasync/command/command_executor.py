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

        for command in command_plan.get_breadth_first_list():
            total += command.get_total_work()
            if command.needs_gdrive():
                needs_gdrive = True

        dispatcher.send(signal=actions.START_PROGRESS, sender=actions.ID_COMMAND_EXECUTOR, total=total)
        try:
            context = CommandContext(self.staging_dir, self.application, actions.ID_COMMAND_EXECUTOR, needs_gdrive)

            for command_num, uid in enumerate(command_plan):
                command = command_plan.get_item_for_uid(uid)

                if command.status() != CommandStatus.NOT_STARTED:
                    logger.info(f'Skipping command: {command}')
                else:
                    parent_cmd = command_plan.get_parent(command.identifier)
                    if parent_cmd and not parent_cmd.completed_without_error():
                        logger.info(f'Skipping execution of command {command}: parent did not complete ({parent_cmd})')
                    else:
                        try:
                            logger.info(f'Executing command {(command_num + 1)} of {len(command_plan)}')
                            command.execute(context)
                        except Exception as err:
                            # If caught here, it indicates a hole in our command logic
                            logger.exception(f'Unexpected exception while running command {command}')
                            command.set_error(err)

                    dispatcher.send(signal=actions.PROGRESS_MADE, sender=actions.ID_COMMAND_EXECUTOR, progress=command.get_total_work())

        finally:
            dispatcher.send(signal=actions.STOP_PROGRESS, sender=actions.ID_COMMAND_EXECUTOR)

        logger.info(f'{command_plan.get_total_completed()} out of {len(command_plan)} completed')
