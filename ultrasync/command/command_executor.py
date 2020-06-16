from pydispatch import dispatcher
import logging

import file_util
from command.command_interface import CommandContext, CommandBatch, CommandStatus
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

    def enqueue(self, command_batch: CommandBatch):
        # TODO: expand this framework

        # - Add planning nodes to the cache trees when we enqueue
        # - Maintain a mapping in the CacheMan for PlanningNode -> Command (also allows for context menus to cancel commands!)
        # - Also persist each command plan at enqueue time
        # - When each command completes, cache is notified of a planning node update

        # At any given time... TODO
        # self.cache_manager.add_to_command_queue(command_batch)

        self.application.task_runner.enqueue(self._execute_all, command_batch)

    def _execute_all(self, command_batch: CommandBatch):
        total = 0
        needs_gdrive = False
        count_commands = len(command_batch)

        if count_commands == 0:
            logger.error(f'Command plan (uid="{command_batch.uid}") is empty!')
            return

        logger.debug(f'Executing command plan uid="{command_batch.uid}", size={count_commands}: ' + command_batch.tree.show(stdout=False))

        command_list = command_batch.get_breadth_first_list()

        for command in command_list:
            total += command.get_total_work()
            if command.needs_gdrive():
                needs_gdrive = True

            # TODO: put this in command queue
            # Fire events so that trees can display the planning nodes
            if logger.isEnabledFor(logging.DEBUG):
                logger.debug(f'Sending signal "{actions.NODE_ADDED_OR_UPDATED}" to display preview for node: {command.get_model()}')
            dispatcher.send(signal=actions.NODE_ADDED_OR_UPDATED, sender=actions.ID_COMMAND_EXECUTOR, node=command.get_model())

        dispatcher.send(signal=actions.START_PROGRESS, sender=actions.ID_COMMAND_EXECUTOR, total=total)
        try:
            context = CommandContext(self.staging_dir, self.application, actions.ID_COMMAND_EXECUTOR, needs_gdrive)

            for command_num, command in enumerate(command_list):
                if command.status() != CommandStatus.NOT_STARTED:
                    logger.info(f'Skipping command: {command}')
                else:
                    parent_cmd = command_batch.get_parent(command.identifier)
                    if parent_cmd and not parent_cmd.completed_without_error():
                        logger.info(f'Skipping execution of command {command}: parent did not complete ({parent_cmd})')
                    else:
                        try:
                            status = f'Executing command {(command_num + 1)} of {len(command_batch)}'
                            dispatcher.send(signal=actions.SET_PROGRESS_TEXT, sender=actions.ID_COMMAND_EXECUTOR, msg=status)
                            logger.info(f'{status}: {repr(command)}')
                            command.execute(context)
                        except Exception as err:
                            # If caught here, it indicates a hole in our command logic
                            logger.exception(f'Unexpected exception while running command {command}')
                            command.set_error(err)

                    dispatcher.send(signal=actions.PROGRESS_MADE, sender=actions.ID_COMMAND_EXECUTOR, progress=command.get_total_work())

        finally:
            dispatcher.send(signal=actions.STOP_PROGRESS, sender=actions.ID_COMMAND_EXECUTOR)

        logger.info(f'{command_batch.get_total_completed()} out of {len(command_batch)} completed')
