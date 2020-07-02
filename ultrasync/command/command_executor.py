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

    def execute_batch(self, command_batch: CommandBatch):
        total = 0
        needs_gdrive = False
        count_commands = len(command_batch)

        if count_commands == 0:
            logger.error(f'Command plan (uid="{command_batch.uid}") is empty!')
            return

        logger.debug(f'Executing command batch uid="{command_batch.uid}", size={count_commands}: ' + command_batch.tree.show(stdout=False))

        command_list = command_batch.get_breadth_first_list()

        for command in command_list:
            total += command.get_total_work()
            if command.needs_gdrive():
                needs_gdrive = True

        dispatcher.send(signal=actions.START_PROGRESS, sender=actions.ID_COMMAND_EXECUTOR, total=total)
        try:
            context = CommandContext(self.staging_dir, self.application, actions.ID_COMMAND_EXECUTOR, needs_gdrive)

            for command_num, command in enumerate(command_list):
                if command.status() != CommandStatus.NOT_STARTED:
                    logger.info(f'Skipping command: {command} because it has status {command.status()}')
                else:
                    parent_cmd = command_batch.get_parent(command.identifier)
                    if parent_cmd and not parent_cmd.completed_without_error():
                        logger.info(f'Skipping execution of command {command}: parent did not complete ({parent_cmd})')
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

                        if command.status() == CommandStatus.STOPPED_ON_ERROR:
                            # TODO: notify/display error messages somewhere in the UI?
                            logger.error(f'Command failed with error: {command.get_error()}')
                        else:
                            logger.info(f'Command returned with status: "{command.status().name}"')

                        # Add/update nodes in central cache:
                        if command.result.nodes_to_upsert:
                            for upsert_node in command.result.nodes_to_upsert:
                                context.cache_manager.add_or_update_node(upsert_node)

                        # Remove nodes in central cache:
                        if command.result.nodes_to_delete:
                            try:
                                to_trash = command.to_trash
                            except AttributeError:
                                to_trash = False

                            logger.debug(f'Deleted {len(command.result.nodes_to_delete)} nodes: notifying cacheman')
                            for deleted_node in command.result.nodes_to_delete:
                                context.cache_manager.remove_node(deleted_node, to_trash)

                    dispatcher.send(signal=actions.PROGRESS_MADE, sender=actions.ID_COMMAND_EXECUTOR, progress=command.get_total_work())

        finally:
            dispatcher.send(signal=actions.STOP_PROGRESS, sender=actions.ID_COMMAND_EXECUTOR)
            dispatcher.send(signal=actions.COMMAND_BATCH_COMPLETE, sender=actions.ID_COMMAND_EXECUTOR, batch_uid=command_batch.uid)

        logger.info(f'{command_batch.get_total_completed()} out of {len(command_batch)} completed without error')
