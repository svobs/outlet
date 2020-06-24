import logging
from typing import Dict

from pydispatch import dispatcher

from command.command_interface import Command, CommandBatch
from index.uid import UID
from ui import actions

logger = logging.getLogger(__name__)


class CommandLedger:
    def __init__(self, application):
        self.application = application

        self.model_command_dict: Dict[UID, Command] = {}
        """Convenient up-to-date mapping for DisplayNode UID -> Command (also allows for context menus to cancel commands!)"""

        self.batches_to_run: Dict[UID, CommandBatch] = {}
        """Present and future batches, kept in insertion order. Each batch is removed after it is completed."""

        dispatcher.connect(signal=actions.COMMAND_BATCH_COMPLETE, receiver=self._on_batch_completed)

    def add_command_batch(self, command_batch: CommandBatch):

        # - Add planning nodes to the cache trees when we enqueue.
        # - We will store planning nodes in the in-memory tree, but will store them in different databases
        # - Also persist each command plan at enqueue time
        # - When each command completes, cache is notified of a planning node update

        self.batches_to_run[command_batch.uid] = command_batch

        command_list = command_batch.get_breadth_first_list()

        for command in command_list:
            # TODO: (1) persist each command first

            # (2) Add model to lookup table
            display_node = command.get_target_node()
            self.model_command_dict[display_node.uid] = command

            # (3) add each model node to the relevant cache trees
            self.application.cache_manager.add_or_update_node(display_node)

        # Finally, kick off command execution:
        self.application.task_runner.enqueue(self.application.command_executor.execute_batch, command_batch)

    def _on_batch_completed(self, batch_uid: UID):
        logger.debug(f'Received signal: "{actions.COMMAND_BATCH_COMPLETE}" with batch_uid={batch_uid}')
        # TODO: archive the batch in DB

        completed_batch = self.batches_to_run.pop(batch_uid)
        if not completed_batch:
            raise RuntimeError(f'OnBatchCompleted(): Batch not found: uid={batch_uid}')


