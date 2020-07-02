import logging
import os
from typing import Dict, Iterable, List

from pydispatch import dispatcher

from command.change_action import ChangeAction, ChangeActionRef
from command.command_builder import CommandBuilder
from command.command_interface import Command, CommandBatch
from constants import PENDING_CHANGES_FILE_NAME
from index.sqlite.change_db import PendingChangeDatabase
from index.uid import UID
from ui import actions

logger = logging.getLogger(__name__)


class ChangeLedger:
    def __init__(self, application):
        self.application = application
        self._cmd_builder = CommandBuilder(self.application)

        self.pending_changes_db_path = os.path.join(self.application.cache_manager.cache_dir_path, PENDING_CHANGES_FILE_NAME)

        self.model_command_dict: Dict[UID, Command] = {}
        """Convenient up-to-date mapping for DisplayNode UID -> Command (also allows for context menus to cancel commands!)"""

        self.batches_to_run: Dict[UID, CommandBatch] = {}
        """Present and future batches, kept in insertion order. Each batch is removed after it is completed."""

        dispatcher.connect(signal=actions.COMMAND_BATCH_COMPLETE, receiver=self._on_batch_completed)

    def _load_pending_changes_from_disk(self) -> Iterable[ChangeAction]:
        # first load refs from disk
        with PendingChangeDatabase(self.pending_changes_db_path, self.application) as change_db:
            change_ref_list: List[ChangeActionRef] = change_db.get_all_changes()

        change_list: List[ChangeAction] = []
        # dereference src and dst:
        for change_ref in change_ref_list:
            src_node = self.application.cache_manager.get_item_for_uid(change_ref.src_uid)
            if not src_node:
                raise RuntimeError(f'Could not find node in cache with UID={change_ref.src_uid} (for change_action={change_ref})')

            if change_ref.dst_uid:
                dst_node = self.application.cache_manager.get_item_for_uid(change_ref.dst_uid)
                if not dst_node:
                    raise RuntimeError(f'Could not find node in cache with UID={change_ref.dst_uid} (for change_action={change_ref})')
            else:
                dst_node = None

            change = ChangeAction(action_uid=change_ref.action_uid, change_type=change_ref.change_type, src_node=src_node, dst_node=dst_node)
            change_list.append(change)

        logger.debug(f'Found {len(change_list)} pending changes in the cache')
        return change_list

    def _save_pending_changes_to_disk(self, change_list: Iterable[ChangeAction]):
        with PendingChangeDatabase(self.pending_changes_db_path, self.application) as change_db:
            change_db.upsert_changes(change_list, overwrite=False)

    # FIXME: call this at startup
    def load_pending_changes(self):
        """Call this at startup, to resume pending changes which have not yet been applied."""
        change_list = self._load_pending_changes_from_disk()
        self._enqueue_changes(change_list)

    # FIXME: add lookup of changes and check for conflicts when user requests more changes

    def append_new_pending_changes(self, change_list: Iterable[ChangeAction]):
        """Call this after the user requests a new set of changes."""
        self._save_pending_changes_to_disk(change_list)
        self._enqueue_changes(change_list)

    def _enqueue_changes(self, change_list: Iterable[ChangeAction]):
        command_batch: CommandBatch = self._cmd_builder.build_command_batch(change_list=change_list)
        logger.debug(f'Built a CommandBatch with {len(command_batch)} commands')

        self.batches_to_run[command_batch.uid] = command_batch

        command_list = command_batch.get_breadth_first_list()

        for command in command_list:
            # (1) Add model to lookup table (both src and dst if applicable)
            self.model_command_dict[command.change_action.src_node.uid] = command
            if command.change_action.dst_node:
                self.model_command_dict[command.change_action.dst_node.uid] = command

        # FIXME: add master dependency tree logic

        # Finally, kick off command execution:
        self.application.executor.execute_command_batch(command_batch)

    def _on_batch_completed(self, batch_uid: UID):
        logger.debug(f'Received signal: "{actions.COMMAND_BATCH_COMPLETE}" with batch_uid={batch_uid}')
        # TODO: archive the batch in DB

        completed_batch = self.batches_to_run.pop(batch_uid)
        if not completed_batch:
            raise RuntimeError(f'OnBatchCompleted(): Batch not found: uid={batch_uid}')


