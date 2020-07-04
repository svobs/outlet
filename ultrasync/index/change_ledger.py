import logging
import os
from typing import Dict, Iterable, List

from pydispatch import dispatcher

from index.dep_tree import DepTree
from model.change_action import ChangeAction, ChangeActionRef, ChangeType
from cmd.cmd_builder import CommandBuilder
from cmd.cmd_interface import Command
from constants import PENDING_CHANGES_FILE_NAME
from index.sqlite.change_db import PendingChangeDatabase
from index.uid.uid import UID
from ui import actions

logger = logging.getLogger(__name__)


# CLASS ChangeLedger
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼

class ChangeLedger:
    def __init__(self, application):
        self.application = application
        self.cacheman = self.application.cache_manager
        self._cmd_builder = CommandBuilder(self.application)

        self.pending_changes_db_path = os.path.join(self.application.cache_manager.cache_dir_path, PENDING_CHANGES_FILE_NAME)

        self.model_command_dict: Dict[UID, Command] = {}
        """Convenient up-to-date mapping for DisplayNode UID -> Command (also allows for context menus to cancel commands!)"""

        self.dep_tree: DepTree = DepTree()
        """Present and future batches, kept in insertion order. Each batch is removed after it is completed."""

        dispatcher.connect(signal=actions.COMMAND_BATCH_COMPLETE, receiver=self._on_batch_completed)

    def _load_pending_changes_from_disk(self) -> List[ChangeAction]:
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

            change = ChangeAction(action_uid=change_ref.action_uid, batch_uid=change_ref.batch_uid, change_type=change_ref.change_type,
                                  src_node=src_node, dst_node=dst_node)
            change_list.append(change)

        logger.debug(f'Found {len(change_list)} pending changes in the cache')
        return change_list

    def _save_pending_changes_to_disk(self, change_list: Iterable[ChangeAction]):
        with PendingChangeDatabase(self.pending_changes_db_path, self.application) as change_db:
            change_db.upsert_changes(change_list, overwrite=False)

    def _add_missing_nodes(self, change_action: ChangeAction):
        """Looks at the given ChangeAction and adds any given "planning node" to it."""
        if change_action.change_type == ChangeType.MKDIR:
            self.cacheman.add_or_update_node(change_action.src_node)
        elif change_action.change_type == ChangeType.CP:
            self.cacheman.add_or_update_node(change_action.dst_node)
        elif change_action.change_type == ChangeType.MV:
            self.cacheman.add_or_update_node(change_action.dst_node)
        else:
            assert self.cacheman.get_item_for_uid(change_action.src_node.uid), f'Expected src node already present for change: {change_action}'
            if change_action.dst_node:
                assert self.cacheman.get_item_for_uid(change_action.dst_node.uid), f'Expected dst node already present for change: {change_action}'

    def load_pending_changes(self):
        """Call this at startup, to resume pending changes which have not yet been applied."""
        change_list = self._load_pending_changes_from_disk()
        logger.info(f'Found {len(change_list)} pending changes from the disk cache')
        self._enqueue_changes(change_list)

    # FIXME: add lookup of changes and check for conflicts when user requests more changes

    def append_new_pending_changes(self, change_list: Iterable[ChangeAction]):
        """
        Call this after the user requests a new set of changes.

         - First store "planning nodes" to the list of cached nodes (but each will have exists=False until we execute its associated command).
         - The list of to-be-completed changes is also cached on disk.
         - When each command completes, cacheman is notified of any node updates required as well.
         - When batch completes, ledger archives the changes.
        """

        # TODO: resolve redundancies in change_list

        # TODO: reconcile changes against master change tree


        for change_action in change_list:
            # Add dst nodes for to-be-created nodes if they are not present:
            self._add_missing_nodes(change_action)



        self._save_pending_changes_to_disk(change_list)
        # TODO
        # self._enqueue_changes(change_list)

    def _enqueue_changes(self, change_list: Iterable[ChangeAction]):

        for change_action in change_list:
            # TODO: each change list can resolve into multiple commands
            command = self._cmd_builder.build_command(change_action)

            # (1) Add model to lookup table (both src and dst if applicable)
            self.model_command_dict[change_action.src_node.uid] = command
            if change_action.dst_node:
                self.model_command_dict[command.change_action.dst_node.uid] = command

            self.application.executor.execute_batch([command])

        # FIXME: add master dependency tree logic


    def _on_batch_completed(self, command_batch: List[Command]):
        logger.debug(f'Received signal: "{actions.COMMAND_BATCH_COMPLETE}"')
        # TODO: archive the batch in DB

        # completed_batch = self.batches_to_run.pop(batch_uid)
        # if not completed_batch:
        #     raise RuntimeError(f'OnBatchCompleted(): Batch not found: uid={batch_uid}')


