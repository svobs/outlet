import logging
import os
from collections import defaultdict
from typing import Callable, DefaultDict, Dict, Iterable, List

from pydispatch import dispatcher

from index.op_tree import OpTree
from model.change_action import ChangeAction, ChangeActionRef, ChangeType
from cmd.cmd_builder import CommandBuilder
from cmd.cmd_interface import Command, CommandStatus
from constants import PENDING_CHANGES_FILE_NAME
from index.sqlite.change_db import PendingChangeDatabase
from index.uid.uid import UID
from model.node.display_node import DisplayNode
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

        self.dep_tree: OpTree = OpTree(self.application)
        """Present and future batches, kept in insertion order. Each batch is removed after it is completed."""

        dispatcher.connect(signal=actions.COMMAND_COMPLETE, receiver=self._on_command_completed)

    def _load_pending_changes_from_disk(self) -> List[ChangeAction]:
        # first load refs from disk
        with PendingChangeDatabase(self.pending_changes_db_path, self.application) as change_db:
            change_ref_list: List[ChangeActionRef] = change_db.get_all_pending_changes()

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
            change_db.upsert_pending_changes(change_list, overwrite=False)

    def _archive_pending_changes_to_disk(self, change_list: Iterable[ChangeAction]):
        with PendingChangeDatabase(self.pending_changes_db_path, self.application) as change_db:
            change_db.archive_changes(change_list)

    def _add_missing_nodes(self, change_action: ChangeAction):
        """Looks at the given ChangeAction and adds any given "planning node" to it."""
        if change_action.change_type == ChangeType.MKDIR:
            assert not change_action.src_node.exists(), f'Expected to not exist: {change_action.src_node}'
            self.cacheman.add_or_update_node(change_action.src_node)
        elif change_action.change_type == ChangeType.CP:
            assert not change_action.dst_node.exists(), f'Expected to not exist: {change_action.dst_node}'
            self.cacheman.add_or_update_node(change_action.dst_node)
        elif change_action.change_type == ChangeType.MV:
            assert not change_action.dst_node.exists(), f'Expected to not exist: {change_action.dst_node}'
            self.cacheman.add_or_update_node(change_action.dst_node)
        else:
            assert self.cacheman.get_item_for_uid(change_action.src_node.uid), f'Expected src node already present for: {change_action}'
            if change_action.dst_node:
                assert self.cacheman.get_item_for_uid(change_action.dst_node.uid), f'Expected dst node already present for change: {change_action}'

    # Reduce Changes logic
    # ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼

    def _reduce_changes(self, change_list: Iterable[ChangeAction]) -> Iterable[ChangeAction]:
        # FIXME: need to rethink all of this
        final_list: List[ChangeAction] = []

        # Put all affected nodes in map.
        # Is there a hit? Yes == there is overlap
        mkdir_dict: Dict[UID, ChangeAction] = {}
        rm_dict: Dict[UID, ChangeAction] = {}
        cp_dst_dict: Dict[UID, ChangeAction] = {}
        # src node is not necessarily mutually exclusive:
        cp_src_dict: DefaultDict[UID, List[ChangeAction]] = defaultdict(lambda: list())
        count_changes_orig = 0
        for change in change_list:
            count_changes_orig += 1
            if change.change_type == ChangeType.MKDIR:
                # remove dups
                mkdir_dict[change.src_node.uid] = change
            elif change.change_type == ChangeType.RM:
                # remove dups
                rm_dict[change.src_node.uid] = change
            elif change.change_type == ChangeType.CP or change.change_type == ChangeType.UP or change.change_type == ChangeType.MV:
                existing = cp_dst_dict.get(change.dst_node.uid, None)
                if existing:
                    # Fail for anything but a duplicate
                    if existing.src_node.uid != change.src_node.uid:
                        logger.error(f'Conflict: Change1: {existing}; Change2: {change}')
                        raise RuntimeError(f'Batch change conflict: trying to copy different nodes into the same destination!')
                    elif existing.change_type != change.change_type:
                        logger.error(f'Conflict: Change1: {existing}; Change2: {change}')
                        raise RuntimeError(f'Batch change conflict: trying to copy differnt change types into the same destination!')
                    else:
                        assert change.dst_node.uid == existing.dst_node.uid and existing.src_node.uid == change.src_node.uid and \
                               existing.change_type == change.change_type, f'Conflict: Change1: {existing}; Change2: {change}'
                        logger.info(f'Discarding duplicate change: {change}')
                else:
                    cp_dst_dict[change.dst_node.uid] = change
                    cp_src_dict[change.src_node.uid].append(change)

        def eval_rm_ancestor(chg: ChangeAction, par: DisplayNode) -> bool:
            conflict = mkdir_dict.get(par.uid, None)
            if conflict:
                logger.error(f'Conflict: Change1: {conflict}; Change2: {change}')
                raise RuntimeError(f'Batch change conflict: trying to create a node and remove its descendant at the same time!')

            redundant = rm_dict.get(par.uid, None)
            if redundant:
                logger.info(f'Removing redundant RM change because its descendant is already being removed: {chg}')
                rm_dict.pop(chg.src_node.uid)
                return False
            return True

        def eval_mkdir_ancestor(chg: ChangeAction, par: DisplayNode) -> bool:
            conflict = rm_dict.get(par.uid, None)
            if conflict:
                logger.error(f'Conflict: Change1: {conflict}; Change2: {change}')
                raise RuntimeError(f'Batch change conflict: trying to remove a node and create its descendant at the same time!')
            return True

        # For each element, traverse up the tree and compare each parent node to map
        for change in change_list:
            if change.change_type == ChangeType.RM:
                self._check_ancestors(change, eval_rm_ancestor)
            elif change.change_type == ChangeType.MKDIR:
                self._check_ancestors(change, eval_mkdir_ancestor)
            elif change.change_type == ChangeType.CP or change.change_type == ChangeType.UP or change.change_type == ChangeType.MV:
                self._check_cp_ancestors(change, mkdir_dict, rm_dict, cp_src_dict, cp_dst_dict)

        final_list += mkdir_dict.values()
        final_list += rm_dict.values()
        final_list += cp_dst_dict.values()

        logger.debug(f'Reduced {count_changes_orig} changes to {len(final_list)} changes')
        return final_list

    def _check_cp_ancestors(self, change: ChangeAction, mkdir_dict, rm_dict, cp_src_dict, cp_dst_dict):
        """Checks all ancestors of both src and dst for mapped ChangeActions. The following are the only valid situations:
         1. No ancestors of src or dst correspond to any ChangeActions.
         2. Ancestor(s) of the src node correspond to the src node of a CP or UP action (i.e. they will not change)
         3. A ChangeAction exists which is a perfect superset of this one. This means that both changes are the same type,
          and src and dst of the super-change are the same number of levels above the src and dst of the sub-change."""
        src_ancestor = change.src_node
        dst_ancestor = change.dst_node
        src_generation = 0
        dst_generation = 0
        while True:
            if src_ancestor:
                src_ancestor = self.application.cache_manager.get_parent_for_item(src_ancestor)
                src_generation += 1

            if dst_ancestor:
                dst_ancestor = self.application.cache_manager.get_parent_for_item(dst_ancestor)
                dst_generation += 1

            src_ancestor_cp = None
            dst_ancestor_cp = None

            if src_ancestor:
                logger.debug(f'(Change={change}): evaluating src ancestor: {src_ancestor}')
                if mkdir_dict.get(src_ancestor.uid, None):
                    raise RuntimeError(f'Batch change conflict: copy from a descendant of a node being created!')
                if rm_dict.get(src_ancestor.uid, None):
                    raise RuntimeError(f'Batch change conflict: copy from a descendant of a node being deleted!')
                if cp_dst_dict.get(dst_ancestor.uid, None):
                    raise RuntimeError(f'Batch change conflict: copy from a descendant of a node being copied to!')

                src_ancestor_cp = cp_src_dict.get(src_ancestor.uid, None)

            if dst_ancestor:
                logger.debug(f'(Change={change}): evaluating dst ancestor: {dst_ancestor}')
                if mkdir_dict.get(dst_ancestor.uid, None):
                    raise RuntimeError(f'Batch change conflict: copy to a descendant of a node being created!')
                if rm_dict.get(dst_ancestor.uid, None):
                    raise RuntimeError(f'Batch change conflict: copy to a descendant of a node being deleted!')
                if cp_src_dict.get(dst_ancestor.uid, None):
                    raise RuntimeError(f'Batch change conflict: copy to a descendant of a node being copied from!')

                dst_ancestor_cp = cp_dst_dict.get(dst_ancestor.uid, None)

            if src_ancestor_cp and dst_ancestor_cp and src_ancestor_cp.change_type == dst_ancestor_cp.change_type \
                    and src_ancestor_cp.action_uid == dst_ancestor_cp.action_uid and src_generation == dst_generation:
                logger.info(f'Found a superset for change; will remove: {change}')
                cp_src_dict.pop(src_ancestor.uid)
                cp_dst_dict.pop(dst_ancestor.uid)
            elif src_ancestor_cp or dst_ancestor_cp:
                logger.error(f'(Change={change}): conflict src: {src_ancestor_cp} or dst: {dst_ancestor_cp}')
                raise RuntimeError(f'Batch change conflict: copy to a descendant of a node being copied from!')

    def _check_ancestors(self, change: ChangeAction, eval_func: Callable[[ChangeAction, DisplayNode], bool]):
        ancestor = change.src_node
        while True:
            ancestor = self.application.cache_manager.get_parent_for_item(ancestor)
            if not ancestor:
                return
            logger.debug(f'(Change={change}): evaluating ancestor: {ancestor}')
            if not eval_func(change, ancestor):
                return

    # ▲ ▲ ▲ ▲ ▲ ▲ ▲ ▲ ▲ ▲ ▲ ▲ ▲ ▲ ▲
    # Reduce Changes logic

    def load_pending_changes(self):
        """Call this at startup, to resume pending changes which have not yet been applied."""
        change_list: List[ChangeAction] = self._load_pending_changes_from_disk()
        if not change_list:
            logger.debug(f'No pending changes found in the disk cache')
            return

        logger.info(f'Found {len(change_list)} pending changes from the disk cache')

        # Sort into batches
        batch_dict: DefaultDict[UID, List[ChangeAction]] = defaultdict(lambda: list())
        for change in change_list:
            batch_dict[change.batch_uid].append(change)

        batch_dict_keys = batch_dict.keys()
        logger.info(f'Sorted changes into {len(batch_dict_keys)} batches')
        sorted_keys = sorted(batch_dict_keys)

        for key in sorted_keys:
            # Assume batch has already been reduced and reconciled against master tree.
            batch_items: List[ChangeAction] = batch_dict[key]
            batch_root = self.dep_tree.make_tree_to_insert(batch_items)
            self.dep_tree.add_batch(batch_root)

    def append_new_pending_changes(self, change_batch: Iterable[ChangeAction]):
        """
        Call this after the user requests a new set of changes.

         - First store "planning nodes" to the list of cached nodes (but each will have exists=False until we execute its associated command).
         - The list of to-be-completed changes is also cached on disk.
         - When each command completes, cacheman is notified of any node updates required as well.
         - When batch completes, we archive the changes on disk.
        """
        if not change_batch:
            return

        change_iter = iter(change_batch)
        batch_uid = next(change_iter).batch_uid
        for change in change_iter:
            if change.batch_uid != batch_uid:
                raise RuntimeError(f'Changes in batch do not all contain the same batch_uid (found {change.batch_uid} and {batch_uid})')

        # Simplify and remove redundancies in change_list
        reduced_batch = self._reduce_changes(change_batch)

        tree_root = self.dep_tree.make_tree_to_insert(reduced_batch)

        # Reconcile changes against master change tree before adding nodes
        if not self.dep_tree.can_add_batch(tree_root):
            raise RuntimeError('Invalid batch!')

        # Add dst nodes for to-be-created nodes if they are not present (this will also save to disk).
        # Important: do this *before* saving or queuing up the pending changes. In the case of a crash, we can find and remove
        # these orphaned nodes
        for change_action in reduced_batch:
            self._add_missing_nodes(change_action)

        self._save_pending_changes_to_disk(reduced_batch)

        self.dep_tree.add_batch(tree_root)

    def get_next_command(self) -> Command:
        # Call this from Executor.

        # This will block until a change is ready:
        change_action: ChangeAction = self.dep_tree.get_next_change()

        return self._cmd_builder.build_command(change_action)

    def _on_command_completed(self, command: Command):
        logger.debug(f'Received signal: "{actions.COMMAND_COMPLETE}"')

        if command.status() == CommandStatus.STOPPED_ON_ERROR:
            # TODO: notify/display error messages somewhere in the UI?
            logger.error(f'Command failed with error: {command.get_error()}')
            # TODO: how to recover?
            return
        else:
            logger.info(f'Command returned with status: "{command.status().name}"')

        # Ensure command is one that we are expecting
        self.dep_tree.change_completed(command.change_action)

        # Add/update nodes in central cache:
        if command.result.nodes_to_upsert:
            for upsert_node in command.result.nodes_to_upsert:
                self.cacheman.add_or_update_node(upsert_node)

        # Remove nodes in central cache:
        if command.result.nodes_to_delete:
            try:
                to_trash = command.to_trash
            except AttributeError:
                to_trash = False

            logger.debug(f'Deleted {len(command.result.nodes_to_delete)} nodes: notifying cacheman')
            for deleted_node in command.result.nodes_to_delete:
                self.cacheman.remove_node(deleted_node, to_trash)

        self._archive_pending_changes_to_disk([command.change_action])

