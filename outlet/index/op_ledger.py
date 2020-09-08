import logging
import os
from collections import defaultdict
from enum import IntEnum
from typing import Callable, DefaultDict, Dict, Iterable, List, Optional

from pydispatch import dispatcher

from index.op_graph import OpGraph
from model.op import Op, OpType
from command.cmd_builder import CommandBuilder
from command.cmd_interface import Command, CommandStatus
from constants import OPS_FILE_NAME
from index.sqlite.op_db import OpDatabase
from index.uid.uid import UID
from model.node.display_node import DisplayNode
from ui import actions

logger = logging.getLogger(__name__)

SUPER_DEBUG = False


# ENUM FailureBehavior
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
class ErrorHandlingBehavior(IntEnum):
    RAISE_ERROR = 1
    IGNORE = 2
    DISCARD = 3


# CLASS OpLedger
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼

class OpLedger:
    def __init__(self, application):
        self.application = application
        self.cacheman = self.application.cache_manager
        self._cmd_builder = CommandBuilder(self.application)

        self.op_db_path = os.path.join(self.application.cache_manager.cache_dir_path, OPS_FILE_NAME)

        self.op_graph: OpGraph = OpGraph(self.application)
        """Present and future batches, kept in insertion order. Each batch is removed after it is completed."""

        dispatcher.connect(signal=actions.COMMAND_COMPLETE, receiver=self._on_command_completed)

    def __del__(self):
        self.shutdown()

    def shutdown(self):
        if self.op_graph:
            self.op_graph.shutdown()

        self.cacheman = None
        self.application = None

    def _cancel_pending_ops_from_disk(self):
        with OpDatabase(self.op_db_path, self.application) as op_db:
            op_list: List[Op] = op_db.get_all_pending_ops()
            if op_list:
                op_db.archive_failed_ops(op_list, 'Cancelled on startup per user config')
                logger.info(f'Cancelled {len(op_list)} pending ops found in cache')

    def _load_pending_ops_from_disk(self, error_handling_behavior: ErrorHandlingBehavior) -> List[Op]:
        # first load refs from disk
        with OpDatabase(self.op_db_path, self.application) as op_db:
            op_list: List[Op] = op_db.get_all_pending_ops()

        if not op_list:
            return op_list

        logger.debug(f'Found {len(op_list)} pending ops in cache')

        # TODO: check for invalid nodes?

        return op_list

    def _remove_pending_ops(self, op_list: Iterable[Op]):
        with OpDatabase(self.op_db_path, self.application) as op_db:
            op_db.delete_pending_ops(op_list)

    def _save_pending_ops_to_disk(self, op_list: Iterable[Op]):
        with OpDatabase(self.op_db_path, self.application) as op_db:
            # This will save each of the planning nodes, if any:
            op_db.upsert_pending_ops(op_list, overwrite=False)

    def _archive_pending_ops_to_disk(self, op_list: Iterable[Op]):
        with OpDatabase(self.op_db_path, self.application) as op_db:
            op_db.archive_completed_ops(op_list)

    def _update_nodes_in_memcache(self, op: Op):
        """Looks at the given Op and notifies cacheman so that it can send out update notifications. The nodes involved may not have
        actually changed (i.e., only their statuses have changed)"""
        self.cacheman.add_or_update_node(op.src_node)
        if op.has_dst():
            self.cacheman.add_or_update_node(op.dst_node)

    # Reduce Changes logic
    # ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼

    def _derive_cp_dst_key(self, dst_node: DisplayNode) -> str:
        parent_uid = self.cacheman.get_parent_uid_for_node(dst_node)
        return f'{parent_uid}/{dst_node.name}'

    def _reduce_ops(self, op_list: Iterable[Op]) -> Iterable[Op]:
        final_list: List[Op] = []

        # Put all affected nodes in map.
        # Is there a hit? Yes == there is overlap
        mkdir_dict: Dict[UID, Op] = {}
        rm_dict: Dict[UID, Op] = {}
        # Uses _derive_cp_dst_key() to make key:
        cp_dst_dict: Dict[str, Op] = {}
        # src node is not necessarily mutually exclusive:
        cp_src_dict: DefaultDict[UID, List[Op]] = defaultdict(lambda: list())
        count_ops_orig = 0
        for op in op_list:
            count_ops_orig += 1
            if op.op_type == OpType.MKDIR:
                # remove dups
                if mkdir_dict.get(op.src_node.uid, None):
                    logger.info(f'ReduceChanges(): Removing duplicate MKDIR for node: {op.src_node}')
                else:
                    logger.info(f'ReduceChanges(): Adding MKDIR-type: {op}')
                    final_list.append(op)
                    mkdir_dict[op.src_node.uid] = op
            elif op.op_type == OpType.RM:
                # remove dups
                if rm_dict.get(op.src_node.uid, None):
                    logger.info(f'ReduceChanges(): Removing duplicate RM for node: {op.src_node}')
                else:
                    logger.info(f'ReduceChanges(): Adding RM-type: {op}')
                    final_list.append(op)
                    rm_dict[op.src_node.uid] = op
            elif op.op_type == OpType.CP or op.op_type == OpType.UP or op.op_type == OpType.MV:
                # GDrive nodes' UIDs are derived from their goog_ids; nodes with no goog_id can have different UIDs.
                # So for GDrive nodes with no goog_id, we must rely on a combination of their parent UID and name to check for uniqueness
                dst_key: str = self._derive_cp_dst_key(op.dst_node)
                existing = cp_dst_dict.get(dst_key, None)
                if existing:
                    # It is an error for anything but an exact duplicate to share the same dst node; if duplicate, then discard
                    if existing.src_node.uid != op.src_node.uid:
                        logger.error(f'ReduceChanges(): Conflict: Change1: {existing}; Change2: {op}')
                        raise RuntimeError(f'Batch op conflict: trying to copy different nodes into the same destination!')
                    elif existing.op_type != op.op_type:
                        logger.error(f'ReduceChanges(): Conflict: Change1: {existing}; Change2: {op}')
                        raise RuntimeError(f'Batch op conflict: trying to copy different op types into the same destination!')
                    elif op.dst_node.uid != existing.dst_node.uid:
                        # GDrive nodes almost certainly
                        raise RuntimeError(f'Batch op conflict: trying to copy same node into the same destination with a different UID!')
                    else:
                        assert op.dst_node.uid == existing.dst_node.uid and existing.src_node.uid == op.src_node.uid and \
                               existing.op_type == op.op_type, f'Conflict: Change1: {existing}; Change2: {op}'
                        logger.info(f'ReduceChanges(): Discarding op (dup dst): {op}')
                else:
                    logger.info(f'ReduceChanges(): Adding CP-like type: {op}')
                    cp_src_dict[op.src_node.uid].append(op)
                    cp_dst_dict[dst_key] = op
                    final_list.append(op)

        def eval_rm_ancestor_func(op_arg: Op, par: DisplayNode) -> bool:
            conflict = mkdir_dict.get(par.uid, None)
            if conflict:
                logger.error(f'ReduceChanges(): Conflict! Op1={conflict}; Op2={op_arg}')
                raise RuntimeError(f'Batch op conflict: trying to create a node and remove its descendant at the same time!')

            return True

        def eval_mkdir_ancestor_func(op_arg: Op, par: DisplayNode) -> bool:
            conflict = rm_dict.get(par.uid, None)
            if conflict:
                logger.error(f'ReduceChanges(): Conflict! Op1={conflict}; Op2={op_arg}')
                raise RuntimeError(f'Batch op conflict: trying to remove a node and create its descendant at the same time!')
            return True

        # For each element, traverse up the tree and compare each parent node to map
        for op in op_list:
            if op.op_type == OpType.RM:
                self._check_ancestors(op, eval_rm_ancestor_func)
            elif op.op_type == OpType.MKDIR:
                self._check_ancestors(op, eval_mkdir_ancestor_func)
            elif op.op_type == OpType.CP or op.op_type == OpType.UP or op.op_type == OpType.MV:
                self._check_cp_ancestors(op, mkdir_dict, rm_dict, cp_src_dict, cp_dst_dict)

        logger.debug(f'Reduced {count_ops_orig} ops to {len(final_list)} ops')
        return final_list

    def _check_cp_ancestors(self, op: Op, mkdir_dict, rm_dict, cp_src_dict, cp_dst_dict):
        """Checks all ancestors of both src and dst for mapped Ops. The following are the only valid situations:
         1. No ancestors of src or dst correspond to any Ops.
         2. Ancestor(s) of the src node correspond to the src node of a CP or UP action (i.e. they will not change)
         """
        src_ancestor = op.src_node
        dst_ancestor = op.dst_node
        logger.debug(f'Evaluating ancestors for op: {op}')
        while src_ancestor:
            if SUPER_DEBUG:
                logger.debug(f'Evaluating src ancestor (op={op.op_uid}): {src_ancestor}')
            if mkdir_dict.get(src_ancestor.uid, None):
                raise RuntimeError(f'Batch op conflict: copy from a descendant of a node being created!')
            if rm_dict.get(src_ancestor.uid, None):
                raise RuntimeError(f'Batch op conflict: copy from a descendant of a node being deleted!')
            if cp_dst_dict.get(src_ancestor.uid, None):
                raise RuntimeError(f'Batch op conflict: copy from a descendant of a node being copied to!')

            src_ancestor = self.application.cache_manager.get_parent_for_node(src_ancestor)

        while dst_ancestor:
            if SUPER_DEBUG:
                logger.debug(f'Evaluating dst ancestor (op={op.op_uid}): {dst_ancestor}')
            if rm_dict.get(dst_ancestor.uid, None):
                raise RuntimeError(f'Batch op conflict: copy to a descendant of a node being deleted!')
            if cp_src_dict.get(dst_ancestor.uid, None):
                raise RuntimeError(f'Batch op conflict: copy to a descendant of a node being copied from!')

            dst_ancestor = self.application.cache_manager.get_parent_for_node(dst_ancestor)

    def _check_ancestors(self, op: Op, eval_func: Callable[[Op, DisplayNode], bool]):
        ancestor = op.src_node
        while True:
            ancestor = self.application.cache_manager.get_parent_for_node(ancestor)
            if not ancestor:
                return
            if SUPER_DEBUG:
                logger.debug(f'(Op={op}): evaluating ancestor: {ancestor}')
            if not eval_func(op, ancestor):
                return

    # ▲ ▲ ▲ ▲ ▲ ▲ ▲ ▲ ▲ ▲ ▲ ▲ ▲ ▲ ▲
    # Reduce Changes logic

    def load_pending_ops(self):
        """Call this at startup, to resume pending ops which have not yet been applied."""

        # We may need various things from the cacheman...
        # For now, just tell the CacheManager to load all the caches. Can optimize in the future.
        self.cacheman.load_all_caches()

        if self.cacheman.cancel_all_pending_ops_on_startup:
            logger.debug(f'User configuration specifies cancelling all pending ops on startup')
            self._cancel_pending_ops_from_disk()
            return

        op_list: List[Op] = self._load_pending_ops_from_disk(ErrorHandlingBehavior.DISCARD)
        if not op_list:
            logger.debug(f'No pending ops found in the disk cache')
            return

        logger.info(f'Found {len(op_list)} pending ops from the disk cache')

        # Sort into batches
        batch_dict: DefaultDict[UID, List[Op]] = defaultdict(lambda: list())
        for op in op_list:
            batch_dict[op.batch_uid].append(op)

        batch_dict_keys = batch_dict.keys()
        logger.info(f'Sorted ops into {len(batch_dict_keys)} batches')
        sorted_keys = sorted(batch_dict_keys)

        for batch_uid in sorted_keys:
            # Assume batch has already been reduced and reconciled against master tree.
            batch_items: List[Op] = batch_dict[batch_uid]
            batch_root = self.op_graph.make_graph_from_batch(batch_items)
            self._add_batch_to_op_graph_and_remove_discarded(batch_root, batch_uid)

    def append_new_pending_ops(self, op_batch: Iterable[Op]):
        """
        Call this after the user requests a new set of ops.

         - First store "planning nodes" to the list of cached nodes (but each will have exists=False until we execute its associated command).
         - The list of to-be-completed ops is also cached on disk.
         - When each command completes, cacheman is notified of any node updates required as well.
         - When batch completes, we archive the ops on disk.
        """
        if not op_batch:
            return

        op_iter = iter(op_batch)
        batch_uid = next(op_iter).batch_uid
        for op in op_iter:
            if op.batch_uid != batch_uid:
                raise RuntimeError(f'Changes in batch do not all contain the same batch_uid (found {op.batch_uid} and {batch_uid})')

        # Simplify and remove redundancies in op_list
        reduced_batch: Iterable[Op] = self._reduce_ops(op_batch)

        batch_root = self.op_graph.make_graph_from_batch(reduced_batch)

        # Reconcile ops against master op tree before adding nodes
        if not self.op_graph.can_nq_batch(batch_root):
            raise RuntimeError('Invalid batch!')

        # Save ops and their planning nodes to disk
        self._save_pending_ops_to_disk(reduced_batch)

        # Add dst nodes for to-be-created nodes if they are not present
        for op in reduced_batch:
            self._update_nodes_in_memcache(op)

        self._add_batch_to_op_graph_and_remove_discarded(batch_root, batch_uid)

    def _add_batch_to_op_graph_and_remove_discarded(self, batch_root, batch_uid):
        logger.info(f'Adding batch {batch_uid} to OpTree')
        discarded_op_list: List[Op] = self.op_graph.nq_batch(batch_root)
        if discarded_op_list:
            logger.debug(f'{len(discarded_op_list)} ops were discarded: removing from disk cache')
            self._remove_pending_ops(discarded_op_list)

    def get_last_pending_op_for_node(self, node_uid: UID) -> Optional[Op]:
        return self.op_graph.get_last_pending_op_for_node(node_uid)

    def get_next_command(self) -> Optional[Command]:
        # Call this from Executor. Only returns None if shutting down

        # This will block until a op is ready:
        op: Op = self.op_graph.get_next_op()

        if not op:
            logger.debug('Received None; looks like we are shutting down')
            return None

        return self._cmd_builder.build_command(op)

    def _on_command_completed(self, sender, command: Command):
        logger.debug(f'Received signal: "{actions.COMMAND_COMPLETE}"')

        if command.status() == CommandStatus.STOPPED_ON_ERROR:
            # TODO: notify/display error messages somewhere in the UI?
            logger.error(f'Command {command.uid} (op {command.op.op_uid}) failed with error: {command.get_error()}')
            # TODO: how to recover?
            return
        else:
            logger.info(f'Command {command.uid} (op {command.op.op_uid}) returned with status: "{command.status().name}"')

        # Need to set this here to resolve chicken-and-egg scenario.
        # When we tell cacheman to upsert node, it will notify DisplayMutator which will then look up here, and we have not yet popped the op.
        # Need a way for DisplayMutator to know that it's complete.
        command.op.set_completed()

        # Add/update nodes in central cache:
        if command.result.nodes_to_upsert:
            logger.debug(f'Upserted {len(command.result.nodes_to_upsert)} nodes: notifying cacheman')
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

        logger.debug(f'Archiving op: {command.op}')
        self._archive_pending_ops_to_disk([command.op])

        # Ensure command is one that we are expecting.
        # Important: wait until after we have finished updating cacheman, as popping here will cause the next op to immediately execute:
        self.op_graph.pop_op(command.op)



