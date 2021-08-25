import logging
import os

from pydispatch import dispatcher

from backend.diff.diff_content_first import ContentFirstDiffer
from backend.display_tree.active_tree_meta import ActiveDisplayTreeMeta
from constants import TreeLoadState, TreeType
from global_actions import GlobalActions
from model.display_tree.build_struct import DiffResultTreeIds
from model.display_tree.display_tree import DisplayTree
from model.node.node import SPIDNodePair
from model.node_identifier import LocalNodeIdentifier, NodeIdentifier
from signal_constants import ID_CENTRAL_EXEC, Signal
from util.stopwatch_sec import Stopwatch
from util.task_runner import Task

logger = logging.getLogger(__name__)


class TreeDiffTask:
    """
    ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
    CLASS TreeDiffTask
    ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼
    """
    
    @staticmethod
    def do_tree_diff(this_task: Task, backend, sender, tree_id_left: str, tree_id_right: str, new_tree_ids: DiffResultTreeIds):
        stopwatch_diff_total = Stopwatch()
        try:
            meta_left: ActiveDisplayTreeMeta = backend.cacheman.get_active_display_tree_meta(tree_id_left)
            if not meta_left:
                raise RuntimeError(f'Cannot start tree diff: failed to find meta for display tree "{tree_id_left}"')
            if meta_left.load_state != TreeLoadState.COMPLETELY_LOADED:
                raise RuntimeError(f'Cannot start tree diff: display tree "{tree_id_left}" is not finished loading '
                                   f'(current load state: {meta_left.load_state.name})')

            meta_right: ActiveDisplayTreeMeta = backend.cacheman.get_active_display_tree_meta(tree_id_right)
            if not meta_left:
                raise RuntimeError(f'Cannot start tree diff: failed to find meta for display tree "{tree_id_right}"')
            if meta_right.load_state != TreeLoadState.COMPLETELY_LOADED:
                raise RuntimeError(f'Cannot start tree diff: display tree "{tree_id_right}" is not finished loading '
                                   f'(current load state: {meta_right.load_state.name})')

            left_root_sn: SPIDNodePair = meta_left.root_sn
            right_root_sn: SPIDNodePair = meta_right.root_sn

            logger.debug(f'Source tree {tree_id_left} has root: {left_root_sn}')
            logger.debug(f'Source tree {tree_id_right} has root: {right_root_sn}')

            logger.debug(f'Sending START_PROGRESS_INDETERMINATE for ID: {sender}')
            dispatcher.send(Signal.START_PROGRESS_INDETERMINATE, sender=sender)
            msg = 'Computing bidrectional content-first diff...'
            dispatcher.send(Signal.SET_PROGRESS_TEXT, sender=sender, msg=msg)

            stopwatch_diff = Stopwatch()
            differ = ContentFirstDiffer(backend, left_root_sn, right_root_sn, new_tree_ids.tree_id_left, new_tree_ids.tree_id_right,
                                        tree_id_left, tree_id_right)
            change_tree_left, change_tree_right, = differ.diff(compare_paths_also=True)
            logger.info(f'{stopwatch_diff} Diff completed')

            dispatcher.send(Signal.SET_PROGRESS_TEXT, sender=sender, msg='Populating UI trees...')

            # Send each side's result to its UI tree:
            left_change_tree: DisplayTree = backend.cacheman.register_change_tree(change_tree_left, src_tree_id=tree_id_left)
            right_change_tree: DisplayTree = backend.cacheman.register_change_tree(change_tree_right, src_tree_id=tree_id_right)

            # Send general notification that we are done:
            dispatcher.send(Signal.STOP_PROGRESS, sender=sender)

            # Rather than sending the usual DISPLAY_TREE_CHANGED signal, we send both new trees simultaneously when the diff is done.
            # This is necessary to avoid a race condition on the client where certain UI elements (e.g. button bar) need to be updated at the same
            # time as each of the two trees.
            logger.debug(f'Sending signal {Signal.DIFF_TREES_DONE.name} for sender={sender}')
            dispatcher.send(signal=Signal.DIFF_TREES_DONE, sender=sender, tree_left=left_change_tree, tree_right=right_change_tree)
            logger.debug(f'{stopwatch_diff_total} Finished diff')
        except Exception as err:
            # Clean up progress bar:
            dispatcher.send(Signal.STOP_PROGRESS, sender=sender)
            dispatcher.send(signal=Signal.DIFF_TREES_FAILED, sender=sender)
            logger.exception(err)
            GlobalActions.display_error_in_ui(ID_CENTRAL_EXEC, 'Diff task failed due to unexpected error', repr(err))

    @staticmethod
    def _tree_exists_on_disk(node_identifier: NodeIdentifier) -> bool:
        assert isinstance(node_identifier, LocalNodeIdentifier)
        for path in node_identifier.get_path_list():
            if os.path.exists(path):
                return True
        return False
