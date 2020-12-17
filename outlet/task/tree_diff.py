import logging
import os
from typing import List

from pydispatch import dispatcher

from app.backend import DiffResultTreeIds
from constants import TREE_TYPE_LOCAL_DISK
from diff.diff_content_first import ContentFirstDiffer
from global_actions import GlobalActions
from model.node.node import SPIDNodePair
from model.node_identifier import LocalNodeIdentifier, NodeIdentifier
from ui.signal import ID_CENTRAL_EXEC, Signal
from util.stopwatch_sec import Stopwatch

logger = logging.getLogger(__name__)


class TreeDiffAction:
    """
    ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
    CLASS TreeDiffAction
    ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼
    """
    
    @staticmethod
    def do_tree_diff(backend, sender, tree_id_left: str, tree_id_right: str, new_tree_ids: DiffResultTreeIds):
        stopwatch_diff_total = Stopwatch()
        try:
            meta_left = backend.cacheman.get_active_display_tree_meta(tree_id_left)
            meta_right = backend.cacheman.get_active_display_tree_meta(tree_id_right)
            assert meta_left and meta_right, f'Missing tree meta! Left={meta_left}, Right={meta_right}'
            left_root_sn: SPIDNodePair = meta_left.root_sn
            right_root_sn: SPIDNodePair = meta_right.root_sn
            if left_root_sn.spid.tree_type == TREE_TYPE_LOCAL_DISK and not TreeDiffAction._tree_exists_on_disk(left_root_sn.spid):
                logger.info(f'Skipping diff because the left path does not exist: "{left_root_sn.spid.get_path_list()}"')
                dispatcher.send(signal=Signal.DIFF_TREES_FAILED, sender=sender)
                return
            elif right_root_sn.spid.tree_type == TREE_TYPE_LOCAL_DISK and not TreeDiffAction._tree_exists_on_disk(right_root_sn.spid):
                logger.info(f'Skipping diff because the right path does not exist: "{right_root_sn.spid.get_path_list()}"')
                dispatcher.send(signal=Signal.DIFF_TREES_FAILED, sender=sender)
                return

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
            backend.cacheman.register_change_tree(change_tree_left, src_tree_id=tree_id_left)
            backend.cacheman.register_change_tree(change_tree_right, src_tree_id=tree_id_right)

            # Send general notification that we are done:
            dispatcher.send(Signal.STOP_PROGRESS, sender=sender)
            dispatcher.send(signal=Signal.DIFF_TREES_DONE, sender=sender)
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

    @staticmethod
    def generate_merge_tree(backend, sender, tree_id_left: str, tree_id_right: str, new_tree_ids: DiffResultTreeIds,
                            selected_changes_left: List[SPIDNodePair], selected_changes_right: List[SPIDNodePair]):
        if len(selected_changes_left) == 0 and len(selected_changes_right) == 0:
            # TODO: make info msg instead
            GlobalActions.display_error_in_ui(sender, 'You must select change(s) first.')
            return None

        sw = Stopwatch()

        try:
            meta_left = backend.cacheman.get_active_display_tree_meta(tree_id_left)
            meta_right = backend.cacheman.get_active_display_tree_meta(tree_id_right)
            left_sn = meta_left.root_sn
            right_sn = meta_right.root_sn
            differ = ContentFirstDiffer(backend, left_sn, right_sn, new_tree_ids.tree_id_left, new_tree_ids.tree_id_right,
                                        tree_id_left, tree_id_right)
            merged_changes_tree = differ.merge_change_trees(selected_changes_left, selected_changes_right)
            # FIXME: need to clean up this mechanism: src_tree_id makes no sense
            backend.cacheman.register_change_tree(merged_changes_tree, src_tree_id=None)

            dispatcher.send(signal=Signal.GENERATE_MERGE_TREE_DONE, sender=sender, tree=merged_changes_tree)
            logger.debug(f'{sw} Finished generating merge tree')

            conflict_pairs = []
            if conflict_pairs:
                # TODO: more informative error
                GlobalActions.display_error_in_ui(sender, 'Cannot merge', f'{len(conflict_pairs)} conflicts found')
                return None

            logger.info(f'Generated merge preview tree: {merged_changes_tree.get_summary()}')
        except Exception as err:
            logger.exception(err)
            dispatcher.send(signal=Signal.GENERATE_MERGE_TREE_FAILED, sender=sender)
            GlobalActions.display_error_in_ui(sender, 'Failed to generate merge preview due to unexpected error', repr(err))
