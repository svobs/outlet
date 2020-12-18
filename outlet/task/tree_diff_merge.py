import logging
from typing import List

from pydispatch import dispatcher

from constants import TREE_TYPE_MIXED
from global_actions import GlobalActions
from model.display_tree.change_display_tree import ChangeDisplayTree
from model.display_tree.display_tree import DisplayTreeUiState
from model.node.container_node import RootTypeNode
from model.node.node import SPIDNodePair
from model.node_identifier import SinglePathNodeIdentifier
from model.node_identifier_factory import NodeIdentifierFactory
from store.tree.active_tree_meta import ActiveDisplayTreeMeta
from ui.signal import ID_MERGE_TREE, Signal
from util.stopwatch_sec import Stopwatch

logger = logging.getLogger(__name__)


class TreeDiffMergeAction:
    """
    ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
    CLASS TreeDiffMergeAction

    For generating a "merge preview" tree from two change trees.
    ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼
    """

    @staticmethod
    def generate_merge_tree(backend, sender,
                            tree_id_left: str, tree_id_right: str,
                            selected_changes_left: List[SPIDNodePair], selected_changes_right: List[SPIDNodePair]):
        if len(selected_changes_left) == 0 and len(selected_changes_right) == 0:
            # TODO: make info msg instead
            GlobalActions.display_error_in_ui(sender, 'You must select change(s) first.')
            return

        sw = Stopwatch()

        try:
            meta_left: ActiveDisplayTreeMeta = backend.cacheman.get_active_display_tree_meta(tree_id_left)
            if not meta_left:
                raise RuntimeError(f'Could not generate merge tree: could not find record of tree: {tree_id_left}')
            if not meta_left.change_tree:
                raise RuntimeError(f'Could not generate merge tree: no ChangeTree in record: {tree_id_left}')

            meta_right: ActiveDisplayTreeMeta = backend.cacheman.get_active_display_tree_meta(tree_id_right)
            if not meta_right:
                raise RuntimeError(f'Could not generate merge tree: could not find record of tree: {tree_id_right}')
            if not meta_right.change_tree:
                raise RuntimeError(f'Could not generate merge tree: no ChangeTree in record: {tree_id_right}')

            merged_changes_tree = TreeDiffMergeAction.merge_change_trees(backend,
                                                                         meta_left.change_tree, selected_changes_left,
                                                                         meta_right.change_tree, selected_changes_right)

            # FIXME: need to clean up this mechanism: src_tree_id makes no sense
            backend.cacheman.register_change_tree(merged_changes_tree, src_tree_id=None)

            dispatcher.send(signal=Signal.GENERATE_MERGE_TREE_DONE, sender=sender, tree=merged_changes_tree)
            logger.debug(f'{sw} Finished generating merge tree')

            logger.info(f'Generated merge preview tree: {merged_changes_tree.get_summary()}')
        except Exception as err:
            logger.exception(err)
            dispatcher.send(signal=Signal.GENERATE_MERGE_TREE_FAILED, sender=sender)
            GlobalActions.display_error_in_ui(sender, 'Failed to generate merge preview due to unexpected error', repr(err))

    @staticmethod
    def merge_change_trees(backend,
                           tree_left: ChangeDisplayTree, left_selected_changes: List[SPIDNodePair],
                           tree_right: ChangeDisplayTree, right_selected_changes: List[SPIDNodePair]) -> ChangeDisplayTree:

        super_root_spid: SinglePathNodeIdentifier = NodeIdentifierFactory.get_root_constant_single_path_identifier(TREE_TYPE_MIXED)
        super_root_sn = SPIDNodePair(super_root_spid, RootTypeNode(super_root_spid))
        state: DisplayTreeUiState = DisplayTreeUiState(tree_id=ID_MERGE_TREE, root_sn=super_root_sn)
        merged_tree = ChangeDisplayTree(backend=backend, state=state, show_whole_forest=True)

        for sn in left_selected_changes:
            op = tree_left.get_op_for_node(sn.node)
            if op:
                merged_tree.add_node(sn, op)
            else:
                logger.debug(f'merge_change_trees(): Skipping left-side node because it is not associated with an UserOp: {sn.node}')

        for sn in right_selected_changes:
            op = tree_right.get_op_for_node(sn.node)
            if op:
                merged_tree.add_node(sn, op)
            else:
                logger.debug(f'merge_change_trees(): Skipping right-side node because it is not associated with an UserOp: {sn.node}')

        # TODO: check for conflicts

        return merged_tree
