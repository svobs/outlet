import logging
from typing import List

from pydispatch import dispatcher

from backend.display_tree.active_tree_meta import ActiveDisplayTreeMeta
from backend.display_tree.change_tree import ChangeTree
from constants import SUPER_ROOT_DEVICE_UID, TreeDisplayMode, TreeID, TreeType
from global_actions import GlobalActions
from logging_constants import SUPER_DEBUG_ENABLED
from model.display_tree.display_tree import DisplayTreeUiState
from model.node.container_node import RootTypeNode
from model.node.node import SPIDNodePair
from model.node_identifier import GUID, SinglePathNodeIdentifier
from model.node_identifier_factory import NodeIdentifierFactory
from signal_constants import ID_MERGE_TREE, Signal
from util.stopwatch_sec import Stopwatch

logger = logging.getLogger(__name__)


class TreeDiffMergeTask:
    """
    ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
    CLASS TreeDiffMergeTask

    For generating a "merge preview" tree from two change trees.
    The resulting tree will have a tree_id of ID_MERGE_TREE
    ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼
    """
    def __init__(self, backend):
        self.backend = backend

    def generate_merge_tree(self, sender,
                            tree_id_left: TreeID, tree_id_right: TreeID,
                            selected_guid_list_left: List[GUID], selected_guid_list_right: List[GUID]):

        if len(selected_guid_list_left) == 0 and len(selected_guid_list_right) == 0:
            # TODO: make info msg instead
            GlobalActions.display_error_in_ui(sender, 'You must select change(s) first.')
            dispatcher.send(signal=Signal.GENERATE_MERGE_TREE_FAILED, sender=sender)
            return

        sw = Stopwatch()

        try:
            meta_left: ActiveDisplayTreeMeta = self.backend.cacheman.get_active_display_tree_meta(tree_id_left)
            if not meta_left:
                raise RuntimeError(f'Could not generate merge tree: could not find record of tree: {tree_id_left}')
            if not meta_left.change_tree:
                raise RuntimeError(f'Could not generate merge tree: no ChangeTree in record: {tree_id_left}')

            meta_right: ActiveDisplayTreeMeta = self.backend.cacheman.get_active_display_tree_meta(tree_id_right)
            if not meta_right:
                raise RuntimeError(f'Could not generate merge tree: could not find record of tree: {tree_id_right}')
            if not meta_right.change_tree:
                raise RuntimeError(f'Could not generate merge tree: no ChangeTree in record: {tree_id_right}')

            merged_change_tree = self._generate_merged_tree(meta_left.change_tree, selected_guid_list_left,
                                                            meta_right.change_tree, selected_guid_list_right)

            # TODO: surely there is a cleaner solution than src_tree_id
            self.backend.cacheman.register_change_tree(merged_change_tree, src_tree_id=None)

            dispatcher.send(signal=Signal.GENERATE_MERGE_TREE_DONE, sender=sender, tree=merged_change_tree)
            logger.debug(f'{sw} Finished generating merge tree')
        except Exception as err:
            logger.exception(err)
            dispatcher.send(signal=Signal.GENERATE_MERGE_TREE_FAILED, sender=sender)
            GlobalActions.display_error_in_ui(sender, 'Failed to generate merge preview due to unexpected error', repr(err))

    def _generate_merged_tree(self,
                              change_tree_left: ChangeTree, selected_guid_list_left: List[GUID],
                              change_tree_right: ChangeTree, selected_guid_list_right: List[GUID]) -> ChangeTree:

        super_root_spid: SinglePathNodeIdentifier = NodeIdentifierFactory.get_root_constant_spid(tree_type=TreeType.MIXED,
                                                                                                 device_uid=SUPER_ROOT_DEVICE_UID)
        super_root_sn = SPIDNodePair(super_root_spid, RootTypeNode(super_root_spid))
        state: DisplayTreeUiState = DisplayTreeUiState(tree_id=ID_MERGE_TREE, root_sn=super_root_sn,
                                                       tree_display_mode=TreeDisplayMode.CHANGES_ONE_TREE_PER_CATEGORY)

        merged_tree = ChangeTree(backend=self.backend, state=state, is_super_root_tree=True)

        for guid in selected_guid_list_left:
            self._add_node_and_op_list_and_ancestors(change_tree_left, guid, merged_tree)

        for guid in selected_guid_list_right:
            self._add_node_and_op_list_and_ancestors(change_tree_right, guid, merged_tree)

        # TODO: check for conflicts

        return merged_tree

    @staticmethod
    def _find_batch_uid(change_tree_left, change_tree_right):
        if change_tree_left.get_op_list():
            return change_tree_left.get_op_list()[0].batch_uid

        if change_tree_right.get_op_list():
            return change_tree_right.get_op_list()[0].batch_uid
        raise RuntimeError(f'find_batch_uid(): no ops found in either ChangeTree!')

    def _add_node_and_op_list_and_ancestors(self, src_tree: ChangeTree, guid, merged_tree):
        sn = src_tree.get_sn_for_guid(guid)

        # Search recursively to make sure parents are all added:
        parent_sn_from_cache = self.backend.cacheman.get_parent_for_sn(sn)
        if not parent_sn_from_cache:
            if SUPER_DEBUG_ENABLED:
                logger.debug(f'merge_change_trees(): Parent for {sn.spid} not found in main cache; checking src ChangeTree...')
            parent_sn_from_change_tree = src_tree.get_parent_sn_for_guid(guid)
            if not parent_sn_from_change_tree:
                raise RuntimeError(f'Missing GUID {guid} (ancestor of node: {sn.spid})')
            parent_guid = parent_sn_from_change_tree.spid.guid
            # Already added to merge tree?
            if not merged_tree.contains_guid(parent_guid):
                # Recurse and then add to merge tree:
                self._add_node_and_op_list_and_ancestors(src_tree, parent_guid, merged_tree)

        op_list = src_tree.get_op_list_for_guid(guid)
        if op_list:
            if SUPER_DEBUG_ENABLED:
                logger.debug(f'merge_change_trees(): Adding node {sn.spid} with ops {op_list}')
            merged_tree.add_op_list_with_target_sn(sn, op_list)
        else:
            if sn and sn.node.is_container_node():
                # If a MKDIR is required for an ancestor, it should have been created already in the src_tree
                mkdir_op = src_tree.get_mkdir_for_sn(sn)
                if mkdir_op:
                    existing_op_list = merged_tree.get_op_list_for_guid(guid)
                    if existing_op_list and existing_op_list[0].op_uid == mkdir_op.op_uid:
                        logger.debug(f'merge_change_trees(): Already added MKDIR for: {guid}; skipping')
                    else:
                        logger.debug(f'merge_change_trees(): Adding MKDIR for: {sn.spid}')
                        real_sn = SPIDNodePair(sn.spid, mkdir_op.src_node)  # add_op_list_with_target_sn() expects a real_sn, not container node
                        merged_tree.add_op_list_with_target_sn(real_sn, [mkdir_op], category_override=sn.spid.category)
                elif SUPER_DEBUG_ENABLED:
                    # TODO: if op is an RM, check whether all its descendants are going to be deleted as well, and if so, append
                    # an RM operation for the directory. This will be non-trivial because it will have to check for descendant dirs...
                    logger.debug(f'merge_change_trees(): Skipping node because it is only a display node: {guid}')
            else:
                if SUPER_DEBUG_ENABLED:
                    logger.error(f'merge_change_trees(): Skipping node because no associated UserOp found: {guid}')
