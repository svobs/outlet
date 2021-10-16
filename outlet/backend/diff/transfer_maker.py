import collections
import copy
import logging
import os
import re
from typing import Callable, Deque, Dict, List, Optional, Tuple

from backend.diff.change_maker import ChangeMaker
from backend.tree_store.local import content_hasher
from constants import DirConflictPolicy, DragOperation, FileConflictPolicy, ReplaceDirWithFilePolicy, SrcNodeMovePolicy, \
    SUPER_DEBUG_ENABLED, \
    TRACE_ENABLED, TreeID
from model.node.node import SPIDNodePair
from model.node_identifier import SinglePathNodeIdentifier
from model.user_op import UserOpType

logger = logging.getLogger(__name__)


class TransferMeta:
    def __init__(self, drag_op: DragOperation, dir_conflict_policy: DirConflictPolicy, file_conflict_policy: FileConflictPolicy,
                 sn_dst_parent: SPIDNodePair, dst_existing_sn_dict: Dict[str, List[SPIDNodePair]]):
        self.drag_op: DragOperation = drag_op
        self.dir_conflict_policy: DirConflictPolicy = dir_conflict_policy
        self.file_conflict_policy: FileConflictPolicy = file_conflict_policy

        # hard-code these for now
        self.src_node_move_policy: SrcNodeMovePolicy = SrcNodeMovePolicy.DELETE_SRC_IF_NOT_SKIPPED
        self.replace_dir_with_file_policy: ReplaceDirWithFilePolicy = ReplaceDirWithFilePolicy.FAIL

        self.op_type = UserOpType.CP if drag_op == DragOperation.COPY else UserOpType.MV

        self.sn_dst_parent: SPIDNodePair = sn_dst_parent

        # This is a Dict containing all the child nodes of the destination parent, indexed by node name.
        self.dst_existing_sn_dict: Dict[str, List[SPIDNodePair]] = dst_existing_sn_dict


class TransferMaker(ChangeMaker):
    def __init__(self, backend, left_tree_root_sn: SPIDNodePair, right_tree_root_sn: SPIDNodePair,
                 tree_id_left_src: TreeID, tree_id_right_src: TreeID):
        super().__init__(backend, left_tree_root_sn, right_tree_root_sn, tree_id_left_src, tree_id_right_src)

    def drag_and_drop(self, sn_src_list: List[SPIDNodePair], sn_dst_parent: SPIDNodePair,
                      drag_op: DragOperation, dir_conflict_policy: DirConflictPolicy, file_conflict_policy: FileConflictPolicy):
        """Populates the destination parent in "change_tree_right" with a subset of the given source nodes (assumed to be from the left side)
        based on the given DragOperation and policies. NOTE: this may actually result in UserOps created in the left
        ChangeTree (in particular RM), so use the get_all_op_list() method to get the complete list of resulting UserOps."""

        assert sn_dst_parent and sn_dst_parent.node.is_dir()
        if not (drag_op == DragOperation.COPY or drag_op == DragOperation.MOVE):
            raise RuntimeError(f'Unsupported DragOperation: {drag_op.name}')

        dst_existing_sn_dict: Dict[str, List[SPIDNodePair]] = self._get_dict_of_name_to_child_list(sn_dst_parent.spid, self.right_side.tree_id_src)
        dd_meta = TransferMeta(drag_op, dir_conflict_policy, file_conflict_policy, sn_dst_parent, dst_existing_sn_dict)

        logger.debug(f'Preparing {len(sn_src_list)} nodes for {drag_op.name}...')

        for sn_src in sn_src_list:
            # In general, we will throw an error rather than attempt to replace or merge more than 1 node with the same name
            list_sn_dst_conflicting: List[SPIDNodePair] = dst_existing_sn_dict.get(sn_src.node.name)

            if not list_sn_dst_conflicting:
                if SUPER_DEBUG_ENABLED:
                    logger.debug(f'Node "{sn_src.node.name}": no name conflicts found')
                self._handle_no_conflicts_found(dd_meta, sn_src)

            else:  # Conflict(s)
                if sn_src.node.is_dir():
                    self._handle_dir_conflict(dd_meta, sn_src, list_sn_dst_conflicting)
                else:
                    self._handle_file_conflict(dd_meta, sn_src, list_sn_dst_conflicting)

    def _handle_dir_conflict(self, dd_meta: TransferMeta, sn_src: SPIDNodePair, list_sn_dst_conflicting):
        name_src = sn_src.node.name
        policy = dd_meta.dir_conflict_policy
        assert list_sn_dst_conflicting
        has_multiple_name_conflicts: bool = len(list_sn_dst_conflicting) > 1

        if SUPER_DEBUG_ENABLED:
            logger.debug(f'Dir "{name_src}" has {len(list_sn_dst_conflicting)} conflicts: following policy {policy.name}')

        if policy == DirConflictPolicy.SKIP:
            # SKIP DIR: trivial
            pass
        elif policy == DirConflictPolicy.REPLACE:
            # REPLACE DIR: similar to MERGE
            if has_multiple_name_conflicts:
                raise RuntimeError(f'For folder "{name_src}": found {len(list_sn_dst_conflicting) > 1} items '
                                   f'at the destination with the same name, and cannot determine which to replace!')
            self._handle_dir_replace(dd_meta, sn_src, list_sn_dst_conflicting[0])

        elif policy == DirConflictPolicy.RENAME:
            # RENAME DIR
            self._handle_rename(dd_meta, sn_src, skip_condition_func=None)
        elif policy == DirConflictPolicy.MERGE:
            # MERGE DIR
            if has_multiple_name_conflicts:
                raise RuntimeError(f'For folder "{sn_src}": found {len(list_sn_dst_conflicting) > 1} items '
                                   f'at the destination with the same name, and cannot determine which to merge with!')
            self._handle_dir_merge(dd_meta, sn_src, list_sn_dst_conflicting[0])

        elif policy == DirConflictPolicy.PROMPT:
            # TODO
            pass

        else:
            raise RuntimeError(f'Unrecognized DirConflictPolicy: {policy}')

    def _handle_file_conflict(self, dd_meta: TransferMeta, sn_src: SPIDNodePair, list_sn_dst_conflicting: List[SPIDNodePair]):
        name_src = sn_src.node.name
        policy = dd_meta.file_conflict_policy
        assert list_sn_dst_conflicting
        has_multiple_name_conflicts: bool = len(list_sn_dst_conflicting) > 1

        if SUPER_DEBUG_ENABLED:
            logger.debug(f'File "{name_src}" has {len(list_sn_dst_conflicting)} conflicts: following policy {policy.name}')

        if policy == FileConflictPolicy.SKIP:
            pass

        elif policy == FileConflictPolicy.REPLACE_ALWAYS:
            # REPLACE ALWAYS (FILE)
            if has_multiple_name_conflicts:
                raise RuntimeError(f'For file "{name_src}": found {len(list_sn_dst_conflicting) > 1} items '
                                   f'at the destination with the same name, and cannot determine which to replace!')
            self._handle_replace_with_file(dd_meta, sn_src, list_sn_dst_conflicting[0])

        elif policy == FileConflictPolicy.REPLACE_IF_OLDER_AND_DIFFERENT:
            # REPLACE ALWAYS (FILE)
            if has_multiple_name_conflicts:
                raise RuntimeError(f'For file "{name_src}": found {len(list_sn_dst_conflicting) > 1} items '
                                   f'at the destination with the same name, and cannot determine which to replace!')
            self._handle_replace_with_file(dd_meta, sn_src, list_sn_dst_conflicting[0], skip_condition_func=self._is_same_content_and_not_older)

        elif policy == FileConflictPolicy.RENAME_ALWAYS:
            # RENAME ALWAYS (FILE)
            self._handle_rename(dd_meta, sn_src)

        elif policy == FileConflictPolicy.RENAME_IF_OLDER_AND_DIFFERENT:
            # RENAME IF CONTENT DIFFERS AND OLDER (FILE)
            self._handle_rename(dd_meta, sn_src, skip_condition_func=self._is_same_content_and_not_older)

        elif policy == FileConflictPolicy.RENAME_IF_DIFFERENT:
            # RENAME IF CONTENT DIFFERS (FILE)
            self._handle_rename(dd_meta, sn_src, skip_condition_func=self._is_same_content)

        elif policy == FileConflictPolicy.PROMPT:
            # TODO
            pass

        else:
            raise RuntimeError(f'Unrecognized FileConflictPolicy: {policy}')

    def _handle_dir_replace(self, dd_meta: TransferMeta, sn_src: SPIDNodePair, sn_dst_conflicting: SPIDNodePair):
        """Rather than just deleting the whole tree and then adding the new tree, we try to dive in and see what files already exist so as
        to minimize the work involved. For that reason, the method is pretty similar to _handle_dir_merge()"""
        assert sn_src.node.is_dir(), f'Not a dir: {sn_src.node}'

        logger.debug(f'Replacing {sn_dst_conflicting.spid} with dir {sn_src.spid}...')

        queue: Deque[Tuple[SPIDNodePair, SPIDNodePair]] = collections.deque()
        # assume this dir has already been validated and has exactly 1 conflict
        queue.append((sn_src, sn_dst_conflicting))

        while len(queue) > 0:
            sn_dir_src, sn_dir_dst_existing = queue.popleft()
            if SUPER_DEBUG_ENABLED:
                logger.debug(f'Replace: examining dir: {sn_dir_src.spid}')

            if sn_dir_dst_existing.node.is_file():
                if SUPER_DEBUG_ENABLED:
                    logger.debug(f'Replacing {sn_dir_dst_existing.spid} with dir {sn_dir_src.spid}')
                # Remove file:
                self.right_side.add_node_and_new_op(op_type=UserOpType.RM, sn_src=sn_dir_dst_existing)
                # Now just transfer the src subtree as though the conflicts never existed:
                self._handle_no_conflicts_found(dd_meta, sn_dir_src)
            else:
                dict_sn_dst_existing_child_list = self._get_dict_of_name_to_child_list(sn_dir_dst_existing.spid, self.right_side.tree_id_src)

                # Get children for src & dst, compare all
                for sn_src_child in self.backend.cacheman.get_child_list(sn_dir_src.spid, self.left_side.tree_id_src):
                    # see if there is a corresponding dst node:
                    list_sn_dst_conflicting: List[SPIDNodePair] = dict_sn_dst_existing_child_list.pop(sn_src_child.node.name)

                    if len(list_sn_dst_conflicting) == 0:
                        # No conflicts? Just transfer the file or dir subtree:
                        self._handle_no_conflicts_found(dd_meta, sn_src_child)
                    else:
                        # CONFLICT(S)
                        if len(list_sn_dst_conflicting) > 1 or list_sn_dst_conflicting[0].node.is_dir():
                            # Multiple conflicting nodes with same name? Just delete all of them. Too rare an occurrence to optimize.
                            # If dst is dir, we simply delete it before replacing it (no need to consult ReplaceDirWithFilePolicy)
                            for sn_dst_conflicting in list_sn_dst_conflicting:
                                self._delete_subtree(sn_dst_conflicting)
                            # Now just transfer the src subtree as though the conflicts never existed:
                            self._handle_no_conflicts_found(dd_meta, sn_src_child)
                        else:
                            if sn_src_child.node.is_dir():
                                # DIR with exactly 1 conflict: dive in deeper
                                queue.append((sn_src_child, list_sn_dst_conflicting[0]))
                            else:
                                # SRC is FILE:
                                self._handle_replace_with_file(dd_meta, sn_src, list_sn_dst_conflicting[0])

                # Remaining nodes in the dict must all be deleted:
                for list_sn_dst_child in dict_sn_dst_existing_child_list.values():
                    for sn_dst_child in list_sn_dst_child:
                        self._delete_subtree(sn_dst_child)

    def _handle_dir_merge(self, dd_meta: TransferMeta, sn_src: SPIDNodePair, sn_dst_conflicting: SPIDNodePair):
        assert sn_src.node.is_dir()

        logger.debug(f'Merging {sn_dst_conflicting.spid} with dir {sn_src.spid}...')

        queue: Deque[Tuple[SPIDNodePair, SPIDNodePair]] = collections.deque()
        # assume this dir has already been validated and has exactly 1 conflict
        queue.append((sn_src, sn_dst_conflicting))

        while len(queue) > 0:
            sn_dir_src, sn_dir_dst_existing = queue.popleft()
            if SUPER_DEBUG_ENABLED:
                logger.debug(f'DirMerge: examining dir: {sn_dir_src.spid}')

            if sn_dir_dst_existing.node.is_file():
                # TODO: maybe just follow the file conflict policy in this case?
                raise RuntimeError(f'Cannot merge: {dd_meta.drag_op.name} of a directory onto a file!')
            else:
                dict_sn_dst_existing_child_list = self._get_dict_of_name_to_child_list(sn_dir_dst_existing.spid, self.right_side.tree_id_src)

                # Get children for src & dst, compare all
                for sn_src_child in self.backend.cacheman.get_child_list(sn_dir_src.spid, self.left_side.tree_id_src):
                    # see if there is a corresponding dst node:
                    list_sn_dst_conflicting: List[SPIDNodePair] = dict_sn_dst_existing_child_list.pop(sn_src_child.node.name)

                    if SUPER_DEBUG_ENABLED:
                        logger.debug(f'DirMerge: found {len(list_sn_dst_conflicting)} conflicts for child dir: {sn_src_child.spid}')

                    if len(list_sn_dst_conflicting) == 0:
                        # No conflicts? Just transfer the file or dir subtree:
                        self._handle_no_conflicts_found(dd_meta, sn_src_child)
                    else:
                        # CONFLICTS
                        if sn_src_child.node.is_dir():
                            # SRC is DIR:
                            if len(list_sn_dst_conflicting) > 1:
                                raise RuntimeError(f'For folder "{sn_src_child}": found {len(list_sn_dst_conflicting) > 1} items '
                                                   f'at the destination with the same name, and cannot determine which to merge with!')

                            # DIR with exactly 1 conflict: dive in deeper
                            queue.append((sn_src_child, list_sn_dst_conflicting[0]))
                        else:
                            # SRC is FILE: replace
                            if len(list_sn_dst_conflicting) > 1:
                                # Just fail: the user may not have considered this scenario.
                                raise RuntimeError(f'For item "{sn_src.node.name}": found {len(list_sn_dst_conflicting) > 1} items '
                                                   f'at the destination with the same name, and cannot determine which to replace!')

                            self._handle_replace_with_file(dd_meta, sn_src_child, list_sn_dst_conflicting[0])

    def _delete_subtree(self, sn_dst_subtree_root: SPIDNodePair):
        if sn_dst_subtree_root.node.is_dir():
            for sn in self.backend.cacheman.get_subtree_bfs_sn_list(sn_dst_subtree_root.node.node_identifier):
                self.right_side.add_node_and_new_op(op_type=UserOpType.RM, sn_src=sn)
        else:
            self.right_side.add_node_and_new_op(op_type=UserOpType.RM, sn_src=sn_dst_subtree_root)

    @staticmethod
    def _increment_node_name(node_name: str) -> str:
        # Search for an existing copy number, starting from the end:
        match = re.search("(?s:.*)[0-9]*", node_name)
        if match:
            matching_str = match.group(1)  # get first matching group
            copy_number = int(matching_str) + 1  # try next
            new_node_name_prefix = node_name.removesuffix(matching_str).rstrip()
        else:
            copy_number = 2  # first copy number to start with
            new_node_name_prefix = node_name.rstrip()

        return f'{new_node_name_prefix} {copy_number}'

    @staticmethod
    def _calculate_signatures_if_missing_and_local(sn):
        """Ensure both nodes have signatures filled in (if local nodes)"""
        if not sn.node.md5 or not sn.node.sha256:
            node_with_sigs = content_hasher.try_calculating_signatures(sn.node)
            if node_with_sigs:
                sn.node = node_with_sigs

    # SKIP CONDITION:
    def _is_same_content(self, sn_src: SPIDNodePair, sn_dst: SPIDNodePair) -> bool:
        if sn_src.node.is_dir() != sn_dst.node.is_dir():
            # If one is a file and one is a dir, obviously they don't have the same content
            return False

        self._calculate_signatures_if_missing_and_local(sn_src)
        self._calculate_signatures_if_missing_and_local(sn_dst)

        # TODO: decide how to handle GDrive non-file types which don't have signatures (e.g. shortcuts, Google Docs...)
        return sn_src.node.is_signature_match(sn_dst.node)

    # SKIP CONDITION:
    def _is_same_content_and_not_older(self, sn_src: SPIDNodePair, sn_dst: SPIDNodePair) -> bool:
        if sn_src.node.is_dir() != sn_dst.node.is_dir():
            # If one is a file and one is a dir, obviously they don't have the same content
            return False

        self._calculate_signatures_if_missing_and_local(sn_src)
        self._calculate_signatures_if_missing_and_local(sn_dst)

        # TODO: decide how to handle GDrive non-file types which don't have signatures (e.g. shortcuts, Google Docs...)
        if sn_src.node.modify_ts == 0 or sn_dst.node.modify_ts == 0:
            logger.error(f'One of these has modify_ts=0. Src: {sn_src.node}, Dst: {sn_dst.node}')
            raise RuntimeError(f'Cannot compare modification times: at least one node is missing modify_ts')
        return sn_src.node.is_signature_match(sn_dst.node) and \
               sn_src.node.modify_ts <= sn_dst.node.modify_ts

    def _handle_replace_with_file(self, dd_meta: TransferMeta, sn_src: SPIDNodePair, sn_dst_conflicting: SPIDNodePair,
                                  skip_condition_func: Optional[Callable[[SPIDNodePair, SPIDNodePair], bool]] = None):
        if TRACE_ENABLED:
            logger.debug(f'Entered _handle_replace_with_file() for sn_src={sn_src.spid}')
        assert sn_src.node.is_file(), f'Expected to be a file: {sn_src.node}'

        name_src: str = sn_src.node.name
        if skip_condition_func:
            if self._execute_skip_condition(dd_meta, sn_src, name_src, sn_dst_conflicting, skip_condition_func):
                return
        else:
            if TRACE_ENABLED:
                logger.debug(f'No skip condition func supplied; assuming skip=never for conflict name="{name_src}"')

        if sn_dst_conflicting.node.is_dir():
            policy = dd_meta.replace_dir_with_file_policy
            if policy == ReplaceDirWithFilePolicy.FAIL:
                raise RuntimeError(f'Cannot replace a directory with a file: {sn_dst_conflicting.spid.get_single_path()}')
            elif policy == ReplaceDirWithFilePolicy.FOLLOW_FILE_POLICY_FOR_DIR:
                # If we got here, we have already evaluated the skip condition and have not skipped it.
                # Proceed to replace it.
                pass
            elif policy == ReplaceDirWithFilePolicy.PROMPT:
                # TODO
                raise NotImplementedError

        # If we got here, we are going to replace (AKA update) the node:

        if sn_dst_conflicting.node.is_dir():
            # Special handling for dir node
            self._delete_subtree(sn_dst_conflicting)
            # Now just transfer the src subtree as though the conflicts never existed:
            self._handle_no_conflicts_found(dd_meta, sn_src)
        else:
            sn_dst = copy.deepcopy(sn_dst_conflicting)
            sn_dst.node.set_is_live(False)
            sn_dst.node.sync_ts = None
            sn_dst.node.update_signature_and_timestamps_from(sn_src.node)

            if dd_meta.drag_op == DragOperation.COPY:
                self.right_side.add_node_and_new_op(op_type=UserOpType.CP_ONTO, sn_src=sn_src, sn_dst=sn_dst)
            elif dd_meta.drag_op == DragOperation.MOVE:
                self.right_side.add_node_and_new_op(op_type=UserOpType.MV_ONTO, sn_src=sn_src, sn_dst=sn_dst)

    def _handle_rename(self, dd_meta: TransferMeta, sn_src: SPIDNodePair,
                       skip_condition_func: Optional[Callable[[SPIDNodePair, SPIDNodePair], bool]] = None):
        """COPY or MOVE where target will be renamed so as to avoid any possible conflicts.

        Param "skip_condition_func", if specified, will be called for all conflicting nodes and result in the operation being partially or
        wholly aborted if it evaulates to true for any of the conflicting nodes. If "skip_condition_func" is not specified, then this method
        will always result in a rename.

        If the renamed node also results in one or more conflicts, this method will loop as many times as needed until an unused name is found or
        until any of the new conflicts results in "skip_condition_func" evaluating to true.

        If "skip_condition_func" does evaluate to true, resulting in a skip, but the operation is a MV, the SrcNodeMovePolicy is consulted to
        determine whether to still delete the src node.
        """
        if TRACE_ENABLED:
            logger.debug(f'Entered _handle_rename() for sn_src={sn_src.spid}')

        name_src: str = sn_src.node.name

        while True:
            list_sn_dst_conflicting = dd_meta.dst_existing_sn_dict.get(name_src)
            if not list_sn_dst_conflicting:
                break

            if skip_condition_func:
                for sn_dst_conflicting in list_sn_dst_conflicting:
                    if self._execute_skip_condition(dd_meta, sn_src, name_src, sn_dst_conflicting, skip_condition_func):
                        return

                if TRACE_ENABLED:
                    logger.debug(f'No conflicting nodes matched the skip condition for conflict name="{name_src}"')
            else:
                if TRACE_ENABLED:
                    logger.debug(f'No skip condition func supplied; assuming skip=never for conflict name="{name_src}"')
                # fall through

            # No match for skip condition, or no skip condition supplied
            name_src = self._increment_node_name(name_src)
            logger.debug(f'Incremented name_src to "{name_src}"')

        logger.debug(f'Renaming "{sn_src.spid.get_single_path()}" to "{name_src}"')
        self._handle_no_conflicts_found(dd_meta, sn_src, name_new_dst=name_src)

    def _execute_skip_condition(self, dd_meta: TransferMeta, sn_src: SPIDNodePair, name_src: str, sn_dst_conflicting: SPIDNodePair,
                                skip_condition_func: Optional[Callable[[SPIDNodePair, SPIDNodePair], bool]]) -> bool:
        if skip_condition_func(sn_src, sn_dst_conflicting):
            # True -> Do skip
            if dd_meta.op_type == UserOpType.MV:
                mv_policy = dd_meta.src_node_move_policy

                if mv_policy == SrcNodeMovePolicy.DELETE_SRC_IF_NOT_SKIPPED:
                    logger.debug(f'Skipping MV for node ({sn_src.spid}): it matched the skip condition '
                                 f'and policy={mv_policy.name} for name="{name_src}"')
                elif mv_policy == SrcNodeMovePolicy.DELETE_SRC_ALWAYS:
                    logger.debug(f'Adding RM op for src node ({sn_src.spid}) despite not making changes to dst,'
                                 f' due to policy={mv_policy.name} for name="{name_src}"')
                    self.left_side.add_node_and_new_op(op_type=UserOpType.RM, sn_src=sn_src)
                else:
                    raise RuntimeError(f'Unrecognized SrcNodeMovePolicy: {mv_policy}')
            else:
                logger.debug(f'Skipping node ({sn_src.spid}): it matched the skip condition for name="{name_src}"')
            return True
        else:
            # False -> Do not skip
            if TRACE_ENABLED:
                logger.debug(f'Node {sn_dst_conflicting.spid} did not match the skip condition for conflict name="{name_src}"')
            return False

    def _handle_no_conflicts_found(self, dd_meta: TransferMeta, sn_src: SPIDNodePair, name_new_dst: Optional[str] = None):
        """COPY or MOVE where target does not already exist. Source node can either be a file or dir (in which case all its descendants will
        also be handled.
        The optional "name_new_dst" param, if supplied, will rename the target."""
        if sn_src.node.is_dir():
            # Need to get all the nodes in its whole subtree and add them individually:
            list_sn_subtree: List[SPIDNodePair] = self.backend.cacheman.get_subtree_bfs_sn_list(sn_src.node.node_identifier)
            logger.debug(f'Unpacking subtree with {len(list_sn_subtree)} nodes for {dd_meta.op_type.name}...')

            for sn_src_descendent in list_sn_subtree:
                dst_path = self._change_base_path(orig_target_path=sn_src_descendent.spid.get_single_path(), orig_base=sn_src,
                                                  new_base=dd_meta.sn_dst_parent, new_target_name=name_new_dst)
                sn_dst_descendent: SPIDNodePair = self.right_side.migrate_single_node_to_this_side(sn_src_descendent, dst_path)

                if sn_src_descendent.node.is_dir():
                    if dd_meta.drag_op == DragOperation.MOVE:
                        # When all nodes in a dir have been moved, the src dir itself should be deleted.
                        # TODO: test MOVE of a subtree. Do we need to add this in reverse?
                        self.left_side.add_node_and_new_op(op_type=UserOpType.RM, sn_src=sn_src_descendent)

                    # Add explicit MKDIR here, so that we don't omit empty dirs
                    self.right_side.add_node_and_new_op(op_type=UserOpType.MKDIR, sn_src=sn_dst_descendent)
                else:
                    # this will add any missing ancestors, and populate the parent list if applicable:
                    self.right_side.add_node_and_new_op(op_type=dd_meta.op_type, sn_src=sn_src_descendent, sn_dst=sn_dst_descendent)
        else:
            # Src node is file: easy case:
            sn_dst: SPIDNodePair = self._migrate_sn_to_right(sn_src, dd_meta.sn_dst_parent, name_new_dst)
            self.right_side.add_node_and_new_op(op_type=dd_meta.op_type, sn_src=sn_src, sn_dst=sn_dst)

    def _migrate_sn_to_right(self, sn_src: SPIDNodePair, sn_dst_parent: SPIDNodePair, sn_dst_name: Optional[str] = None) -> SPIDNodePair:
        # note: sn_dst_parent should be on right side
        if not sn_dst_name:
            sn_dst_name = sn_src.node.name
        dst_path = os.path.join(sn_dst_parent.spid.get_single_path(), sn_dst_name)
        return self.right_side.migrate_single_node_to_this_side(sn_src, dst_path)

    def _get_dict_of_name_to_child_list(self, parent_spid: SinglePathNodeIdentifier, tree_id: TreeID):
        dict_name_to_list_sn: Dict[str, List[SPIDNodePair]] = {}
        for existing_sn in self.backend.cacheman.get_child_list(parent_spid, tree_id=tree_id):
            name = existing_sn.node.name
            entry = dict_name_to_list_sn.get(name, [])
            if not entry:
                dict_name_to_list_sn[name] = entry
            entry.append(existing_sn)
        return dict_name_to_list_sn
