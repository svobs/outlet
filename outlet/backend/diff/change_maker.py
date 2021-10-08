import collections
import logging
import os
import pathlib
from collections import deque
from typing import Callable, Deque, Dict, Iterable, List, Optional, Tuple

from backend.display_tree.change_tree import ChangeTree
from constants import DIFF_DEBUG_ENABLED, DirConflictPolicy, DragOperation, FileConflictPolicy, ReplaceDirWithFilePolicy, SrcNodeMovePolicy, \
    SUPER_DEBUG_ENABLED, \
    TRACE_ENABLED, TrashStatus, TreeID, TreeType
from model.display_tree.display_tree import DisplayTreeUiState
from model.node.gdrive_node import GDriveFile, GDriveFolder, GDriveNode
from model.node.local_disk_node import LocalFileNode
from model.node.node import Node, SPIDNodePair
from model.node_identifier import GDriveIdentifier, LocalNodeIdentifier, SinglePathNodeIdentifier
from model.uid import UID
from model.user_op import UserOp, UserOpType
from util import file_util

logger = logging.getLogger(__name__)


class OneSide:
    """
    ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
    CLASS OneSide

    Internal class, used only by ChangeMaker and its descendants.
    ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼
    """

    def __init__(self, backend, change_tree_id: TreeID, root_sn: SPIDNodePair, batch_uid: UID, tree_id_src: Optional[str]):
        self.backend = backend
        change_tree_state = DisplayTreeUiState.create_change_tree_state(change_tree_id, root_sn)
        self.change_tree: ChangeTree = ChangeTree(backend, change_tree_state)
        self._batch_uid: UID = batch_uid
        if not self._batch_uid:
            self._batch_uid: UID = self.backend.uid_generator.next_uid()
        self.tree_id_src: Optional[TreeID] = tree_id_src
        self._added_folders: Dict[str, SPIDNodePair] = {}

    @property
    def tree_id(self) -> TreeID:
        return self.change_tree.tree_id

    @property
    def root_sn(self) -> SPIDNodePair:
        return self.change_tree.get_root_sn()

    def get_root_sn(self) -> SPIDNodePair:
        return self.change_tree.get_root_sn()

    def add_node_and_new_op(self, op_type: UserOpType, sn_src: SPIDNodePair, sn_dst: SPIDNodePair = None):
        """Adds a node to the op tree (dst_node; unless dst_node is None, in which case it will use src_node), and also adds a UserOp
        of the given type"""

        if sn_dst:
            target_sn = sn_dst
            dst_node = sn_dst.node
        else:
            target_sn = sn_src
            dst_node = None

        op: UserOp = UserOp(op_uid=self.backend.uid_generator.next_uid(), batch_uid=self._batch_uid, op_type=op_type,
                            src_node=sn_src.node, dst_node=dst_node)
        if DIFF_DEBUG_ENABLED:
            logger.debug(f'[{self.change_tree.tree_id}] Created new UserOp(uid={op.op_uid} op_type={op.op_type.name}). '
                         f'Adding to ChangeTree along with node: {target_sn.spid}')

        self.change_tree.add_sn_and_op(target_sn, op)

    def derive_relative_path(self, spid: SinglePathNodeIdentifier) -> str:
        return file_util.strip_root(spid.get_single_path(), self.root_sn.spid.get_single_path())

    def migrate_single_node_to_this_side(self, sn_src: SPIDNodePair, dst_path: str) -> SPIDNodePair:
        dst_device_uid: UID = self.root_sn.spid.device_uid
        dst_tree_type: TreeType = self.root_sn.spid.tree_type

        # First figure out UID and other identifying info for dst node.
        if dst_tree_type == TreeType.LOCAL_DISK:
            dst_node_uid: UID = self.backend.cacheman.get_uid_for_local_path(dst_path)  # always returns something
            dst_node_goog_id = None  # N/A, but need to make compiler happy
        elif dst_tree_type == TreeType.GDRIVE:
            existing_dst_node_list = self.backend.cacheman.get_node_list_for_path_list([dst_path], device_uid=dst_device_uid)
            if len(existing_dst_node_list) == 1:
                existing_node = existing_dst_node_list[0]
                # If a single node is already there with given name, use its identification; we will overwrite its content with a new version
                dst_node_uid = existing_node.uid
                dst_node_goog_id = existing_node.goog_id
            elif len(existing_dst_node_list) > 1:
                if self._is_same_md5_and_name_for_all(existing_dst_node_list):
                    logger.warning(f'Found {len(existing_dst_node_list)} identical nodes already present at at GDrive dst path '
                                   f'("{repr(dst_path)}"). Will overwrite all starting with UID {existing_dst_node_list[0].uid}')
                    existing_node = existing_dst_node_list[0]
                    dst_node_uid = existing_node.uid
                    dst_node_goog_id = existing_node.goog_id
                else:
                    # FIXME: what to do in this case? Perhaps collect these errors and display them all to the user.
                    # TODO: Also do an audit for this issue as soon as all the user's GDrive metadata is downloaded
                    raise RuntimeError(f'Found multiple non-identical nodes ({len(existing_dst_node_list)}) already present at '
                                       f'GDrive dst path ("{repr(dst_path)}"). Cannot proceed')
            else:
                # Not exist: assign new UID. We will later associate this with a goog_id once it's made existent
                dst_node_uid = self.backend.uid_generator.next_uid()
                dst_node_goog_id = None
        else:
            raise RuntimeError(f'Invalid tree_type: {dst_tree_type}')

        # Now build the node:
        node_identifier = self.backend.node_identifier_factory.for_values(tree_type=dst_tree_type, path_list=[dst_path], uid=dst_node_uid,
                                                                          device_uid=dst_device_uid)
        src_node: Node = sn_src.node
        if dst_tree_type == TreeType.LOCAL_DISK:
            assert isinstance(node_identifier, LocalNodeIdentifier)
            dst_parent_path = self.backend.cacheman.derive_parent_path(dst_path)
            dst_parent_uid: UID = self.backend.cacheman.get_uid_for_local_path(dst_parent_path)
            dst_node: Node = LocalFileNode(node_identifier, dst_parent_uid, src_node.md5, src_node.sha256, src_node.get_size_bytes(),
                                           sync_ts=None, modify_ts=None, change_ts=None, trashed=TrashStatus.NOT_TRASHED, is_live=False)
        elif dst_tree_type == TreeType.GDRIVE:
            dst_node: Node = GDriveFile(node_identifier=node_identifier, goog_id=dst_node_goog_id, node_name=os.path.basename(dst_path),
                                        mime_type_uid=None, trashed=TrashStatus.NOT_TRASHED, drive_id=None, version=None, md5=src_node.md5,
                                        is_shared=False, create_ts=None, modify_ts=None, size_bytes=src_node.get_size_bytes(),
                                        owner_uid=None, shared_by_user_uid=None, sync_ts=None)
        else:
            raise RuntimeError(f"Cannot create file node for tree type: {dst_tree_type} (node_identifier={node_identifier}")

        spid = self.backend.node_identifier_factory.for_values(tree_type=dst_tree_type, path_list=[dst_path], uid=dst_node_uid,
                                                               device_uid=dst_device_uid, must_be_single_path=True)
        sn_dst: SPIDNodePair = SPIDNodePair(spid, dst_node)

        # ANCESTORS:
        self._add_needed_ancestors(sn_dst)

        if DIFF_DEBUG_ENABLED:
            logger.debug(f'[{self.change_tree.tree_id}] Migrated single node: {sn_src.spid} -> {spid}')
        return sn_dst

    @staticmethod
    def _is_same_md5_and_name_for_all(existing_node_list: List[Node]) -> bool:
        first_node: Node = existing_node_list[0]
        for node in existing_node_list[1:]:
            assert isinstance(node, GDriveNode)
            if node.name != first_node.name or node.md5 != first_node.md5:
                return False
        return True

    def _add_needed_ancestors(self, new_sn: SPIDNodePair):
        """Determines what ancestor directories need to be created, and appends them to the op tree (as well as ops for them).
        Appends the migrated node as well, but the op for it is omitted so that the caller can provide its own."""

        # Lowest node in the stack will always be orig node. Stack size > 1 iff need to add parent folders
        ancestor_stack: Deque[SPIDNodePair] = self._generate_missing_ancestor_nodes(new_sn)
        while len(ancestor_stack) > 0:
            ancestor_sn: SPIDNodePair = ancestor_stack.pop()
            # Create an accompanying MKDIR action which will create the new folder/dir
            self.add_node_and_new_op(op_type=UserOpType.MKDIR, sn_src=ancestor_sn)

    def _generate_missing_ancestor_nodes(self, new_sn: SPIDNodePair) -> Deque[SPIDNodePair]:
        tree_type: int = new_sn.spid.tree_type
        device_uid: UID = new_sn.spid.device_uid
        ancestor_stack: Deque[SPIDNodePair] = deque()

        child_path: str = new_sn.spid.get_single_path()
        child: Node = new_sn.node
        if DIFF_DEBUG_ENABLED:
            logger.debug(f'[{self.change_tree.tree_id}] Checking for missing ancestors for node with path: "{child_path}"')

        # Determine ancestors:
        while True:
            parent_path = str(pathlib.Path(child_path).parent)

            if parent_path == self.root_sn.spid.get_single_path():
                if DIFF_DEBUG_ENABLED:
                    logger.debug(f'[{self.change_tree.tree_id}] Parent of new node has the same path as tree root; no more ancestors to create')
                child.set_parent_uids(self.root_sn.node.uid)
                break

            # AddedFolder already generated and added?
            prev_added_ancestor: Optional[SPIDNodePair] = self._added_folders.get(parent_path, None)
            if prev_added_ancestor:
                child.set_parent_uids(prev_added_ancestor.node.uid)
                break

            # Folder already existed in original tree?
            existing_ancestor_list: List[Node] = self.backend.cacheman.get_node_list_for_path_list([parent_path], device_uid)
            if existing_ancestor_list:
                # Add all parents which match the path, even if they are duplicates or do not yet exist (i.e., pending ops)
                child.set_parent_uids(list(map(lambda x: x.uid, existing_ancestor_list)))
                break

            # Need to create ancestor
            if tree_type == TreeType.GDRIVE:
                if DIFF_DEBUG_ENABLED:
                    logger.debug(f'[{self.change_tree.tree_id}] Creating GoogFolderToAdd for {parent_path}')
                new_ancestor_uid = self.backend.uid_generator.next_uid()
                folder_name = os.path.basename(parent_path)
                new_ancestor_node = GDriveFolder(GDriveIdentifier(uid=new_ancestor_uid, device_uid=device_uid, path_list=parent_path),
                                                 goog_id=None, node_name=folder_name,
                                                 trashed=TrashStatus.NOT_TRASHED, create_ts=None, modify_ts=None, owner_uid=None,
                                                 drive_id=None, is_shared=False, shared_by_user_uid=None, sync_ts=None, all_children_fetched=True)
            elif tree_type == TreeType.LOCAL_DISK:
                if DIFF_DEBUG_ENABLED:
                    logger.debug(f'[{self.change_tree.tree_id}] Creating LocalDirToAdd for {parent_path}')
                new_ancestor_node = self.backend.cacheman.build_local_dir_node(parent_path, is_live=False, all_children_fetched=True)
            else:
                raise RuntimeError(f'Invalid tree type: {tree_type} for node {new_sn.node}')

            spid = self.backend.node_identifier_factory.for_values(uid=new_ancestor_node.uid, device_uid=device_uid, tree_type=tree_type,
                                                                   path_list=parent_path, must_be_single_path=True)
            new_ancestor_sn: SPIDNodePair = SPIDNodePair(spid, new_ancestor_node)
            self._added_folders[parent_path] = new_ancestor_sn
            ancestor_stack.append(new_ancestor_sn)

            child.set_parent_uids(new_ancestor_sn.node.uid)

            child_path = parent_path
            child = new_ancestor_sn.node

        return ancestor_stack


class DragAndDropMeta:
    def __init__(self, drag_op: DragOperation, dir_conflict_policy: DirConflictPolicy, file_conflict_policy: FileConflictPolicy,
                 dst_existing_sn_dict: Dict[str, List[SPIDNodePair]]):

        self.drag_op: DragOperation = drag_op
        self.dir_conflict_policy: DirConflictPolicy = dir_conflict_policy
        self.file_conflict_policy: FileConflictPolicy = file_conflict_policy

        # hard-code these for now
        self.src_node_move_policy: SrcNodeMovePolicy = SrcNodeMovePolicy.DELETE_SRC_IF_NOT_SKIPPED
        self.replace_dir_with_file_policy: ReplaceDirWithFilePolicy = ReplaceDirWithFilePolicy.FAIL

        self.op_type = UserOpType.CP if drag_op == DragOperation.COPY else UserOpType.MV

        # This is a Dict containing all the child nodes of the destination parent, indexed by node name.
        self.dst_existing_sn_dict: Dict[str, List[SPIDNodePair]] = dst_existing_sn_dict


class ChangeMaker:
    """
    ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
    CLASS ChangeMaker
    ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼
    """

    def __init__(self, backend, left_tree_root_sn: SPIDNodePair, right_tree_root_sn: SPIDNodePair,
                 tree_id_left_src: TreeID, tree_id_right_src: TreeID,
                 tree_id_left: TreeID = 'ChangeTreeLeft', tree_id_right: TreeID = 'ChangeTreeRight'):

        self.backend = backend
        # both trees share batch_uid:
        batch_uid: UID = self.backend.uid_generator.next_uid()

        self.left_side = OneSide(backend, tree_id_left, left_tree_root_sn, batch_uid, tree_id_left_src)
        self.right_side = OneSide(backend, tree_id_right, right_tree_root_sn, batch_uid, tree_id_right_src)

    def drag_nodes_left_to_right(self, sn_src_list: List[SPIDNodePair], sn_dst_parent: SPIDNodePair,
                                 drag_op: DragOperation, dir_conflict_policy: DirConflictPolicy, file_conflict_policy: FileConflictPolicy):
        """Populates the destination parent in "change_tree_right" with a subset of the given source nodes
        based on the given DragOperation and policies. NOTE: this may actually result in UserOps created in the left
        ChangeTree (in particular RM), so use the get_all_op_list() method to get the complete list of resulting UserOps."""

        assert sn_dst_parent and sn_dst_parent.node.is_dir()
        if not (drag_op == DragOperation.COPY or drag_op == DragOperation.MOVE):
            raise RuntimeError(f'Unsupported DragOperation: {drag_op.name}')

        dst_existing_sn_dict: Dict[str, List[SPIDNodePair]] = self._get_name_child_list_dict(sn_dst_parent.spid, self.right_side.tree_id_src)
        dd_meta = DragAndDropMeta(drag_op, dir_conflict_policy, file_conflict_policy, dst_existing_sn_dict)

        logger.debug(f'Preparing {len(sn_src_list)} nodes for {drag_op.name}...')

        for sn_src in sn_src_list:
            # In general, we will throw an error rather than attempt to replace or merge more than 1 node with the same name
            conflicting_dst_sn_list: List[SPIDNodePair] = dst_existing_sn_dict.get(sn_src.node.name)

            if not conflicting_dst_sn_list:
                if SUPER_DEBUG_ENABLED:
                    logger.debug(f'Node "{sn_src.node.name}": no name conflicts found')
                self._handle_no_conflicts_found(dd_meta, sn_src, sn_dst_parent)

            else:  # Conflict(s)
                if sn_src.node.is_dir():
                    self._handle_dir_conflict(dd_meta, sn_src, sn_dst_parent, conflicting_dst_sn_list)
                else:
                    self._handle_file_conflict(dd_meta, sn_src, sn_dst_parent, conflicting_dst_sn_list)

    def _handle_dir_conflict(self, dd_meta: DragAndDropMeta, sn_src: SPIDNodePair, sn_dst_parent: SPIDNodePair, conflicting_dst_sn_list):
        src_name = sn_src.node.name
        policy = dd_meta.dir_conflict_policy
        has_multiple_name_conflicts: bool = len(conflicting_dst_sn_list) > 1

        if SUPER_DEBUG_ENABLED:
            logger.debug(f'Dir "{src_name}" has {len(conflicting_dst_sn_list)} conflicts: following policy {policy.name}')

        if policy == DirConflictPolicy.SKIP:
            # SKIP DIR: trivial
            pass
        elif policy == DirConflictPolicy.REPLACE:
            # REPLACE DIR
            if has_multiple_name_conflicts:
                raise RuntimeError(f'For folder "{src_name}": found {len(conflicting_dst_sn_list) > 1} items '
                                   f'at the destination with the same name, and cannot determine which to replace!')
            # TODO
            pass

        elif policy == DirConflictPolicy.RENAME:
            # RENAME DIR
            self._handle_rename(dd_meta, sn_src, sn_dst_parent, skip_condition_func=None)
        elif policy == DirConflictPolicy.MERGE:
            if has_multiple_name_conflicts:
                raise RuntimeError(f'For folder "{src_name}": found {len(conflicting_dst_sn_list) > 1} items '
                                   f'at the destination with the same name, and cannot determine which to merge with!')
            # TODO
            pass

        elif policy == DirConflictPolicy.PROMPT:
            # TODO
            pass

        else:
            raise RuntimeError(f'Unrecognized DirConflictPolicy: {policy}')

    def _handle_file_conflict(self, dd_meta: DragAndDropMeta, sn_src: SPIDNodePair, sn_dst_parent: SPIDNodePair, conflicting_dst_sn_list):
        src_name = sn_src.node.name
        policy = dd_meta.file_conflict_policy
        has_multiple_name_conflicts: bool = len(conflicting_dst_sn_list) > 1

        if SUPER_DEBUG_ENABLED:
            logger.debug(f'File "{src_name}" has {len(conflicting_dst_sn_list)} conflicts: following policy {policy.name}')

        if policy == FileConflictPolicy.SKIP:
            pass

        elif policy == FileConflictPolicy.REPLACE_ALWAYS:
            if has_multiple_name_conflicts:
                # Just fail: the user may not have considered this scenario.
                raise RuntimeError(f'For item "{src_name}": found {len(conflicting_dst_sn_list) > 1} items '
                                   f'at the destination with the same name, and cannot determine which to replace!')
            # TODO
            pass

        elif policy == FileConflictPolicy.REPLACE_IF_OLDER_AND_DIFFERENT:
            if has_multiple_name_conflicts:
                # Just fail: the user may not have considered this scenario.
                raise RuntimeError(f'For item "{src_name}": found {len(conflicting_dst_sn_list) > 1} items '
                                   f'at the destination with the same name, and cannot determine which to replace!')
            # TODO
            pass

        elif policy == FileConflictPolicy.RENAME_ALWAYS:
            # RENAME ALWAYS (FILE)
            self._handle_rename(dd_meta, sn_src, sn_dst_parent)

        elif policy == FileConflictPolicy.RENAME_IF_OLDER_AND_DIFFERENT:
            # RENAME IF CONTENT DIFFERS AND OLDER (FILE)
            self._handle_rename(dd_meta, sn_src, sn_dst_parent, skip_condition_func=self._is_same_content_and_not_older)

        elif policy == FileConflictPolicy.RENAME_IF_DIFFERENT:
            # RENAME IF CONTENT DIFFERS (FILE)
            self._handle_rename(dd_meta, sn_src, sn_dst_parent, skip_condition_func=self._is_same_content)

        elif policy == FileConflictPolicy.PROMPT:
            # TODO
            pass

        else:
            raise RuntimeError(f'Unrecognized FileConflictPolicy: {policy}')

    def _increment_node_name(self, node_name: str) -> str:
        # TODO


        raise NotImplementedError

    def _is_same_content(self, sn_src_conflict: SPIDNodePair, sn_dst_conflict: SPIDNodePair) -> bool:
        # TODO: ensure both nodes have signatures filled in (if local nodes)
        # TODO: decide how to handle GDrive non-file types which don't have signatures (e.g. shortcuts, Google Docs...)
        return sn_src_conflict.node.is_signature_match(sn_dst_conflict.node)

    def _is_same_content_and_not_older(self, sn_src_conflict: SPIDNodePair, sn_dst_conflict: SPIDNodePair) -> bool:
        # TODO: ensure both nodes have signatures filled in (if local nodes)
        # TODO: decide how to handle GDrive non-file types which don't have signatures (e.g. shortcuts, Google Docs...)
        if sn_src_conflict.node.modify_ts == 0 or sn_dst_conflict.node.modify_ts == 0:
            logger.error(f'One of these has modify_ts=0. Src: {sn_src_conflict.node}, Dst: {sn_dst_conflict.node}')
            raise RuntimeError(f'Cannot compare modification times: at least one node is missing modify_ts')
        return sn_src_conflict.node.is_signature_match(sn_dst_conflict.node) and sn_src_conflict.node.modify_ts <= sn_dst_conflict.node.modify_ts

    def _handle_rename(self, dd_meta: DragAndDropMeta, sn_src: SPIDNodePair, sn_dst_parent: SPIDNodePair,
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

        src_name: str = sn_src.node.name

        while True:
            conflicting_dst_sn_list = dd_meta.dst_existing_sn_dict.get(src_name)
            if not conflicting_dst_sn_list:
                break

            if skip_condition_func:
                for conflicting_dst_sn in conflicting_dst_sn_list:
                    if skip_condition_func(sn_src, conflicting_dst_sn):
                        if dd_meta.op_type == UserOpType.MV:
                            mv_policy = dd_meta.src_node_move_policy

                            if mv_policy == SrcNodeMovePolicy.DELETE_SRC_IF_NOT_SKIPPED:
                                logger.debug(f'Skipping MV for node ({sn_src.spid}): it matched the skip condition '
                                             f'and policy={mv_policy.name} for name="{src_name}"')
                            elif mv_policy == SrcNodeMovePolicy.DELETE_SRC_ALWAYS:
                                logger.debug(f'Adding RM op for src node ({sn_src.spid}) despite not making changes to dst,'
                                             f' due to policy={mv_policy.name} for name="{src_name}"')
                                self.left_side.add_node_and_new_op(op_type=UserOpType.RM, sn_src=sn_src)
                            else:
                                raise RuntimeError(f'Unrecognized SrcNodeMovePolicy: {mv_policy}')
                        else:
                            logger.debug(f'Skipping node ({sn_src.spid}): it matched the skip condition for name="{src_name}"')
                        return
                    else:
                        if TRACE_ENABLED:
                            logger.debug(f'Node {conflicting_dst_sn.spid} did not match the skip condition for conflict name="{src_name}"')

                if TRACE_ENABLED:
                    logger.debug(f'No conflicting nodes matched the skip condition for conflict name="{src_name}"')
            else:
                if TRACE_ENABLED:
                    logger.debug(f'No skip condition func supplied; assuming skip=never for conflict name="{src_name}"')
                # fall through

            # No match for skip condition, or no skip condition supplied
            src_name = self._increment_node_name(src_name)

        logger.debug(f'Renaming "{sn_src.spid.get_single_path()}" to "{src_name}"')
        self._handle_no_conflicts_found(dd_meta, sn_src, sn_dst_parent, new_dst_name=src_name)

    def _handle_no_conflicts_found(self, dd_meta: DragAndDropMeta, sn_src: SPIDNodePair, sn_dst_parent: SPIDNodePair, new_dst_name: Optional[str] = None):
        """COPY or MOVE where target does not already exist. Source node can either be a file or dir (in which case all its descendants will
        also be handled.
        The optional "new_dst_name" param, if supplied, will rename the target."""
        if sn_src.node.is_dir():
            # Need to get all the nodes in its whole subtree and add them individually:
            sn_list_subtree: List[SPIDNodePair] = self.backend.cacheman.get_subtree_bfs_sn_list(sn_src.node.node_identifier)
            logger.debug(f'Unpacking subtree with {len(sn_list_subtree)} nodes for {dd_meta.op_type.name}...')

            for sn_src_descendent in sn_list_subtree:
                dst_path = self._change_base_path(orig_target_path=sn_src_descendent.spid.get_single_path(), orig_base=sn_src, new_base=sn_dst_parent, new_target_name=new_dst_name)
                sn_dst_descendent: SPIDNodePair = self.right_side.migrate_single_node_to_this_side(sn_src_descendent, dst_path)

                if sn_src_descendent.node.is_dir() and sn_src == UserOpType.MV:
                    # TODO: test MOVE of a subtree. Do we need to add this in reverse?
                    # When all nodes in a dir have been moved, the src dir itself should be deleted.
                    self.left_side.add_node_and_new_op(op_type=UserOpType.RM, sn_src=sn_src_descendent)

                    # Add explicit MKDIR here, so that we don't omit empty dirs
                    self.right_side.add_node_and_new_op(op_type=UserOpType.MKDIR, sn_src=sn_dst_descendent)
                else:
                    # this will add any missing ancestors, and populate the parent list if applicable:
                    self.right_side.add_node_and_new_op(op_type=dd_meta.op_type, sn_src=sn_src_descendent, sn_dst=sn_dst_descendent)
        else:
            # Src node is file: easy case:
            sn_dst: SPIDNodePair = self._migrate_sn_to_right(sn_src, sn_dst_parent, new_dst_name)
            self.right_side.add_node_and_new_op(op_type=dd_meta.op_type, sn_src=sn_src, sn_dst=sn_dst)

    def _migrate_sn_to_right(self, sn_src: SPIDNodePair, sn_dst_parent: SPIDNodePair, sn_dst_name: Optional[str] = None) -> SPIDNodePair:
        # note: sn_dst_parent should be on right side
        if not sn_dst_name:
            sn_dst_name = sn_src.node.name
        dst_path = os.path.join(sn_dst_parent.spid.get_single_path(), sn_dst_name)
        return self.right_side.migrate_single_node_to_this_side(sn_src, dst_path)

    def _get_name_child_list_dict(self, parent_spid, tree_id):
        name_sn_list_dict: Dict[str, List[SPIDNodePair]] = {}
        for existing_sn in self.backend.cacheman.get_child_list(parent_spid, tree_id=tree_id):
            name = existing_sn.node.name
            entry = name_sn_list_dict.get(name, [])
            if not entry:
                name_sn_list_dict[name] = entry
            entry.append(existing_sn)
        return name_sn_list_dict

    def get_all_op_list(self) -> List[UserOp]:
        """Returns all UserOps, from both sides."""
        return [] + self.left_side.change_tree.get_op_list() + self.right_side.change_tree.get_op_list()

    def visit_each_file_for_subtree(self, subtree_root: SPIDNodePair, on_file_found: Callable[[SPIDNodePair], None], tree_id_src: TreeID):
        """Note: here, param "tree_id_src" indicates which tree_id from which to request nodes from CacheManager
        (via repeated calls to get_child_list()). This includes ChangeTrees, if tree_id_src resolves to a ChangeTree."""

        assert isinstance(subtree_root, Tuple), \
            f'Expected NamedTuple with SinglePathNodeIdentifier but got {type(subtree_root)}: {subtree_root}'
        queue: Deque[SPIDNodePair] = collections.deque()
        queue.append(subtree_root)

        count_total_nodes = 0
        count_file_nodes = 0

        while len(queue) > 0:
            sn: SPIDNodePair = queue.popleft()
            count_total_nodes += 1
            if not sn.node:
                raise RuntimeError(f'Node is null for: {sn.spid}')

            if sn.node.is_live():  # avoid pending op nodes
                if sn.node.is_dir():
                    child_sn_list = self.backend.cacheman.get_child_list(sn.spid, tree_id=tree_id_src)
                    if child_sn_list:
                        for child_sn in child_sn_list:
                            if child_sn.spid.get_single_path() not in child_sn.node.get_path_list():
                                logger.error(f'[{tree_id_src}] Bad SPIDNodePair found in children of {sn}, see following error')
                                raise RuntimeError(f'Invalid SPIDNodePair! Path from SPID ({child_sn.spid}) not found in node: {child_sn.node}')
                            queue.append(child_sn)
                else:
                    count_file_nodes += 1
                    on_file_found(sn)

        logger.debug(f'[{tree_id_src}] visit_each_file_for_subtree(): Visited {count_file_nodes} file nodes out of {count_total_nodes} total nodes')

    @staticmethod
    def _change_base_path(orig_target_path: str, orig_base: SPIDNodePair, new_base: SPIDNodePair, new_target_name: Optional[str] = None) -> str:
        dst_rel_path: str = file_util.strip_root(orig_target_path, orig_base.spid.get_single_parent_path())
        if new_target_name:
            # target is being renamed
            orig_target_name = os.path.basename(orig_target_path)
            dst_rel_path_minus_name = dst_rel_path.removesuffix(orig_target_name)
            assert dst_rel_path_minus_name != dst_rel_path, f'Not equal: "{dst_rel_path_minus_name}" and "{orig_target_name}"'
            dst_rel_path = dst_rel_path_minus_name + new_target_name
        return os.path.join(new_base.spid.get_single_path(), dst_rel_path)

    @staticmethod
    def _change_tree_path(src_side: OneSide, dst_side: OneSide, spid_from_src_tree: SinglePathNodeIdentifier) -> str:
        return ChangeMaker._change_base_path(orig_target_path=spid_from_src_tree.get_single_path(), orig_base=src_side.root_sn,
                                             new_base=dst_side.root_sn)

    def migrate_rel_path_to_right_tree(self, spid_left: SinglePathNodeIdentifier) -> str:
        return ChangeMaker._change_tree_path(self.left_side, self.right_side, spid_left)

    def migrate_rel_path_to_left_tree(self, spid_right: SinglePathNodeIdentifier) -> str:
        return ChangeMaker._change_tree_path(self.right_side, self.left_side, spid_right)

    def _migrate_node_to_right(self, sn_s: SPIDNodePair) -> SPIDNodePair:
        dst_path = self.migrate_rel_path_to_right_tree(sn_s.spid)
        return self.right_side.migrate_single_node_to_this_side(sn_s, dst_path)

    def _migrate_node_to_left(self, sn_r: SPIDNodePair) -> SPIDNodePair:
        dst_path = self.migrate_rel_path_to_left_tree(sn_r.spid)
        return self.left_side.migrate_single_node_to_this_side(sn_r, dst_path)

    def append_mv_op_r_to_r(self, sn_s: SPIDNodePair, sn_r: SPIDNodePair):
        """Make a dst node which will rename a file within the right tree to match the relative path of the file on the left"""
        self.right_side.add_node_and_new_op(op_type=UserOpType.MV, sn_src=sn_r, sn_dst=self._migrate_node_to_right(sn_s))

    def append_mv_op_s_to_s(self, sn_s: SPIDNodePair, sn_r: SPIDNodePair):
        """Make a FileToMove node which will rename a file within the left tree to match the relative path of the file on right"""
        self.left_side.add_node_and_new_op(op_type=UserOpType.MV, sn_src=sn_s, sn_dst=self._migrate_node_to_left(sn_r))

    def append_cp_op_s_to_r(self, sn_s: SPIDNodePair):
        """COPY: Left -> Right. (Node on Right does not yet exist)"""
        self.right_side.add_node_and_new_op(op_type=UserOpType.CP, sn_src=sn_s, sn_dst=self._migrate_node_to_right(sn_s))

    def append_cp_op_r_to_s(self, sn_r: SPIDNodePair):
        """COPY: Left <- Right. (Node on Left does not yet exist)"""
        self.left_side.add_node_and_new_op(op_type=UserOpType.CP, sn_src=sn_r, sn_dst=self._migrate_node_to_left(sn_r))

    def append_up_op_s_to_r(self, sn_s: SPIDNodePair, sn_r: SPIDNodePair):
        """UPDATE: Left -> Right. Both nodes already exist, but one will overwrite the other"""
        self.right_side.add_node_and_new_op(op_type=UserOpType.UP, sn_src=sn_s, sn_dst=sn_r)

    def append_up_op_r_to_s(self, sn_s: SPIDNodePair, sn_r: SPIDNodePair):
        """UPDATE: Left <- Right. Both nodes already exist, but one will overwrite the other"""
        self.left_side.add_node_and_new_op(op_type=UserOpType.UP, sn_src=sn_r, sn_dst=sn_s)
