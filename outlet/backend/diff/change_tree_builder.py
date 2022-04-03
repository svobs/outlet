import logging
import os
import pathlib
from collections import deque
from typing import Deque, Dict, List, Optional

from backend.display_tree.change_tree import ChangeTree
from constants import TrashStatus, TreeID, TreeType
from logging_constants import DIFF_DEBUG_ENABLED, SUPER_DEBUG_ENABLED
from model.display_tree.display_tree import DisplayTreeUiState
from model.node.gdrive_node import GDriveFile, GDriveFolder, GDriveNode
from model.node.local_disk_node import LocalDirNode, LocalFileNode
from model.node.node import Node, SPIDNodePair
from model.node_identifier import GDriveIdentifier, LocalNodeIdentifier, SinglePathNodeIdentifier
from model.uid import UID
from model.user_op import UserOp, UserOpType
from util import file_util
from util.local_file_util import LocalFileUtil

logger = logging.getLogger(__name__)


class ChangeTreeBuilder:
    """
    ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
    CLASS ChangeTreeBuilder

    Encapsulates a ChangeTree which is being built, which itself represents a group of operations on a given tree (the 'source tree') organized
    by their dependencies on each other.
    ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼
    """

    def __init__(self, backend, change_tree: ChangeTree, batch_uid: UID, tree_id_src: Optional[str]):
        self.backend = backend
        self.change_tree: ChangeTree = change_tree
        self._batch_uid: UID = batch_uid
        if not self._batch_uid:
            self._batch_uid: UID = self.backend.uid_generator.next_uid()
        self.tree_id_src: Optional[TreeID] = tree_id_src  # need to keep track of this for certain external processes
        self._dict_added_dirs_by_path: Dict[str, SPIDNodePair] = {}

    @property
    def tree_id(self) -> TreeID:
        return self.change_tree.tree_id

    @property
    def batch_uid(self) -> UID:
        return self._batch_uid

    @property
    def root_sn(self) -> SPIDNodePair:
        return self.change_tree.get_root_sn()

    def get_root_sn(self) -> SPIDNodePair:
        return self.change_tree.get_root_sn()

    def _build_new_op(self, src_node: Node, dst_node: Node, op_type: UserOpType) -> UserOp:
        return UserOp(op_uid=self.backend.uid_generator.next_uid(), batch_uid=self._batch_uid, op_type=op_type, src_node=src_node, dst_node=dst_node)

    def add_new_compound_op_and_target_sn_to_tree(self, op_type_list: List[UserOpType], sn_src: SPIDNodePair, sn_dst: SPIDNodePair = None):
        """Adds a node to the ChangeTree (dst_node; unless dst_node is None, in which case it will use src_node), and also adds a UserOp
        of the given type"""

        if sn_dst:
            target_sn = sn_dst
            dst_node = sn_dst.node
        else:
            target_sn = sn_src
            dst_node = None

        op_list: List[UserOp] = []
        for op_type in op_type_list:
            new_op = self._build_new_op(src_node=sn_src.node, dst_node=dst_node, op_type=op_type)
            op_list.append(new_op)

        if SUPER_DEBUG_ENABLED:
            logger.debug(f'[{self.change_tree.tree_id}] Created {len(op_list)} new UserOp(s) for tgt {target_sn.spid}: {op_list}')

        self.change_tree.add_op_list_with_target_sn(target_sn, op_list)

    def add_new_op_and_target_sn_to_tree(self, op_type: UserOpType, sn_src: SPIDNodePair, sn_dst: SPIDNodePair = None):
        """Adds a node to the ChangeTree (dst_node; unless dst_node is None, in which case it will use src_node), and also adds a UserOp
        of the given type"""

        self.add_new_compound_op_and_target_sn_to_tree([op_type], sn_src, sn_dst)

    def derive_relative_path(self, spid: SinglePathNodeIdentifier) -> str:
        return file_util.strip_root(spid.get_single_path(), self.root_sn.spid.get_single_path())

    def migrate_single_node_to_this_side(self, sn_src: SPIDNodePair, dst_path: str) -> SPIDNodePair:
        dst_device_uid: UID = self.root_sn.spid.device_uid
        dst_tree_type: TreeType = self.root_sn.spid.tree_type
        assert not dst_path.endswith('/')

        if DIFF_DEBUG_ENABLED:
            logger.debug(f'[{self.change_tree.tree_id}] Migrating single node: {sn_src.spid} -> "{dst_path}"')

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
                if self._is_same_signature_and_name_for_all(existing_dst_node_list):
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
        nid = self.backend.node_identifier_factory.build_node_id(node_uid=dst_node_uid, device_uid=dst_device_uid, path_list=[dst_path])

        node_src: Node = sn_src.node
        if dst_tree_type == TreeType.LOCAL_DISK:
            assert isinstance(nid, LocalNodeIdentifier)
            dst_parent_path = self.backend.cacheman.derive_parent_path(dst_path)
            dst_parent_uid: UID = self.backend.cacheman.get_uid_for_local_path(dst_parent_path)
            if node_src.is_dir():
                node_dst: Node = LocalDirNode(nid, dst_parent_uid, trashed=TrashStatus.NOT_TRASHED, is_live=False,
                                              sync_ts=None, create_ts=None, modify_ts=None, change_ts=None,
                                              all_children_fetched=True)
            else:
                assert isinstance(node_src, LocalFileNode)
                node_dst: Node = LocalFileNode(nid, dst_parent_uid, node_src.content_meta, size_bytes=node_src.get_size_bytes(),
                                               sync_ts=None, create_ts=None, modify_ts=None, change_ts=None,
                                               trashed=TrashStatus.NOT_TRASHED, is_live=False)
        elif dst_tree_type == TreeType.GDRIVE:
            if node_src.is_dir():
                node_dst: Node = GDriveFolder(node_identifier=nid, goog_id=dst_node_goog_id, node_name=node_src.name,
                                              trashed=TrashStatus.NOT_TRASHED, create_ts=None, modify_ts=None, owner_uid=None, drive_id=None,
                                              is_shared=False, shared_by_user_uid=None, sync_ts=None, all_children_fetched=True)
            else:
                assert isinstance(node_src, GDriveFile)
                node_dst: Node = GDriveFile(node_identifier=nid, goog_id=dst_node_goog_id, node_name=os.path.basename(dst_path),
                                            mime_type_uid=None, trashed=TrashStatus.NOT_TRASHED, drive_id=None, version=None,
                                            content_meta=node_src.content_meta, size_bytes=node_src.get_size_bytes(),
                                            is_shared=False, create_ts=None, modify_ts=None, owner_uid=None, shared_by_user_uid=None, sync_ts=None)
        else:
            raise RuntimeError(f"Cannot create file node for tree type: {dst_tree_type} (node_identifier={nid}")

        spid = self.backend.node_identifier_factory.build_spid(node_uid=dst_node_uid, device_uid=dst_device_uid, single_path=dst_path)
        sn_dst: SPIDNodePair = SPIDNodePair(spid, node_dst)

        # Dst nodes may need some missing ancestors to be created first:
        self.add_needed_ancestors(sn_dst)

        if DIFF_DEBUG_ENABLED:
            logger.debug(f'[{self.change_tree.tree_id}] Done migrating single node: {sn_src.spid} -> {spid}')
        return sn_dst

    @staticmethod
    def _is_same_signature_and_name_for_all(existing_node_list: List[Node]) -> bool:
        first_node: Node = existing_node_list[0]
        for node in existing_node_list[1:]:
            assert isinstance(node, GDriveNode)
            if node.name != first_node.name or not node.is_signature_equal(first_node):
                return False
        return True

    def add_op_list_with_target_sn_and_ancestors(self, sn: SPIDNodePair, op_list: List[UserOp]):
        self.add_needed_ancestors(sn)
        self.change_tree.add_op_list_with_target_sn(sn, op_list)

    def add_needed_ancestors(self, new_sn: SPIDNodePair):
        """Determines what ancestor directories need to be created, and appends them to the op tree (as well as ops for them).
        Appends the migrated node as well, but the op for it is omitted so that the caller can provide its own."""

        if new_sn.node.is_dir():
            self._dict_added_dirs_by_path[new_sn.spid.get_single_path()] = new_sn

        # Lowest node in the stack will always be orig node. Stack size > 1 iff need to add parent folders
        ancestor_stack: Deque[SPIDNodePair] = self._generate_missing_ancestor_nodes(new_sn)
        while len(ancestor_stack) > 0:
            ancestor_sn: SPIDNodePair = ancestor_stack.pop()
            # Create an accompanying MKDIR action which will create the new folder/dir
            self.add_new_op_and_target_sn_to_tree(op_type=UserOpType.MKDIR, sn_src=ancestor_sn)

    def _generate_missing_ancestor_nodes(self, new_sn: SPIDNodePair) -> Deque[SPIDNodePair]:
        tree_type: int = self.backend.cacheman.get_tree_type_for_device_uid(new_sn.spid.device_uid)
        device_uid: UID = new_sn.spid.device_uid
        ancestor_stack: Deque[SPIDNodePair] = deque()
        stop_at_path: str = self.root_sn.spid.get_single_path()

        child_path: str = new_sn.spid.get_single_path()
        child: Node = new_sn.node

        assert not pathlib.PurePosixPath(stop_at_path).is_relative_to(child_path), f'Should not be inserting at or above root: {child_path}'
        if DIFF_DEBUG_ENABLED:
            logger.debug(f'[{self.change_tree.tree_id}] Checking for missing ancestors between node with path: "{child_path}" '
                         f'and tree root "{stop_at_path}"')

        # Determine ancestors:
        while True:
            parent_path = str(pathlib.Path(child_path).parent)

            if parent_path == stop_at_path:
                if DIFF_DEBUG_ENABLED:
                    logger.debug(f'[{self.change_tree.tree_id}] Parent of new node has the same path as tree root; no more ancestors to create')
                child.set_parent_uids(self.root_sn.node.uid)
                break

            # AddedFolder already generated and added?
            prev_added_ancestor: Optional[SPIDNodePair] = self._dict_added_dirs_by_path.get(parent_path, None)
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

            spid = self.backend.node_identifier_factory.build_spid(node_uid=new_ancestor_node.uid, device_uid=device_uid,
                                                                   single_path=parent_path)
            new_ancestor_sn: SPIDNodePair = SPIDNodePair(spid, new_ancestor_node)
            self._dict_added_dirs_by_path[parent_path] = new_ancestor_sn
            ancestor_stack.append(new_ancestor_sn)

            child.set_parent_uids(new_ancestor_sn.node.uid)

            child_path = parent_path
            child = new_ancestor_sn.node

        return ancestor_stack


class TwoTreeChangeBuilder:
    """
    ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
    CLASS TwoTreeChangeBuilder
    Base class for building list of UserOps
    ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼
    """

    def __init__(self, backend, left_tree_root_sn: SPIDNodePair, right_tree_root_sn: SPIDNodePair,
                 tree_id_left_src: TreeID, tree_id_right_src: TreeID,
                 tree_id_left: TreeID = 'ChangeTreeLeft', tree_id_right: TreeID = 'ChangeTreeRight'):
        self.backend = backend
        self.local_file_util = LocalFileUtil(self.backend.cacheman)
        # both trees share batch_uid:
        batch_uid: UID = self.backend.uid_generator.next_uid()

        change_tree_left = self._new_change_tree(self.backend, tree_id_left, left_tree_root_sn)
        change_tree_right = self._new_change_tree(self.backend, tree_id_right, right_tree_root_sn)
        self.left_side = ChangeTreeBuilder(backend, change_tree_left, batch_uid, tree_id_left_src)
        self.right_side = ChangeTreeBuilder(backend, change_tree_right, batch_uid, tree_id_right_src)

    @staticmethod
    def _new_change_tree(backend, change_tree_id: TreeID, root_sn: SPIDNodePair) -> ChangeTree:
        change_tree_state = DisplayTreeUiState.create_change_tree_state(change_tree_id, root_sn)
        return ChangeTree(backend, change_tree_state)

    @staticmethod
    def _change_base_path(orig_target_path: str, orig_base_path: str, new_base_path: str, new_target_name: Optional[str] = None) -> str:
        dst_rel_path: str = file_util.strip_root(orig_target_path, orig_base_path)
        if SUPER_DEBUG_ENABLED:
            logger.debug(f'change_base_path(): new_base_path="{new_base_path}", dst_rel_path1="{dst_rel_path}"')

        if new_target_name:
            # target is being renamed
            orig_target_name = os.path.basename(orig_target_path)
            dst_rel_path_minus_name = dst_rel_path.removesuffix(orig_target_name)
            assert dst_rel_path_minus_name != dst_rel_path, f'Should not be equal: "{dst_rel_path_minus_name}" and "{orig_target_name}"'
            dst_rel_path = dst_rel_path_minus_name + new_target_name

            if SUPER_DEBUG_ENABLED:
                logger.debug(f'change_base_path(): new_target_name="{new_target_name}", dst_rel_path2="{dst_rel_path}"')

        if dst_rel_path:
            result = os.path.join(new_base_path, dst_rel_path)
        else:
            # do not use os.path.join() here, or we will end up with a '/' at the end which we don't want
            result = new_base_path
        logger.debug(f'change_base_path(): OrigTarget="{orig_target_path}", NewTarget="{result}"')
        return result

    @staticmethod
    def _change_tree_path(src_side: ChangeTreeBuilder, dst_side: ChangeTreeBuilder,
                          spid_from_src_tree: SinglePathNodeIdentifier, new_target_name: Optional[str] = None) -> str:
        return TwoTreeChangeBuilder._change_base_path(orig_target_path=spid_from_src_tree.get_single_path(),
                                                      orig_base_path=src_side.root_sn.spid.get_single_path(),
                                                      new_base_path=dst_side.root_sn.spid.get_single_path(), new_target_name=new_target_name)

    def migrate_rel_path_to_right_tree(self, spid_left: SinglePathNodeIdentifier) -> str:
        return TwoTreeChangeBuilder._change_tree_path(self.left_side, self.right_side, spid_left)

    def migrate_rel_path_to_left_tree(self, spid_right: SinglePathNodeIdentifier) -> str:
        return TwoTreeChangeBuilder._change_tree_path(self.right_side, self.left_side, spid_right)

    def _migrate_node_to_right(self, sn_s: SPIDNodePair) -> SPIDNodePair:
        dst_path = self.migrate_rel_path_to_right_tree(sn_s.spid)
        return self.right_side.migrate_single_node_to_this_side(sn_s, dst_path)

    def _migrate_node_to_left(self, sn_r: SPIDNodePair) -> SPIDNodePair:
        dst_path = self.migrate_rel_path_to_left_tree(sn_r.spid)
        return self.left_side.migrate_single_node_to_this_side(sn_r, dst_path)

    def append_mv_op_r_to_r(self, sn_s: SPIDNodePair, sn_r: SPIDNodePair):
        """Make a dst node which will rename a file within the right tree to match the relative path of the file on the left"""
        self.right_side.add_new_op_and_target_sn_to_tree(op_type=UserOpType.MV, sn_src=sn_r, sn_dst=self._migrate_node_to_right(sn_s))

    def append_mv_op_s_to_s(self, sn_s: SPIDNodePair, sn_r: SPIDNodePair):
        """Make a FileToMove node which will rename a file within the left tree to match the relative path of the file on right"""
        self.left_side.add_new_op_and_target_sn_to_tree(op_type=UserOpType.MV, sn_src=sn_s, sn_dst=self._migrate_node_to_left(sn_r))

    def append_cp_op_s_to_r(self, sn_s: SPIDNodePair):
        """COPY: Left -> Right. (Node on Right does not yet exist)"""
        self.right_side.add_new_op_and_target_sn_to_tree(op_type=UserOpType.CP, sn_src=sn_s, sn_dst=self._migrate_node_to_right(sn_s))

    def append_cp_op_r_to_s(self, sn_r: SPIDNodePair):
        """COPY: Left <- Right. (Node on Left does not yet exist)"""
        self.left_side.add_new_op_and_target_sn_to_tree(op_type=UserOpType.CP, sn_src=sn_r, sn_dst=self._migrate_node_to_left(sn_r))

    def append_up_op_s_to_r(self, sn_s: SPIDNodePair, sn_r: SPIDNodePair):
        """UPDATE: Left -> Right. Both nodes already exist, but one will overwrite the other"""
        self.right_side.add_new_op_and_target_sn_to_tree(op_type=UserOpType.CP_ONTO, sn_src=sn_s, sn_dst=sn_r)

    def append_up_op_r_to_s(self, sn_s: SPIDNodePair, sn_r: SPIDNodePair):
        """UPDATE: Left <- Right. Both nodes already exist, but one will overwrite the other"""
        self.left_side.add_new_op_and_target_sn_to_tree(op_type=UserOpType.CP_ONTO, sn_src=sn_r, sn_dst=sn_s)
