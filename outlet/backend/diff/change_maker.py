import collections
import logging
import os
import pathlib
from collections import deque
from typing import Callable, Deque, Dict, List, Optional, Tuple

from backend.display_tree.change_tree import ChangeTree
from constants import SUPER_DEBUG_ENABLED, TrashStatus, TreeID, TreeType
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

    def __init__(self, change_tree, batch_uid: UID, tree_id_src: Optional[str]):
        self.backend = change_tree.backend
        self.change_tree = change_tree
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

    def add_op(self, op_type: UserOpType, src_sn: SPIDNodePair, dst_sn: SPIDNodePair = None):
        """Adds a node to the op tree (dst_node; unless dst_node is None, in which case it will use src_node), and also adds a UserOp
        of the given type"""

        if dst_sn:
            target_sn = dst_sn
            dst_node = dst_sn.node
        else:
            target_sn = src_sn
            dst_node = None

        op: UserOp = UserOp(op_uid=self.backend.uid_generator.next_uid(), batch_uid=self._batch_uid, op_type=op_type,
                            src_node=src_sn.node, dst_node=dst_node)
        if SUPER_DEBUG_ENABLED:
            logger.debug(f'[{self.change_tree.tree_id}] Created new UserOp(op_uid={op.op_uid} op_type={op.op_type})')

        self.change_tree.add_node(target_sn, op)

    def derive_relative_path(self, spid: SinglePathNodeIdentifier) -> str:
        return file_util.strip_root(spid.get_single_path(), self.root_sn.spid.get_single_path())

    def migrate_single_node_to_this_side(self, sn_src: SPIDNodePair, dst_path: str) -> SPIDNodePair:
        dst_device_uid: UID = self.root_sn.spid.device_uid
        dst_tree_type: TreeType = self.root_sn.spid.tree_type

        # First figure out UID and other identifying info for dst node.
        if dst_tree_type == TreeType.LOCAL_DISK:
            dst_node_uid: UID = self.backend.cacheman.get_uid_for_local_path(dst_path)
            dst_node_goog_id = None  # N/A, but need to make compiler happy
        elif dst_tree_type == TreeType.GDRIVE:
            existing_dst_node_list = self.backend.cacheman.get_node_list_for_path_list([dst_path], device_uid=dst_device_uid)
            if len(existing_dst_node_list) == 1:
                existing_node = existing_dst_node_list[0]
                # If a single node is already there with given name, use its identification; we will overwrite its content with a new version
                dst_node_uid = existing_node.uid
                dst_node_goog_id = existing_node.goog_id
            elif len(existing_dst_node_list) > 1:
                if self._all_same(existing_dst_node_list):
                    logger.warning(f'Found {len(existing_dst_node_list)} identical nodes already present at at GDrive dst path '
                                   f'("{repr(dst_path)}"). Will overwrite all starting with UID {existing_dst_node_list[0].uid}')
                    dst_node_uid = existing_dst_node_list[0].uid
                    dst_node_goog_id = existing_dst_node_list[0].goog_id
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
            dst_node: Node = GDriveFile(node_identifier=node_identifier, goog_id=dst_node_goog_id, node_name=src_node.name, mime_type_uid=None,
                                        trashed=TrashStatus.NOT_TRASHED, drive_id=None, version=None, md5=src_node.md5,
                                        is_shared=False, create_ts=None, modify_ts=None, size_bytes=src_node.get_size_bytes(),
                                        owner_uid=None, shared_by_user_uid=None, sync_ts=None)
        else:
            raise RuntimeError(f"Cannot create file node for tree type: {dst_tree_type} (node_identifier={node_identifier}")

        spid = self.backend.node_identifier_factory.for_values(tree_type=dst_tree_type, path_list=[dst_path], uid=dst_node_uid,
                                                               device_uid=dst_device_uid, must_be_single_path=True)
        dst_sn: SPIDNodePair = SPIDNodePair(spid, dst_node)

        # ANCESTORS:
        self._add_needed_ancestors(dst_sn)

        logger.debug(f'[{self.change_tree.tree_id}] Migrated single node: {spid}')
        return dst_sn

    @staticmethod
    def _all_same(existing_node_list: List[Node]) -> bool:
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
            self.add_op(op_type=UserOpType.MKDIR, src_sn=ancestor_sn)

    def _generate_missing_ancestor_nodes(self, new_sn: SPIDNodePair) -> Deque[SPIDNodePair]:
        tree_type: int = new_sn.spid.tree_type
        device_uid: UID = new_sn.spid.device_uid
        ancestor_stack: Deque[SPIDNodePair] = deque()

        child_path: str = new_sn.spid.get_single_path()
        child: Node = new_sn.node

        # Determine ancestors:
        while True:
            parent_path = str(pathlib.Path(child_path).parent)

            # AddedFolder already generated and added?
            existing_ancestor: Optional[SPIDNodePair] = self._added_folders.get(parent_path, None)
            if existing_ancestor:
                child.set_parent_uids(existing_ancestor.node.uid)
                break

            # Folder already existed in original tree?
            existing_ancestor_list: List[Node] = self.backend.cacheman.get_node_list_for_path_list([parent_path], device_uid)
            if existing_ancestor_list:
                # Add all parents which match the path, even if they are duplicates or do not yet exist (i.e., pending ops)
                child.set_parent_uids(list(map(lambda x: x.uid, existing_ancestor_list)))
                break

            # Need to create ancestor
            if tree_type == TreeType.GDRIVE:
                logger.debug(f'Creating GoogFolderToAdd for {parent_path}')
                new_ancestor_uid = self.backend.uid_generator.next_uid()
                folder_name = os.path.basename(parent_path)
                new_ancestor_node = GDriveFolder(GDriveIdentifier(uid=new_ancestor_uid, device_uid=device_uid, path_list=parent_path),
                                                 goog_id=None, node_name=folder_name,
                                                 trashed=TrashStatus.NOT_TRASHED, create_ts=None, modify_ts=None, owner_uid=None,
                                                 drive_id=None, is_shared=False, shared_by_user_uid=None, sync_ts=None, all_children_fetched=True)
            elif tree_type == TreeType.LOCAL_DISK:
                logger.debug(f'Creating LocalDirToAdd for {parent_path}')
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
        batch_uid: UID = self.backend.uid_generator.next_uid()

        left_state = DisplayTreeUiState.create_change_tree_state(tree_id_left, left_tree_root_sn)
        left_change_tree: ChangeTree = ChangeTree(backend, left_state)
        self.left_side = OneSide(left_change_tree, batch_uid, tree_id_left_src)

        right_state = DisplayTreeUiState.create_change_tree_state(tree_id_right, right_tree_root_sn)
        right_change_tree: ChangeTree = ChangeTree(backend, right_state)
        self.right_side = OneSide(right_change_tree, batch_uid, tree_id_right_src)

    def copy_nodes_left_to_right(self, src_sn_list: List[SPIDNodePair], sn_dst_parent: SPIDNodePair, op_type: UserOpType):
        """Populates the destination parent in "change_tree_right" with the given source nodes.
        We won't deal with update logic here. A node being copied will be treated as an "add" unless a node exists at the given path,
        in which case the conflict strategy determines whether it's it's an update or something else."""
        assert sn_dst_parent and sn_dst_parent.node.is_dir()
        assert op_type == UserOpType.CP or op_type == UserOpType.MV or op_type == UserOpType.UP
        dst_parent_path: str = sn_dst_parent.spid.get_single_path()

        # We won't deal with update logic here. A node being copied will be treated as an "add"
        # unless a node exists at the given path, in which case the conflict strategy determines whether it's
        # it's an update or something else

        logger.debug(f'Preparing {len(src_sn_list)} nodes for copy...')

        for src_sn in src_sn_list:
            if src_sn.node.is_dir():
                # Unpack dir and add it and its descendants:
                src_parent_path = src_sn.spid.get_single_parent_path()

                subtree_sn_list: List[SPIDNodePair] = self._get_file_sn_list_for_left_subtree(src_sn)
                logger.debug(f'Unpacking subtree with {len(subtree_sn_list)} nodes for copy...')
                for subtree_sn in subtree_sn_list:
                    dst_rel_path: str = file_util.strip_root(subtree_sn.spid.get_single_path(), src_parent_path)
                    dst_path = os.path.join(dst_parent_path, dst_rel_path)
                    # this will add any missing ancestors, and populate the parent list if applicable:
                    dst_sn: SPIDNodePair = self.right_side.migrate_single_node_to_this_side(subtree_sn, dst_path)
                    self.right_side.add_op(op_type=op_type, src_sn=subtree_sn, dst_sn=dst_sn)
            else:
                # Single file; easy case:
                dst_path = os.path.join(dst_parent_path, src_sn.node.name)
                dst_sn: SPIDNodePair = self.right_side.migrate_single_node_to_this_side(src_sn, dst_path)
                self.right_side.add_op(op_type=op_type, src_sn=src_sn, dst_sn=dst_sn)

    def _build_child_spid(self, child_node: Node, parent_path: str):
        return self.backend.node_identifier_factory.for_values(uid=child_node.uid, device_uid=child_node.device_uid,
                                                               tree_type=child_node.tree_type,
                                                               path_list=os.path.join(parent_path, child_node.name), must_be_single_path=True)

    def visit_each_sn_for_subtree(self, subtree_root: SPIDNodePair, on_file_found: Callable[[SPIDNodePair], None], tree_id_src: Optional[TreeID]):
        """Note: here, param "tree_id_src" indicates which tree_id from which to request nodes from CacheManager (or None to indicate master cache)"""
        assert isinstance(subtree_root, Tuple), \
            f'Expected NamedTuple with SinglePathNodeIdentifier but got {type(subtree_root)}: {subtree_root}'
        queue: Deque[SPIDNodePair] = collections.deque()
        queue.append(subtree_root)

        while len(queue) > 0:
            sn: SPIDNodePair = queue.popleft()
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
                    on_file_found(sn)

    def _get_file_sn_list_for_left_subtree(self, src_sn: SPIDNodePair):
        subtree_files: List[SPIDNodePair] = []

        self.visit_each_sn_for_subtree(src_sn, lambda file_sn: subtree_files.append(file_sn), self.left_side.tree_id_src)
        return subtree_files

    @staticmethod
    def _change_tree_path(src_side: OneSide, dst_side: OneSide, spid_from_src_tree: SinglePathNodeIdentifier) -> str:
        return os.path.join(dst_side.root_sn.spid.get_single_path(), file_util.strip_root(spid_from_src_tree.get_single_path(),
                                                                                          src_side.root_sn.spid.get_single_path()))

    def get_path_moved_to_right(self, spid_left: SinglePathNodeIdentifier) -> str:
        return self._change_tree_path(self.left_side, self.right_side, spid_left)

    def get_path_moved_to_left(self, spid_right: SinglePathNodeIdentifier) -> str:
        return self._change_tree_path(self.right_side, self.left_side, spid_right)

    def _migrate_node_to_right(self, sn_s: SPIDNodePair) -> SPIDNodePair:
        dst_path = self.get_path_moved_to_right(sn_s.spid)
        return self.right_side.migrate_single_node_to_this_side(sn_s, dst_path)

    def _migrate_node_to_left(self, sn_r: SPIDNodePair) -> SPIDNodePair:
        dst_path = self.get_path_moved_to_left(sn_r.spid)
        return self.left_side.migrate_single_node_to_this_side(sn_r, dst_path)

    def append_mv_op_r_to_r(self, sn_s: SPIDNodePair, sn_r: SPIDNodePair):
        """Make a dst node which will rename a file within the right tree to match the relative path of the file on the left"""
        self.right_side.add_op(op_type=UserOpType.MV, src_sn=sn_r, dst_sn=self._migrate_node_to_right(sn_s))

    def append_mv_op_s_to_s(self, sn_s: SPIDNodePair, sn_r: SPIDNodePair):
        """Make a FileToMove node which will rename a file within the left tree to match the relative path of the file on right"""
        self.left_side.add_op(op_type=UserOpType.MV, src_sn=sn_s, dst_sn=self._migrate_node_to_left(sn_r))

    def append_cp_op_s_to_r(self, sn_s: SPIDNodePair):
        """COPY: Left -> Right"""
        self.right_side.add_op(op_type=UserOpType.CP, src_sn=sn_s, dst_sn=self._migrate_node_to_right(sn_s))

    def append_cp_op_r_to_s(self, sn_r: SPIDNodePair):
        """COPY: Left <- Right"""
        self.left_side.add_op(op_type=UserOpType.CP, src_sn=sn_r, dst_sn=self._migrate_node_to_left(sn_r))

    def append_up_op_s_to_r(self, sn_s: SPIDNodePair, sn_r: SPIDNodePair):
        """UPDATE: Left -> Right. Both nodes already exist, but one will overwrite the other"""
        self.right_side.add_op(op_type=UserOpType.UP, src_sn=sn_s, dst_sn=sn_r)

    def append_up_op_r_to_s(self, sn_s: SPIDNodePair, sn_r: SPIDNodePair):
        """UPDATE: Left <- Right. Both nodes already exist, but one will overwrite the other"""
        self.left_side.add_op(op_type=UserOpType.UP, src_sn=sn_r, dst_sn=sn_s)
