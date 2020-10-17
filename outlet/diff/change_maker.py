import logging
import os
import pathlib
from collections import deque
from typing import Deque, Dict, List

from index.uid.uid import UID
from util import file_util
from model.op import Op, OpType
from constants import NULL_UID, TrashStatus, TREE_TYPE_GDRIVE, TREE_TYPE_LOCAL_DISK
from model.node.display_node import DisplayNode
from model.node.local_disk_node import LocalDirNode, LocalFileNode
from model.node.gdrive_node import GDriveFile, GDriveFolder, GDriveNode
from model.node_identifier import GDriveIdentifier, LocalFsIdentifier, NodeIdentifier
from model.display_tree.display_tree import DisplayTree
from ui.actions import ID_LEFT_TREE, ID_RIGHT_TREE
from model.display_tree.category import CategoryDisplayTree

logger = logging.getLogger(__name__)


def _migrate_file_node(node_identifier: NodeIdentifier, src_node: DisplayNode) -> DisplayNode:
    """Translates the stuff from the src_node to the new tree and location given by node_identifier"""
    md5 = src_node.md5
    sha256 = src_node.sha256
    size_bytes = src_node.get_size_bytes()

    tree_type = node_identifier.tree_type
    if tree_type == TREE_TYPE_LOCAL_DISK:
        assert isinstance(node_identifier, LocalFsIdentifier)
        return LocalFileNode(node_identifier, md5, sha256, size_bytes, None, None, None, False)
    elif tree_type == TREE_TYPE_GDRIVE:
        assert isinstance(node_identifier, GDriveIdentifier)
        return GDriveFile(node_identifier, None, src_node.name, None, TrashStatus.NOT_TRASHED, None, None, None, md5, False, None, None,
                          size_bytes, None, None, None)
    else:
        raise RuntimeError(f"Cannot create file node for tree type: {tree_type} (node_identifier={node_identifier}")


# CLASS OneSide
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
class OneSide:
    def __init__(self, underlying_tree: DisplayTree, app, tree_id: str):
        self.underlying_tree: DisplayTree = underlying_tree
        self.app = app
        self.change_tree: CategoryDisplayTree = CategoryDisplayTree(app, self.underlying_tree.node_identifier, tree_id)
        # TODO: shouldn't batch_uid be the same for both sides ?
        self._batch_uid: UID = self.app.uid_generator.next_uid()
        self._added_folders: Dict[str, DisplayNode] = {}

    def migrate_single_node_to_this_side(self, src_node: DisplayNode, new_path: str) -> DisplayNode:
        dst_tree_type = self.underlying_tree.tree_type
        # (Kludge) just assign the NULL UID for now, so we don't auto-generate a new UID. It will just get overwritten anyway if GDrive
        dst_node_identifier = self.app.node_identifier_factory.for_values(tree_type=dst_tree_type, full_path=new_path, uid=NULL_UID)
        dst_node: DisplayNode = _migrate_file_node(node_identifier=dst_node_identifier, src_node=src_node)
        self.add_needed_ancestors(dst_node)

        if dst_tree_type == TREE_TYPE_LOCAL_DISK:
            dst_node.uid = self.app.cacheman.get_uid_for_path(new_path)
        elif dst_tree_type == TREE_TYPE_GDRIVE:
            assert isinstance(dst_node, GDriveNode) and dst_node.get_parent_uids(), f'Bad data: {dst_node}'
            existing_node = self.app.cacheman.get_goog_node_for_name_and_parent_uid(dst_node.name, dst_node.get_parent_uids()[0])
            if existing_node:
                # If node is already there with given name, use its identification; we will overwrite its content with a new version
                dst_node.uid = existing_node.uid
                dst_node.goog_id = existing_node.goog_id
            else:
                # Not exist: assign new UID. We will later associate this with a goog_id once it's made existent
                dst_node.uid = self.app.uid_generator.next_uid()

        logger.debug(f'Migrated single node (UID={dst_node.uid} path="{new_path}")')
        return dst_node

    def _create_op(self, op_type: OpType, src_node: DisplayNode, dst_node: DisplayNode = None):
        assert src_node, f'No src node!'
        return Op(op_uid=self.app.uid_generator.next_uid(), batch_uid=self._batch_uid, op_type=op_type,
                  src_node=src_node, dst_node=dst_node)

    def add_op(self, op_type: OpType, src_node: DisplayNode, dst_node: DisplayNode = None):
        """Adds a node to the op tree (dst_node; unless dst_node is None, in which case it will use src_node), and also adds a Op
        of the given type"""

        op: Op = self._create_op(op_type, src_node, dst_node)

        if dst_node:
            target_node = dst_node
        else:
            target_node = src_node
        self.change_tree.add_node(target_node, op, self.underlying_tree)

    def add_needed_ancestors(self, new_node: DisplayNode):
        """Determines what ancestor directories need to be created, and appends them to the op tree (as well as ops for them).
        Appends the migrated node as well, but the op for it is omitted so that the caller can provide its own"""

        # Lowest node in the stack will always be orig node. Stack size > 1 iff need to add parent folders
        ancestor_stack: Deque[DisplayNode] = self._generate_missing_ancestor_nodes(new_node)
        while len(ancestor_stack) > 0:
            ancestor: DisplayNode = ancestor_stack.pop()
            # Create an accompanying MKDIR action which will create the new folder/dir
            self.add_op(op_type=OpType.MKDIR, src_node=ancestor)

    def _generate_missing_ancestor_nodes(self, new_node: DisplayNode):
        tree_type: int = new_node.node_identifier.tree_type
        ancestor_stack = deque()

        child_path = new_node.full_path
        child = new_node

        # TODO: think about how we might make a deterministic path lookup in dst tree

        # Determine ancestors:
        while True:
            parent_path = str(pathlib.Path(child_path).parent)

            # AddedFolder already generated and added?
            existing_ancestor = self._added_folders.get(parent_path, None)
            if existing_ancestor:
                if tree_type == TREE_TYPE_GDRIVE:
                    child.set_parent_uids(existing_ancestor.uid)
                break

            # Folder already existed in original tree?
            existing_ancestor_list = self.underlying_tree.get_for_path(parent_path)
            if existing_ancestor_list and existing_ancestor_list[0].exists():
                if tree_type == TREE_TYPE_GDRIVE:
                    child.set_parent_uids(list(map(lambda x: x.uid, existing_ancestor_list)))
                break

            if tree_type == TREE_TYPE_GDRIVE:
                logger.debug(f'Creating GoogFolderToAdd for {parent_path}')
                new_uid = self.app.uid_generator.next_uid()
                folder_name = os.path.basename(parent_path)
                new_parent = GDriveFolder(GDriveIdentifier(uid=new_uid, full_path=parent_path), goog_id=None, node_name=folder_name,
                                          trashed=False, create_ts=None, modify_ts=None, owner_uid=None,
                                          drive_id=None, is_shared=False, shared_by_user_uid=None, sync_ts=None, all_children_fetched=True)
            elif tree_type == TREE_TYPE_LOCAL_DISK:
                logger.debug(f'Creating LocalDirToAdd for {parent_path}')
                new_uid = self.app.cacheman.get_uid_for_path(parent_path)
                node_identifier = LocalFsIdentifier(parent_path, new_uid)
                new_parent = LocalDirNode(node_identifier, exists=False)
            else:
                raise RuntimeError(f'Invalid tree type: {tree_type} for node {new_node}')

            self._added_folders[parent_path] = new_parent
            ancestor_stack.append(new_parent)

            if tree_type == TREE_TYPE_GDRIVE:
                child.set_parent_uids(new_parent.uid)

            child_path = parent_path
            child = new_parent

        return ancestor_stack


# CLASS ChangeMaker
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼

class ChangeMaker:
    def __init__(self, left_tree: DisplayTree, right_tree: DisplayTree, app):
        self.app = app
        self.left_side = OneSide(left_tree, app, ID_LEFT_TREE)
        self.right_side = OneSide(right_tree, app, ID_RIGHT_TREE)

    def copy_nodes_left_to_right(self, src_node_list: List[DisplayNode], dst_parent: DisplayNode, op_type: OpType):
        """Populates the destination parent in "change_tree_right" with the given source nodes."""
        assert dst_parent.is_dir()
        assert op_type == OpType.CP or op_type == OpType.MV or op_type == OpType.UP
        dst_parent_path = dst_parent.full_path

        # We won't deal with update logic here. A node being copied will be treated as an "add"
        # unless a node exists at the given path, in which case the conflict strategy determines whether it's
        # it's an update or something else

        logger.debug(f'Preparing {len(src_node_list)} nodes for copy...')

        for src_node in src_node_list:
            if src_node.is_dir():
                # Add all its descendants. Assume that we came from a display tree which may not have all its children.
                # Need to look things up in the central cache. We will focus on copying files, and add prerequisite parent dirs
                # as needed
                subtree_files, subtree_dirs = self.app.cacheman.get_all_files_and_dirs_for_subtree(src_node.node_identifier)
                src_path_minus_dirname = str(pathlib.Path(src_node.full_path).parent)
                logger.debug(f'Unpacking subtree with {len(subtree_files)} nodes for copy...')
                for node in subtree_files:
                    dst_rel_path = file_util.strip_root(node.full_path, src_path_minus_dirname)
                    new_path = os.path.join(dst_parent_path, dst_rel_path)
                    dst_node = self.right_side.migrate_single_node_to_this_side(node, new_path)
                    self.right_side.add_op(op_type=op_type, src_node=node, dst_node=dst_node)
            else:
                file_name = os.path.basename(src_node.full_path)
                new_path = os.path.join(dst_parent_path, file_name)
                dst_node = self.right_side.migrate_single_node_to_this_side(src_node, new_path)
                self.right_side.add_op(op_type=op_type, src_node=src_node, dst_node=dst_node)

    def get_path_moved_to_right(self, left_node) -> str:
        return os.path.join(self.right_side.underlying_tree.root_path, left_node.get_relative_path(self.left_side.underlying_tree))

    def get_path_moved_to_left(self, right_node) -> str:
        return os.path.join(self.left_side.underlying_tree.root_path, right_node.get_relative_path(self.right_side.underlying_tree))

    def _migrate_node_to_right(self, left_node) -> DisplayNode:
        new_path = self.get_path_moved_to_right(left_node)
        return self.right_side.migrate_single_node_to_this_side(left_node, new_path)

    def _migrate_node_to_left(self, right_node) -> DisplayNode:
        new_path = os.path.join(self.left_side.underlying_tree.root_path, right_node.get_relative_path(self.right_side.underlying_tree))
        return self.left_side.migrate_single_node_to_this_side(right_node, new_path)

    def append_rename_right_to_right(self, left_node: DisplayNode, right_node: DisplayNode):
        """Make a dst node which will rename a file within the right tree to match the relative path of
        the file on the left"""
        dst_node: DisplayNode = self._migrate_node_to_right(left_node)
        # "src node" can be either left_node or right_node. We'll use right_node because it is closer (in theory)
        self.right_side.add_op(op_type=OpType.MV, src_node=right_node, dst_node=dst_node)

    def append_rename_left_to_left(self, left_node, right_node):
        """Make a FileToMove node which will rename a file within the left tree to match the relative path of the file on right"""
        dst_node: DisplayNode = self._migrate_node_to_left(right_node)
        # "src node" can be either left_node or right_node. We'll use right_node because it is closer (in theory)
        self.left_side.add_op(op_type=OpType.MV, src_node=left_node, dst_node=dst_node)

    def append_copy_left_to_right(self, left_node):
        """COPY: Left -> Right"""
        dst_node: DisplayNode = self._migrate_node_to_right(left_node)

        # "src node" can be either left_node or right_node. We'll use left_node because it is closer (in theory)
        self.right_side.add_op(op_type=OpType.CP, src_node=left_node, dst_node=dst_node)

    def append_copy_right_to_left(self, right_node):
        """COPY: Left <- Right"""
        dst_node: DisplayNode = self._migrate_node_to_left(right_node)

        # "src node" can be either left_node or right_node. We'll use left_node because it is closer (in theory)
        self.left_side.add_op(op_type=OpType.CP, src_node=right_node, dst_node=dst_node)

    def append_update_left_to_right(self, left_node, right_node_to_overwrite):
        """UPDATE: Left -> Right"""
        dst_node: DisplayNode = self._migrate_node_to_right(left_node)
        assert dst_node.uid == right_node_to_overwrite.uid

        # "src node" can be either left_node or right_node. We'll use left_node because it is closer (in theory)
        self.right_side.add_op(op_type=OpType.UP, src_node=left_node, dst_node=dst_node)

    def append_update_right_to_left(self, right_node, left_node_to_overwrite):
        """UPDATE: Left <- Right"""
        dst_node: DisplayNode = self._migrate_node_to_left(right_node)
        assert dst_node.uid == left_node_to_overwrite.uid

        # "src node" can be either left_node or right_node. We'll use left_node because it is closer (in theory)
        self.left_side.add_op(op_type=OpType.UP, src_node=right_node, dst_node=dst_node)
