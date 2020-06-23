import logging
import os
import pathlib
from collections import deque
from typing import Deque, Dict, List

import file_util
from command.change_action import ChangeAction, ChangeType
from constants import NOT_TRASHED, TREE_TYPE_GDRIVE, TREE_TYPE_LOCAL_DISK
from model.display_node import ContainerNode, DisplayNode
from model.fmeta import LocalDirNode, LocalFileNode
from model.goog_node import GoogFile, GoogFolder, GoogNode
from model.node_identifier import GDriveIdentifier, LocalFsIdentifier, NodeIdentifier
from model.subtree_snapshot import SubtreeSnapshot
from ui.actions import ID_LEFT_TREE, ID_RIGHT_TREE
from ui.tree.category_display_tree import CategoryDisplayTree

logger = logging.getLogger(__name__)

DUMMY_UID = -1


def _migrate_file_node(node_identifier: NodeIdentifier, src_node: DisplayNode) -> DisplayNode:
    """Translates the stuff from the src_node to the new tree and location given by node_identifier"""
    md5 = src_node.md5
    sha256 = src_node.sha256
    size_bytes = src_node.get_size_bytes()

    tree_type = node_identifier.tree_type
    if tree_type == TREE_TYPE_LOCAL_DISK:
        assert isinstance(node_identifier, LocalFsIdentifier)
        return LocalFileNode(node_identifier, md5, sha256, size_bytes, None, None, None, True)
    elif tree_type == TREE_TYPE_GDRIVE:
        assert isinstance(node_identifier, GDriveIdentifier)
        return GoogFile(node_identifier, None, src_node.name, NOT_TRASHED, None, None, None, md5, False, None, None, size_bytes, None, None)
    else:
        raise RuntimeError(f"Cannot create file node for tree type: {tree_type} (node_identifier={node_identifier}")


# CLASS OneSide
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
class OneSide:
    def __init__(self, underlying_tree: SubtreeSnapshot, application, tree_id: str):
        self.underlying_tree: SubtreeSnapshot = underlying_tree
        self.application = application
        self.uid_generator = application.uid_generator
        self.change_tree: CategoryDisplayTree = CategoryDisplayTree(application, self.underlying_tree.node_identifier, tree_id)
        self.added_folders: Dict[str, ContainerNode] = {}

    def migrate_single_node_to_this_side(self, src_node: DisplayNode, new_path: str):
        logger.debug(f'New path for migrated item: {new_path}')
        dst_tree_type = self.underlying_tree.tree_type
        # (Kludge) set UID to -1 because we want to leave it unset for now. We need to wait until after we have determined parent nodes
        # so that we can determine if there is an existing GDrive UID for it
        dst_node_identifier = self.application.node_identifier_factory.for_values(tree_type=dst_tree_type, full_path=new_path, uid=DUMMY_UID)
        dst_node: DisplayNode = _migrate_file_node(node_identifier=dst_node_identifier, src_node=src_node)
        self.add_needed_ancestors(dst_node)

        if dst_tree_type == TREE_TYPE_LOCAL_DISK:
            dst_node.uid = self.application.cache_manager.get_uid_for_path(new_path)
        elif dst_tree_type == TREE_TYPE_GDRIVE:
            assert isinstance(dst_node, GoogNode) and dst_node.get_parent_uids(), f'Bad data: {dst_node}'
            existing_node = self.application.cache_manager.get_goog_node_for_name_and_parent_uid(dst_node.name, dst_node.get_parent_uids()[0])
            if existing_node:
                # If item is already there with given name, use its identification; we will overwrite its content with a new version
                dst_node.uid = existing_node.uid
                dst_node.goog_id = existing_node.goog_id
            else:
                # Not exist: assign new UID. We will later associate this with a goog_id once it's made existent
                dst_node.uid = self.application.uid_generator.get_new_uid()

        return dst_node

    def create_change_action(self, change_type: ChangeType, src_node: DisplayNode, dst_node: DisplayNode = None):
        if src_node:
            src_uid = src_node.uid
        else:
            src_uid = None

        if dst_node:
            dst_uid = dst_node.uid
        else:
            dst_uid = None

        action_uid = self.uid_generator.get_new_uid()
        return ChangeAction(change_type=change_type, action_uid=action_uid, src_uid=src_uid, dst_uid=dst_uid)

    def add_change_item(self, change_type: ChangeType, src_node: DisplayNode, dst_node: DisplayNode = None):
        """Adds a node to the change tree (dst_node; unless dst_node is None, in which case it will use src_node), and also adds a ChangeAction
        of the given type"""

        change_action: ChangeAction = self.create_change_action(change_type, src_node, dst_node)

        if dst_node:
            target_node = dst_node
        else:
            target_node = src_node
        self.change_tree.add_item(target_node, change_action, self.underlying_tree)

    def add_needed_ancestors(self, new_item: DisplayNode):
        """Determines what ancestor directories need to be created, and appends them to the change tree (as well as change actions for them).
        Appends the migrated item as well, but the change action for it is omitted so that the caller can provide its own"""

        # Lowest item in the stack will always be orig item. Stack size > 1 iff need to add parent folders
        ancestor_stack: Deque[ContainerNode] = self._generate_missing_ancestor_nodes(new_item)
        while len(ancestor_stack) > 0:
            ancestor: DisplayNode = ancestor_stack.pop()
            # Create an accompanying MKDIR action which will create the new folder/dir
            self.add_change_item(change_type=ChangeType.MKDIR, src_node=ancestor)

    def _generate_missing_ancestor_nodes(self, new_item: DisplayNode):
        tree_type: int = new_item.node_identifier.tree_type
        ancestor_stack = deque()

        child_path = new_item.full_path
        child = new_item

        # TODO: think about how we might make a deterministic path lookup in dst tree

        # Determine ancestors:
        while True:
            parent_path = str(pathlib.Path(child_path).parent)

            # AddedFolder already generated and added?
            existing_ancestor = self.added_folders.get(parent_path, None)
            if existing_ancestor:
                if tree_type == TREE_TYPE_GDRIVE:
                    child.set_parent_uids(existing_ancestor.uid)
                break

            # Folder already existed in original tree?
            existing_ancestor_list = self.underlying_tree.get_for_path(parent_path)
            if existing_ancestor_list:
                if tree_type == TREE_TYPE_GDRIVE:
                    child.set_parent_uids(list(map(lambda x: x.uid, existing_ancestor_list)))
                break

            if tree_type == TREE_TYPE_GDRIVE:
                logger.debug(f'Creating GoogFolderToAdd for {parent_path}')
                new_uid = self.uid_generator.get_new_uid()
                folder_name = os.parent_path.basename(parent_path)
                new_parent = GoogFolder(GDriveIdentifier(uid=new_uid, full_path=None), goog_id=None, item_name=folder_name, trashed=False,
                                        drive_id=None, my_share=False, sync_ts=None, all_children_fetched=True)
            elif tree_type == TREE_TYPE_LOCAL_DISK:
                logger.debug(f'Creating LocalDirToAdd for {parent_path}')
                new_uid = self.application.cache_manager.get_uid_for_path(parent_path)
                node_identifier = LocalFsIdentifier(parent_path, new_uid)
                new_parent = LocalDirNode(node_identifier, exists=False)
            else:
                raise RuntimeError(f'Invalid tree type: {tree_type} for item {new_item}')

            self.added_folders[parent_path] = new_parent
            ancestor_stack.append(new_parent)

            if tree_type == TREE_TYPE_GDRIVE:
                child.set_parent_uids(new_parent.uid)

            child_path = parent_path
            child = new_parent

        return ancestor_stack


# CLASS ChangeMaker
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼

class ChangeMaker:
    def __init__(self, left_tree: SubtreeSnapshot, right_tree: SubtreeSnapshot, application):
        self.left_side = OneSide(left_tree, application, ID_LEFT_TREE)
        self.right_side = OneSide(right_tree, application, ID_RIGHT_TREE)

        self.application = application
        self.uid_generator = application.uid_generator

    def copy_nodes_left_to_right(self, src_node_list: List[DisplayNode], dst_parent: DisplayNode):
        """Populates the destination parent in "change_tree_right" with the given source nodes."""
        assert dst_parent.is_dir()
        dst_parent_path = dst_parent.full_path

        # We won't deal with update logic here. A node being copied will be treated as an "add"
        # unless an item exists at the given path, in which case the conflict strategy determines whether it's
        # it's an update or something else

        logger.debug(f'Preparing {len(src_node_list)} items for copy...')

        for src_node in src_node_list:
            if src_node.is_dir():
                # Add all its descendants. Assume that we came from a display tree which may not have all its children.
                # Need to look things up in the central cache. We will focus on copying files, and add prerequisite parent dirs
                # as needed
                subtree_files, subtree_dirs = self.application.cache_manager.get_all_files_and_dirs_for_subtree(src_node.node_identifier)
                src_path_minus_dirname = str(pathlib.Path(src_node.full_path).parent)
                logger.debug(f'Unpacking subtree with {len(subtree_files)} items for copy...')
                for node in subtree_files:
                    dst_rel_path = file_util.strip_root(node.full_path, src_path_minus_dirname)
                    new_path = os.path.join(dst_parent_path, dst_rel_path)
                    dst_node = self.right_side.migrate_single_node_to_this_side(node, new_path)
                    self.right_side.add_change_item(change_type=ChangeType.CP, src_node=node, dst_node=dst_node)
            else:
                file_name = os.path.basename(src_node.full_path)
                new_path = os.path.join(dst_parent_path, file_name)
                dst_node = self.right_side.migrate_single_node_to_this_side(src_node, new_path)
                self.right_side.add_change_item(change_type=ChangeType.CP, src_node=src_node, dst_node=dst_node)

    def get_path_moved_to_right(self, left_item) -> str:
        return os.path.join(self.right_side.underlying_tree.root_path, left_item.get_relative_path(self.left_side.underlying_tree))

    def get_path_moved_to_left(self, right_item) -> str:
        return os.path.join(self.left_side.underlying_tree.root_path, right_item.get_relative_path(self.right_side.underlying_tree))

    def _migrate_node_to_right(self, left_item) -> DisplayNode:
        new_path = self.get_path_moved_to_right(left_item)
        return self.right_side.migrate_single_node_to_this_side(left_item, new_path)

    def _migrate_node_to_left(self, right_item) -> DisplayNode:
        new_path = os.path.join(self.left_side.underlying_tree.root_path, right_item.get_relative_path(self.right_side.underlying_tree))
        return self.left_side.migrate_single_node_to_this_side(right_item, new_path)

    def append_rename_right_to_right(self, left_item: DisplayNode, right_item: DisplayNode):
        """Make a dst node which will rename a file within the right tree to match the relative path of
        the file on the left"""
        dst_node: DisplayNode = self._migrate_node_to_right(left_item)
        # "src node" can be either left_item or right_item. We'll use right_item because it is closer (in theory)
        self.right_side.add_change_item(change_type=ChangeType.MV, src_node=right_item, dst_node=dst_node)

    def append_rename_left_to_left(self, left_item, right_item):
        """Make a FileToMove node which will rename a file within the left tree to match the relative path of the file on right"""
        dst_node: DisplayNode = self._migrate_node_to_left(right_item)
        # "src node" can be either left_item or right_item. We'll use right_item because it is closer (in theory)
        self.left_side.add_change_item(change_type=ChangeType.MV, src_node=left_item, dst_node=dst_node)

    def append_copy_left_to_right(self, left_item):
        """COPY: Left -> Right"""
        dst_node: DisplayNode = self._migrate_node_to_right(left_item)

        # "src node" can be either left_item or right_item. We'll use left_item because it is closer (in theory)
        self.right_side.add_change_item(change_type=ChangeType.CP, src_node=left_item, dst_node=dst_node)

    def append_copy_right_to_left(self, right_item):
        """COPY: Left <- Right"""
        dst_node: DisplayNode = self._migrate_node_to_left(right_item)

        # "src node" can be either left_item or right_item. We'll use left_item because it is closer (in theory)
        self.left_side.add_change_item(change_type=ChangeType.CP, src_node=right_item, dst_node=dst_node)

    def append_update_left_to_right(self, left_item, right_item_to_overwrite):
        """UPDATE: Left -> Right"""
        dst_node: DisplayNode = self._migrate_node_to_right(left_item)
        assert dst_node.uid == right_item_to_overwrite.uid

        # "src node" can be either left_item or right_item. We'll use left_item because it is closer (in theory)
        self.right_side.add_change_item(change_type=ChangeType.UP, src_node=left_item, dst_node=dst_node)

    def append_update_right_to_left(self, right_item, left_item_to_overwrite):
        """UPDATE: Left <- Right"""
        dst_node: DisplayNode = self._migrate_node_to_left(right_item)
        assert dst_node.uid == left_item_to_overwrite.uid

        # "src node" can be either left_item or right_item. We'll use left_item because it is closer (in theory)
        self.left_side.add_change_item(change_type=ChangeType.UP, src_node=right_item, dst_node=dst_node)


