import logging
import os
import pathlib
from collections import deque
from typing import Deque, Dict, List

import file_util
from constants import NOT_TRASHED, TREE_TYPE_GDRIVE, TREE_TYPE_LOCAL_DISK
from model.category import Category
from model.display_node import ContainerNode, DisplayNode
from model.fmeta import LocalDirNode, LocalFileNode
from model.goog_node import GoogFile, GoogFolder
from model.node_identifier import LocalFsIdentifier, NodeIdentifier
from model.subtree_snapshot import SubtreeSnapshot
from ui.actions import ID_LEFT_TREE, ID_RIGHT_TREE
from ui.tree.category_display_tree import CategoryDisplayTree

logger = logging.getLogger(__name__)


# CLASS ChangeMaker
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼

class ChangeMaker:
    def __init__(self, left_tree: SubtreeSnapshot, right_tree: SubtreeSnapshot, application):
        self.left_tree = left_tree
        self.right_tree = right_tree
        self.application = application
        self.uid_generator = application.uid_generator

        self.change_tree_left: CategoryDisplayTree = CategoryDisplayTree(application, self.left_tree.node_identifier, ID_LEFT_TREE)
        self.change_tree_right: CategoryDisplayTree = CategoryDisplayTree(application, self.right_tree.node_identifier, ID_RIGHT_TREE)

        self.added_folders_left: Dict[str, ContainerNode] = {}
        self.added_folders_right: Dict[str, ContainerNode] = {}

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
                logger.debug(f'Preparing subtree with {len(subtree_files)} items for copy...')
                for node in subtree_files:
                    dst_rel_path = file_util.strip_root(node.full_path, src_path_minus_dirname)
                    self._copy_single_node_left_to_right(node, dst_parent_path, dst_rel_path)
            else:
                file_name = os.path.basename(src_node.full_path)
                self._copy_single_node_left_to_right(src_node, dst_parent_path, file_name)

    def _copy_single_node_left_to_right(self, src_node: DisplayNode, dst_parent_path: str, dst_rel_path: str):
        new_path = os.path.join(dst_parent_path, dst_rel_path)
        logger.debug(f'New path for copied item: {new_path}')
        tree_type = self.right_tree.tree_type
        if tree_type == TREE_TYPE_LOCAL_DISK:
            uid = self.application.cache_manager.get_uid_for_path(new_path)
        else:
            # Assign new UID. We will later associate this with a goog_id once it's made existent
            uid = self.application.uid_generator.get_new_uid()
        dst_node_identifier = self.application.node_identifier_factory.for_values(tree_type=tree_type, full_path=new_path,
                                                                              uid=uid, category=Category.Added)
        node: DisplayNode = self._migrate_file_node(node_identifier=dst_node_identifier, src_node=src_node)
        assert not node.exists()
        self._add_item_and_needed_ancestors(self.change_tree_right, self.right_tree, self.added_folders_right, node)

    def _migrate_file_node(self, node_identifier: NodeIdentifier, src_node: DisplayNode) -> DisplayNode:
        md5 = src_node.md5
        sha256 = src_node.sha256
        size_bytes = src_node.get_size_bytes()

        tree_type = node_identifier.tree_type
        if tree_type == TREE_TYPE_LOCAL_DISK:
            return LocalFileNode(node_identifier.uid, md5, sha256, size_bytes, None, None, None, node_identifier.full_path, node_identifier.category)
        elif tree_type == TREE_TYPE_GDRIVE:
            return GoogFile(node_identifier.uid, None, src_node.name, NOT_TRASHED, None, None, None, md5, False, None, None, size_bytes, None, None)
        else:
            raise RuntimeError(f"Cannot create file node for tree type: {tree_type} (node_identifier={node_identifier}")

    def move_to_right(self, left_item) -> NodeIdentifier:
        left_rel_path = left_item.get_relative_path(self.left_tree)
        path = os.path.join(self.right_tree.root_path, left_rel_path)
        tree_type = self.right_tree.tree_type
        if tree_type == TREE_TYPE_LOCAL_DISK:
            uid = self.application.cache_manager.get_uid_for_path(path)
        else:
            uid = self.application.uid_generator.get_new_uid()
        return self.application.node_identifier_factory.for_values(tree_type=tree_type, full_path=path, uid=uid)

    def move_to_left(self, right_item) -> NodeIdentifier:
        right_rel_path = right_item.get_relative_path(self.right_tree)
        path = os.path.join(self.left_tree.root_path, right_rel_path)
        tree_type = self.left_tree.tree_type
        if tree_type == TREE_TYPE_LOCAL_DISK:
            uid = self.application.cache_manager.get_uid_for_path(path)
        else:
            uid = self.application.uid_generator.get_new_uid()
        return self.application.node_identifier_factory.for_values(tree_type=tree_type, full_path=path, uid=uid)

    def _generate_missing_ancestor_nodes(self, source_tree: SubtreeSnapshot, added_folders_dict: Dict[str, ContainerNode], new_item: DisplayNode):
        tree_type: int = new_item.node_identifier.tree_type
        ancestor_stack = deque()

        child_path = new_item.full_path
        child = new_item

        # TODO: think about how we might make a deterministic path lookup in dst tree

        # Determine ancestors:
        while True:
            parent_path = str(pathlib.Path(child_path).parent)

            # AddedFolder already generated and added?
            existing_ancestor = added_folders_dict.get(parent_path, None)
            if existing_ancestor:
                child.add_parent(existing_ancestor.uid)
                break

            # Folder already existed in original tree?
            existing_ancestor_list = source_tree.get_for_path(parent_path)
            if existing_ancestor_list:
                child.set_parent_uids(list(map(lambda x: x.uid, existing_ancestor_list)))
                break

            if tree_type == TREE_TYPE_GDRIVE:
                logger.debug(f'Creating GoogFolderToAdd for {parent_path}')
                new_uid = self.uid_generator.get_new_uid()
                folder_name = os.parent_path.basename(parent_path)
                new_parent = GoogFolder(uid=new_uid, goog_id=None, item_name=folder_name, trashed=False, drive_id=None, my_share=False, sync_ts=None,
                                        all_children_fetched=True)
            elif tree_type == TREE_TYPE_LOCAL_DISK:
                logger.debug(f'Creating LocalDirToAdd for {parent_path}')
                new_uid = self.application.cache_manager.get_uid_for_path(parent_path)
                node_identifier = LocalFsIdentifier(parent_path, new_uid, Category.Added)
                new_parent = LocalDirNode(node_identifier, exists=False)
            else:
                raise RuntimeError(f'Invalid tree type: {tree_type} for item {new_item}')

            child.add_parent(new_parent.uid)

            added_folders_dict[parent_path] = new_parent
            ancestor_stack.append(new_parent)

            child.set_parent_uids(new_parent.uid)
            child_path = parent_path
            child = new_parent

        return ancestor_stack

    def _add_item_and_needed_ancestors(self, dst_tree: CategoryDisplayTree, source_tree: SubtreeSnapshot,
                                       added_folders_dict: Dict[str, ContainerNode], new_item: DisplayNode):
        """Adds the migrated item """

        # Lowest item in the stack will always be orig item. Stack size > 1 iff need to add parent folders
        ancestor_stack: Deque[ContainerNode] = self._generate_missing_ancestor_nodes(source_tree, added_folders_dict, new_item)
        while len(ancestor_stack) > 0:
            ancestor = ancestor_stack.pop()
            dst_tree.add_item(ancestor, ancestor.category, source_tree)

        dst_tree.add_item(new_item, new_item.category, source_tree)

    def add_rename_right(self, left_item, right_item):
        """Make a FileToMove node which will rename a file within the right tree to match the relative path of
        the file on the left"""
        node_identifier = self.move_to_right(left_item)
        node_identifier.category = Category.Moved
        node = FileToMove(node_identifier=node_identifier, src_node=right_item)
        self._add_item_and_needed_ancestors(self.change_tree_right, self.right_tree, self.added_folders_right, node)

    def add_rename_left(self, left_item, right_item):
        """Make a FileToMove node which will rename a file within the left tree to match the relative path of the file on right"""
        node_identifier = self.move_to_left(right_item)
        node_identifier.category = Category.Moved
        node = FileToMove(node_identifier=node_identifier, src_node=left_item)
        self._add_item_and_needed_ancestors(self.change_tree_left, self.left_tree, self.added_folders_left, node)

    def add_filetoadd_left_to_right(self, left_item):
        """ADD: Left -> Right"""
        node_identifier = self.move_to_right(left_item)
        node_identifier.category = Category.Added
        node = FileToAdd(node_identifier=node_identifier, src_node=left_item)
        self._add_item_and_needed_ancestors(self.change_tree_right, self.right_tree, self.added_folders_right, node)

    def add_filetoadd_right_to_left(self, right_item):
        """ADD: Left <- Right"""
        node_identifier = self.move_to_left(right_item)
        node_identifier.category = Category.Added
        node = FileToAdd(node_identifier=node_identifier, src_node=right_item)
        self._add_item_and_needed_ancestors(self.change_tree_left, self.left_tree, self.added_folders_left, node)

    def add_fileupdate_left_to_right(self, left_item, right_item_to_overwrite):
        """UPDATE: Left -> Right"""
        node_identifier = self.move_to_right(left_item)
        node_identifier.category = Category.Updated
        node = FileToUpdate(node_identifier=node_identifier, src_node=left_item, dst_node=right_item_to_overwrite)
        self._add_item_and_needed_ancestors(self.change_tree_right, self.right_tree, self.added_folders_right, node)

    def add_fileupdate_right_to_left(self, right_item, left_item_to_overwrite):
        """UPDATE: Left <- Right"""
        node_identifier = self.move_to_left(right_item)
        node_identifier.category = Category.Updated
        node = FileToUpdate(node_identifier=node_identifier, src_node=right_item, dst_node=left_item_to_overwrite)
        self._add_item_and_needed_ancestors(self.change_tree_left, self.left_tree, self.added_folders_left, node)

