import logging
import os
import pathlib
from collections import deque
from typing import Dict, List

import file_util
from constants import TREE_TYPE_GDRIVE, TREE_TYPE_LOCAL_DISK
from model.category import Category
from model.display_node import DisplayNode
from model.goog_node import FolderToAdd
from model.node_identifier import LocalFsIdentifier, NodeIdentifier
from model.planning_node import FileDecoratorNode, FileToAdd, FileToMove, FileToUpdate, LocalDirToAdd, PlanningNode
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

        self.added_folders_left: Dict[str, PlanningNode] = {}
        self.added_folders_right: Dict[str, PlanningNode] = {}

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
            uid = self.application.uid_generator.get_new_uid()
        node_identifier = self.application.node_identifier_factory.for_values(tree_type=tree_type, full_path=new_path,
                                                                              uid=uid, category=Category.Added)
        node = FileToAdd(node_identifier=node_identifier, src_node=src_node)
        self._add_items_and_missing_parents(self.change_tree_right, self.right_tree, self.added_folders_right, node)

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

    def _add_items_and_missing_parents(self, change_tree: CategoryDisplayTree, source_tree: SubtreeSnapshot,
                                       added_folders_dict: Dict[str, PlanningNode], new_item: FileDecoratorNode):
        tree_type: int = new_item.node_identifier.tree_type

        # Lowest item in the stack will always be orig item. Stack size > 1 iff need to add parent folders
        stack = deque()
        stack.append(new_item)

        parents = None
        path = new_item.full_path
        while True:
            path = str(pathlib.Path(path).parent)

            # AddedFolder already known and created?
            parents = added_folders_dict.get(path, None)
            if parents:
                break

            # Folder already existed in original tree?
            parents = source_tree.get_for_path(path)
            if parents:
                break

            if tree_type == TREE_TYPE_GDRIVE:
                logger.debug(f'Creating GoogFolderToAdd for {path}')
                new_uid = self.uid_generator.get_new_uid()
                new_folder = FolderToAdd(new_uid, path)
            elif tree_type == TREE_TYPE_LOCAL_DISK:
                logger.debug(f'Creating LocalDirToAdd for {path}')
                new_uid = self.application.cache_manager.get_uid_for_path(path)
                node_identifier = LocalFsIdentifier(path, new_uid, Category.Added)
                new_folder = LocalDirToAdd(node_identifier)
            else:
                raise RuntimeError(f'Invalid tree type: {tree_type} for item {new_item}')

            added_folders_dict[path] = new_folder
            stack.append(new_folder)

        while len(stack) > 0:
            item = stack.pop()
            # Attach parents list to the child:
            if isinstance(parents, list):
                item.parent_uids = list(map(lambda x: x.uid, parents))
            else:
                assert isinstance(parents, DisplayNode), f'Found instead: {type(parents)}'
                item.parent_uids = parents.uid
            change_tree.add_item(item, item.category, source_tree)
            parents = [item]

    def add_rename_right(self, left_item, right_item):
        """Make a FileToMove node which will rename a file within the right tree to match the relative path of
        the file on the left"""
        node_identifier = self.move_to_right(left_item)
        node_identifier.category = Category.Moved
        node = FileToMove(node_identifier=node_identifier, src_node=right_item)
        self._add_items_and_missing_parents(self.change_tree_right, self.right_tree, self.added_folders_right, node)

    def add_rename_left(self, left_item, right_item):
        """Make a FileToMove node which will rename a file within the left tree to match the relative path of the file on right"""
        node_identifier = self.move_to_left(right_item)
        node_identifier.category = Category.Moved
        node = FileToMove(node_identifier=node_identifier, src_node=left_item)
        self._add_items_and_missing_parents(self.change_tree_left, self.left_tree, self.added_folders_left, node)

    def add_filetoadd_left_to_right(self, left_item):
        """ADD: Left -> Right"""
        node_identifier = self.move_to_right(left_item)
        node_identifier.category = Category.Added
        node = FileToAdd(node_identifier=node_identifier, src_node=left_item)
        self._add_items_and_missing_parents(self.change_tree_right, self.right_tree, self.added_folders_right, node)

    def add_filetoadd_right_to_left(self, right_item):
        """ADD: Left <- Right"""
        node_identifier = self.move_to_left(right_item)
        node_identifier.category = Category.Added
        node = FileToAdd(node_identifier=node_identifier, src_node=right_item)
        self._add_items_and_missing_parents(self.change_tree_left, self.left_tree, self.added_folders_left, node)

    def add_fileupdate_left_to_right(self, left_item, right_item_to_overwrite):
        """UPDATE: Left -> Right"""
        node_identifier = self.move_to_right(left_item)
        node_identifier.category = Category.Updated
        node = FileToUpdate(node_identifier=node_identifier, src_node=left_item, dst_node=right_item_to_overwrite)
        self._add_items_and_missing_parents(self.change_tree_right, self.right_tree, self.added_folders_right, node)

    def add_fileupdate_right_to_left(self, right_item, left_item_to_overwrite):
        """UPDATE: Left <- Right"""
        node_identifier = self.move_to_left(right_item)
        node_identifier.category = Category.Updated
        node = FileToUpdate(node_identifier=node_identifier, src_node=right_item, dst_node=left_item_to_overwrite)
        self._add_items_and_missing_parents(self.change_tree_left, self.left_tree, self.added_folders_left, node)

