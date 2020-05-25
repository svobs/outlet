"""Content-first diff. See diff function below."""
import pathlib
import time
from collections import deque
from typing import Dict, Iterable, List, Optional, Tuple

import os
import logging

from constants import OBJ_TYPE_GDRIVE, OBJ_TYPE_LOCAL_DISK, OBJ_TYPE_MIXED, ROOT_PATH
from index import uid_generator
from index.two_level_dict import TwoLevelDict
from model.category import Category
from model.node_identifier import NodeIdentifier, LogicalNodeIdentifier, NodeIdentifierFactory
from model.display_node import DisplayNode
from model.goog_node import FolderToAdd
from model.planning_node import FileDecoratorNode, FileToAdd, FileToMove, FileToUpdate
from model.subtree_snapshot import SubtreeSnapshot
from stopwatch_sec import Stopwatch
from ui.tree.category_display_tree import CategoryDisplayTree

logger = logging.getLogger(__name__)


class ContentFirstDiffer:
    def __init__(self, left_tree: SubtreeSnapshot, right_tree: SubtreeSnapshot, application):
        self.left_tree = left_tree
        self.right_tree = right_tree
        self.application = application
        self.uid_generator = application.uid_generator

        self.change_tree_left: CategoryDisplayTree = CategoryDisplayTree(application, self.left_tree.node_identifier)
        self.change_tree_right: CategoryDisplayTree = CategoryDisplayTree(application, self.right_tree.node_identifier)

        self.added_folders_left: Dict[str, FolderToAdd] = {}
        self.added_folders_right: Dict[str, FolderToAdd] = {}

    def move_to_right(self, left_item) -> str:
        left_rel_path = left_item.get_relative_path(self.left_tree)
        return os.path.join(self.right_tree.root_path, left_rel_path)

    def move_to_left(self, right_item) -> str:
        right_rel_path = right_item.get_relative_path(self.right_tree)
        return os.path.join(self.left_tree.root_path, right_rel_path)

    def _add_items_and_missing_parents(self, change_tree: CategoryDisplayTree, source_tree: SubtreeSnapshot,
                                       added_folders_dict: Dict[str, FolderToAdd] , new_item: FileDecoratorNode):
        # This only applies to GoogNodes. Just a no-op for regular files at present because we don't yet care about local dirs very much
        if new_item.node_identifier.tree_type != OBJ_TYPE_GDRIVE:
            change_tree.add_item(new_item, new_item.category, source_tree)
            return
        path = new_item.full_path
        queue = deque()
        queue.append(new_item)

        parents = None
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

            logger.debug(f'Adding new GoogFolder for {path}')
            new_uid = self.uid_generator.get_new_uid()
            new_folder = FolderToAdd(new_uid, path)
            added_folders_dict[path] = new_folder
            queue.append(new_folder)

        while len(queue) > 0:
            item = queue.pop()
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
        dest_path = self.move_to_right(left_item)
        new_uid = self.uid_generator.get_new_uid()
        node_identifier = self.right_tree.create_identifier(full_path=dest_path, uid=new_uid, category=Category.Moved)
        node = FileToMove(node_identifier=node_identifier, src_node=right_item)
        self._add_items_and_missing_parents(self.change_tree_right, self.right_tree, self.added_folders_right, node)

    def add_rename_left(self, left_item, right_item):
        """Make a FileToMove node which will rename a file within the left tree to match the relative path of the file on right"""
        dest_path = self.move_to_left(right_item)
        new_uid = self.uid_generator.get_new_uid()
        node_identifier = self.left_tree.create_identifier(full_path=dest_path, uid=new_uid, category=Category.Moved)
        node = FileToMove(node_identifier=node_identifier, src_node=left_item)
        self._add_items_and_missing_parents(self.change_tree_left, self.left_tree, self.added_folders_left, node)

    def add_filetoadd_left_to_right(self, left_item):
        """ADD - Left -> Right"""
        dest_path = self.move_to_right(left_item)
        new_uid = self.uid_generator.get_new_uid()
        node_identifier = self.right_tree.create_identifier(full_path=dest_path, uid=new_uid, category=Category.Added)
        node = FileToAdd(node_identifier=node_identifier, src_node=left_item)
        self._add_items_and_missing_parents(self.change_tree_right, self.right_tree, self.added_folders_right, node)

    def add_filetoadd_right_to_left(self, right_item):
        """ADD - Left <- Right"""
        dest_path = self.move_to_left(right_item)
        new_uid = self.uid_generator.get_new_uid()
        node_identifier = self.left_tree.create_identifier(full_path=dest_path, uid=new_uid, category=Category.Added)
        node = FileToAdd(node_identifier=node_identifier, src_node=right_item)
        self._add_items_and_missing_parents(self.change_tree_left, self.left_tree, self.added_folders_left, node)

    def add_fileupdate_left_to_right(self, left_item, right_item_to_overwrite):
        """UPDATE - Left -> Right"""
        dest_path = self.move_to_right(left_item)
        new_uid = self.uid_generator.get_new_uid()
        node_identifier = self.right_tree.create_identifier(full_path=dest_path, uid=new_uid, category=Category.Updated)
        node = FileToUpdate(node_identifier=node_identifier, src_node=left_item, dst_node=right_item_to_overwrite)
        self._add_items_and_missing_parents(self.change_tree_right, self.left_tree, self.added_folders_right, node)

    def add_fileupdate_right_to_left(self, right_item, left_item_to_overwrite):
        """ADD - Left <- Right"""
        dest_path = self.move_to_left(right_item)
        new_uid = self.uid_generator.get_new_uid()
        node_identifier = self.left_tree.create_identifier(full_path=dest_path, uid=new_uid, category=Category.Updated)
        node = FileToUpdate(node_identifier=node_identifier, src_node=right_item, dst_node=left_item_to_overwrite)
        self._add_items_and_missing_parents(self.change_tree_left, self.left_tree, self.added_folders_left, node)

    def _compare_paths_for_same_md5(self, lefts: Iterable[DisplayNode], rights: Iterable[DisplayNode]) \
            -> List[Tuple[Optional[DisplayNode], Optional[DisplayNode]]]:
        if lefts is None:
            lefts = []
        if rights is None:
            rights = []

        orphaned_left: List[DisplayNode] = []
        orphaned_right: List[DisplayNode] = []

        for left in lefts:
            left_on_right: str = self.move_to_right(left)
            matches: List[DisplayNode] = self.right_tree.get_for_path(left_on_right)
            if not matches:
                orphaned_left.append(left)
            else:
                assert left.md5 == matches[0].md5
            # Else we matched path exactly: we can discard this entry

        for right in rights:
            right_on_left: str = self.move_to_left(right)
            matches: List[DisplayNode] = self.left_tree.get_for_path(right_on_left)
            if not matches:
                orphaned_right.append(right)
            # Else we matched path exactly: we can discard this entry

        num_lefts: int = len(orphaned_left)
        num_rights: int = len(orphaned_right)

        compare_result: List[Tuple[Optional[DisplayNode], Optional[DisplayNode]]] = []
        i = 0
        while i < num_lefts and i < num_rights:
            compare_result.append((orphaned_left[i], orphaned_right[i]))
            i += 1

        j = i
        while j < num_lefts:
            compare_result.append((orphaned_left[j], None))
            j += 1

        j = i
        while j < num_rights:
            compare_result.append((None, orphaned_right[j]))
            j += 1

        return compare_result

    def diff(self, compare_paths_also=False) -> Tuple[CategoryDisplayTree, CategoryDisplayTree]:
        """Use this method if we mostly care about having the same unique files *somewhere* in
           each tree (in other words, we care about file contents, and care less about where each
           file is placed). If a file is found with the same signature on both sides but with
           different paths, it is assumed to be renamed/moved.

           Rough algorithm for categorization:
           1. Is file an ignored type? -> IGNORED
           2. For all unique signatures:
           2a. File's signature and path exists on both sides? --> NONE
           2b. File's signature is found on both sides but path is different? --> MOVED
           2c. All files not matched in (2) are orphans.
           3. For all orphans:
           3a. File's path is same on both sides but signature is different? --> UPDATED
           3b. File's signature and path are unique to target side --> DELETED
           3c. File's signature and path are unique to opposite side --> ADDED
           """
        logger.info('Diffing files by MD5...')
        count_add_delete_pairs = 0
        count_moved_pairs = 0
        count_updated_pairs = 0

        # the set of MD5s already processed
        md5_set_stopwatch = Stopwatch()
        left_md5s: TwoLevelDict = self.left_tree.get_md5_dict()
        right_md5s: TwoLevelDict = self.right_tree.get_md5_dict()
        md5_set = left_md5s.keys() | right_md5s.keys()
        logger.info(f'{md5_set_stopwatch} Found {len(md5_set)} combined MD5s')

        # List of lists of FMetas which do not have a matching md5 on the other side.
        # We will compare these by path.
        # Note: each list within this list contains duplicates (FMetas with the same md5)
        md5s_left_only: List[Iterable[DisplayNode]] = []
        md5s_right_only: List[Iterable[DisplayNode]] = []

        """Compares the two trees, and populates the change sets of both. The order of 'left' and which is 'right'
         is not important, because the changes are computed from each tree's perspective (e.g. a file which is in
         Left but not Right will be determined to be an 'added' file from the perspective of Left but a 'deleted'
         file from the perspective of Right)"""

        sw = Stopwatch()
        for md5 in md5_set:
            # Grant just a tiny bit of time to other tasks in the CPython thread (e.g. progress bar):
            time.sleep(0.00001)

            # Set of items on left with same MD5:
            left_items_single_md5: Iterable[DisplayNode] = left_md5s.get_second_dict(md5)
            if isinstance(left_items_single_md5, dict):
                left_items_single_md5 = left_items_single_md5.values()

            right_items_single_md5: Iterable[DisplayNode] = right_md5s.get_second_dict(md5)
            if isinstance(right_items_single_md5, dict):
                right_items_single_md5 = right_items_single_md5.values()

            if left_items_single_md5 is None:
                md5s_right_only.append(right_items_single_md5)
            elif right_items_single_md5 is None:
                md5s_left_only.append(left_items_single_md5)
            elif compare_paths_also:
                """If we do this, we care about what the files are named, where they are located, and how many
                duplicates exist. When it comes to determining the direction of renamed files, we simply don't
                have enough info to be conclusive, but as this is a secondary concern (our first is ensuring
                we have *any* matching signatures (see above)) I am ok with making a best guess and letting the
                user make the final call via the UI. Here we can choose either to use the modification times
                (newer is assumed to be the rename destination), or for each side to assume it is the destination
                (similar to how we handle missing signatures above)"""

                orphaned_left_dup_md5: List[DisplayNode] = []
                orphaned_right_dup_md5: List[DisplayNode] = []

                compare_result = self._compare_paths_for_same_md5(left_items_single_md5, right_items_single_md5)
                for (changed_left, changed_right) in compare_result:
                    # Did we at least find a pair?
                    if changed_left is not None and changed_right is not None:
                        # MOVED: the file already exists in each tree, so just do a rename within the tree
                        # (it is possible that the trees are on different disks, so keep performance in mind)
                        self.add_rename_right(changed_left, changed_right)

                        self.add_rename_left(changed_left, changed_right)
                        count_moved_pairs += 1
                    else:
                        """Looks like one side has additional file(s) with same signature 
                           - essentially a duplicate.. Remember, we know each side already contains
                           at least one copy with the given signature"""
                        if changed_left is None:
                            orphaned_right_dup_md5.append(changed_right)
                        elif changed_right is None:
                            orphaned_left_dup_md5.append(changed_left)
                if orphaned_left_dup_md5:
                    md5s_left_only.append(orphaned_left_dup_md5)
                if orphaned_right_dup_md5:
                    md5s_right_only.append(orphaned_right_dup_md5)
        logger.info(f'{sw} Finished first pass of MD5 set')

        sw = Stopwatch()
        # Each is a list of duplicate MD5s (but different paths) on left side only:
        for dup_md5s_left in md5s_left_only:
            # TODO: Duplicate content (options):
            #  - No special handling of duplicates / treat like other files [default]
            #  - Flag added/missing duplicates as Duplicates
            #  - For each unique, compare only the best match on each side and ignore the rest
            for left_item in dup_md5s_left:
                if compare_paths_also:
                    left_on_right_path = self.move_to_right(left_item)
                    path_matches_right: List[DisplayNode] = self.right_tree.get_for_path(left_on_right_path)
                    if path_matches_right:
                        if len(path_matches_right) > 1:
                            # If this ever happens it is a bug
                            raise RuntimeError(f'More than one match for path: {left_on_right_path}')
                        # UPDATED
                        if logger.isEnabledFor(logging.DEBUG):
                            logger.debug(f'File updated: {left_item.md5} <- "{self.left_tree.get_full_path_for_item(left_item)}" -> {path_matches_right[0].md5}')
                        # Same path, different md5 -> Updated
                        self.add_fileupdate_right_to_left(path_matches_right[0], left_item)
                        self.add_fileupdate_left_to_right(left_item, path_matches_right[0])
                        count_updated_pairs += 1
                        continue
                    # No match? fall through
                # DUPLICATE ADDED on right + DELETED on left
                if logger.isEnabledFor(logging.DEBUG):
                    logger.debug(f'Left has new file: "{self.left_tree.get_full_path_for_item(left_item)}"')
                self.add_filetoadd_left_to_right(left_item)

                # Dead node walking:
                self.change_tree_left.add_item(left_item, Category.Deleted, self.left_tree)
                count_add_delete_pairs += 1
        logger.info(f'{sw} Finished path comparison for left tree')

        sw = Stopwatch()
        for dup_md5s_right in md5s_right_only:
            for right_item in dup_md5s_right:
                if compare_paths_also:
                    right_on_left = self.move_to_left(right_item)
                    if self.left_tree.get_for_path(right_on_left):
                        # UPDATED. Logically this has already been covered (above) since our iteration is symmetrical:
                        continue
                # DUPLICATE ADDED on right + DELETED on left
                if logger.isEnabledFor(logging.DEBUG):
                    logger.debug(f'Right has new file: "{self.right_tree.get_full_path_for_item(right_item)}"')
                self.add_filetoadd_right_to_left(right_item)

                # Dead node walking:
                self.change_tree_right.add_item(right_item, Category.Deleted, self.right_tree)
                count_add_delete_pairs += 1

        logger.info(f'Done with diff (pairs: add/del={count_add_delete_pairs} upd={count_updated_pairs} moved={count_moved_pairs})'
                    f' Left:[{self.change_tree_left.get_summary()}] Right:[{self.change_tree_right.get_summary()}]')
        logger.info(f'{sw} Finished path comparison for right tree')

        # Copy ignored items to change trees:
        for item in self.left_tree.get_ignored_items():
            self.change_tree_left.add_item(item, Category.Ignored, self.left_tree)
        for item in self.right_tree.get_ignored_items():
            self.change_tree_right.add_item(item, Category.Ignored, self.right_tree)

        return self.change_tree_left, self.change_tree_right

    def merge_change_trees(self, left_selected_changes: List[DisplayNode], right_selected_changes: List[DisplayNode],
                           check_for_conflicts=False) -> CategoryDisplayTree:

        # always root path, but tree type may differ
        is_mixed_tree = self.left_tree.tree_type != self.right_tree.tree_type
        if is_mixed_tree:
            root_node_identifier = LogicalNodeIdentifier(uid=uid_generator.ROOT_UID, full_path=ROOT_PATH, category=Category.NA,
                                                         tree_type=OBJ_TYPE_MIXED)
        else:
            root_node_identifier: NodeIdentifier = self.application.node_identifier_factory.for_values(tree_type=self.left_tree.tree_type,
                                                                                           full_path=ROOT_PATH, uid=uid_generator.ROOT_UID)

        merged_tree = CategoryDisplayTree(root_node_identifier=root_node_identifier, show_whole_forest=True,
                                          application=self.application)

        for item in left_selected_changes:
            merged_tree.add_item(item, item.category, self.left_tree)

        for item in right_selected_changes:
            merged_tree.add_item(item, item.category, self.right_tree)

        # TODO: check for conflicts

        return merged_tree
