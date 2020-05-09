"""Content-first diff. See diff function below."""
from typing import List, Optional

import file_util
import os
import logging

from model.category import Category
from model.display_node import DisplayNode
from model.fmeta_tree import FMetaTree
from model.planning_node import FileToAdd, FileToMove
from model.subtree_snapshot import SubtreeSnapshot
from stopwatch_sec import Stopwatch

logger = logging.getLogger(__name__)


def _compare_paths_for_same_md5(lefts, left_tree: SubtreeSnapshot, rights: Optional[List[DisplayNode]], right_tree: SubtreeSnapshot, fixer):
    if lefts is None:
        lefts = []
    if rights is None:
        rights = []

    orphaned_left: List[DisplayNode] = []
    orphaned_right: List[DisplayNode] = []

    for left in lefts:
        left_on_right = fixer.move_to_right(left)
        match = right_tree.get_for_path(left_on_right)
        if not match:
            orphaned_left.append(left)
        # Else we matched path exactly: we can discard this entry

    for right in rights:
        right_on_left = fixer.move_to_left(right)
        match = left_tree.get_for_path(right_on_left)
        if not match:
            orphaned_right.append(right)
        # Else we matched path exactly: we can discard this entry

    num_lefts = len(orphaned_left)
    num_rights = len(orphaned_right)

    compare_result = []
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


class PathTransplanter:
    def __init__(self, left_tree: SubtreeSnapshot, right_tree: SubtreeSnapshot):
        self.left_tree = left_tree
        self.right_tree = right_tree

    def move_to_right(self, left_item) -> str:
        left_rel_path = left_item.get_relative_path(self.left_tree)
        return os.path.join(self.right_tree.root_path, left_rel_path)

    def move_to_left(self, right_item) -> str:
        right_rel_path = right_item.get_relative_path(self.right_tree)
        return os.path.join(self.left_tree.root_path, right_rel_path)

    def plan_rename_file_right(self, left_item, right_item):
        """Make a FileToMove node which will rename a file in the right tree to the name of the file on left"""
        dest_path = self.move_to_right(left_item)
        orig_path = self.right_tree.get_full_path_for_item(right_item)
        identifier = self.right_tree.create_identifier(full_path=dest_path, category=Category.Moved)
        move_right_to_right = FileToMove(identifier=identifier, orig_path=orig_path, original_node=right_item)
        self.right_tree.add_item(move_right_to_right)

    def plan_rename_file_left(self, left_item, right_item):
        """Make a FileToMove node which will rename a file in the left tree to the name of the file on right"""
        dest_path = self.move_to_left(right_item)
        orig_path = self.left_tree.get_full_path_for_item(left_item)
        identifier = self.left_tree.create_identifier(full_path=dest_path, category=Category.Moved)
        move_left_to_left = FileToMove(identifier=identifier, orig_path=orig_path, original_node=left_item)
        self.left_tree.add_item(move_left_to_left)

    def plan_add_file_left_to_right(self, left_item):
        dest_path = self.move_to_right(left_item)
        orig_path = self.left_tree.get_full_path_for_item(left_item)
        identifier = self.right_tree.create_identifier(full_path=dest_path, category=Category.Added)
        file_to_add_to_right = FileToAdd(identifier=identifier, orig_path=orig_path, original_node=left_item)
        self.right_tree.add_item(file_to_add_to_right)

    def plan_add_file_right_to_left(self, right_item):
        dest_path = self.move_to_left(right_item)
        orig_path = self.right_tree.get_full_path_for_item(right_item)
        identifier = self.left_tree.create_identifier(full_path=dest_path, category=Category.Added)
        file_to_add_to_left = FileToAdd(identifier=identifier, orig_path=orig_path, original_node=right_item)
        self.left_tree.add_item(file_to_add_to_left)


def diff(left_tree: SubtreeSnapshot, right_tree: SubtreeSnapshot, compare_paths_also=False):
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

    fixer = PathTransplanter(left_tree, right_tree)

    left_tree.clear_categories()
    right_tree.clear_categories()

    # the set of MD5s already processed
    md5_set_stopwatch = Stopwatch()
    md5_set = left_tree.get_md5_set() | right_tree.get_md5_set()
    logger.info(f'{md5_set_stopwatch} Found {len(md5_set)} MD5s')

    # List of lists of FMetas which do not have a matching md5 on the other side.
    # We will compare these by path.
    # Note: each list within this list contains duplicates (FMetas with the same md5)
    orphaned_md5s_left = []
    orphaned_md5s_right = []

    """Compares the two trees, and populates the change sets of both. The order of 'left' and which is 'right'
     is not important, because the changes are computed from each tree's perspective (e.g. a file which is in
     Left but not Right will be determined to be an 'added' file from the perspective of Left but a 'deleted'
     file from the perspective of Right)"""
    for md5 in md5_set:
        right_items_dup_md5 = right_tree.get_for_md5(md5)
        if isinstance(right_items_dup_md5, dict):
            right_items_dup_md5 = right_items_dup_md5.values()
        left_items_dup_md5 = left_tree.get_for_md5(md5)
        if isinstance(left_items_dup_md5, dict):
            left_items_dup_md5 = right_items_dup_md5.values()

        if left_items_dup_md5 is None:
            orphaned_md5s_right.append(right_items_dup_md5)
        elif right_items_dup_md5 is None:
            orphaned_md5s_left.append(left_items_dup_md5)
        elif compare_paths_also:
            """If we do this, we care about what the files are named, where they are located, and how many
            duplicates exist. When it comes to determining the direction of renamed files, we simply don't
            have enough info to be conclusive, but as this is a secondary concern (our first is ensuring
            we have *any* matching signatures (see above)) I am ok with making a best guess and letting the
            user make the final call via the UI. Here we can choose either to use the modification times
            (newer is assumed to be the rename destination), or for each side to assume it is the destination
            (similar to how we handle missing signatures above)"""

            orphaned_left_dup_md5 = []
            orphaned_right_dup_md5 = []

            compare_result = _compare_paths_for_same_md5(left_items_dup_md5, left_tree, right_items_dup_md5, right_tree, fixer)
            for (changed_left, changed_right) in compare_result:
                # Did we at least find a pair?
                if changed_left is not None and changed_right is not None:
                    # MOVED: the file already exists in each tree, so just do a rename within the tree
                    # (it is possible that the trees are on different disks, so keep performance in mind)
                    fixer.plan_rename_file_right(changed_left, changed_right)

                    fixer.plan_rename_file_left(changed_left, changed_right)
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
                orphaned_md5s_left.append(orphaned_left_dup_md5)
            if orphaned_right_dup_md5:
                orphaned_md5s_right.append(orphaned_right_dup_md5)

    for item_duplicate_md5s_left in orphaned_md5s_left:
        # TODO: Duplicate content (options):
        #  - No special handling of duplicates / treat like other files [default]
        #  - Flag added/missing duplicates as Duplicates
        #  - For each unique, compare only the best match on each side and ignore the rest
        for left_item in item_duplicate_md5s_left:
            if compare_paths_also:
                left_on_right_path = fixer.move_to_right(left_item)
                matching_right = right_tree.get_for_path(left_on_right_path)
                if matching_right:
                    # UPDATED
                    if logger.isEnabledFor(logging.DEBUG):
                        logger.debug(f'File updated: {left_item.md5} <- "{left_tree.get_full_path_for_item(left_item)}" -> {matching_right.md5}')
                    # Same path, different md5 -> Updated
                    right_tree.categorize(matching_right, Category.Updated)
                    left_tree.categorize(left_item, Category.Updated)
                    count_updated_pairs += 1
                    continue
                # No match? fall through
            # DUPLICATE ADDED on right + DELETED on left
            if logger.isEnabledFor(logging.DEBUG):
                logger.debug(f'Left has new file: "{left_tree.get_full_path_for_item(left_item)}"')
            fixer.plan_add_file_left_to_right(left_item)

            # TODO: rename 'Deleted' category to 'ToDelete'
            # Dead node walking:
            left_tree.categorize(left_item, Category.Deleted)
            count_add_delete_pairs += 1

    for item_duplicate_md5s_right in orphaned_md5s_right:
        for right_item in item_duplicate_md5s_right:
            if compare_paths_also:
                right_on_left = fixer.move_to_left(right_item)
                if left_tree.get_for_path(right_on_left):
                    # UPDATED. Logically this has already been covered (above) since our iteration is symmetrical:
                    continue
            # DUPLICATE ADDED on right + DELETED on left
            if logger.isEnabledFor(logging.DEBUG):
                logger.debug(f'Right has new file: "{right_tree.get_full_path_for_item(right_item)}"')
            fixer.plan_add_file_right_to_left(right_item)

            # Dead node walking:
            right_tree.categorize(right_item, Category.Deleted)
            count_add_delete_pairs += 1

    logger.info(f'Done with diff (pairs: add/del={count_add_delete_pairs} upd={count_updated_pairs} moved={count_moved_pairs})'
                f' Left:[{left_tree.get_category_summary_string()}] Right:[{right_tree.get_category_summary_string()}]')

    if logger.isEnabledFor(logging.DEBUG):
        logger.debug('Validating categories on Left...')
        left_tree.validate_categories()
        logger.debug('Validating categories on Right...')
        right_tree.validate_categories()

    return left_tree, right_tree


def merge_change_trees(left_tree: SubtreeSnapshot, right_tree: SubtreeSnapshot, check_for_conflicts=True):
    new_root_path = file_util.find_nearest_common_ancestor(left_tree.root_path, right_tree.root_path)
    merged_tree = FMetaTree(root_path=new_root_path)

    md5_set = left_tree.get_md5_set() | right_tree.get_md5_set()

    fixer = PathTransplanter(left_tree, right_tree)

    conflict_pairs = []
    for md5 in md5_set:
        right_items_dup_md5 = right_tree.get_for_md5(md5)
        left_items_dup_md5 = left_tree.get_for_md5(md5)

        if check_for_conflicts and left_items_dup_md5 and right_items_dup_md5:
            compare_result = _compare_paths_for_same_md5(left_items_dup_md5, left_tree, right_items_dup_md5, right_tree, fixer)
            # Adds and deletes of the same file cancel each other out via the matching algo...
            # Maybe just delete the conflict detection code because it will now never be hit.
            for (left, right) in compare_result:
                # Finding a pair here indicates a conflict
                if left is not None and right is not None:
                    conflict_pairs.append((left, right))
                    logger.debug(f'CONFLICT: left={left.category.name}:{left_tree.get_full_path_for_item(left)} '
                                 f'right={right.category.name}:{right_tree.get_full_path_for_item(right)}')
        else:
            if left_items_dup_md5:
                for node in left_items_dup_md5:
                    merged_tree.add_item(node)
            if right_items_dup_md5:
                for node in right_items_dup_md5:
                    merged_tree.add_item(node)

    if len(conflict_pairs) > 0:
        logger.info(f'Number of conflicts found: {len(conflict_pairs)}')
        return None, conflict_pairs
    else:
        return merged_tree, None
