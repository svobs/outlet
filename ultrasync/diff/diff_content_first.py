"""Content-first diff. See diff function below."""
from typing import Iterable, List, Optional, Tuple

import file_util
import os
import logging

from constants import OBJ_TYPE_LOCAL_DISK, OBJ_TYPE_MIXED, ROOT_PATH, ROOT_UID
from index.two_level_dict import TwoLevelDict
from model import display_id
from model.category import Category
from model.display_id import Identifier, LogicalNodeIdentifier
from model.display_node import DisplayNode
from model.planning_node import FileToAdd, FileToMove
from model.subtree_snapshot import SubtreeSnapshot
from stopwatch_sec import Stopwatch
from ui.tree.category_display_tree import CategoryDisplayTree

logger = logging.getLogger(__name__)


def _compare_paths_for_same_md5(lefts: Iterable[DisplayNode], left_tree: SubtreeSnapshot,
                                rights: Iterable[DisplayNode], right_tree: SubtreeSnapshot, fixer) \
        -> List[Tuple[Optional[DisplayNode], Optional[DisplayNode]]]:
    if lefts is None:
        lefts = []
    if rights is None:
        rights = []

    orphaned_left: List[DisplayNode] = []
    orphaned_right: List[DisplayNode] = []

    for left in lefts:
        left_on_right: str = fixer.move_to_right(left)
        matches: List[DisplayNode] = right_tree.get_for_path(left_on_right)
        if not matches:
            orphaned_left.append(left)
        else:
            assert left.md5 == matches[0].md5
        # Else we matched path exactly: we can discard this entry

    for right in rights:
        right_on_left: str = fixer.move_to_left(right)
        matches: List[DisplayNode] = left_tree.get_for_path(right_on_left)
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


class PathTransplanter:
    def __init__(self, left_tree: SubtreeSnapshot, right_tree: SubtreeSnapshot,
                 change_tree_left: CategoryDisplayTree, change_tree_right: CategoryDisplayTree):
        self.left_tree = left_tree
        self.right_tree = right_tree
        self.change_tree_left = change_tree_left
        self.change_tree_right = change_tree_right

    def move_to_right(self, left_item) -> str:
        left_rel_path = left_item.get_relative_path(self.left_tree)
        return os.path.join(self.right_tree.root_path, left_rel_path)

    def move_to_left(self, right_item) -> str:
        right_rel_path = right_item.get_relative_path(self.right_tree)
        return os.path.join(self.left_tree.root_path, right_rel_path)

    def plan_rename_file_right(self, left_item, right_item):
        """Make a FileToMove node which will rename a file in the right tree to the name of the file on left"""
        dest_path = self.move_to_right(left_item)
        new_uid = self.right_tree.get_new_uid()
        identifier = self.right_tree.create_identifier(full_path=dest_path, uid=new_uid, category=Category.Moved)
        move_right_to_right = FileToMove(identifier=identifier, original_node=right_item)
        self.change_tree_right.add_item(move_right_to_right, Category.Moved, self.right_tree)

    def plan_rename_file_left(self, left_item, right_item):
        """Make a FileToMove node which will rename a file in the left tree to the name of the file on right"""
        dest_path = self.move_to_left(right_item)
        new_uid = self.left_tree.get_new_uid()
        identifier = self.left_tree.create_identifier(full_path=dest_path, uid=new_uid, category=Category.Moved)
        move_left_to_left = FileToMove(identifier=identifier, original_node=left_item)
        self.change_tree_left.add_item(move_left_to_left, Category.Moved, self.left_tree)

    def plan_add_file_left_to_right(self, left_item):
        dest_path = self.move_to_right(left_item)
        new_uid = self.right_tree.get_new_uid()
        identifier = self.right_tree.create_identifier(full_path=dest_path, uid=new_uid, category=Category.Added)
        file_to_add_to_right = FileToAdd(identifier=identifier, original_node=left_item)
        self.change_tree_right.add_item(file_to_add_to_right, Category.Added, self.right_tree)

    def plan_add_file_right_to_left(self, right_item):
        dest_path = self.move_to_left(right_item)
        new_uid = self.left_tree.get_new_uid()
        identifier = self.left_tree.create_identifier(full_path=dest_path, uid=new_uid, category=Category.Added)
        file_to_add_to_left = FileToAdd(identifier=identifier, original_node=right_item)
        self.change_tree_left.add_item(file_to_add_to_left, Category.Added, self.left_tree)


def diff(left_tree: SubtreeSnapshot, right_tree: SubtreeSnapshot, compare_paths_also=False) \
        -> Tuple[CategoryDisplayTree, CategoryDisplayTree]:
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

    change_tree_left = CategoryDisplayTree(left_tree.identifier)
    change_tree_right = CategoryDisplayTree(right_tree.identifier)
    fixer = PathTransplanter(left_tree, right_tree, change_tree_left, change_tree_right)

    # the set of MD5s already processed
    md5_set_stopwatch = Stopwatch()
    left_md5s: TwoLevelDict = left_tree.get_md5_dict()
    right_md5s: TwoLevelDict = right_tree.get_md5_dict()
    md5_set = left_md5s.keys() | right_md5s.keys()
    logger.info(f'{md5_set_stopwatch} Found {len(md5_set)} MD5s')

    # List of lists of FMetas which do not have a matching md5 on the other side.
    # We will compare these by path.
    # Note: each list within this list contains duplicates (FMetas with the same md5)
    orphaned_md5s_left: List[Iterable[DisplayNode]] = []
    orphaned_md5s_right: List[Iterable[DisplayNode]] = []

    """Compares the two trees, and populates the change sets of both. The order of 'left' and which is 'right'
     is not important, because the changes are computed from each tree's perspective (e.g. a file which is in
     Left but not Right will be determined to be an 'added' file from the perspective of Left but a 'deleted'
     file from the perspective of Right)"""
    for md5 in md5_set:
        left_items_dup_md5: Iterable[DisplayNode] = left_md5s.get_second_dict(md5)
        if isinstance(left_items_dup_md5, dict):
            left_items_dup_md5 = left_items_dup_md5.values()

        right_items_dup_md5: Iterable[DisplayNode] = right_md5s.get_second_dict(md5)
        if isinstance(right_items_dup_md5, dict):
            right_items_dup_md5 = right_items_dup_md5.values()

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

            orphaned_left_dup_md5: List[DisplayNode] = []
            orphaned_right_dup_md5: List[DisplayNode] = []

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
                path_matches_right: List[DisplayNode] = right_tree.get_for_path(left_on_right_path)
                if path_matches_right:
                    if len(path_matches_right) > 1:
                        # If this ever happens it is a bug
                        raise RuntimeError(f'More than one match for path: {left_on_right_path}')
                    # UPDATED
                    if logger.isEnabledFor(logging.DEBUG):
                        logger.debug(f'File updated: {left_item.md5} <- "{left_tree.get_full_path_for_item(left_item)}" -> {path_matches_right[0].md5}')
                    # Same path, different md5 -> Updated
                    change_tree_right.add_item(path_matches_right[0], Category.Updated, right_tree)
                    change_tree_left.add_item(left_item, Category.Updated, left_tree)
                    count_updated_pairs += 1
                    continue
                # No match? fall through
            # DUPLICATE ADDED on right + DELETED on left
            if logger.isEnabledFor(logging.DEBUG):
                logger.debug(f'Left has new file: "{left_tree.get_full_path_for_item(left_item)}"')
            fixer.plan_add_file_left_to_right(left_item)

            # TODO: rename 'Deleted' category to 'ToDelete'
            # Dead node walking:
            change_tree_left.add_item(left_item, Category.Deleted, left_tree)
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
            change_tree_right.add_item(right_item, Category.Deleted, right_tree)
            count_add_delete_pairs += 1

    logger.info(f'Done with diff (pairs: add/del={count_add_delete_pairs} upd={count_updated_pairs} moved={count_moved_pairs})'
                f' Left:[{change_tree_left.get_summary()}] Right:[{change_tree_right.get_summary()}]')

    # Copy ignored items to change trees:
    for item in left_tree.get_ignored_items():
        change_tree_left.add_item(item, Category.Ignored, left_tree)
    for item in right_tree.get_ignored_items():
        change_tree_right.add_item(item, Category.Ignored, right_tree)

    return change_tree_left, change_tree_right


def merge_change_trees(left_tree: SubtreeSnapshot, right_tree: SubtreeSnapshot,
                       left_selected_changes: List[DisplayNode], right_selected_changes: List[DisplayNode],
                       check_for_conflicts=False) -> CategoryDisplayTree:
    is_mixed_tree = left_tree.tree_type != right_tree.tree_type
    if is_mixed_tree:
        root = LogicalNodeIdentifier(uid=ROOT_UID, full_path=ROOT_PATH, category=Category.NA, tree_type=OBJ_TYPE_MIXED)
    else:
        # FIXME: this needs support for GDrive<->GDrive
        assert left_tree.tree_type == OBJ_TYPE_LOCAL_DISK

        new_root_path = file_util.find_nearest_common_ancestor(left_tree.root_path, right_tree.root_path)
        root: Identifier = display_id.for_values(tree_type=left_tree.tree_type, full_path=new_root_path, uid=left_tree.get_new_uid())

    merged_tree = CategoryDisplayTree(root=root, extra_node_for_type=True)

    for item in left_selected_changes:
        merged_tree.add_item(item, item.category, left_tree)

    for item in right_selected_changes:
        merged_tree.add_item(item, item.category, right_tree)

    # TODO: check for conflicts

    return merged_tree
