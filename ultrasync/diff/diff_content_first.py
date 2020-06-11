"""Content-first diff. See diff function below."""
import logging
import time
from typing import Dict, Iterable, List, Optional, Tuple

from constants import ROOT_PATH, TREE_TYPE_MIXED
from diff.change_maker import ChangeMaker
from index import uid_generator
from index.two_level_dict import TwoLevelDict
from model.category import Category
from model.display_node import DisplayNode
from model.node_identifier import LogicalNodeIdentifier, NodeIdentifier
from model.subtree_snapshot import SubtreeSnapshot
from stopwatch_sec import Stopwatch
from ui.actions import ID_MERGE_TREE
from ui.tree.category_display_tree import CategoryDisplayTree

logger = logging.getLogger(__name__)


class DisplayNodePair:
    def __init__(self, left: DisplayNode = None, right: DisplayNode = None):
        self.left: Optional[DisplayNode] = left
        self.right: Optional[DisplayNode] = right


class ContentFirstDiffer(ChangeMaker):
    def __init__(self, left_tree: SubtreeSnapshot, right_tree: SubtreeSnapshot, application):
        super().__init__(left_tree, right_tree, application)

    def _compare_paths_for_same_md5(self, lefts: Iterable[DisplayNode], rights: Iterable[DisplayNode]) -> Iterable[DisplayNodePair]:
        compare_result: List[DisplayNodePair] = []

        if lefts is None:
            for right in rights:
                compare_result.append(DisplayNodePair(None, right))
            return compare_result
        if rights is None:
            for left in lefts:
                compare_result.append(DisplayNodePair(None, left))
            return compare_result

        # key is a relative path
        left_dict: Dict[str, DisplayNode] = {}

        for left in lefts:
            left_rel_path = left.get_relative_path(self.left_tree)
            left_dict[left_rel_path] = left

        right_list: List[DisplayNode] = []
        for right in rights:
            right_rel_path = right.get_relative_path(self.right_tree)
            match = left_dict.pop(right_rel_path, None)
            # a match means same MD5, same path: we can ignore
            if not match:
                right_list.append(right)

        # Assign arbitrary items on left and right to pairs (treat as renames/moves):
        while len(left_dict) > 0 and len(right_list) > 0:
            pair = DisplayNodePair()
            pair.left = left_dict.popitem()[1]
            pair.right = right_list.pop()
            compare_result.append(pair)

        # Lefts without a matching right
        while len(left_dict) > 0:
            pair = DisplayNodePair()
            pair.left = left_dict.popitem()[1]
            compare_result.append(pair)

        # Rights without a matching left
        while len(right_list) > 0:
            pair = DisplayNodePair()
            pair.right = right_list.pop()
            compare_result.append(pair)

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
        logger.debug(f'{md5_set_stopwatch} Found {len(md5_set)} combined MD5s')

        # List of list of items which do not have a matching md5 on the other side.
        # We will compare these by path.
        # Note: each list within this list contains duplicates (item with the same md5)
        list_of_lists_of_left_items_for_given_md5: List[Iterable[DisplayNode]] = []
        list_of_lists_of_right_items_for_given_md5: List[Iterable[DisplayNode]] = []

        """Compares the two trees, and populates the change sets of both. The order of 'left' and which is 'right'
         is not important, because the changes are computed from each tree's perspective (e.g. a file which is in
         Left but not Right will be determined to be an 'added' file from the perspective of Left but a 'deleted'
         file from the perspective of Right)"""

        sw = Stopwatch()
        for md5 in md5_set:
            # Grant just a tiny bit of time to other tasks in the CPython thread (e.g. progress bar):
            time.sleep(0.00001)

            # Set of items on left with same MD5:
            left_items_for_given_md5: Iterable[DisplayNode] = left_md5s.get_second_dict(md5).values()
            right_items_for_given_md5: Iterable[DisplayNode] = right_md5s.get_second_dict(md5).values()

            if left_items_for_given_md5 is None:
                # Content is only present on RIGHT side
                list_of_lists_of_right_items_for_given_md5.append(right_items_for_given_md5)
            elif right_items_for_given_md5 is None:
                # Content is only present on LEFT side
                list_of_lists_of_left_items_for_given_md5.append(left_items_for_given_md5)
            elif compare_paths_also:
                # Content is present on BOTH sides but paths may be different
                """If we do this, we care about what the files are named, where they are located, and how many
                duplicates exist. When it comes to determining the direction of renamed files, we simply don't
                have enough info to be conclusive, but as this is a secondary concern (our first is ensuring
                we have *any* matching signatures (see above)) I am ok with making a best guess and letting the
                user make the final call via the UI. Here we can choose either to use the modification times
                (newer is assumed to be the rename destination), or for each side to assume it is the destination
                (similar to how we handle missing signatures above)"""

                orphaned_left_dup_md5: List[DisplayNode] = []
                orphaned_right_dup_md5: List[DisplayNode] = []

                compare_result: Iterable[DisplayNodePair] = self._compare_paths_for_same_md5(left_items_for_given_md5, right_items_for_given_md5)
                for pair in compare_result:
                    # Did we at least find a pair?
                    if pair.left and pair.right:
                        # MOVED: the file already exists in each tree, so just do a rename within the tree
                        # (it is possible that the trees are on different disks, so keep performance in mind)
                        self.add_rename_right(pair.left, pair.right)

                        self.add_rename_left(pair.left, pair.right)
                        count_moved_pairs += 1
                    else:
                        """Looks like one side has additional file(s) with same signature 
                           - essentially a duplicate.. Remember, we know each side already contains
                           at least one copy with the given signature"""
                        if pair.left is None:
                            orphaned_right_dup_md5.append(pair.right)
                        elif pair.right is None:
                            orphaned_left_dup_md5.append(pair.left)
                if orphaned_left_dup_md5:
                    list_of_lists_of_left_items_for_given_md5.append(orphaned_left_dup_md5)
                if orphaned_right_dup_md5:
                    list_of_lists_of_right_items_for_given_md5.append(orphaned_right_dup_md5)
        logger.debug(f'{sw} Finished first pass of MD5 set')

        sw = Stopwatch()
        # Each is a list of duplicate MD5s (but different paths) on left side only (i.e. orphaned):
        for list_of_left_items_for_given_md5 in list_of_lists_of_left_items_for_given_md5:
            # TODO: Duplicate content (options):
            #  - No special handling of duplicates / treat like other files [default]
            #  - Flag added/missing duplicates as Duplicates
            #  - For each unique, compare only the best match on each side and ignore the rest
            for left_item in list_of_left_items_for_given_md5:
                if compare_paths_also:
                    left_on_right_path = self.move_to_right(left_item)
                    path_matches_right: List[DisplayNode] = self.right_tree.get_for_path(left_on_right_path)
                    if path_matches_right:
                        if len(path_matches_right) > 1:
                            # If this ever happens it is a bug
                            raise RuntimeError(f'More than one match for path: {left_on_right_path}')
                        # UPDATED
                        if logger.isEnabledFor(logging.DEBUG):
                            left_path = self.left_tree.get_full_path_for_item(left_item)
                            logger.debug(f'File updated: {left_item.md5} <- "{left_path}" -> {path_matches_right[0].md5}')
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
        for dup_md5s_right in list_of_lists_of_right_items_for_given_md5:
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

        return self.change_tree_left, self.change_tree_right

    def merge_change_trees(self, left_selected_changes: List[DisplayNode], right_selected_changes: List[DisplayNode],
                           check_for_conflicts=False) -> CategoryDisplayTree:

        # always root path, but tree type may differ
        is_mixed_tree = self.left_tree.tree_type != self.right_tree.tree_type
        if is_mixed_tree:
            root_node_identifier = LogicalNodeIdentifier(uid=uid_generator.ROOT_UID, full_path=ROOT_PATH, category=Category.NA,
                                                         tree_type=TREE_TYPE_MIXED)
        else:
            root_node_identifier: NodeIdentifier = self.application.node_identifier_factory.for_values(
                tree_type=self.left_tree.tree_type, full_path=ROOT_PATH, uid=uid_generator.ROOT_UID)

        merged_tree = CategoryDisplayTree(root_node_identifier=root_node_identifier, show_whole_forest=True,
                                          application=self.application, tree_id=ID_MERGE_TREE)

        for item in left_selected_changes:
            merged_tree.add_item(item, item.category, self.left_tree)

        for item in right_selected_changes:
            merged_tree.add_item(item, item.category, self.right_tree)

        # TODO: check for conflicts

        return merged_tree
