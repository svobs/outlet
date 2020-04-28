"""Content-first diff. See diff function below."""
import file_util
import os
import copy
import logging
from model.fmeta import Category, FMeta
from model.fmeta_tree import FMetaTree
from model.planning_node import FileToAdd, FileToMove

logger = logging.getLogger(__name__)


def _compare_paths_for_same_md5(lefts, left_tree, rights, right_tree, fixer):
    if lefts is None:
        lefts = []
    if rights is None:
        rights = []

    orphaned_left = []
    orphaned_right = []

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

#
# def mv_left_to_right(left_fmeta, left_tree, right_tree):
#     left_rel_path = left_fmeta.get_relative_path(left_tree.root_path)
#     new_full_path = os.path.join(right_tree.root_path, left_rel_path)
#     return new_full_path
#
#
# def mv_right_to_left(right_fmeta, right_tree, left_tree):
#     right_rel_path = right_fmeta.get_relative_path(right_tree.root_path)
#     new_full_path = os.path.join(left_tree.root_path, right_rel_path)
#     return new_full_path


class PathTransplanter:
    def __init__(self, left_tree: FMetaTree, right_tree: FMetaTree):
        self.left_root = left_tree.root_path
        self.right_root = right_tree.root_path

    def move_to_right(self, left_fmeta: FMeta) -> str:
        left_rel_path = left_fmeta.get_relative_path(self.left_root)
        return os.path.join(self.right_root, left_rel_path)

    def move_to_left(self, right_fmeta: FMeta) -> str:
        right_rel_path = right_fmeta.get_relative_path(self.right_root)
        return os.path.join(self.left_root, right_rel_path)


def diff(left_tree: FMetaTree, right_tree: FMetaTree, compare_paths_also=False):
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

    fixer = PathTransplanter(left_tree, right_tree)

    left_tree.clear_categories()
    right_tree.clear_categories()

    # the set of MD5s already processed
    md5_set = left_tree.get_md5_set() | right_tree.get_md5_set()

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
        right_metas_dup_md5 = right_tree.get_for_md5(md5)
        left_metas_dup_md5 = left_tree.get_for_md5(md5)

        if left_metas_dup_md5 is None:
            orphaned_md5s_right.append(right_metas_dup_md5)
        elif right_metas_dup_md5 is None:
            orphaned_md5s_left.append(left_metas_dup_md5)
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

            compare_result = _compare_paths_for_same_md5(left_metas_dup_md5, left_tree, right_metas_dup_md5, right_tree, fixer)
            for (changed_left, changed_right) in compare_result:
                # Did we at least find a pair?
                if changed_left is not None and changed_right is not None:
                    # MOVED
                    dest_path = fixer.move_to_left(changed_right)
                    file_to_move_left = FileToMove(changed_right, dest_path)
                    left_tree.add(file_to_move_left)

                    dest_path = fixer.move_to_right(changed_left)
                    file_to_move_right = FileToMove(changed_left, dest_path)
                    right_tree.add(file_to_move_right)
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

    for fmeta_duplicate_md5s_left in orphaned_md5s_left:
        # TODO: Duplicate content (options):
        #  - No special handling of duplicates / treat like other files [default]
        #  - Flag added/missing duplicates as Duplicates
        #  - For each unique, compare only the best match on each side and ignore the rest
        for left_meta in fmeta_duplicate_md5s_left:
            if compare_paths_also:
                left_on_right_path = fixer.move_to_right(left_meta)
                matching_right = right_tree.get_for_path(left_on_right_path)
                if matching_right:
                    # UPDATED
                    if logger.isEnabledFor(logging.DEBUG):
                        logger.debug(f'File updated: {left_meta.md5} <- "{left_meta.full_path}" -> {matching_right.md5}')
                    # Same path, different md5 -> Updated
                    right_tree.categorize(matching_right, Category.Updated)
                    left_tree.categorize(left_meta, Category.Updated)
                    continue
                # No match? fall through
            # DUPLICATE ADDED on right + DELETED on left
            if logger.isEnabledFor(logging.DEBUG):
                logger.debug(f'Left has new file: "{left_meta.full_path}"')
            dest_path = fixer.move_to_right(left_meta)
            file_to_add_to_right = FileToAdd(left_meta, dest_path)
            right_tree.add(file_to_add_to_right)

            # TODO: rename 'Deleted' category to 'ToDelete'
            # Dead node walking:
            left_tree.categorize(left_meta, Category.Deleted)

    for fmeta_duplicate_md5s_right in orphaned_md5s_right:
        for right_meta in fmeta_duplicate_md5s_right:
            if compare_paths_also:
                right_on_left = fixer.move_to_left(right_meta)
                if left_tree.get_for_path(right_on_left):
                    # UPDATED. Logically this has already been covered (above) since our iteration is symmetrical:
                    continue
            # DUPLICATE ADDED on right + DELETED on left
            if logger.isEnabledFor(logging.DEBUG):
                logger.debug(f'Right has new file: "{right_meta.full_path}"')
            dest_path = fixer.move_to_left(right_meta)
            file_to_add_to_left = FileToAdd(right_meta, dest_path)
            left_tree.add(file_to_add_to_left)

            # Dead node walking:
            right_tree.categorize(right_meta, Category.Deleted)

    logger.debug(f'Done with diff. Left:[{left_tree.get_category_summary_string()}] Right:[{right_tree.get_category_summary_string()}]')

    debug = False
    logger.debug('Validating categories on Left...')
    left_tree.validate_categories()
    logger.debug('Validating categories on Right...')
    right_tree.validate_categories()

    return left_tree, right_tree


def merge_change_trees(left_tree: FMetaTree, right_tree: FMetaTree, check_for_conflicts=True):
    new_root_path = file_util.find_nearest_common_ancestor(left_tree.root_path, right_tree.root_path)
    merged_tree = FMetaTree(root_path=new_root_path)

    md5_set = left_tree.get_md5_set() | right_tree.get_md5_set()

    fixer = PathTransplanter(left_tree, right_tree)

    conflict_pairs = []
    for md5 in md5_set:
        right_metas_dup_md5 = right_tree.get_for_md5(md5)
        left_metas_dup_md5 = left_tree.get_for_md5(md5)

        if check_for_conflicts and left_metas_dup_md5 and right_metas_dup_md5:
            compare_result = _compare_paths_for_same_md5(left_metas_dup_md5, left_tree, right_metas_dup_md5, right_tree, fixer)
            # TODO: wow, adds and deletes of the same file cancel each other out via the matching algo...
            #       Maybe just delete the conflict detection code...
            for (left, right) in compare_result:
                # Finding a pair here indicates a conflict
                if left is not None and right is not None:
                    conflict_pairs.append((left, right))
                    logger.debug(f'CONFLICT: left={left.category.name}:{left.full_path} right={right.category.name}:{right.full_path}')
        else:
            if left_metas_dup_md5:
                for node in left_metas_dup_md5:
                    merged_tree.add(node)
            if right_metas_dup_md5:
                for node in right_metas_dup_md5:
                    merged_tree.add(node)

    if len(conflict_pairs) > 0:
        logger.info(f'Number of conflicts found: {len(conflict_pairs)}')
        return None, conflict_pairs
    else:
        return merged_tree, None
