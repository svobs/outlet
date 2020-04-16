"""Content-first diff. See diff function below."""
import file_util
import os
import copy
import logging
from fmeta.fmeta import FMeta, FMetaTree, Category

logger = logging.getLogger(__name__)


def _compare_paths_for_same_sig(lefts, left_tree, rights, right_tree):
    if lefts is None:
        lefts = []
    if rights is None:
        rights = []

    orphaned_left = []
    orphaned_right = []

    for left in lefts:
        match = right_tree.get_for_path(left.file_path)
        if match is None:
            orphaned_left.append(left)
        # Else we matched path exactly: we can discard this entry

    for right in rights:
        match = left_tree.get_for_path(right.file_path)
        if match is None:
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


def diff(left_tree: FMetaTree, right_tree: FMetaTree, compare_paths_also=False, use_modify_times=False):
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
    logger.info('Diffing files by signature...')

    left_tree.clear_categories()
    right_tree.clear_categories()

    # the set of signatures already processed
    signature_set = left_tree.get_sig_set() | right_tree.get_sig_set()

    # List of lists of FMetas which do not have a matching sig on the other side.
    # We will compare these by path.
    # Note: each list within this list contains duplicates (FMetas with the same sig)
    orphaned_sigs_left = []
    orphaned_sigs_right = []


    """Compares the two trees, and populates the change sets of both. The order of 'left' and which is 'right'
     is not important, because the changes are computed from each tree's perspective (e.g. a file which is in
     Left but not Right will be determined to be an 'added' file from the perspective of Left but a 'deleted'
     file from the perspective of Right)"""
    for sig in signature_set:
        right_metas = right_tree.get_for_sig(sig)
        left_metas = left_tree.get_for_sig(sig)

        if left_metas is None:
            orphaned_sigs_right.append(right_metas)
        elif right_metas is None:
            orphaned_sigs_left.append(left_metas)
        elif compare_paths_also:
            """If we do this, we care about what the files are named, where they are located, and how many
            duplicates exist. When it comes to determining the direction of renamed files, we simply don't
            have enough info to be conclusive, but as this is a secondary concern (our first is ensuring
            we have *any* matching signatures (see above)) I am ok with making a best guess and letting the
            user make the final call via the UI. Here we can choose either to use the modification times
            (newer is assumed to be the rename destination), or for each side to assume it is the destination
            (similar to how we handle missing signatures above)"""

            orphaned_for_sig_left = []
            orphaned_for_sig_right = []

            compare_result = _compare_paths_for_same_sig(left_metas, left_tree, right_metas, right_tree)
            for (changed_left, changed_right) in compare_result:
                # Did we at least find a pair?
                if changed_left is not None and changed_right is not None:
                    # TODO: it never makes sense currently to use modify times, since
                    # TODO: we're always using a symmetric diff. Re-examine this issue with one-sided diff
                    if use_modify_times:
                        if changed_left.modify_ts > changed_right.modify_ts:
                            # renamed from right to left (i.e. left is newer)
                            changed_left.prev_path = changed_right.file_path
                            left_tree.categorize(changed_left, Category.Moved)
                        else:
                            # renamed from left to right (i.e. right is newer)
                            changed_right.prev_path = changed_left.file_path
                            right_tree.categorize(changed_right, Category.Moved)
                    else:
                        # if not using modify times, each side will assume it is newer always:
                        changed_right.prev_path = changed_left.file_path
                        right_tree.categorize(changed_right, Category.Moved)

                        changed_left.prev_path = changed_right.file_path
                        left_tree.categorize(changed_left, Category.Moved)
                else:
                    """Looks like one side has additional file(s) with same signature 
                       - essentially a duplicate.. Remember, we know each side already contains
                       at least one copy with the given signature"""
                    if changed_left is None:
                        orphaned_for_sig_right.append(changed_right)
                    elif changed_right is None:
                        orphaned_for_sig_left.append(changed_left)
            if orphaned_for_sig_left:
                orphaned_sigs_left.append(orphaned_for_sig_left)
            if orphaned_for_sig_right:
                orphaned_sigs_right.append(orphaned_for_sig_right)

    for fmeta_duplicate_sigs_left in orphaned_sigs_left:
        # TODO: Duplicate content (options):
        #  - No special handling of duplicates / treat like other files [default]
        #  - Flag added/missing duplicates as Duplicates
        #  - For each unique, compare only the best match on each side and ignore the rest
        for left_meta in fmeta_duplicate_sigs_left:
            p, n = os.path.split(left_meta.file_path)
            if compare_paths_also:
                matching_right = right_tree.get_for_path(left_meta.file_path)
                if matching_right:
                    if logger.isEnabledFor(logging.DEBUG):
                        logger.debug(f'File updated: {left_meta.signature} <- "{left_meta.file_path}" -> {matching_right.signature}')
                    # Same path, different sig -> Updated
                    right_tree.categorize(matching_right, Category.Updated)
                    left_tree.categorize(left_meta, Category.Updated)
                    continue
                # No match? fall through
            if logger.isEnabledFor(logging.DEBUG):
                logger.debug(f'Left has new file: "{left_meta.file_path}"')
            left_tree.categorize(left_meta, Category.Added)
            left_meta_copy = copy.deepcopy(left_meta)
            left_meta_copy.category = Category.Deleted
            right_tree.add(left_meta_copy)

    for fmeta_duplicate_sigs_right in orphaned_sigs_right:
        for right_meta in fmeta_duplicate_sigs_right:
            if compare_paths_also and left_tree.get_for_path(right_meta.file_path):
                # logically this has already been covered since the handling is symmetrical:
                continue
            if logger.isEnabledFor(logging.DEBUG):
                logger.debug(f'Right has new file: "{right_meta.file_path}"')
            right_tree.categorize(right_meta, Category.Added)
            # Note: deleted nodes should not be thought of like 'real' nodes
            right_meta_copy = copy.deepcopy(right_meta)
            right_meta_copy.category = Category.Deleted
            left_tree.add(right_meta_copy)

    logger.debug(f'Done with diff. Left:[{left_tree.get_category_summary_string()}] Right:[{right_tree.get_category_summary_string()}]')

    debug = False
    logger.debug('Validating categories on Left...')
    left_tree.validate_categories()
    logger.debug('Validating categories on Right...')
    right_tree.validate_categories()

    return left_tree, right_tree


def find_nearest_common_ancestor(path1, path2):
    path_segs1 = file_util.split_path(path1)
    path_segs2 = file_util.split_path(path2)

    i = 0
    ancestor_path = ''
    while True:
        if i < len(path_segs1) and i < len(path_segs2) and path_segs1[i] == path_segs2[i]:
            ancestor_path = os.path.join(ancestor_path, path_segs1[i])
            i += 1
        else:
            logger.info(f'Common ancestor: {ancestor_path}')
            return ancestor_path


def _add_adjusted_metas(side_a_metas, side_a_prefix, side_b_prefix, dst_tree):
    """Note: Adjust all the metas in side_a_metas, with the assumption that the opposite
    side ("Side B") is the target"""
    if side_a_metas is None:
        return

    for side_a_meta in side_a_metas:
        new_fmeta = copy.deepcopy(side_a_meta)
        if side_a_meta.category == Category.Moved:
            # Moves are from Side B to Side B
            new_fmeta.prev_path = os.path.join(side_b_prefix, side_a_meta.prev_path)
            new_fmeta.file_path = os.path.join(side_b_prefix, side_a_meta.file_path)
        elif side_a_meta.category == Category.Added:
            # Copies are from Side A to Side B
            new_fmeta.prev_path = os.path.join(side_a_prefix, side_a_meta.file_path)
            new_fmeta.file_path = os.path.join(side_b_prefix, side_a_meta.file_path)
        elif side_a_meta.category == Category.Updated:
            # Treated like a copy from Side A to overwrite Side B
            new_fmeta.prev_path = os.path.join(side_a_prefix, side_a_meta.file_path)
            new_fmeta.file_path = os.path.join(side_b_prefix, side_a_meta.file_path)
        else:
            new_fmeta.file_path = os.path.join(side_b_prefix, side_a_meta.file_path)
        dst_tree.add(new_fmeta)


def merge_change_trees(left_tree: FMetaTree, right_tree: FMetaTree, check_for_conflicts=True):
    new_root_path = find_nearest_common_ancestor(left_tree.root_path, right_tree.root_path)
    merged_tree = FMetaTree(root_path=new_root_path)

    signature_set = left_tree.get_sig_set() | right_tree.get_sig_set()

    left_old_root_remainder = file_util.strip_root(left_tree.root_path, new_root_path)
    right_old_root_remainder = file_util.strip_root(right_tree.root_path, new_root_path)

    conflict_pairs = []
    for sig in signature_set:
        right_metas = right_tree.get_for_sig(sig)
        left_metas = left_tree.get_for_sig(sig)

        if check_for_conflicts and left_metas is not None and right_metas is not None:
            compare_result = _compare_paths_for_same_sig(left_metas, left_tree, right_metas, right_tree)
            for (left, right) in compare_result:
                # Finding a pair here indicates a conflict
                if left is not None and right is not None:
                    conflict_pairs.append((left, right))
                    logger.debug(f'CONFLICT: left={left.category.name}:{left.file_path} right={right.category.name}:{right.file_path}')
        else:
            _add_adjusted_metas(side_a_metas=left_metas, side_a_prefix=left_old_root_remainder, side_b_prefix=right_old_root_remainder, dst_tree=merged_tree)
            _add_adjusted_metas(side_a_metas=right_metas, side_a_prefix=right_old_root_remainder, side_b_prefix=left_old_root_remainder, dst_tree=merged_tree)

    if len(conflict_pairs) > 0:
        logger.info(f'Number of conflicts found: {len(conflict_pairs)}')
        return None, conflict_pairs
    else:
        return merged_tree, None
