"""Content-first diff. See diff function below."""
import file_util
import os
import copy
from fmeta.fmeta import FMeta, FMetaTree, Category


def _compare_paths_for_same_sig(left_metas, left_meta_set, right_metas, right_meta_set):
    if left_metas is None:
        left_metas = []
    if right_metas is None:
        right_metas = []

    orphaned_left = []
    orphaned_right = []

    for left_meta in left_metas:
        match = right_meta_set.get_for_path(left_meta.file_path)
        if match is None:
            orphaned_left.append(left_meta)

    for right_meta in right_metas:
        match = left_meta_set.get_for_path(right_meta.file_path)
        if match is None:
            orphaned_right.append(right_meta)

    num_lefts = len(left_metas)
    num_rights = len(right_metas)

    compare_result = []
    i = 0
    while i < num_lefts and i < num_rights:
        compare_result.append((left_metas[i], right_metas[i]))
        i += 1

    j = i
    while j < num_lefts:
        compare_result.append((left_metas[j], None))
        j += 1

    j = i
    while j < num_rights:
        compare_result.append((None, right_metas[j]))
        j += 1

    return compare_result


def diff(left_tree: FMetaTree, right_tree: FMetaTree, compare_paths_also=False, use_modify_times=False):
    """Use this method if we mostly care about having the same unique files *somewhere* in
       each tree (in other words, we care about file contents, and care less about where each
       file is placed). If a file is found with the same signature on both sides but with
       different paths, it is assumed to be renamed/moved."""
    print('Computing naive diff of file sets by signature...')

    left_tree.clear_categories()
    right_tree.clear_categories()

    # the set of signatures already processed
    signature_set = left_tree.sig_dict.keys() | right_tree.sig_dict.keys()

    """Compares the two trees, and populates the change sets of both. The order of 'left' and which is 'right'
     is not important, because the changes are computed from each tree's perspective (e.g. a file which is in
     Left but not Right will be determined to be an 'added' file from the perspective of Left but a 'deleted'
     file from the perspective of Right)"""
    for sig in signature_set:
        right_metas = right_tree.get_for_sig(sig)
        left_metas = left_tree.get_for_sig(sig)

        if left_metas is None:
            right_meta = right_metas[0]
            #print(f'Right has new file: "{right_meta.file_path}"')
            right_tree.categorize(right_meta, Category.Added)
            # Note: deleted nodes should not be thought of like 'real' nodes
            right_meta_copy = copy.deepcopy(right_meta)
            left_tree.categorize(right_meta_copy, Category.Deleted)
        elif right_metas is None:
            left_meta = left_metas[0]
            #print(f'Left has new file: "{left_meta.file_path}"')
            left_tree.categorize(left_meta, Category.Added)
            left_meta_copy = copy.deepcopy(left_meta)
            right_tree.categorize(left_meta_copy, Category.Deleted)
        elif compare_paths_also:
            """If we do this, we care about what the files are named, where they are located, and how many
            duplicates exist. When it comes to determining the direction of renamed files, we simply don't
            have enough info to be conclusive, but as this is a secondary concern (our first is ensuring
            we have *any* matching signatures (see above)) I am ok with making a best guess and letting the
            user make the final call via the UI. Here we can choose either to use the modification times
            (newer is assumed to be the rename destination), or for each side to assume it is the destination
            (similar to how we handle missing signatures above)"""
            compare_result = _compare_paths_for_same_sig(left_metas, left_tree, right_metas, right_tree)
            for (changed_left, changed_right) in compare_result:
                # Did we at least find a pair?
                if changed_left is not None and changed_right is not None:
                    # TODO: it never makes sense currently to use modify times, since
                    # TODO: we're always using a symmetric diff. Re-examine this issue with
                    # TODO: one-sided diff
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
                        right_tree.categorize(changed_right, Category.Added)
                        changed_right_copy = copy.deepcopy(changed_right)
                        left_tree.categorize(changed_right_copy, Category.Deleted)
                        continue

                    if changed_right is None:
                        left_tree.categorize(changed_left, Category.Added)
                        changed_left_copy = copy.deepcopy(changed_left)
                        right_tree.categorize(changed_left_copy, Category.Deleted)
                        continue

    print(f'Done with diff. Left:[{left_tree.get_category_summary_string()}] Right:[{right_tree.get_category_summary_string()}]')

    print('Validating categories on Left...')
    left_tree.validate_categories()
    print('Validating categories on Right...')
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
            print(f'Ancestor path: {ancestor_path}')
            return ancestor_path


def merge_change_trees(left_tree: FMetaTree, right_tree: FMetaTree):
    new_root_path = find_nearest_common_ancestor(left_tree.root_path, right_tree.root_path)
    merged_tree = FMetaTree(root_path=new_root_path)
    # TODO



    return merged_tree
