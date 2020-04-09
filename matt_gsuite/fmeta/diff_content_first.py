"""Content-first diff. See diff function below."""

from fmeta.fmeta import FMeta, FMetaTree, FMetaMoved, Category


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
            left_tree.categorize(right_meta, Category.Deleted)
            continue
        # (else):
        if right_metas is None:
            left_meta = left_metas[0]
            #print(f'Right has new file: "{left_meta.file_path}"')
            left_tree.categorize(left_meta, Category.Added)
            right_tree.categorize(left_meta, Category.Deleted)
            continue
        # (else):
        if compare_paths_also:
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
                    if use_modify_times:
                        if changed_left.modify_ts > changed_right.modify_ts:
                            # renamed from right to left (i.e. left is newer)
                            change = FMetaMoved(changed_left, changed_right.file_path)
                        else:
                            # renamed from left to right (i.e. right is newer)
                            change = FMetaMoved(changed_right, changed_left.file_path)
                        right_tree.categorize(change, Category.Moved)
                        left_tree.categorize(change, Category.Moved)
                    else:
                        # if not using modify times, each side will assume it is newer always:
                        change = FMetaMoved(changed_right, changed_left.file_path)
                        right_tree.categorize(change, Category.Moved)

                        change = FMetaMoved(changed_left, changed_right.file_path)
                        left_tree.categorize(change, Category.Moved)
                else:
                    """Looks like one side has additional file(s) with same signature 
                       - essentially a duplicate.. Remember, we know each side already contains
                       at least one copy with the given signature"""
                    if changed_left is None:
                        right_tree.categorize(changed_right, Category.Added)
                        left_tree.categorize(changed_right, Category.Deleted)
                        continue

                    if changed_right is None:
                        left_tree.categorize(changed_left, Category.Added)
                        right_tree.categorize(changed_left, Category.Deleted)
                        continue

    print(f'Done with diff. Left:[{left_tree.get_category_summary_string()}] Right:[{right_tree.get_category_summary_string()}]')
    return left_tree, right_tree


def merge_change_trees(left_tree: FMetaTree, right_tree: FMetaTree):
    simplified_tree = FMetaTree(root_path='dunno')
    return simplified_tree
