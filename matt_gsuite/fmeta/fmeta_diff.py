import runpy as np
from fmeta.fmeta import FMeta, FMetaMoved


def _compare_paths_for_same_sig(left_metas, left_meta_set, right_metas, right_meta_set):
    if left_metas is None:
        left_metas = []
    if right_metas is None:
        right_metas = []

    orphaned_left = []
    orphaned_right = []

    for left_meta in left_metas:
        match = right_meta_set.path_dict.get(left_meta.file_path, None)
        if match is None:
            orphaned_left.append(left_meta)

    for right_meta in right_metas:
        match = left_meta_set.path_dict.get(right_meta.file_path, None)
        if match is None:
            orphaned_right.append(right_meta)

    num_lefts = len(left_metas)
    num_rights = len(right_metas)

    copmare_result = []
    i = 0
    while i < num_lefts and i < num_rights:
        copmare_result.append((left_metas[i], right_metas[i]))
        i += 1

    j = i
    while j < num_lefts:
        copmare_result.append((left_metas[j], None))
        j += 1

    j = i
    while j < num_rights:
        copmare_result.append((None, right_metas[j]))
        j += 1

    return copmare_result


def diff(left_tree, right_tree, compare_paths_also=False, use_modify_times=False):
    """Use this method if we mostly care about having the same unique files *somewhere* in
       each tree (in other words, we care about file contents, and care less about where each
       file is placed). If a file is found with the same signature on both sides but with
       different paths, it is assumed to be renamed/moved."""
    print('Computing naive diff of file sets by signature...')

    # the set of signatures already processed
    signature_set = left_tree.fmeta_set.sig_dict.keys() | right_tree.fmeta_set.sig_dict.keys()

    """Compares the two trees, and populates the change sets of both. The order of 'left' and which is 'right'
     is not important, because the changes are computed from each tree's perspective (e.g. a file which is in
     Left but not Right will be determined to be an 'added' file from the perspective of Left but a 'deleted'
     file from the perspective of Right)"""
    for sig in signature_set:
        right_metas = right_tree.fmeta_set.sig_dict.get(sig, None)
        left_metas = left_tree.fmeta_set.sig_dict.get(sig, None)

        if left_metas is None:
            right_meta = right_metas[0]
            #print(f'Right has new file: "{right_meta.file_path}"')
            right_tree.change_set.adds.append(right_meta)
            left_tree.change_set.dels.append(right_meta)
            continue
        # (else):
        if right_metas is None:
            left_meta = left_metas[0]
            #print(f'Right has new file: "{left_meta.file_path}"')
            left_tree.change_set.adds.append(left_meta)
            right_tree.change_set.dels.append(left_meta)
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
            compare_result = _compare_paths_for_same_sig(left_metas, left_tree.fmeta_set, right_metas, right_tree.fmeta_set)
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
                        right_tree.change_set.moves.append(change)
                        left_tree.change_set.moves.append(change)
                    else:
                        # if not using modify times, each side will assume it is newer always:
                        change = FMetaMoved(changed_right, changed_left.file_path)
                        right_tree.change_set.moves.append(change)

                        change = FMetaMoved(changed_left, changed_right.file_path)
                        left_tree.change_set.moves.append(change)
                else:
                    """Looks like one side has additional file(s) with same signature 
                       - essentially a duplicate.. Remember, we know each side already contains
                       at least one copy with the given signature"""
                    if changed_left is None:
                        right_tree.change_set.adds.append(changed_right)
                        left_tree.change_set.dels.append(changed_right)
                        continue

                    if changed_right is None:
                        left_tree.change_set.adds.append(changed_left)
                        right_tree.change_set.dels.append(changed_left)
                        continue

    print(f'Done with diff. Left:[adds={len(left_tree.change_set.adds)} dels={len(left_tree.change_set.dels)} moves={len(left_tree.change_set.moves)} updates={len(left_tree.change_set.updates)}] Right:[adds={len(right_tree.change_set.adds)} dels={len(right_tree.change_set.dels)} moves={len(right_tree.change_set.moves)} updates={len(right_tree.change_set.updates)}]')

    @staticmethod
    def diff_by_path(left_tree, right_tree):
        print('Comparing file sets by path...')
        # left represents a unique path
        for left in left_tree.fmeta_set.path_dict.values():
            right_samepath = right_tree.fmeta_set.path_dict.get(left.file_path, None)
            if right_samepath is None:
                print(f'Left has new file: "{left.file_path}"')
                # File is added, moved, or copied here.
                # TODO: in the future, be smarter about this
                left.change_set.adds.append(left)
                continue
            # Do we know this item?
            if right_samepath.signature == left.signature:
                if left.is_valid() and right_samepath.is_valid():
                    # Exact match! Nothing to do.
                    continue
                if left.is_deleted() and right_samepath.is_deleted():
                    # Exact match! Nothing to do.
                    continue
                if left.is_moved() and right_samepath.is_moved():
                    # TODO: figure out where to move to
                    print("DANGER! UNHANDLED 1!")
                    continue

                print(f'DANGER! UNHANDLED 2:{left.file_path}')
                continue
            else:
                print(f'In Left path {left.file_path}: expected signature "{right_samepath.signature}"; actual is "{left.signature}"')
                # Conflict! Need to determine which is most recent
                matching_sig_master = right_tree.fmeta_set.sig_dict[left.signature]
                if matching_sig_master is None:
                    # This is a new file, from the standpoint of the remote
                    # TODO: in the future, be smarter about this
                    left_tree.change_set.updates.append(left)
                # print("CONFLICT! UNHANDLED 3!")
                continue

        for right in right_tree.fmeta_set.path_dict.values():
            left_samepath = left_tree.fmeta_set.path_dict.get(right.file_path, None)
            if left_samepath is None:
                print(f'Left is missing file: "{right.file_path}"')
                # File is added, moved, or copied here.
                # TODO: in the future, be smarter about this
                right_tree.change_set.adds.append(right)
                continue

        print('Done with diff')

