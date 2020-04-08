"""Path-first diff. See diff function below."""


def diff_by_path(left_tree, right_tree):
    """Use this diff algorithm if are primarily concerned about syncing directory structures
    (similar to Dropbox or Google Drive)."""
    print('Comparing file sets by path...')
    # left represents a unique path
    for left in left_tree.fmeta_tree._path_dict.values():
        right_samepath = right_tree.fmeta_tree._path_dict.get(left.file_path, None)
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
            matching_sig_master = right_tree.fmeta_tree.sig_dict[left.signature]
            if matching_sig_master is None:
                # This is a new file, from the standpoint of the remote
                # TODO: in the future, be smarter about this
                left_tree.change_set.updates.append(left)
            # print("CONFLICT! UNHANDLED 3!")
            continue

    for right in right_tree.fmeta_tree._path_dict.values():
        left_samepath = left_tree.fmeta_tree._path_dict.get(right.file_path, None)
        if left_samepath is None:
            print(f'Left is missing file: "{right.file_path}"')
            # File is added, moved, or copied here.
            # TODO: in the future, be smarter about this
            right_tree.change_set.adds.append(right)
            continue

    print('Done with diff')

