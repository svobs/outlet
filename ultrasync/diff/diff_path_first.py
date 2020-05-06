"""Path-first diff. See diff function below."""

import logging

logger = logging.getLogger(__name__)


def diff_by_path(left_tree, right_tree):
    """Use this diff algorithm if are primarily concerned about syncing directory structures
    (similar to Dropbox or Google Drive)."""
    logger.debug('Comparing file sets by path...')
    # left represents a unique path
    for left in left_tree.fmeta_tree._path_dict.values():
        right_samepath = right_tree.fmeta_tree._path_dict.get(left.full_path, None)
        if right_samepath is None:
            logger.debug(f'Left has new file: "{left.full_path}"')
            # File is added, moved, or copied here.
            # TODO: in the future, be smarter about this
            left.change_set.adds.append(left)
            continue
        # Do we know this item?
        if right_samepath.md5 == left.md5:
            if left.is_valid() and right_samepath.is_valid():
                # Exact match! Nothing to do.
                continue
            if left.is_deleted() and right_samepath.is_deleted():
                # Exact match! Nothing to do.
                continue
            if left.is_moved() and right_samepath.is_moved():
                # TODO: figure out where to move to
                logger.error("DANGER! UNHANDLED 1!")
                continue

            logger.error(f'DANGER! UNHANDLED 2:{left.full_path}')
            continue
        else:
            logger.debug(f'In Left path {left.full_path}: expected MD5 "{right_samepath.md5}"; actual is "{left.md5}"')
            # Conflict! Need to determine which is most recent
            matching_sig_master = right_tree.fmeta_tree._md5_dict[left.md5]
            if matching_sig_master is None:
                # This is a new file, from the standpoint of the remote
                # TODO: in the future, be smarter about this
                left_tree.change_set.updates.append(left)
            logger.error("CONFLICT! UNHANDLED 3!")
            continue

    for right in right_tree.fmeta_tree._path_dict.values():
        left_samepath = left_tree.fmeta_tree._path_dict.get(right.full_path, None)
        if left_samepath is None:
            print(f'Left is missing file: "{right.full_path}"')
            # File is added, moved, or copied here.
            # TODO: in the future, be smarter about this
            right_tree.change_set.adds.append(right)
            continue

    logger.debug('Done with diff')
