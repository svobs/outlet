import errno
import fnmatch
import logging
import os
import pathlib
import platform
import shutil
from pathlib import PurePosixPath
from typing import List, Tuple

from constants import ROOT_PATH

logger = logging.getLogger(__name__)


def is_target_type(file_path: str, valid_suffixes: Tuple[str]):
    """Returns True iff the given file_path ends in one of the suffixes provided (case-insensitive"""
    file_path_lower = file_path.lower()
    for suffix in valid_suffixes:
        regex = '*.' + suffix
        if fnmatch.fnmatch(file_path_lower, regex):
            return True
    return False


def change_path_to_new_root(full_path: str, old_root: str, new_root: str) -> str:
    return str(pathlib.PurePosixPath(new_root).joinpath(strip_root(full_path, old_root)))


def rm_tree(tree_root_path: str):
    logger.warning(f'Removing dir tree: {tree_root_path}')
    shutil.rmtree(tree_root_path)


def rm_file(tree_root_path: str):
    return delete_file(tree_root_path)


def normalize_path(path: str):
    # directories ending in '/' or '/.' are logically equivalent and should be treated as such
    return str(pathlib.PurePosixPath(path))


def is_normalized(path: str):
    return path == ROOT_PATH or not path.endswith('/')


def get_resource_path(rel_path: str, resolve_symlinks=False) -> str:
    """Returns the absolute path from the given relative path (relative to the project dir)"""

    if pathlib.PurePosixPath(rel_path).is_absolute():
        logger.debug(f'get_resource_path(): Already an absolute path: {rel_path}')
        return str(rel_path)
    dir_of_py_file = os.path.dirname(__file__)
    # go up 2 dirs
    project_dir = os.path.join(os.path.join(dir_of_py_file, os.pardir), os.pardir)
    rel_path_to_resource = os.path.join(project_dir, rel_path)
    if resolve_symlinks:
        abs_path_to_resource = os.path.realpath(rel_path_to_resource)
    else:
        abs_path_to_resource = os.path.abspath(rel_path_to_resource)
    logger.debug('Resource path: ' + abs_path_to_resource)
    return abs_path_to_resource


def strip_root(full_path: str, root_path: str) -> str:
    """
    Strips the root_path out of the file path.
    Args:
        full_path: absolute path (starts with '/'; may or may not end with a '/'
        root_path: Root path (must be present in file_path)

    Returns:
        a relative path
    """
    # This should handle all the corner cases for us; see https://docs.python.org/3/library/os.path.html#os.path.relpath
    posix_path = PurePosixPath(full_path)
    # raises ValueError if {full_path} does not start with {root_path}
    posix_path = posix_path.relative_to(root_path)
    rel_path: str = str(posix_path)
    if rel_path.endswith('/') or rel_path.startswith('./'):
        raise RuntimeError(f'Invalid relpath ({rel_path}) after stripping root_path ({root_path}) from full path ({full_path})')
    return rel_path


def split_path(path) -> List[str]:
    """
    Args
        path: a string containing an absolute file path

    Returns:
        an array containing an entry for each path segment
    """
    all_parts = []
    while True:
        parts = os.path.split(path)
        if parts[0] == path:  # sentinel for absolute paths
            all_parts.insert(0, parts[0])
            break
        elif parts[1] == path:  # sentinel for relative paths
            all_parts.insert(0, parts[1])
            break
        else:
            path = parts[0]
            all_parts.insert(0, parts[1])
    return all_parts


def find_nearest_common_ancestor(path1, path2) -> str:
    path_segs1 = split_path(path1)
    path_segs2 = split_path(path2)

    i = 0
    ancestor_path = '/'
    while True:
        if i < len(path_segs1) and i < len(path_segs2) and path_segs1[i] == path_segs2[i]:
            ancestor_path = os.path.join(ancestor_path, path_segs1[i])
            i += 1
        else:
            logger.info(f'Common ancestor of path "{path1}" and "{path2}": {ancestor_path}"')
            return ancestor_path


def creation_date(path_to_file):
    """
    From: https://stackoverflow.com/questions/237079/how-to-get-file-creation-modification-date-times-in-python
    Try to get the date that a file was created, falling back to when it was
    last modified if that isn't possible.
    See http://stackoverflow.com/a/39501288/1709587 for explanation.
    """
    if platform.system() == 'Windows':
        return os.path.getctime(path_to_file)
    else:
        stat = os.stat(path_to_file)
        try:
            return stat.st_birthtime
        except AttributeError:
            # We're probably on Linux. No easy way to get creation dates here,
            # so we'll settle for when its content was last modified.
            return stat.st_mtime


def delete_file(tgt_path: str, to_trash: bool = False):
    if not os.path.exists(tgt_path):
        logger.info(f'Cannot not delete file: file does not exist: {tgt_path}')
        return

    if to_trash:
        # TODO
        logger.warning(f'Moving to trash not implemented! Skipping: {tgt_path}')
        pass
    else:
        logger.debug(f'Deleting file: {tgt_path}')
        os.remove(tgt_path)


def delete_empty_dir(tgt_path: str, to_trash: bool = False):
    if not os.path.exists(tgt_path):
        logger.info(f'Cannot not delete dir: dir does not exist: {tgt_path}')
        return

    if to_trash:
        # TODO
        logger.warning(f'Moving to trash not implemented! Skipping: {tgt_path}')
        pass
    else:
        logger.debug(f'Deleting dir: {tgt_path}')
        os.rmdir(tgt_path)


def move_file(src_path: str, dst_path: str):
    """This should be used to copy a single file. NOT a directory!"""
    # TODO: handle move across file systems
    assert not os.path.isdir(src_path)

    # Make parent directories for dst if not exist
    dst_parent, dst_filename = os.path.split(dst_path)
    try:
        os.makedirs(name=dst_parent, exist_ok=True)
    except Exception as err:
        logger.error(f'Exception while making dest parent dir: {dst_parent}')
        raise

    if not os.path.exists(src_path):
        raise FileNotFoundError(errno.ENOENT, os.strerror(errno.ENOENT), src_path)

    if os.path.exists(dst_path):
        if os.path.isdir(src_path):
            raise IsADirectoryError(errno.EISDIR, os.strerror(errno.EISDIR), dst_path)
        else:
            raise FileNotFoundError(errno.EEXIST, os.strerror(errno.EEXIST), dst_path)

    _move_file(src_path, dst_path)


def _move_file(src, dst):
    """Copied from shutil.move(). But simplified to disallow recursive move"""
    real_dst = dst
    if os.path.isdir(dst):
        raise RuntimeError(f'Cannot move: dst is a directory: {dst}')

    try:
        os.rename(src, real_dst)
    except OSError:
        if os.path.islink(src):
            linkto = os.readlink(src)
            os.symlink(linkto, real_dst)
            os.unlink(src)
        else:
            shutil.copy2(src, real_dst)
            os.unlink(src)
    return real_dst


def replace_file(src_path: str, dst_path: str):
    """Move src_path onto dst_path, replacing it. Both src and dst should be files."""
    if not os.path.exists(src_path):
        raise FileNotFoundError(errno.ENOENT, os.strerror(errno.ENOENT), src_path)

    if os.path.isdir(src_path):
        raise RuntimeError(f'Cannot move: src is a directory: {src_path}')

    if not os.path.exists(dst_path):
        raise FileNotFoundError(errno.ENOENT, os.strerror(errno.ENOENT), dst_path)

    if os.path.isdir(dst_path):
        raise RuntimeError(f'Cannot move: dst is a directory: {dst_path}')

    _replace_file(src_path, dst_path)


def _replace_file(src_path: str, dst_path: str):
    try:
        os.replace(src_path, dst_path)
    except NotImplementedError:
        # Not supported on this platform. Try in pieces:
        os.remove(dst_path)
        _move_file(src_path, dst_path)


def move_to_dst(staging_path: str, dst_path: str, replace: bool):
    try:
        # (Destination) make parent directories if not exist
        dst_parent, dst_file_name = os.path.split(dst_path)
        os.makedirs(name=dst_parent, exist_ok=True)

        # Finally, move the file into its final destination
        if replace:
            _replace_file(staging_path, dst_path)
        else:
            _move_file(staging_path, dst_path)

    except Exception as err:
        logger.error(f'Exception while moving file to dst: {dst_path}')
        raise


def get_valid_or_ancestor(dir_path):
    new_path = dir_path
    while not os.path.exists(new_path):
        parent, last = os.path.split(new_path)
        new_path = parent

    if dir_path != new_path:
        logger.info(f'Path ({dir_path}) is invalid; using closest valid ancestor: {new_path}')
    return new_path


def touch(file_path):
    with open(file_path, 'a'):
        os.utime(file_path, None)
