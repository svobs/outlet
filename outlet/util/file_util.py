import fnmatch
import os
import shutil
import errno
import platform
import logging
from typing import List, Tuple
from backend.tree_store.local import content_hasher

from constants import ROOT_PATH
from error import IdenticalFileExistsError

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
    new_path = os.path.join(new_root, strip_root(full_path, old_root))
    if new_path.endswith('/'):
        # strip off trailing '/' from root:
        new_path = new_path[:-1]
    return new_path


def rm_tree(tree_root_path: str):
    logger.warning(f'Removing dir tree: {tree_root_path}')
    shutil.rmtree(tree_root_path)


def rm_file(tree_root_path: str):
    return delete_file(tree_root_path)


def normalize_path(path: str):
    if path != ROOT_PATH and path.endswith('/'):
        # directories ending in '/' are logically equivalent and should be treated as such
        return path[:-1]
    return path


def is_normalized(path: str):
    return path == ROOT_PATH or not path.endswith('/')


def get_resource_path(rel_path: str, resolve_symlinks=False):
    """Returns the absolute path from the given relative path (relative to the project dir)"""

    if rel_path.startswith('/'):
        logger.debug(f'get_resource_path(): Already an absolute path: {rel_path}')
        return rel_path
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
    assert full_path.find(root_path) >= 0, f'Did not find root_path ({root_path}) in full path ({full_path})'
    if root_path.endswith('/'):
        # strip off trailing '/' from root:
        root_path = root_path[:-1]
    rel_path = full_path.replace(root_path, '', 1)
    if rel_path.endswith('/'):
        # strip off trailing '/'
        rel_path = rel_path[:-1]
    if len(rel_path) < len(full_path) and rel_path.startswith('/'):
        rel_path = rel_path[1:]
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


def move_file(src_path, dst_path):
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

    shutil.move(src_path, dst_path)


def _do_copy_to_staging(src_path, staging_path, dst_path, md5_src, verify):
    # (Staging) make parent directories if not exist
    staging_parent, staging_file = os.path.split(staging_path)
    try:
        os.makedirs(name=staging_parent, exist_ok=True)
    except Exception as err:
        logger.error(f'Exception while making staging dir: {staging_parent}')
        raise

    try:
        shutil.copyfile(src_path, dst=staging_path, follow_symlinks=False)
    except Exception as err:
        logger.error(f'Exception while copying file to staging: {src_path}')
        raise
    try:
        # Copy the permission bits, last access time, last modification time, and flags:
        shutil.copystat(src_path, dst=staging_path, follow_symlinks=False)
    except Exception as err:
        logger.error(f'Exception while copying file meta to staging: {src_path}')
        raise

    if verify:
        # sha256 = fmeta.content_hasher.dropbox_hash(staging_path)
        md5 = content_hasher.compute_md5(staging_path)
        if md5_src != md5:
            raise RuntimeError(f'MD5 of copied file does not match: src_path="{src_path}", '
                               f'src_md5={md5_src}, dst_path="{dst_path}", dst_md5={md5}')


def move_to_dst(staging_path: str, dst_path: str):
    try:
        # (Destination) make parent directories if not exist
        dst_parent, dst_file_name = os.path.split(dst_path)
        os.makedirs(name=dst_parent, exist_ok=True)

        # Finally, move the file into its final destination
        shutil.move(staging_path, dst_path)
    except Exception as err:
        logger.error(f'Exception while moving file to dst: {dst_path}')
        raise


def copy_file_new(src_path, staging_path, dst_path, md5_src, verify):
    """Copies the src (src_path) to the destination path (dst_path), by first doing the copy to an
    intermediary location (staging_path) and then moving it to the destination once its signature
    has been verified.

    Raises an error if a file is already present at the destination."""
    if os.path.exists(dst_path):
        # sha256 = fmeta.content_hasher.dropbox_hash(dst_path)
        md5_encountered = content_hasher.compute_md5(dst_path)
        if md5_src == md5_encountered:
            # TODO: what about if stats are different?
            msg = f'Identical file already exists at dst: {dst_path}'
            logger.info(msg)
            # This will be caught and treated as a no-op
            raise IdenticalFileExistsError(msg)
        else:
            raise FileExistsError(f'Found unexpected file at destination path ("{dst_path}") with MD5: {md5_encountered}')

    _do_copy_to_staging(src_path, staging_path, dst_path, md5_src, verify)

    move_to_dst(staging_path, dst_path)


def copy_file_update(src_path: str, md5_src: str, staging_path: str, dst_path: str, verify: bool):
    """Copies the src (src_path) to the destination path (dst_path) via a staging dir, but first
    verifying that a file already exists there and it has the expected MD5; failing otherwise"""

    if not os.path.exists(dst_path):
        # TODO: custom exception class
        raise FileNotFoundError(errno.ENOENT, os.strerror(errno.ENOENT), dst_path)

    # sha256 = fmeta.content_hasher.dropbox_hash(dst_path)
    md5_encountered = content_hasher.compute_md5(dst_path)
    if md5_encountered != md5_src:
        raise RuntimeError(f'Expected MD5 ({md5_src}) does not match actual ({md5_encountered})')

    _do_copy_to_staging(src_path, staging_path, dst_path, md5_src, verify)

    try:
        # Finally, move the file into its final destination. Move the other file out of the way
        # first to minimize potential loss of data
        tmp_path = dst_path + '.tmp'
        shutil.move(dst_path, tmp_path)
        shutil.move(staging_path, dst_path)
        os.remove(tmp_path)
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
