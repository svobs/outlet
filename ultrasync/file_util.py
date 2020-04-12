import os
import shutil
import re
import platform
from fmeta.fmeta import FMeta, FMetaTree, Category
import fmeta.content_hasher


def get_resource_path(rel_path: str):
    """Returns the absolute path from the given relative path (relative to the project dir)"""

    assert not rel_path.startswith('/')
    dir_of_py_file = os.path.dirname(__file__)
    project_dir = os.path.join(dir_of_py_file, os.pardir)
    rel_path_to_resource = os.path.join(project_dir, rel_path)
    abs_path_to_resource = os.path.abspath(rel_path_to_resource)
    print('Resource path: ' + abs_path_to_resource)
    return abs_path_to_resource


def strip_root(file_path, root_path):
    """
    Strips the root_path out of the file path.
    Args:
        file_path: absolute path (starts with '/'; may or may not end with a '/'
        root_path: Root path (must be present in file_path)

    Returns:
        a relative path
    """
    root_path_with_slash = root_path if root_path.endswith('/') else root_path + '/'
    return re.sub(root_path_with_slash, '', file_path, count=1)


def split_path(path):
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


def apply_changes_atomically(changes: FMetaTree, staging_dir):

    for fmeta in changes.get_for_cat(Category.Added):
        src_path = os.path.join(changes.root_path, fmeta.file_path)
        dst_path = os.path.join(changes.root_path, fmeta.prev_path)
        staging_path = os.path.join(staging_dir, fmeta.signature)
        print(f'CP: src={src_path}')
        print(f'    stg={staging_path}')
        print(f'    dst={dst_path}')
        copy_file_linux_with_attrs(src_path, staging_path, dst_path, fmeta.signature, True)

    # TODO: deleted

    # TODO: moved


def copy_file_linux_with_attrs(src_path, staging_path, dst_path, src_signature, verify):
    """Copies the src (src_path) to the destination path (dst_path), by first doing the copy to an
    intermediary location (staging_path) and then moving it to the destination once its signature
    has been verified."""

    # (Staging) make parent directories if not exist
    staging_parent, staging_file = os.path.split(staging_path)
    os.makedirs(name=staging_parent, exist_ok=True)

    try:
        shutil.copyfile(src_path, dst=staging_path, follow_symlinks=False)
    except Exception as err:
        print(f'Exception while copying file to staging: {src_path}')
        raise
    try:
        # Copy the permission bits, last access time, last modification time, and flags:
        shutil.copystat(src_path, dst=staging_path, follow_symlinks=False)
    except Exception as err:
        print(f'Exception while copying file meta to staging: {src_path}')
        raise

    if verify:
        dst_signature = fmeta.content_hasher.dropbox_hash(staging_path)
        if src_signature != dst_signature:
            raise RuntimeError(f'Signature of copied file does not match: src_path="{src_path}", '
                               f'src_sig={src_signature}, dst_path="{dst_path}", dst_sig={dst_signature}')

    try:
        # (Destination) make parent directories if not exist
        dst_parent, dst_file_name = os.path.split(dst_path)
        os.makedirs(name=dst_parent, exist_ok=True)

        # Finally, move the file into its final destination
        shutil.move(staging_path, dst_path)
    except Exception as err:
        print(f'Exception while moving file to dst: {dst_path}')
        raise

