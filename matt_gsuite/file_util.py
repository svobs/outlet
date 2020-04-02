import os
import shutil
from widget.diff_tree import ChangeSet
from fmeta.fmeta import FMeta
import fmeta.content_hasher


def apply_change_set(change_set, src_root_path, dst_root_path):
    staging_base_dir = os.path.join(dst_root_path, '.sync-tmp')

    for fmeta in change_set.adds:
        src_path = os.path.join(src_root_path, fmeta.file_path)
        dst_path = os.path.join(dst_root_path, fmeta.file_path)
        staging_path = os.path.join(staging_base_dir, fmeta.signature)
        print(f'CP: src={src_path}')
        print(f'    stg={staging_path}')
        print(f'    dst={dst_path}')
        #copy_file_linux_with_attrs(src_path, staging_path, dst_path, fmeta.signature)


def copy_file_linux_with_attrs(src_path, staging_path, dst_path, src_signature):
    """Copies the src (src_path) to the destination path (dst_path), by first doing the copy to an
    intermediary location (staging_path) and then moving it to the destination once its signature
    has been verified."""

    # (Staging) make parent directories if not exist
    staging_parent, staging_file = os.path.split(staging_path)
    os.mkdirs(name=staging_parent, exist_ok=True)

    os.mkdirs(name=staging_parent, exist_ok=True)
    shutil.copyfile(src_path, dst=staging_path, follow_symlinks=False)
    # Copy the permission bits, last access time, last modification time, and flags:
    shutil.copystat(src_path, dst=staging_path, follow_symlinks=False)
    dst_signature = fmeta.content_hasher.dropbox_hash(staging_path)
    if src_signature != dst_signature:
        raise RuntimeError(f'Signature of copied file does not match: src_path="{src_path}", '
                           f'src_sig={src_signature}, dst_path="{dst_path}", dst_sig={dst_signature}')

    # (Destination) make parent directories if not exist
    dst_parent, dst_file_name = os.path.split(dst_path)
    os.mkdirs(name=dst_parent, exist_ok=True)

    # Finally, move the file into its final destination
    shutil.move(staging_path, dst_path)

