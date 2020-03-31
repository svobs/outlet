import os
from widget.diff_tree import ChangeSet
from fmeta.fmeta import FMeta
import fmeta.content_hasher


def apply_change_set(change_set, src_root_path, dst_root_path):
    for fmeta in change_set.adds:
        # TODO: derive src path
        copy_file_linux_with_attrs(fmeta, dst_root_path, fmeta.signature)
    pass
    # TODO


def copy_file_linux_with_attrs(src_path, dst_path, src_signature):
    os.sendfile(dst_path, src_path)
    dst_signature = fmeta.dropbox_hash(dst_path)