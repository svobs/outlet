import logging
import os
import errno
import copy
import time
import uuid
from pathlib import Path

from constants import VALID_SUFFIXES
from model.fmeta import FMeta, Category
from fmeta.tree_recurser import TreeRecurser
import fmeta.content_hasher
import ui.actions as actions
from model.fmeta_tree import FMetaTree

logger = logging.getLogger(__name__)


def build_fmeta(full_path, category=Category.NA):
    if category == Category.Ignored:
        # Do not scan ignored files for content (optimization)
        md5 = None
        sha256 = None
    else:
        # Open,close, read file and calculate hash of its contents
        md5 = fmeta.content_hasher.md5(full_path)
        # sha256 = fmeta.content_hasher.dropbox_hash(full_path)
        sha256 = None

    # Get "now" in UNIX time:
    sync_ts = int(time.time())

    stat = os.stat(full_path)
    size_bytes = int(stat.st_size)
    modify_ts = int(stat.st_mtime * 1000)
    assert modify_ts > 1000000000000, f'modify_ts too small: {modify_ts}'
    change_ts = int(stat.st_ctime * 1000)
    assert change_ts > 1000000000000, f'change_ts too small: {change_ts}'

    return FMeta(md5, sha256, size_bytes, sync_ts, modify_ts, change_ts, full_path, category)


def meta_matches(file_path, fmeta: FMeta):
    stat = os.stat(file_path)
    size_bytes = int(stat.st_size)
    modify_ts = int(stat.st_mtime * 1000)
    assert modify_ts > 1000000000000, f'modify_ts too small: {modify_ts}'
    change_ts = int(stat.st_ctime * 1000)
    assert change_ts > 1000000000000, f'change_ts too small: {change_ts}'

    is_equal = fmeta.size_bytes == size_bytes and fmeta.modify_ts == modify_ts and fmeta.change_ts == change_ts

    if False and logger.isEnabledFor(logging.DEBUG):
        logger.debug(f'Meta Exp=[{fmeta.size_bytes} {fmeta.modify_ts} {fmeta.change_ts}]' +
                     f' Act=[{size_bytes} {modify_ts} {change_ts}] -> {is_equal}')

    return is_equal


def _check_update_sanity(old_fmeta, new_fmeta):
    if new_fmeta.modify_ts < old_fmeta.modify_ts:
        logger.warning(f'File "{new_fmeta.full_path}": update has older modify_ts ({new_fmeta.modify_ts}) than prev version ({old_fmeta.modify_ts})')

    if new_fmeta.change_ts < old_fmeta.change_ts:
        logger.warning(f'File "{new_fmeta.full_path}": update has older change_ts ({new_fmeta.change_ts}) than prev version ({old_fmeta.change_ts})')

    if new_fmeta.size_bytes != old_fmeta.size_bytes and new_fmeta.md5 == old_fmeta.md5:
        logger.warning(f'File "{new_fmeta.full_path}": update has same md5 ({new_fmeta.md5}) ' +
                       f'but different size: (old={old_fmeta.size_bytes}, new={new_fmeta.size_bytes})')


# SUPPORT CLASSES ####################


class FileCounter(TreeRecurser):
    """
    Does a quick walk of the filesystem and counts the files which are of interest
    """

    def __init__(self, root_path):
        TreeRecurser.__init__(self, root_path, valid_suffixes=VALID_SUFFIXES)
        self.files_to_scan = 0

    def handle_target_file_type(self, file_path):
        self.files_to_scan += 1

    def handle_non_target_file(self, file_path):
        self.files_to_scan += 1


class TreeMetaScanner(TreeRecurser):
    """
    Walks the filesystem for a subtree (FMetaTree), using a cache if configured,
    to generate an up-to-date FMetaTree.
    """

    def __init__(self, root_path, stale_tree=None, tree_id=None, track_changes=False):
        TreeRecurser.__init__(self, Path(stale_tree.root_path), valid_suffixes=VALID_SUFFIXES)
        # Note: this tree will be useless after we are done with it
        self.root_path = root_path
        self.stale_tree = stale_tree
        self.tx_id = uuid.uuid1()
        self.progress = 0
        self.total = 0
        self.tree_id = tree_id  # For sending progress updates
        self.added_count = 0
        self.updated_count = 0
        self.deleted_count = 0
        self.unchanged_count = 0
        # When done, this will contain an up-to-date tree.
        self.fresh_tree = FMetaTree(self.root_path)
        self._track_changes = track_changes
        if self._track_changes:
            # Keep track of what's actually changed.
            # This is effectively a diff of stale & fresh trees.
            # Don't need it yet, but have a feeling it will be handy in the future.
            self.change_tree = FMetaTree(self.root_path)
        else:
            self.change_tree = None

    def _find_total_files_to_scan(self):
        # First survey our local files:
        logger.info(f'Scanning path: {self.root_path}')
        file_counter = FileCounter(self.root_path)
        file_counter.recurse_through_dir_tree()

        total = file_counter.files_to_scan
        logger.debug(f'Found {total} files to scan.')
        return total

    def handle_file(self, file_path, category):
        """
        We compare against the cache (aka 'stale tree') using the file_path as a key.
        The item can be one of: unchanged, added/new, deleted, or updated
        """
        cache_diff_status = None
        stale_meta = None
        rebuild_fmeta = True
        if self.stale_tree:
            stale_meta = self.stale_tree.get_for_path(file_path, include_ignored=True)

        if stale_meta is not None:
            # Either unchanged, or updated. Either way, remove from stale tree.
            # (note: set remove_old_md5=True because the md5 will have changed if the file was updated)
            stale_meta = self.stale_tree.remove(full_path=stale_meta.full_path, md5=stale_meta.md5, remove_old_md5=True, ok_if_missing=False)

            if meta_matches(file_path, stale_meta):
                # Found in cache.
                self.unchanged_count += 1
                cache_diff_status = Category.NA
                rebuild_fmeta = False
            else:
                self.updated_count += 1
                cache_diff_status = Category.Updated

        else:
            # Uncached (or no cache)
            self.added_count += 1
            cache_diff_status = Category.Added

        if rebuild_fmeta:
            meta = build_fmeta(full_path=file_path, category=category)
        else:
            meta = stale_meta

        if logger.isEnabledFor(logging.DEBUG) and cache_diff_status == Category.Updated:
            _check_update_sanity(stale_meta, meta)

        if self._track_changes:
            self._add_tracked_copy(meta, cache_diff_status)

        self.fresh_tree.add_item(meta)
        if self.tree_id:
            actions.get_dispatcher().send(actions.PROGRESS_MADE, sender=self.tree_id, tx_id=self.tx_id, progress=1)
            self.progress += 1
            msg = f'Scanning file {self.progress} of {self.total}'
            actions.get_dispatcher().send(actions.SET_PROGRESS_TEXT, sender=self.tree_id, tx_id=self.tx_id, msg=msg)

    def handle_target_file_type(self, file_path):
        self.handle_file(file_path, Category.NA)

    def handle_non_target_file(self, file_path):
        self.handle_file(file_path, Category.Ignored)

    def scan(self):
        """Recurse over disk tree. Gather current stats for each file, and compare to the stale tree.
        For each current file found, remove from the stale tree.
        When recursion is complete, what's left in the stale tree will be deleted/moved files"""

        if not os.path.exists(self.root_path):
            raise FileNotFoundError(errno.ENOENT, os.strerror(errno.ENOENT), self.root_path)

        self.total = self._find_total_files_to_scan()
        if self.tree_id:
            logger.debug(f'Sending START_PROGRESS for tree_id: {self.tree_id}')
            actions.get_dispatcher().send(actions.START_PROGRESS, sender=self.tree_id, tx_id=self.tx_id, total=self.total)

        self.recurse_through_dir_tree()

        if self.stale_tree is not None:
            self.deleted_count += len(self.stale_tree.get_all())
            if self._track_changes:
                # All remaining fmetas in the stale tree represent deleted
                for stale_fmeta in self.stale_tree.get_all():
                    self._add_tracked_copy(stale_fmeta, Category.Deleted)

        logger.debug(f'Sending STOP_PROGRESS for tree_id: {self.tree_id}')
        actions.get_dispatcher().send(actions.STOP_PROGRESS, tx_id=self.tx_id, sender=self.tree_id)
        logger.info(f'Result: {self.added_count} new, {self.updated_count} updated, {self.deleted_count} deleted, '
                    f'and {self.unchanged_count} unchanged from cache')

        return self.fresh_tree

    def _add_tracked_copy(self, fmeta, new_category):
        meta_copy = copy.deepcopy(fmeta)
        if fmeta.category != Category.Ignored:
            meta_copy.category = new_category
        self.change_tree.add(meta_copy)
