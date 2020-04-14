import logging
import os
import errno
import copy
from datetime import datetime
import time
from pathlib import Path
from stopwatch import Stopwatch
from fmeta.fmeta import FMeta, FMetaTree, Category
from fmeta.tree_recurser import TreeRecurser
import fmeta.content_hasher
from database import MetaDatabase
import file_util
from ui.progress_meter import ProgressMeter

logger = logging.getLogger(__name__)


VALID_SUFFIXES = ('jpg', 'jpeg', 'png', 'gif', 'bmp', 'tiff', 'heic', 'mov', 'mp4', 'mpeg', 'mpg', 'm4v', 'avi', 'pdf', 'nef')


def build_fmeta(root_path, file_path, category=Category.NA):
    if category == Category.Ignored:
        # Do not scan ignored files for content (optimization)
        signature_str = None
    else:
        # Open,close, read file and calculate hash of its contents
        signature_str = fmeta.content_hasher.dropbox_hash(file_path)

    relative_path = file_util.strip_root(file_path, root_path)

    # Get "now" in UNIX time:
    date_time_now = datetime.now()
    sync_ts = int(time.mktime(date_time_now.timetuple()))

    stat = os.stat(file_path)
    size_bytes = int(stat.st_size)
    modify_ts = int(stat.st_mtime)
    change_ts = int(stat.st_ctime)

    return FMeta(signature_str, size_bytes, sync_ts, modify_ts, change_ts, relative_path, category)


def meta_matches(file_path, fmeta: FMeta):
    stat = os.stat(file_path)
    size_bytes = int(stat.st_size)
    modify_ts = int(stat.st_mtime)
    change_ts = int(stat.st_ctime)

    is_equal = fmeta.size_bytes == size_bytes and fmeta.modify_ts == modify_ts and fmeta.change_ts == change_ts

    if False and logger.isEnabledFor(logging.DEBUG):
        logger.debug(f'Meta Exp=[{fmeta.size_bytes} {fmeta.modify_ts} {fmeta.change_ts}]' +
                     f' Act=[{size_bytes} {modify_ts} {change_ts}] -> {is_equal}')

    return is_equal


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


class FMetaTreeCache:
    """
    Acts as a cache for a single FMetaTree.
    """
    def __init__(self, db_file_path):
        self.db = MetaDatabase(db_file_path)

    def has_data(self):
        return self.db.has_file_changes()

    def load_fmeta_tree(self, root_path):
        fmeta_tree = FMetaTree(root_path)

        db_file_changes = self.db.get_file_changes()
        if len(db_file_changes) == 0:
            raise RuntimeError('No data in database!')

        counter = 0
        for change in db_file_changes:
            meta = fmeta_tree.get_for_path(change.file_path)
            # Overwrite older changes for the same path:
            if meta is None or meta.sync_ts < change.sync_ts:
                fmeta_tree.add(change)
                counter += 1

        logger.debug(f'Reduced {str(len(db_file_changes))} DB entries into {str(counter)} entries')
        logger.info(fmeta_tree.get_stats_string())
        return fmeta_tree

    def save_fmeta_tree(self, fmeta_tree):
        if self.has_data():
            raise RuntimeError('Will not insert FMeta into DB! It is not empty')

        to_insert = fmeta_tree.get_all()
        self.db.insert_file_changes(to_insert)
        logger.info(f'Inserted {str(len(to_insert))} FMetas into previously empty DB table.')

    def overwrite_fmeta_tree(self, fmeta_tree):
        self.db.truncate_file_changes()
        # TODO: remove all rows!

        self.save_fmeta_tree(fmeta_tree)


class TreeMetaScanner(TreeRecurser):
    """
    Walks the filesystem for a subtree (FMetaTree), using a cache if configured,
    to generate an up-to-date FMetaTree.
    """
    def __init__(self, root_path, stale_tree=None, progress_meter=None, track_changes=False):
        TreeRecurser.__init__(self, Path(stale_tree.root_path), valid_suffixes=VALID_SUFFIXES)
        # Note: this tree will be useless after we are done with it
        self.root_path = root_path
        self.stale_tree = stale_tree
        self.progress_meter = progress_meter
        self.added_count = 0
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

    def find_total_files_to_scan(self):
        # First survey our local files:
        logger.info(f'Scanning path: {self.root_path}')
        file_counter = FileCounter(self.root_path)
        file_counter.recurse_through_dir_tree()
        logger.debug(f'Found {file_counter.files_to_scan} files to scan.')
        if self.progress_meter is not None:
            self.progress_meter.set_total(file_counter.files_to_scan)

    def handle_file(self, file_path, category):
        meta = None
        if self.stale_tree is not None:
            meta = self.stale_tree.get_for_path(file_path, include_ignored=True)

        if meta is not None and meta_matches(file_path, meta):
            # Found in cache.
            self.unchanged_count += 1
            meta = self.stale_tree.remove(file_path=meta.file_path, sig=meta.signature, ok_if_missing=False)
            if self._track_changes:
                self._add_tracked_copy(meta, Category.NA)
        else:
            # Uncached
            self.added_count += 1
            meta = build_fmeta(root_path=self.root_path, file_path=file_path, category=category)
            if self._track_changes:
                self._add_tracked_copy(meta, Category.Added)
        # TODO TODO TODO moved files!
        self.fresh_tree.add(meta)
        self.progress_meter.add_progress(1)

    def handle_target_file_type(self, file_path):
        self.handle_file(file_path, Category.NA)

    def handle_non_target_file(self, file_path):
        self.handle_file(file_path, Category.Ignored)

    def scan(self):
        """Recurse over disk tree. Gather current stats for each file, and compare to the stale tree.stale_tree
        For each current file found, remove from the stale tree.
        When recursion is complete, what's left in the stale tree will be deleted/moved files"""
        # TODO: progress meter with cache

        if not os.path.exists(self.root_path):
            raise FileNotFoundError(errno.ENOENT, os.strerror(errno.ENOENT), self.root_path)

        self.find_total_files_to_scan()
        self.recurse_through_dir_tree()

        if self.stale_tree is not None:
            self.deleted_count += len(self.stale_tree.get_all())
            if self._track_changes:
                # All remaining fmetas in the stale tree represent deleted
                for stale_fmeta in self.stale_tree.get_all():
                    self._add_tracked_copy(stale_fmeta, Category.Deleted)

        logger.info(f'Result: {self.added_count} new, {self.deleted_count} deleted, and {self.unchanged_count} unchanged from cache')

    def _add_tracked_copy(self, fmeta, new_category):
        meta_copy = copy.deepcopy(fmeta)
        if fmeta.category != Category.Ignored:
            meta_copy.category = new_category
        self.change_tree.add(meta_copy)


class FMetaTreeSource:
    """
    Encapsulates all logic needed to retrieve, update and cache a single FMetaTree.
    """
    def __init__(self, nickname, tree_root_path, enable_file_scan, enable_db_cache, db_cache_path):
        self.nickname = nickname
        self.tree_root_path = tree_root_path
        self.enable_file_scan = enable_file_scan
        self.enable_cache_loads = enable_db_cache
        self.enable_cache_updates = enable_db_cache
        self.db_cache_path = db_cache_path
        self.cache = FMetaTreeCache(db_cache_path)

    def get_current_tree(self, status_receiver=None):
        def on_progress_made(progress, total):
            if status_receiver:
                status_receiver.set_status(f'Scanning file {progress} of {total}')

        progress_meter = ProgressMeter(lambda p, t: on_progress_made(p, t))
        if status_receiver:
            status_msg = f'Scanning tree: {self.tree_root_path}'
            status_receiver.set_status(status_msg)

        stopwatch_total = Stopwatch()
        tree = None
        if self.enable_cache_loads and self.cache.has_data():
            if status_receiver:
                status = f'Loading {self.nickname} meta from cache: {self.db_cache_path}'
                logger.debug(status)
                status_receiver.set_status(status)
                tree = self.cache.load_fmeta_tree(self.tree_root_path)

        if self.enable_file_scan:
            logger.debug(f'Scanning: {self.tree_root_path}')
            scanner = TreeMetaScanner(root_path=self.tree_root_path, stale_tree=tree, progress_meter=progress_meter, track_changes=False)
            scanner.scan()
            tree = scanner.fresh_tree
            if self.enable_cache_updates:
                stopwatch_write_cache = Stopwatch()
                # Just overwrite all data - it's pretty fast, and less error prone:
                self.cache.overwrite_fmeta_tree(tree)
                stopwatch_write_cache.stop()
                logger.info(f'{self.nickname} updated cache in: {stopwatch_write_cache}')

        stopwatch_total.stop()
        logger.info(f'{self.nickname} loaded in: {stopwatch_total}')
        if status_receiver:
            status_receiver.set_status(tree.get_summary())
        return tree
