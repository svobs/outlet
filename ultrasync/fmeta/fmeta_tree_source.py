import logging
import os
import errno
import copy
from pathlib import Path
from stopwatch import Stopwatch
from fmeta.fmeta import FMetaTree, Category
from fmeta.tree_recurser import TreeRecurser
import fmeta.fmeta_builder
from fmeta.fmeta_builder import FMetaDirScanner, FMetaDatabase, FileCounter, VALID_SUFFIXES
from ui.progress_meter import ProgressMeter

logger = logging.getLogger(__name__)


class UpdateRecurser(TreeRecurser):
    def __init__(self, stale_tree, progress_meter: ProgressMeter, track_changes=False):
        TreeRecurser.__init__(self, Path(stale_tree.root_path), valid_suffixes=VALID_SUFFIXES)
        # Note: this tree will be useless after we are done with it
        self.stale_tree = stale_tree
        self.progress_meter = progress_meter
        self.added_count = 0
        self.deleted_count = 0
        self.unchanged_count = 0
        # When done, this will contain an up-to-date tree.
        self.fresh_tree = FMetaTree(stale_tree.root_path)
        self._track_changes = track_changes
        if self._track_changes:
            # Keep track of what's actually changed.
            # This is effectively a diff of stale & fresh trees.
            # Don't need it yet, but have a feeling it will be handy in the future.
            self.changes_found = FMetaTree(self.stale_tree.root_path)

    def find_total_files_to_scan(self):
        # First survey our local files:
        logger.info(f'Scanning path: {self.root_path}')
        file_counter = FileCounter(self.root_path)
        file_counter.recurse_through_dir_tree()
        logger.debug(f'Found {file_counter.files_to_scan} files to scan.')
        if self.progress_meter is not None:
            self.progress_meter.set_total(file_counter.files_to_scan)

    def handle_file(self, file_path, category):
        meta = self.stale_tree.get_for_path(file_path, include_ignored=True)
        if meta is not None and fmeta.fmeta_builder.meta_matches(file_path, meta):
            self.unchanged_count += 1
            meta = self.stale_tree.remove(file_path=meta.file_path, sig=meta.signature, ok_if_missing=False)
            if self._track_changes:
                self._add_tracked_copy(meta, Category.NA)
        else:
            self.added_count += 1
            meta = fmeta.fmeta_builder.build_fmeta(root_path=self.stale_tree.root_path, file_path=file_path, category=category)
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

        if not os.path.exists(self.stale_tree.root_path):
            raise FileNotFoundError(errno.ENOENT, os.strerror(errno.ENOENT), self.stale_tree.root_path)

        self.find_total_files_to_scan()
        self.recurse_through_dir_tree()

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
        self.changes_found.add(meta_copy)


class FMetaTreeSource:
    def __init__(self, nickname, tree_root_path, enable_db_cache, db_cache_path):
        self.nickname = nickname
        self.tree_root_path = tree_root_path
        self.enable_db_cache = enable_db_cache
        self.db_cache_path = db_cache_path
        self.cache = FMetaDatabase(db_cache_path)

    def get_current_tree(self, status_receiver=None):
        def on_progress_made(progress, total):
            if status_receiver:
                status_receiver.set_status(f'Scanning file {progress} of {total}')

        progress_meter = ProgressMeter(lambda p, t: on_progress_made(p, t))
        if status_receiver:
            status_msg = f'Scanning tree: {self.tree_root_path}'
            status_receiver.set_status(status_msg)

        tree: FMetaTree
        stopwatch = Stopwatch()
        if self.enable_db_cache:
            if self.cache.has_data():
                if status_receiver:
                    status_receiver.set_status(f'Loading {self.nickname} meta from cache: {self.db_cache_path}')
                tree = self.cache.load_fmeta_tree(self.tree_root_path)
                logger.debug(f'Syncing cache to disk: {self.tree_root_path}')
                tree = self._sync_tree_to_disk(tree, progress_meter=progress_meter)
            else:
                logger.debug(f'Performing full disk scan of: {self.tree_root_path}')
                tree = self._scan_disk(progress_meter=progress_meter)
                self.cache.save_fmeta_tree(tree)
        else:
            tree = self._scan_disk(progress_meter=progress_meter)
        stopwatch.stop()
        logger.info(f'{self.nickname} loaded in: {stopwatch}')
        if status_receiver:
            status_receiver.set_status(tree.get_summary())
        return tree

    def _scan_disk(self, progress_meter=None):
        dir_scanner = FMetaDirScanner(root_path=self.tree_root_path, progress_meter=progress_meter)
        return dir_scanner.scan_local_tree()

    def _sync_tree_to_disk(self, stale_tree, progress_meter):
        updater = UpdateRecurser(stale_tree, progress_meter)
        updater.scan()
        return updater.fresh_tree
