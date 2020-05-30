import logging
import os
import threading
import time
from collections import deque
from typing import Deque, Dict, List, Optional, Union

import treelib
from pydispatch import dispatcher

import file_util
import fmeta.content_hasher
from constants import ROOT_PATH
from fmeta.fmeta_tree_scanner import TreeMetaScanner
from index.cache_manager import PersistedCacheInfo
from index.sqlite.fmeta_db import FMetaDatabase
from index.two_level_dict import FullPathDict, Md5BeforePathDict, ParentPathBeforeFileNameDict, Sha256BeforePathDict
from index.uid_generator import UID
from model.category import Category
from model.fmeta import FMeta
from model.fmeta_tree import FMetaTree
from model.node_identifier import LocalFsIdentifier
from model.planning_node import PlanningNode
from stopwatch_sec import Stopwatch
from ui import actions
from ui.actions import ID_GLOBAL_CACHE

logger = logging.getLogger(__name__)


# ⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛
# CLASS LocalDiskMasterCache
# ⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟


class LocalDiskMasterCache:
    def __init__(self, application):
        """Singleton in-memory cache for local filesystem"""
        self.application = application
        self.use_md5 = application.config.get('cache.enable_md5_lookup')
        if self.use_md5:
            self.md5_dict = Md5BeforePathDict()
        else:
            self.md5_dict = None

        # Every unique path must map to one unique UID
        self._full_path_uid_dict: Dict[str, UID] = {}

        self.use_sha256 = application.config.get('cache.enable_sha256_lookup')
        if self.use_sha256:
            self.sha256_dict = Sha256BeforePathDict()
        else:
            self.sha256_dict = None
        self.full_path_dict = FullPathDict()
        self._lock = threading.Lock()

        # Each item inserted here will have an entry created for its dir.
        self.parent_path_dict = ParentPathBeforeFileNameDict()
        # But we still need a dir tree to look up child dirs:
        self.dir_tree = treelib.Tree()
        self.dir_tree.create_node(identifier=ROOT_PATH)

    def get_uid_for_path(self, path: str, uid_suggestion: Optional[UID] = None) -> UID:
        with self._lock:
            return self._get_uid_for_path(path, uid_suggestion)

    def _get_uid_for_path(self, path: str, uid_suggestion: Optional[UID] = None) -> UID:
        assert path and path.startswith('/')
        uid = self._full_path_uid_dict.get(path, None)
        if not uid:
            if uid_suggestion:
                self._full_path_uid_dict[path] = uid_suggestion
            else:
                uid = self.application.uid_generator.get_new_uid()
            self._full_path_uid_dict[path] = uid
        return uid

    def get_summary(self):
        if self.use_md5:
            md5 = str(self.md5_dict.total_entries)
        else:
            md5 = 'disabled'
        return f'LocalDiskMasterCache size: full_path={self.full_path_dict.total_entries} parent_path={self.parent_path_dict.total_entries} md5={md5}'

    def add_or_update_fmeta(self, item: FMeta, fire_listeners=True):
        assert not self.parent_path_dict.get_single(os.path.dirname(item.full_path), os.path.basename(item.full_path)), f'Conflict for item {item}'

        existing = None
        with self._lock:
            uid = self._get_uid_for_path(item.full_path, item.uid)
            # logger.debug(f'ID: {uid}, path: {item.full_path}')
            assert (not item.uid) or (uid == item.uid)
            item.uid = uid
            existing = self.full_path_dict.put(item)
            if self.use_md5 and item.md5:
                self.md5_dict.put(item, existing)
            if self.use_sha256 and item.sha256:
                self.sha256_dict.put(item, existing)
            self.parent_path_dict.put(item, existing)
            self._add_ancestors_to_tree(item.full_path)

        if fire_listeners:
            if existing is not None:
                signal = actions.NODE_UPDATED
            else:
                signal = actions.NODE_ADDED
            dispatcher.send(signal=signal, sender=ID_GLOBAL_CACHE, node=item)

    def remove_fmeta(self, item: FMeta, fire_listeners=True):
        with self._lock:
            self.full_path_dict.remove(item)
            dirpath, name = os.path.split(item.full_path)
            self.parent_path_dict.remove(dirpath, name)

            if self.use_md5 and item.md5:
                self.md5_dict.remove(item.md5, item.full_path)
            if self.use_sha256 and item.sha256:
                self.sha256_dict.remove(item.sha256, item.full_path)

            if self.dir_tree.get_node(item.full_path):
                count_removed = self.dir_tree.remove_node(item.full_path)
                assert count_removed <= 1, f'Deleted {count_removed} nodes at {item.full_path}'

        if fire_listeners:
            dispatcher.send(signal=actions.NODE_REMOVED, sender=ID_GLOBAL_CACHE, node=item)

    # Loading stuff
    # ⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟

    def _get_subtree_from_memory_only(self, subtree_path: LocalFsIdentifier):
        stopwatch = Stopwatch()
        logger.debug(f'Getting items from in-memory cache for subtree: {subtree_path}')
        fmeta_tree = FMetaTree(root_identifier=subtree_path, application=self.application)
        count_dirs = 0
        count_added_from_cache = 0

        # Loop over all the descendants dirs, and add all of the files in each:
        q: Deque[str] = deque()
        q.append(subtree_path.full_path)
        while len(q) > 0:
            dir_path = q.popleft()
            count_dirs += 1
            files_in_dir: Dict[str, Union[FMeta, PlanningNode]] = self.parent_path_dict.get_second_dict(dir_path)
            for file_name, fmeta in files_in_dir.items():
                fmeta_tree.add_item(fmeta)
                count_added_from_cache += 1
            if self.dir_tree.get_node(dir_path):
                for child_dir in self.dir_tree.children(dir_path):
                    q.append(child_dir.identifier)

        logger.debug(f'{stopwatch} Got {count_added_from_cache} items from in-memory cache (from {count_dirs} dirs)')
        return fmeta_tree

    # Load/save on-disk cache:

    def load_subtree_from_disk(self, cache_info: PersistedCacheInfo, tree_id) -> Optional[FMetaTree]:
        """Loads the given subtree disk cache from disk."""

        stopwatch_load = Stopwatch()

        # Load cache from file, and update with any local FS changes found:
        with FMetaDatabase(cache_info.cache_location, self.application) as fmeta_disk_cache:
            if not fmeta_disk_cache.has_local_files():
                logger.debug('No meta found in cache')
                return None

            status = f'[{tree_id}] Loading meta for "{cache_info.subtree_root}" from cache: "{cache_info.cache_location}"'
            logger.debug(status)
            dispatcher.send(actions.SET_PROGRESS_TEXT, sender=tree_id, msg=status)

            uid = self.get_uid_for_path(cache_info.subtree_root.full_path, cache_info.subtree_root.uid)
            root_identifier = LocalFsIdentifier(full_path=cache_info.subtree_root.full_path, uid=uid)
            fmeta_tree = FMetaTree(root_identifier=root_identifier, application=self.application)

            db_file_changes: List[FMeta] = fmeta_disk_cache.get_local_files()
            if len(db_file_changes) == 0:
                logger.debug('No data found in disk cache')

            count_from_disk = 0
            for change in db_file_changes:
                existing = fmeta_tree.get_for_path(change.full_path)
                # Overwrite older changes for the same path:
                if not existing:
                    fmeta_tree.add_item(change)
                    count_from_disk += 1
                elif existing[0].sync_ts < change.sync_ts:
                    fmeta_tree.add_item(change)

            # logger.debug(f'Reduced {str(len(db_file_changes))} disk cache entries into {str(count_from_disk)} unique entries')
            logger.debug(f'{stopwatch_load} [{tree_id}] Finished loading {fmeta_tree}')

            cache_info.is_loaded = True
            return fmeta_tree

    def save_subtree_disk_cache(self, fmeta_tree: FMetaTree):
        # Get existing cache location if available. We will overwrite it.
        cache_info = self.application.cache_manager.get_or_create_cache_info_entry(fmeta_tree.node_identifier)
        to_insert = fmeta_tree.get_all()

        stopwatch_write_cache = Stopwatch()
        with FMetaDatabase(cache_info.cache_location, self.application) as fmeta_disk_cache:
            # Update cache:
            fmeta_disk_cache.insert_local_files(to_insert, overwrite=True)

        logger.info(f'{stopwatch_write_cache} Wrote {str(len(to_insert))} FMetas to "{cache_info.cache_location}"')

    # Load/save on-disk cache:

    def load_subtree(self, cache_info: PersistedCacheInfo, tree_id, fmeta_tree: Optional[FMetaTree] = None) -> FMetaTree:
        assert cache_info
        stopwatch_total = Stopwatch()
        has_data_to_store_in_memory = False

        if not fmeta_tree:
            if cache_info.is_loaded:
                fmeta_tree = self._get_subtree_from_memory_only(cache_info.subtree_root)
            else:
                # Load from disk

                fmeta_tree: Optional[FMetaTree] = None
                if self.application.cache_manager.enable_load_from_disk:
                    fmeta_tree = self.load_subtree_from_disk(cache_info, tree_id)
                else:
                    logger.debug('Skipping cache load because cache.enable_cache_load is false')

                if fmeta_tree:
                    has_data_to_store_in_memory = True
                else:
                    uid = self.get_uid_for_path(cache_info.subtree_root.full_path, cache_info.subtree_root.uid)
                    root_identifier = LocalFsIdentifier(full_path=cache_info.subtree_root.full_path, uid=uid)
                    fmeta_tree = FMetaTree(root_identifier=root_identifier, application=self.application)

        if cache_info.is_loaded and not self.application.cache_manager.sync_from_local_disk_on_cache_load:
            logger.debug('Skipping filesystem sync because it is disabled for cache loads')
        elif cache_info.is_loaded and not cache_info.needs_refresh:
            logger.debug(f'Skipping filesystem sync because the cache is still fresh for path: {cache_info.subtree_root}')
        else:
            # Update from the file system, and optionally save any changes back to cache:
            fmeta_tree = self._resync_with_file_system(fmeta_tree, tree_id)
            cache_info.needs_refresh = False
            has_data_to_store_in_memory = True

        # Save the updates back to in-memory cache:
        if has_data_to_store_in_memory:
            self._update_in_memory_cache(fmeta_tree)

        logger.info(f'{stopwatch_total} LocalFS cache for {cache_info.subtree_root.full_path} loaded')

        return fmeta_tree

    def _add_ancestors_to_tree(self, item_full_path):
        nid: str = ROOT_PATH
        parent: treelib.Node = self.dir_tree.get_node(nid)
        dirs_str = os.path.dirname(item_full_path)
        path_segments = file_util.split_path(dirs_str)

        for dir_name in path_segments:
            nid: str = os.path.join(nid, dir_name)
            child: treelib.Node = self.dir_tree.get_node(nid=nid)
            if child is None:
                # logger.debug(f'Creating dir node: nid={nid}')
                child = self.dir_tree.create_node(identifier=nid, parent=parent)
            parent = child

    def _update_in_memory_cache(self, fresh_tree):
        for item in fresh_tree.get_all():
            self.add_or_update_fmeta(item, fire_listeners=False)

        logger.debug(f'Updated in-memory cache: {self.get_summary()}')

    def _resync_with_file_system(self, stale_tree: FMetaTree, tree_id: str):
        # Scan directory tree and update where needed.
        logger.debug(f'Scanning filesystem subtree: {stale_tree.root_path}')
        scanner = TreeMetaScanner(application=self.application, root_node_identifer=stale_tree.node_identifier, stale_tree=stale_tree,
                                  tree_id=tree_id, track_changes=False)
        scanner.scan()
        fresh_tree = scanner.fresh_tree

        # Save the updates back to local disk cache:
        cache_man = self.application.cache_manager
        if cache_man.enable_save_to_disk:
            self.save_subtree_disk_cache(fresh_tree)
        else:
            logger.debug('Skipping cache save because it is disabled')

        return fresh_tree

    def build_fmeta(self, full_path: str, category=Category.NA, staging_path=None) -> Optional[FMeta]:
        uid = self.get_uid_for_path(full_path)

        if category == Category.Ignored:
            # Do not scan ignored files for content (optimization)
            md5 = None
            sha256 = None
        else:
            try:
                # Open,close, read file and calculate hash of its contents
                if staging_path:
                    md5 = fmeta.content_hasher.md5(staging_path)
                else:
                    md5 = fmeta.content_hasher.md5(full_path)
                # sha256 = fmeta.content_hasher.dropbox_hash(full_path)
                sha256 = None
            except FileNotFoundError:
                if os.path.islink(full_path):
                    target = os.readlink(full_path)
                    logger.error(f'Broken link, skipping: "{full_path}" -> "{target}"')
                else:
                    # can this actually happen?
                    logger.error(f'File not found; skipping: {full_path}')
                # Return None. Will be assumed to be a deleted file
                return None

        # Get "now" in UNIX time:
        sync_ts = int(time.time())

        if staging_path:
            path = staging_path
        else:
            path = full_path

        stat = os.stat(path)
        size_bytes = int(stat.st_size)
        modify_ts = int(stat.st_mtime * 1000)
        assert modify_ts > 100000000000, f'modify_ts too small: {modify_ts} for path: {path}'
        change_ts = int(stat.st_ctime * 1000)
        assert change_ts > 100000000000, f'change_ts too small: {change_ts} for path: {path}'

        return FMeta(uid, md5, sha256, size_bytes, sync_ts, modify_ts, change_ts, full_path, category)
