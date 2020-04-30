import logging
import os
from queue import Queue

import treelib
from stopwatch import Stopwatch

import file_util
from fmeta.fmeta_tree_loader import TreeMetaScanner
from index.cache_info import CacheInfoEntry
from index.cache_manager import PersistedCacheInfo
from index.meta_store.local import LocalDiskSubtreeMS
from index.sqlite.fmeta_db import FMetaDatabase
from index.two_level_dict import FullPathDict, Md5BeforePathDict, ParentPathBeforeFileNameDict, Sha256BeforePathDict
from constants import CACHE_TYPE_LOCAL_DISK, ROOT
from model.fmeta import FMeta
from model.fmeta_tree import FMetaTree
from model.planning_node import PlanningNode
from ui import actions
from ui.actions import ID_GLOBAL_CACHE

logger = logging.getLogger(__name__)


# ⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛
# CLASS LocalDiskMasterCache
# ⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟


class LocalDiskMasterCache:
    def __init__(self, application):
        self.application = application
        self.use_md5 = application.config.get('cache.enable_md5_lookup')
        if self.use_md5:
            self.md5_dict = Md5BeforePathDict()
        else:
            self.md5_dict = None

        self.use_sha256 = application.config.get('cache.enable_sha256_lookup')
        if self.use_sha256:
            self.sha256_dict = Sha256BeforePathDict()
        else:
            self.sha256_dict = None
        self.full_path_dict = FullPathDict()

        # Each item inserted here will have an entry created for its dir.
        self.parent_path_dict = ParentPathBeforeFileNameDict()
        # But we still need a dir tree to look up child dirs:
        self.dir_tree = treelib.Tree()
        self.dir_tree.create_node(tag=ROOT, identifier=ROOT)

    def get_summary(self):
        if self.use_md5:
            md5 = str(self.md5_dict.total_entries)
        else:
            md5 = 'disabled'
        return f'LocalDiskMasterCache size: full_path={self.full_path_dict.total_entries} parent_path={self.parent_path_dict.total_entries} md5={md5}'

    def add_or_update_item(self, item: FMeta):
        """TODO: Need to make this atomic"""
        existing = self.full_path_dict.put(item)
        if self.use_md5 and item.md5:
            self.md5_dict.put(item, existing)
        if self.use_sha256 and item.sha256:
            self.sha256_dict.put(item, existing)
        self.parent_path_dict.put(item, existing)
        self._add_ancestors_to_tree(item.full_path)

    def init_subtree_localfs_cache(self, cache_info: PersistedCacheInfo):
        """Called at startup to handle a single subtree cache from a local fs"""

        if not os.path.exists(cache_info.cache_info.subtree_root):
            logger.info(f'Subtree not found; assuming it is a removable drive: "{cache_info.cache_info.subtree_root}"')
            cache_info.needs_refresh = True
            return

        stopwatch_total = Stopwatch()

        # 1. Load from disk cache:
        fmeta_tree = self.load_local_disk_cache(cache_info.cache_info)

        # 2. Update from the file system, and optionally save any changes back to cache:
        cache_man = self.application.cache_manager
        # Bring it up to date with the file system, and also update in-memory store:
        if cache_man.sync_from_local_disk_on_cache_load:
            fmeta_tree = self._sync_from_file_system(fmeta_tree, ID_GLOBAL_CACHE)
            # Save the updates back to local disk cache:
            if cache_man.enable_save_to_disk:
                self.save_to_local_disk_cache(fmeta_tree)
        self._update_in_memory_cache(fmeta_tree)

        if cache_man.sync_from_local_disk_on_cache_load:
            self._sync_from_file_system(fmeta_tree, ID_GLOBAL_CACHE)
        else:
            logger.debug('Skipping file system sync because it is disabled for cache loads')
        logger.info(f'LocalFS cache for {cache_info.cache_info.subtree_root} loaded in: {stopwatch_total}')

        cache_info.is_loaded = True

    def _get_subtree_from_memory_only(self, subtree_path):
        logger.debug(f'Getting items from in-memory cache for subtree: {subtree_path}')
        fmeta_tree = FMetaTree(root_path=subtree_path)
        count_dirs = 0
        count_added_from_cache = 0

        # Loop over all the descendants dirs, and add all of the files in each:
        q = Queue()
        q.put(subtree_path)
        while not q.empty():
            dir_path = q.get()
            count_dirs += 1
            files_in_dir = self.parent_path_dict.get(dir_path)
            for file_name, fmeta in files_in_dir.items():
                fmeta_tree.add(fmeta)
                count_added_from_cache += 1
            if self.dir_tree.get_node(dir_path):
                for child_dir in self.dir_tree.children(dir_path):
                    q.put(child_dir.identifier)

        logger.debug(f'Got {count_added_from_cache} items from in-memory cache (from {count_dirs} dirs)')
        return fmeta_tree

    def load_local_disk_cache(self, cache_info: CacheInfoEntry) -> FMetaTree:
        fmeta_tree = FMetaTree(cache_info.subtree_root)

        # Load cache from file, and update with any local FS changes found:
        with FMetaDatabase(cache_info.cache_location) as fmeta_disk_cache:
            if not fmeta_disk_cache.has_local_files():
                logger.debug('No meta found in cache')
                return fmeta_tree

            status = f'Loading meta for subtree "{cache_info.subtree_root}" from disk cache: {cache_info.cache_location}'
            logger.info(status)
            actions.set_status(sender=ID_GLOBAL_CACHE, status_msg=status)

            db_file_changes = fmeta_disk_cache.get_local_files()
            if len(db_file_changes) == 0:
                logger.debug('No data found in disk cache')

            count_from_disk = 0
            for change in db_file_changes:
                existing = fmeta_tree.get_for_path(change.full_path)
                # Overwrite older changes for the same path:
                if existing is None:
                    fmeta_tree.add(change)
                    count_from_disk += 1
                elif existing.sync_ts < change.sync_ts:
                    fmeta_tree.add(change)

            logger.debug(f'Reduced {str(len(db_file_changes))} disk cache entries into {str(count_from_disk)} unique entries')
            logger.debug(fmeta_tree.get_stats_string())

        return fmeta_tree

    def save_to_local_disk_cache(self, fmeta_tree: FMetaTree):
        # Get existing cache location if available. We will overwrite it.
        cache_info = self.application.cache_manager.get_or_create_cache_info_entry(fmeta_tree.root_path)
        to_insert = fmeta_tree.get_all()

        stopwatch_write_cache = Stopwatch()
        with FMetaDatabase(cache_info.cache_location) as fmeta_disk_cache:
            # Update cache:
            fmeta_disk_cache.insert_local_files(to_insert, overwrite=True)

        logger.info(f'Wrote {str(len(to_insert))} FMetas to "{cache_info.cache_location}" in {stopwatch_write_cache}')

    def get_metastore_for_subtree(self, subtree_path: str, tree_id: str) -> LocalDiskSubtreeMS:
        if not os.path.exists(subtree_path):
            raise RuntimeError(f'Cannot load meta for subtree because it does not exist: {subtree_path}')

        cache_man = self.application.cache_manager
        cache_info = cache_man.get_cache_info_entry(CACHE_TYPE_LOCAL_DISK, subtree_path)
        if cache_info and not cache_info.is_loaded:
            # Load from disk
            fmeta_tree = self.load_local_disk_cache(cache_info.cache_info)
            cache_info.is_loaded = True
        else:
            # Load as many items as possible from the in-memory cache.
            fmeta_tree = self._get_subtree_from_memory_only(subtree_path)

        sync_to_fs = True
        if cache_info:
            if not self.application.cache_manager.sync_from_local_disk_on_cache_load:
                logger.debug('Skipping filesystem sync because it is disabled for cache loads')
                sync_to_fs = False
            elif not cache_info.needs_refresh:
                logger.debug(f'Skipping filesystem sync because the cache is still fresh for path: {subtree_path}')
                sync_to_fs = False
        if sync_to_fs:
            # Sync from disk, and save to disk cache again (if configured) and update in-memory-store:
            fmeta_tree = self.application.cache_manager.refresh_from_local_fs(fmeta_tree, tree_id)

        ds = LocalDiskSubtreeMS(tree_id=tree_id, config=self.application.config, fmeta_tree=fmeta_tree)
        return ds

    def _add_ancestors_to_tree(self, item_full_path):
        nid = ROOT
        parent = self.dir_tree.get_node(nid)
        dirs_str = os.path.dirname(item_full_path)
        path_segments = file_util.split_path(dirs_str)

        for dir_name in path_segments:
            nid = os.path.join(nid, dir_name)
            child = self.dir_tree.get_node(nid=nid)
            if child is None:
                # logger.debug(f'Creating dir node: nid={nid}')
                child = self.dir_tree.create_node(tag=dir_name, identifier=nid, parent=parent)
            parent = child

    def _update_in_memory_cache(self, fresh_tree):
        for item in fresh_tree.get_all():
            if not isinstance(item, PlanningNode):  # Planning nodes should not be cached, and should remain in their trees
                self.add_or_update_item(item)
                # FIXME: need to enable track changes, and handle deletes, etc
                # FIXME FIXME FIXME

        logger.debug(self.get_summary())

    def _sync_from_file_system(self, stale_tree: FMetaTree, tree_id: str):
        # Scan directory tree and update where needed.
        logger.debug(f'Scanning filesystem subtree: {stale_tree.root_path}')
        scanner = TreeMetaScanner(root_path=stale_tree.root_path, stale_tree=stale_tree, tree_id=tree_id, track_changes=False)
        scanner.scan()
        fresh_tree = scanner.fresh_tree

        return fresh_tree
