import logging
import os
import time
from typing import Dict

from pydispatch import dispatcher
from stopwatch import Stopwatch

from index.master_gdrive import GDriveMasterCache
from index.master_local import LocalDiskMasterCache
from index.sqlite.cache_registry_db import CacheInfoEntry, CacheRegistry
from index.sqlite.fmeta_db import FMetaDatabase
from constants import CACHE_TYPE_GDRIVE, CACHE_TYPE_LOCAL_DISK, MAIN_REGISTRY_FILE_NAME
from file_util import get_resource_path
from fmeta.fmeta_tree_loader import TreeMetaScanner
from model.fmeta_tree import FMetaTree
from model.planning_node import PlanningNode
from ui import actions
from ui.actions import ID_GLOBAL_CACHE

logger = logging.getLogger(__name__)


def _ensure_cache_dir_path(config):
    cache_dir_path = get_resource_path(config.get('cache.cache_dir_path'))
    if not os.path.exists(cache_dir_path):
        logger.info(f'Cache directory does not exist; attempting to create: "{cache_dir_path}"')
    os.makedirs(name=cache_dir_path, exist_ok=True)
    return cache_dir_path


# ⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛
# CLASS PersistedCacheInfo
# ⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟


class PersistedCacheInfo:
    def __init__(self, cache_info: CacheInfoEntry):
        self.cache_info = cache_info
        self.is_loaded = False
        # Indicates the data needs to be loaded from disk again.
        # TODO: replace this with a more sophisticated mechanism
        self.needs_refresh = False


# ⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛
# CLASS CacheManager
# ⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟

# -> only the "rea1" nodes should go in the cache. Other nodes (e.g. 'planning nodes') should not
class CacheManager:
    def __init__(self, application):
        self.application = application

        self.cache_dir_path = _ensure_cache_dir_path(self.application.config)
        self.main_registry_path = os.path.join(self.cache_dir_path, MAIN_REGISTRY_FILE_NAME)
        self.persisted_cache_info: Dict[str, PersistedCacheInfo] = {}

        self.enable_load_from_disk = application.config.get('cache.enable_cache_load')
        self.enable_save_to_disk = application.config.get('cache.enable_cache_save')
        self.load_all_caches_on_startup = application.config.get('cache.load_all_caches_on_startup')
        self.sync_from_local_disk_on_cache_load = application.config.get('cache.sync_from_local_disk_on_cache_load')

        if not self.load_all_caches_on_startup:
            logger.debug('Configured not to fetch all caches on startup; will lazy load instead')

        self.local_disk_cache = None
        self.gdrive_cache = None

    def load_all_caches(self, sender):
        """Should be called during startup. Loop over all caches and load/merge them into a
        single large in-memory cache"""
        logger.debug(f'Received signal: {actions.LOAD_ALL_CACHES}')
        if self.local_disk_cache:
            logger.info(f'Caches already loaded. Ignoring signal from {sender}.')
            return
        logger.debug(f'CacheManager.load_all_caches() initiated by {sender}')
        self.local_disk_cache = LocalDiskMasterCache(self.application)
        self.gdrive_cache = GDriveMasterCache(self.application)

        with CacheRegistry(self.main_registry_path) as cache_registry_db:
            if cache_registry_db.has_cache_info():
                exisiting_caches = cache_registry_db.get_cache_info()
                logger.debug(f'Found {len(exisiting_caches)} caches listed in registry')
            else:
                exisiting_caches = []
                logger.debug('Registry has no caches listed')

        for existing_disk_cache in exisiting_caches:
            already_in_memory = self.persisted_cache_info.get(existing_disk_cache.subtree_root, None)
            if already_in_memory:
                if existing_disk_cache.sync_ts < already_in_memory.cache_info.sync_ts:
                    logger.info(f'Skipping cache load: a newer cache already exists for the same subtree: {existing_disk_cache.subtree_root}')
                    continue
            info = PersistedCacheInfo(existing_disk_cache)
            self.persisted_cache_info[existing_disk_cache.subtree_root] = info
            if existing_disk_cache.cache_type == CACHE_TYPE_LOCAL_DISK:
                if not self.load_all_caches_on_startup:
                    info.needs_refresh = True
                elif os.path.exists(existing_disk_cache.subtree_root):
                    stopwatch_total = Stopwatch()
                    # 1. Load from disk cache:
                    fmeta_tree = self.load_local_disk_cache(existing_disk_cache)
                    # 2. Update from the file system, and optionally save any changes back to cache:
                    if self.application.cache_manager.sync_from_local_disk_on_cache_load:
                        self._refresh_from_local_fs(fmeta_tree, ID_GLOBAL_CACHE)
                    else:
                        logger.debug('Skipping file system sync because it is disabled for cache loads')
                    logger.info(f'Tree loaded in: {stopwatch_total}')
                    info.is_loaded = True
                else:
                    logger.info(f'Subtree not found; assuming it is a removable drive: "{existing_disk_cache.subtree_root}"')
                    info.needs_refresh = True

            elif existing_disk_cache.cache_type == CACHE_TYPE_GDRIVE:
                self.load_gdrive_cache(existing_disk_cache)
            else:
                raise RuntimeError(f'Unrecognized value for cache_type: {existing_disk_cache.cache_type}')

        logger.debug('Done loading caches')
        dispatcher.send(signal=actions.LOAD_ALL_CACHES_DONE, sender=ID_GLOBAL_CACHE)

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

    def _sync_from_file_system(self, stale_tree: FMetaTree, tree_id: str):
        # Scan directory tree and update where needed.
        logger.debug(f'Scanning filesystem subtree: {stale_tree.root_path}')
        scanner = TreeMetaScanner(root_path=stale_tree.root_path, stale_tree=stale_tree, tree_id=tree_id, track_changes=False)
        scanner.scan()
        fresh_tree = scanner.fresh_tree
        # Update in-memory cache:
        for item in fresh_tree.get_all():
            if not isinstance(item, PlanningNode):  # Planning nodes should not be cached, and should remain in their trees
                self.local_disk_cache.add_or_update_item(item)
                # FIXME: need to enable track changes, and handle deletes, etc
                # FIXME FIXME FIXME

        logger.debug(self.local_disk_cache.get_summary())
        return fresh_tree

    def get_metastore_for_local_subtree(self, subtree_path, tree_id):
        """
        Performs a read-through retreival of all the FMetas in the given subtree
        on the local filesystem.
        """
        return self.local_disk_cache.get_metastore_for_subtree(subtree_path, tree_id)

    def load_gdrive_cache(self, existing: CacheInfoEntry):
        # TODO
        pass

    def get_gdrive_subtree(self, subtree_path):
        pass

    def save_to_local_disk_cache(self, fmeta_tree: FMetaTree):
        # Get existing cache location if available. We will overwrite it.
        cache_info = self._get_or_create_cache_info_entry(fmeta_tree.root_path)
        to_insert = fmeta_tree.get_all()

        stopwatch_write_cache = Stopwatch()
        with FMetaDatabase(cache_info.cache_location) as fmeta_disk_cache:
            # Update cache:
            fmeta_disk_cache.insert_local_files(to_insert, overwrite=True)

        logger.info(f'Wrote {str(len(to_insert))} FMetas to "{cache_info.cache_location}" in {stopwatch_write_cache}')

    def _get_or_create_cache_info_entry(self, subtree_root: str) -> CacheInfoEntry:
        existing = self.persisted_cache_info.get(subtree_root, None)
        if existing:
            return existing.cache_info

        mangled_file_name = 'FS' + subtree_root.replace('/', '_')
        cache_location = os.path.join(self.cache_dir_path, mangled_file_name)
        now_ms = int(time.time())
        new_cache_info = CacheInfoEntry(cache_location=cache_location,
                                        cache_type=CACHE_TYPE_LOCAL_DISK,
                                        subtree_root=subtree_root, sync_ts=now_ms,
                                        is_complete=True)

        with CacheRegistry(self.main_registry_path) as cache_registry_db:
            cache_registry_db.create_cache_registry_if_not_exist()
            cache_registry_db.insert_cache_info(new_cache_info, append=True, overwrite=False)

        return new_cache_info

    def _refresh_from_local_fs(self, stale_tree: FMetaTree, tree_id: str) -> FMetaTree:
        # Bring it up to date with the file system, and also update in-memory store:
        fresh_tree = self._sync_from_file_system(stale_tree, tree_id)
        # Save the updates back to local disk cache:
        if self.enable_save_to_disk:
            self.save_to_local_disk_cache(fresh_tree)
        return fresh_tree

        return stale_tree
