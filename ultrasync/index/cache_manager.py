import logging
import os
import threading
import time
import uuid
from typing import Dict, List

from pydispatch import dispatcher

from constants import OBJ_TYPE_GDRIVE, OBJ_TYPE_LOCAL_DISK, MAIN_REGISTRY_FILE_NAME
from file_util import get_resource_path
from index.cache_info import CacheInfoEntry, PersistedCacheInfo
from index.master_gdrive import GDriveMasterCache
from index.master_local import LocalDiskMasterCache
from index.sqlite.cache_registry_db import CacheRegistry
from ui import actions
from ui.actions import ID_GLOBAL_CACHE

logger = logging.getLogger(__name__)


def _ensure_cache_dir_path(config):
    cache_dir_path = get_resource_path(config.get('cache.cache_dir_path'))
    if not os.path.exists(cache_dir_path):
        logger.info(f'Cache directory does not exist; attempting to create: "{cache_dir_path}"')
    os.makedirs(name=cache_dir_path, exist_ok=True)
    return cache_dir_path


#    CLASS CacheManager
# ⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟


# -> only the "rea1" nodes should go in the cache. Other nodes (e.g. 'planning nodes') should not
class CacheManager:
    def __init__(self, application):
        self.application = application

        self.cache_dir_path = _ensure_cache_dir_path(self.application.config)
        self.main_registry_path = os.path.join(self.cache_dir_path, MAIN_REGISTRY_FILE_NAME)

        self.persisted_localfs_cache_info: Dict[str, PersistedCacheInfo] = {}
        self.persisted_gdrive_cache_info: Dict[str, PersistedCacheInfo] = {}

        self.enable_load_from_disk = application.config.get('cache.enable_cache_load')
        self.enable_save_to_disk = application.config.get('cache.enable_cache_save')
        self.load_all_caches_on_startup = application.config.get('cache.load_all_caches_on_startup')
        self.sync_from_local_disk_on_cache_load = application.config.get('cache.sync_from_local_disk_on_cache_load')

        if not self.load_all_caches_on_startup:
            logger.info('Configured not to fetch all caches on startup; will lazy load instead')

        self.local_disk_cache = None
        self.gdrive_cache = None

        # Create an Event object.
        self.all_caches_loaded = threading.Event()

    def load_all_caches(self, sender):
        """Should be called during startup. Loop over all caches and load/merge them into a
        single large in-memory cache"""
        logger.debug(f'Received signal: {actions.LOAD_ALL_CACHES}')
        if self.local_disk_cache:
            logger.info(f'Caches already loaded. Ignoring signal from {sender}.')
            return

        logger.debug(f'CacheManager.load_all_caches() initiated by {sender}')
        logger.debug(f'Sending START_PROGRESS_INDETERMINATE for ID: {ID_GLOBAL_CACHE}')
        tx_id = uuid.uuid1()
        dispatcher.send(actions.START_PROGRESS_INDETERMINATE, sender=ID_GLOBAL_CACHE, tx_id=tx_id)

        try:
            self.local_disk_cache = LocalDiskMasterCache(self.application)
            self.gdrive_cache = GDriveMasterCache(self.application)

            for existing_disk_cache in self._get_cache_info_from_registry():
                try:
                    self._init_existing_cache(existing_disk_cache)
                except Exception:
                    logger.exception(f'Failed to load cache: {existing_disk_cache.cache_location}')

            logger.debug('Done loading caches')
        finally:
            dispatcher.send(actions.STOP_PROGRESS, sender=ID_GLOBAL_CACHE, tx_id=tx_id)
            self.all_caches_loaded.set()
            dispatcher.send(signal=actions.LOAD_ALL_CACHES_DONE, sender=ID_GLOBAL_CACHE)

    def _get_cache_info_from_registry(self) -> List[CacheInfoEntry]:
        with CacheRegistry(self.main_registry_path) as cache_registry_db:
            if cache_registry_db.has_cache_info():
                exisiting_caches = cache_registry_db.get_cache_info()
                logger.debug(f'Found {len(exisiting_caches)} caches listed in registry')
                return exisiting_caches
            else:
                logger.debug('Registry has no caches listed')
                return []

    def _init_existing_cache(self, existing_disk_cache: CacheInfoEntry):
        logger.debug(f'Loading cache: type={existing_disk_cache.cache_type} subtree="{existing_disk_cache.subtree_root}"')

        from_memory: PersistedCacheInfo = self.get_cache_info_entry(existing_disk_cache.cache_type, existing_disk_cache.subtree_root)

        if from_memory and existing_disk_cache.sync_ts < from_memory.cache_info.sync_ts:
            logger.info(f'Skipping cache load: a newer cache already exists for the same subtree: {existing_disk_cache.subtree_root}')
            return

        info = PersistedCacheInfo(existing_disk_cache)

        if existing_disk_cache.cache_type == OBJ_TYPE_LOCAL_DISK:
            self.persisted_localfs_cache_info[existing_disk_cache.subtree_root] = info
        elif existing_disk_cache.cache_type == OBJ_TYPE_GDRIVE:
            self.persisted_gdrive_cache_info[existing_disk_cache.subtree_root] = info

        if self.load_all_caches_on_startup:
            if existing_disk_cache.cache_type == OBJ_TYPE_LOCAL_DISK:
                self.local_disk_cache.init_subtree_localfs_cache(info, ID_GLOBAL_CACHE)
            elif existing_disk_cache.cache_type == OBJ_TYPE_GDRIVE:
                self.gdrive_cache.init_subtree_gdrive_cache(info, ID_GLOBAL_CACHE)
            else:
                raise RuntimeError(f'Unrecognized value for cache_type: {existing_disk_cache.cache_type}')

    def get_metastore_for_subtree(self, subtree_path: str, tree_type: int, tree_id: str):
        """
        Performs a read-through retreival of all the FMetas in the given subtree
        on the local filesystem.
        """
        if tree_type == OBJ_TYPE_LOCAL_DISK:
            return self.local_disk_cache.get_metastore_for_subtree(subtree_path, tree_id)
        elif tree_type == OBJ_TYPE_GDRIVE:
            return self.gdrive_cache.get_metastore_for_subtree(subtree_path, tree_id)
        else:
            raise RuntimeError(f'Unrecognized tree type: {tree_type}')

    def get_metastore_for_local_subtree(self, subtree_path, tree_id):
        """
        Performs a read-through retreival of all the FMetas in the given subtree
        on the local filesystem.
        """
        return self.local_disk_cache.get_metastore_for_subtree(subtree_path, tree_id)

    def get_metastore_for_gdrive_subtree(self, subtree_path, tree_id):
        return self.gdrive_cache.get_metastore_for_subtree(subtree_path, tree_id)

    def download_all_gdrive_meta(self, tree_id):
        return self.gdrive_cache.download_all_gdrive_meta(tree_id)

    def get_cache_info_entry(self, cache_type, subtree_root) -> PersistedCacheInfo:
        if cache_type == OBJ_TYPE_LOCAL_DISK:
            already_in_memory = self.persisted_localfs_cache_info.get(subtree_root, None)
        elif cache_type == OBJ_TYPE_GDRIVE:
            already_in_memory = self.persisted_gdrive_cache_info.get(subtree_root, None)
        else:
            raise RuntimeError(f'Unknown cache type: {cache_type}')
        return already_in_memory

    def get_or_create_cache_info_entry(self, tree_type, subtree_root: str) -> PersistedCacheInfo:
        existing = self.get_cache_info_entry(tree_type, subtree_root)
        if existing:
            logger.debug(f'Found existing cache for type={tree_type} subtree="{subtree_root}"')
            return existing
        else:
            logger.debug(f'No existing cache found for type={tree_type} subtree="{subtree_root}"')

        if tree_type == OBJ_TYPE_LOCAL_DISK:
            prefix = 'FS'
        elif tree_type == OBJ_TYPE_GDRIVE:
            prefix = 'GD'
        else:
            raise RuntimeError(f'Unrecognized tree type: {tree_type}')

        mangled_file_name = prefix + subtree_root.replace('/', '_')
        cache_location = os.path.join(self.cache_dir_path, mangled_file_name)
        now_ms = int(time.time())
        new_cache_info = CacheInfoEntry(cache_location=cache_location,
                                        cache_type=tree_type,
                                        subtree_root=subtree_root, sync_ts=now_ms,
                                        is_complete=True)

        with CacheRegistry(self.main_registry_path) as cache_registry_db:
            cache_registry_db.create_cache_registry_if_not_exist()
            cache_registry_db.insert_cache_info(new_cache_info, append=True, overwrite=False)

        info_info = PersistedCacheInfo(new_cache_info)

        # Save reference in memory
        if tree_type == OBJ_TYPE_LOCAL_DISK:
            self.persisted_localfs_cache_info[subtree_root] = info_info
        elif tree_type == OBJ_TYPE_GDRIVE:
            self.persisted_gdrive_cache_info[subtree_root] = info_info
        return info_info

    def get_gdrive_path_for_id(self, goog_id) -> str:
        return self.gdrive_cache.get_path_for_id(goog_id)

