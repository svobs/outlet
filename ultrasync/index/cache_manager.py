import logging
import os
import threading
import time
import uuid
from typing import Dict, List

from pydispatch import dispatcher

from constants import CACHE_LOAD_TIMEOUT_SEC, GDRIVE_PATH_PREFIX, OBJ_TYPE_GDRIVE, OBJ_TYPE_LOCAL_DISK, MAIN_REGISTRY_FILE_NAME, ROOT
from file_util import get_resource_path
from index.cache_info import CacheInfoEntry, PersistedCacheInfo
from index.master_gdrive import GDriveMasterCache
from index.master_local import LocalDiskMasterCache
from index.sqlite.cache_registry_db import CacheRegistry
from index.two_level_dict import TwoLevelDict
from model.display_id import GDriveIdentifier, Identifier, LocalFsIdentifier
from model.goog_node import GoogNode
from model.subtree_snapshot import SubtreeSnapshot
from stopwatch_sec import Stopwatch
from ui import actions
from ui.actions import ID_GLOBAL_CACHE

logger = logging.getLogger(__name__)

CFG_ENABLE_LOAD_FROM_DISK = 'cache.enable_cache_load'


def _ensure_cache_dir_path(config):
    cache_dir_path = get_resource_path(config.get('cache.cache_dir_path'))
    if not os.path.exists(cache_dir_path):
        logger.info(f'Cache directory does not exist; attempting to create: "{cache_dir_path}"')
    os.makedirs(name=cache_dir_path, exist_ok=True)
    return cache_dir_path

#    CLASS CacheInfoByType
# ⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟


class CacheInfoByType(TwoLevelDict):
    def __init__(self):
        super().__init__(lambda x: x.subtree_root.tree_type, lambda x: x.subtree_root.uid, lambda x, y: True)


#    CLASS CacheManager
# ⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟

# -> only the "rea1" nodes should go in the cache. Other nodes (e.g. 'planning nodes') should not
class CacheManager:
    def __init__(self, application):
        self.application = application

        self.cache_dir_path = _ensure_cache_dir_path(self.application.config)
        self.main_registry_path = os.path.join(self.cache_dir_path, MAIN_REGISTRY_FILE_NAME)

        self.caches_by_type: CacheInfoByType = CacheInfoByType()

        self.enable_load_from_disk = application.config.get(CFG_ENABLE_LOAD_FROM_DISK)
        self.enable_save_to_disk = application.config.get('cache.enable_cache_save')
        self.load_all_caches_on_startup = application.config.get('cache.load_all_caches_on_startup')
        self.sync_from_local_disk_on_cache_load = application.config.get('cache.sync_from_local_disk_on_cache_load')
        self.reload_tree_on_root_path_update = application.config.get('cache.load_cache_when_tree_root_selected')

        if not self.load_all_caches_on_startup:
            logger.info('Configured not to fetch all caches on startup; will lazy load instead')

        self.local_disk_cache = None
        self.gdrive_cache = None

        # Create an Event object.
        self.all_caches_loaded = threading.Event()

    def load_all_caches(self, sender):
        """Should be called during startup. Loop over all caches and load/merge them into a
        single large in-memory cache"""
        logger.debug(f'Received signal: "{actions.LOAD_ALL_CACHES}"')
        if self.local_disk_cache:
            logger.info(f'Caches already loaded. Ignoring signal from {sender}.')
            return

        logger.debug(f'CacheManager.load_all_caches() initiated by {sender}')
        logger.debug(f'Sending START_PROGRESS_INDETERMINATE for ID: {ID_GLOBAL_CACHE}')
        stopwatch = Stopwatch()
        tx_id = uuid.uuid1()
        dispatcher.send(actions.START_PROGRESS_INDETERMINATE, sender=ID_GLOBAL_CACHE, tx_id=tx_id)

        try:
            self.local_disk_cache = LocalDiskMasterCache(self.application)
            self.gdrive_cache = GDriveMasterCache(self.application)

            # First put into map, to eliminate possible duplicates
            existing_caches = self._get_cache_info_from_registry()
            unique_cache_count = 0
            for existing_cache in existing_caches:
                info = PersistedCacheInfo(existing_cache)
                duplicate = self.caches_by_type.get_single(info.subtree_root.tree_type, info.subtree_root.full_path)
                if duplicate:
                    if duplicate.sync_ts < info.sync_ts:
                        logger.debug(f'Overwriting older duplicate cache info entry: {duplicate.subtree_root}')
                    else:
                        logger.debug(f'Skipping duplicate cache info entry: {duplicate.subtree_root}')
                        continue
                else:
                    unique_cache_count += 1
                self.caches_by_type.put(info)

            if self.load_all_caches_on_startup:
                existing_caches = self.caches_by_type.get_all()
                for cache_num, existing_disk_cache in enumerate(existing_caches):
                    try:
                        info = PersistedCacheInfo(existing_disk_cache)
                        self.caches_by_type.put(info)
                        logger.info(f'Init cache {(cache_num+1)}/{len(existing_caches)}: id={existing_disk_cache.subtree_root}')
                        self._init_existing_cache(info)
                    except Exception:
                        logger.exception(f'Failed to load cache: {existing_disk_cache.cache_location}')
                logger.info(f'{stopwatch} Load All Caches complete')
            else:
                logger.info(f'{stopwatch} Found {unique_cache_count} existing caches but configured not to load on startup')
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

    def _init_existing_cache(self, existing_disk_cache: PersistedCacheInfo):
        cache_type = existing_disk_cache.subtree_root.tree_type
        if cache_type != OBJ_TYPE_LOCAL_DISK and cache_type != OBJ_TYPE_GDRIVE:
            raise RuntimeError(f'Unrecognized tree type: {cache_type}')

        if cache_type == OBJ_TYPE_LOCAL_DISK:
            self.local_disk_cache.init_subtree_localfs_cache(existing_disk_cache, ID_GLOBAL_CACHE)
        elif cache_type == OBJ_TYPE_GDRIVE:
            self.gdrive_cache.init_subtree_gdrive_cache(existing_disk_cache, ID_GLOBAL_CACHE)

    def load_subtree(self, identifier: Identifier, tree_id: str) -> SubtreeSnapshot:
        """
        Performs a read-through retreival of all the FMetas in the given subtree
        on the local filesystem.
        """

        dispatcher.send(signal=actions.LOAD_TREE_STARTED, sender=tree_id)

        if identifier.tree_type == OBJ_TYPE_LOCAL_DISK:
            assert self.local_disk_cache
            return self.local_disk_cache.load_subtree(identifier, tree_id)
        elif identifier.tree_type == OBJ_TYPE_GDRIVE:
            assert self.gdrive_cache
            return self.gdrive_cache.load_subtree(identifier, tree_id)
        else:
            raise RuntimeError(f'Unrecognized tree type: {identifier.tree_type}')

    def load_local_subtree(self, subtree_path, tree_id) -> SubtreeSnapshot:
        """
        Performs a read-through retreival of all the FMetas in the given subtree
        on the local filesystem.
        """
        return self.local_disk_cache.load_subtree(subtree_path, tree_id)

    def load_gdrive_subtree(self, subtree_path: GDriveIdentifier, tree_id) -> SubtreeSnapshot:
        return self.gdrive_cache.load_subtree(subtree_path, tree_id)

    def download_all_gdrive_meta(self, tree_id):
        return self.gdrive_cache.download_all_gdrive_meta(tree_id)

    def get_cache_info_entry(self, subtree_root: Identifier) -> PersistedCacheInfo:
        return self.caches_by_type.get_single(subtree_root.tree_type, subtree_root.full_path)

    def get_or_create_cache_info_entry(self, subtree_root: Identifier) -> PersistedCacheInfo:
        existing = self.get_cache_info_entry(subtree_root)
        if existing:
            logger.debug(f'Found existing cache for type={subtree_root.tree_type} subtree="{subtree_root.uid}"')
            return existing
        else:
            logger.debug(f'No existing cache found for type={subtree_root.tree_type} subtree="{subtree_root.uid}"')

        if subtree_root.tree_type == OBJ_TYPE_LOCAL_DISK:
            prefix = 'FS'
        elif subtree_root.tree_type == OBJ_TYPE_GDRIVE:
            prefix = 'GD'
        else:
            raise RuntimeError(f'Unrecognized tree type: {subtree_root.tree_type}')

        mangled_file_name = prefix + subtree_root.uid.replace('/', '_') + '.db'
        cache_location = os.path.join(self.cache_dir_path, mangled_file_name)
        now_ms = int(time.time())
        db_entry = CacheInfoEntry(cache_location=cache_location,
                                        subtree_root=subtree_root, sync_ts=now_ms,
                                        is_complete=True)

        with CacheRegistry(self.main_registry_path) as cache_registry_db:
            cache_registry_db.create_cache_registry_if_not_exist()
            cache_registry_db.insert_cache_info(db_entry, append=True, overwrite=False)

        cache_info = PersistedCacheInfo(db_entry)

        # Save reference in memory
        self.caches_by_type.put(cache_info)

        return cache_info

    def get_all_for_path(self, path_string: str) -> List[Identifier]:
        if path_string.startswith(GDRIVE_PATH_PREFIX):
            # Need to wait until all caches are loaded:
            if not self.all_caches_loaded.wait(CACHE_LOAD_TIMEOUT_SEC):
                logger.error('Timed out waiting for all caches to load!')

            gdrive_path = path_string[len(GDRIVE_PATH_PREFIX):]
            matches = self.gdrive_cache.get_all_for_path(gdrive_path)
            if len(matches) == 0:
                return [GDriveIdentifier('NULL', gdrive_path)]
            else:
                return matches
        else:
            return [LocalFsIdentifier(full_path=path_string)]
