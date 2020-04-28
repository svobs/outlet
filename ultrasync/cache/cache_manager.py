import logging
import os
import time
from queue import Queue
from typing import Dict, List

import treelib
from pydispatch import dispatcher
from stopwatch import Stopwatch

import file_util
from cache.cache_registry_db import CACHE_TYPE_GDRIVE, CACHE_TYPE_LOCAL_DISK, CacheInfoEntry, CacheRegistry
from cache.fmeta_db import FMetaDatabase
from cache.two_level_dict import FullPathBeforeMd5Dict, FullPathDict, Md5BeforePathDict, ParentPathBeforeFileNameDict, Sha256BeforePathDict
from file_util import get_resource_path
from model.fmeta import FMeta
from fmeta.fmeta_tree_loader import TreeMetaScanner
from model.fmeta_tree import FMetaTree
from model.planning_node import PlanningNode
from ui import actions
from ui.actions import ID_GLOBAL_CACHE
from ui.tree.meta_store import BaseMetaStore

MAIN_REGISTRY_FILE_NAME = 'registry.db'
ROOT = '/'


logger = logging.getLogger(__name__)


def _ensure_cache_dir_path(config):
    cache_dir_path = get_resource_path(config.get('cache_dir_path'))
    if not os.path.exists(cache_dir_path):
        logger.info(f'Cache directory does not exist; attempting to create: "{cache_dir_path}"')
    os.makedirs(name=cache_dir_path, exist_ok=True)
    return cache_dir_path

# ⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛
# CLASS LocalDiskSubtreeMS
# ⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟


class LocalDiskSubtreeMS(BaseMetaStore):
    """Meta store for a subtree on disk
    """
    def __init__(self, tree_id, config, fmeta_tree):
        super().__init__(tree_id, config)
        self._fmeta_tree = fmeta_tree

    def get_root_path(self):
        return self._fmeta_tree.root_path

    def get_whole_tree(self):
        return self._fmeta_tree


# ⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛
# CLASS LocalDiskMasterCache
# ⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟

class LocalDiskMasterCache:
    def __init__(self, application):
        self.application = application
        self.use_md5 = True
        if self.use_md5:
            self.md5_dict = Md5BeforePathDict()
        else:
            self.md5_dict = None

        self.use_sha256 = False
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

    def _get_subtree_from_memory_only(self, subtree_path):
        logger.debug(f'Getting items from in-memory cache for subtree: {subtree_path}')
        fmeta_tree = FMetaTree(root_path=subtree_path)
        count_added_from_cache = 0

        # Loop over all the descendants dirs, and add all of the files in each:
        q = Queue()
        q.put(subtree_path)
        while not q.empty():
            dir_path = q.get()
            files_in_dir = self.parent_path_dict.get(dir_path)
            for file_name, fmeta in files_in_dir.items():
                fmeta_tree.add(fmeta)
                count_added_from_cache += 1
            if self.dir_tree.get_node(dir_path):
                for child_dir in self.dir_tree.children(dir_path):
                    q.put(child_dir)

        logger.debug(f'Found {count_added_from_cache} items in memory cache')
        return fmeta_tree

    def get_metastore_for_subtree(self, subtree_path: str, tree_id: str) -> LocalDiskSubtreeMS:
        if not os.path.exists(subtree_path):
            raise RuntimeError(f'Cannot load meta for subtree because it does not exist: {subtree_path}')

        cache_man = self.application.cache_manager
        cache_info = cache_man.persisted_cache_info.get(subtree_path)
        if cache_info and not cache_info.is_loaded:
            # Load from disk
            in_memory_tree = cache_man.load_local_disk_cache(cache_info.cache_info)
            cache_info.is_loaded = True
        else:
            # Load as many items as possible from the in-memory cache.
            in_memory_tree = self._get_subtree_from_memory_only(subtree_path)

        # 2. Sync from disk, and save to disk cache again (if configured) and in-memory-store:
        fresh_tree = self.application.cache_manager.refresh_from_local_fs(in_memory_tree, tree_id)

        ds = LocalDiskSubtreeMS(tree_id=tree_id, config=self.application.config, fmeta_tree=fresh_tree)
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

# ⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛
# CLASS GDriveMasterCache
# ⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟


class GDriveMasterCache:
    def __init__(self, application):
        self.application = application
        self.full_path_dict = FullPathDict()
        self.md5_dict = Md5BeforePathDict()

    def get_subtree(self, subtree_path, tree_id):
        pass
        # TODO!

# ⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛
# CLASS PersistedCacheInfo
# ⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟


class PersistedCacheInfo:
    def __init__(self, cache_info: CacheInfoEntry):
        self.cache_info = cache_info
        self.is_loaded = False


# ⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛
# CLASS CacheManager
# ⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟

# TODO: what about "faux nodes" (moved/deleted)?
# -> only the "real" nodes should go in the cache. Other nodes ('planning nodes') should not
class CacheManager:
    def __init__(self, application):
        self.application = application

        self.cache_dir_path = _ensure_cache_dir_path(self.application.config)
        self.main_registry_path = os.path.join(self.cache_dir_path, MAIN_REGISTRY_FILE_NAME)
        self.persisted_cache_info: Dict[str, PersistedCacheInfo] = {}

        self.enable_load_from_disk = True
        self.enable_save_to_disk = True  # TODO: put in config

        self.local_disk_cache = None
        self.gdrive_cache = None

        """
        TODO!
def from_config(config, tree_id):
    enable_cache = config.get(f'transient.{tree_id}.cache.enable')
    if enable_cache:
        cache_file_path = config.get(f'transient.{tree_id}.cache.full_path')
        enable_load = config.get(f'transient.{tree_id}.cache.enable_load')
        enable_update = config.get(f'transient.{tree_id}.cache.enable_update')
        return SqliteCache(tree_id, cache_file_path, enable_load, enable_update)

    return NullCache()
"""

    def load_all_caches(self, sender):
        """Should be called during startup. Loop over all caches and load/merge them into a
        single large in-memory cache"""
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
                if os.path.exists(existing_disk_cache.subtree_root):
                    stopwatch_total = Stopwatch()
                    # 1. Load from disk cache:
                    fmeta_tree = self.load_local_disk_cache(existing_disk_cache)
                    # 2. Update from the file system, and optionally save any changes back to cache:
                    self.refresh_from_local_fs(fmeta_tree, ID_GLOBAL_CACHE)
                    logger.info(f'Tree loaded in: {stopwatch_total}')
                    info.is_loaded = True
                else:
                    logger.info(f'Subtree not found; assuming it is a removable drive: "{existing_disk_cache.subtree_root}"')

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
            logger.debug(status)
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
            logger.info(fmeta_tree.get_stats_string())

        return fmeta_tree

    def sync_from_file_system(self, stale_tree: FMetaTree, tree_id: str):
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
        cache_info = self.get_or_create_cache_info(fmeta_tree.root_path)
        to_insert = fmeta_tree.get_all()

        stopwatch_write_cache = Stopwatch()
        with FMetaDatabase(cache_info.cache_location) as fmeta_disk_cache:
            # Update cache:
            fmeta_disk_cache.insert_local_files(to_insert, overwrite=True)

        logger.info(f'Wrote {str(len(to_insert))} FMetas to disk cache in {stopwatch_write_cache}')

    def get_or_create_cache_info(self, subtree_root: str) -> CacheInfoEntry:
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

    def refresh_from_local_fs(self, stale_tree: FMetaTree, tree_id: str) -> FMetaTree:
        # 2. Bring it up to date with the file system, and also update in-memory store
        fresh_tree = self.sync_from_file_system(stale_tree, tree_id)
        # 3. Save the updates back to disk cache
        if self.enable_save_to_disk:
            self.save_to_local_disk_cache(fresh_tree)

        if tree_id:
            actions.set_status(sender=tree_id, status_msg=fresh_tree.get_summary())
        return fresh_tree
