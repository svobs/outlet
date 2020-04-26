import logging
import os

from pydispatch import dispatcher

from cache.cache_registry_db import CACHE_TYPE_GDRIVE, CACHE_TYPE_LOCAL_DISK, CacheInfoEntry, CacheRegistry
from cache.fmeta_tree_cache import SqliteCache
from cache.two_level_dict import FullPathBeforeMd5Dict, Md5BeforePathDict, ParentPathBeforeFileNameDict, Sha256BeforePathDict
from file_util import get_resource_path
from model.fmeta import FMeta
from fmeta.fmeta_tree_loader import FMetaTreeLoader
from model.fmeta_tree import FMetaTree
from ui import actions
from ui.actions import ID_GLOBAL_CACHE
from ui.diff_tree.bulk_fmeta_data_store import BulkLoadFMetaStore
from ui.tree.meta_store import BaseMetaStore

MAIN_REGISTRY_FILE_NAME = 'registry.db'


logger = logging.getLogger(__name__)


def _ensure_cache_dir_path(config):
    cache_dir_path = get_resource_path(config.get('cache_dir_path'))
    if not os.path.exists(cache_dir_path):
        logger.info(f'Cache directory does not exist; attempting to create: "{cache_dir_path}"')
    os.makedirs(name=cache_dir_path, exist_ok=True)
    return cache_dir_path


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


class LocalDiskMasterCache:
    def __init__(self, application):
        self.application = application
        self.full_path_dict = FullPathBeforeMd5Dict()
        self.md5_dict = Md5BeforePathDict()
        self.sha256_dict = Sha256BeforePathDict()
        self.parent_path_dict = ParentPathBeforeFileNameDict()

    def add_or_update_item(self, fmeta: FMeta):
        logger.debug('Adding item')
        existing = self.full_path_dict.put(fmeta)
        self.md5_dict.put(fmeta, existing)
        self.sha256_dict.put(fmeta, existing)
        self.parent_path_dict.put(fmeta, existing)

    def get_subtree(self, subtree_path, tree_id):
        logger.debug(f'Getting items from in-memory cache for subtree: {subtree_path}')
        fmeta_tree = FMetaTree(root_path=subtree_path)

        # 1. Load as many items as possible from the in-memory cache


        # TODO: 2. Sync from disk
        # TODO: 3. Save to disk cache again (if configured)

        # ds = LocalDiskSubtreeMS(tree_id=tree_id, config=self.application.config, fmeta_tree=fmeta_tree)
        # return ds

        return BulkLoadFMetaStore(tree_id=tree_id, config=self.application.config, root_path=subtree_path)


class GDriveMasterCache:
    def __init__(self, application):
        self.application = application
        self.full_path_dict = FullPathBeforeMd5Dict()
        self.md5_dict = Md5BeforePathDict()

    def get_subtree(self, subtree_path, tree_id):
        pass
        # TODO!


class CacheManager:
    def __init__(self, application):
        self.application = application

        self.cache_dir_path = _ensure_cache_dir_path(self.application.config)

        self.main_registry_path = os.path.join(self.cache_dir_path, MAIN_REGISTRY_FILE_NAME)

        self.local_disk_cache = None
        self.gdrive_cache = None

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

        for existing in exisiting_caches:
            if existing.cache_type == CACHE_TYPE_LOCAL_DISK:
                self.load_local_disk_cache(existing)
            elif existing.cache_type == CACHE_TYPE_GDRIVE:
                self.load_gdrive_cache(existing)
            else:
                raise RuntimeError(f'Unrecognized value for cache_type: {existing.cache_type}')

        logger.debug('Done loading caches')
        dispatcher.send(signal=actions.LOAD_ALL_CACHES_DONE, sender=ID_GLOBAL_CACHE)

    def load_local_disk_cache(self, cache_info: CacheInfoEntry):
        #### Legacy code follows...

        # Load cache from file, and update with any local FS changes found:
        legacy_cache = SqliteCache(tree_id=ID_GLOBAL_CACHE, db_file_path=cache_info.subtree_root, enable_load=True, enable_update=True)
        tree_loader = FMetaTreeLoader(tree_root_path=cache_info.subtree_root, cache=legacy_cache, tree_id=ID_GLOBAL_CACHE)
        # TODO: dump tree loader code here
        fmeta_tree = tree_loader.get_current_tree()
        fmeta_list = fmeta_tree.get_all()
        for fmeta in fmeta_list:
            self.local_disk_cache.add_or_update_item(fmeta)

        # TODO: migrate cache save from FMetaTreeLoader to here

    def get_local_disk_subtree(self, subtree_path, tree_id):
        # TODO: query our registry for a tree with the given path
        # TODO: return a Cache instance which will handle all loading and persistence, but
        # TODO: will delegate file resolution to the central registry, which may or may
        # TODO: not have information for each file. The central registry will also be
        # TODO: responsible for keeping the metadata up-to-date and recalculating MD5s etc.

        return self.local_disk_cache.get_subtree(subtree_path, tree_id)

    def load_gdrive_cache(self, existing: CacheInfoEntry):
        # TODO
        pass

    def get_gdrive_subtree(self, subtree_path):
        pass
