import logging
import os

from cache import fmeta_tree_cache
from cache.cache_registry_db import CACHE_TYPE_GDRIVE, CACHE_TYPE_LOCAL_DISK, CacheInfoEntry, CacheRegistry
from cache.fmeta_tree_cache import SqliteCache
from file_util import get_resource_path
from fmeta.fmeta import FMeta, FMetaTree
from fmeta.fmeta_tree_loader import FMetaTreeLoader
from ui.actions import ID_GLOBAL_CACHE
from ui.tree.data_store import BaseStore

MAIN_REGISTRY_FILE_NAME = 'registry.db'


logger = logging.getLogger(__name__)

def get_md5(item):
    return item.md5

def get_sha256(item):
    return item.sha256

def get_full_path(item):
    return item.full_path

def get_file_name(item):
    return item.file_name


def _ensure_cache_dir_path(config):
    cache_dir_path = get_resource_path(config.get('cache_dir_path'))
    if not os.path.exists(cache_dir_path):
        logger.info(f'Cache directory does not exist; attempting to create: "{cache_dir_path}"')
    os.makedirs(name=cache_dir_path, exist_ok=True)
    return cache_dir_path


class TwoLevelDict:
    def __init__(self, key_func1, key_func2):
        self._dict = {}
        self._key_func1 = key_func1
        self._key_func2 = key_func2
        self._total = 0

    def put(self, item, overwrite_existing=False):
        key1 = self._key_func1(item)
        if not key1:
            raise RuntimeError(f'Key1 is null for item: {item}')
        dict2 = self._dict.get(key1, None)
        if dict2 is None:
            dict2 = {key1: {}}
            self._dict[key1] = dict2
        else:
            key2 = self._key_func2(item)
            if not key2:
                raise RuntimeError(f'Key2 is null for item: {item}')
            existing = dict2.get(key2, None)
            if not existing or overwrite_existing:
                dict2[key2] = item
            return existing
        return None


class FullPathBeforeMd5Dict(TwoLevelDict):
    def __init__(self):
        super().__init__(get_full_path, get_md5)


class Md5BeforePathDict(TwoLevelDict):
    def __init__(self):
        super().__init__(get_md5, get_full_path)


class Sha256BeforePathDict(TwoLevelDict):
    def __init__(self):
        super().__init__(get_sha256, get_full_path)


class PathBeforeSha256Dict(TwoLevelDict):
    def __init__(self):
        super().__init__(get_full_path, get_sha256)


# TODO: rename BaseStore to BaseMetaStore
class LocalDiskSubtreeMS(BaseStore):
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

        # TODO: dir tree...
      #  self.parent_path_dict = ParentPathBeforeMd5Dict()

    def add_or_update_item(self, fmeta: FMeta):
        # TODO!
        pass

    def get_subtree(self, subtree_path, tree_id):
        fmeta_tree = FMetaTree(root_path=subtree_path)

        # TODO: 1. Load as many items as possible from the in-memory cache
        # TODO: 2. Sync from disk
        # TODO: 3. Save to disk cache again (if configured)

        ds = LocalDiskSubtreeMS(tree_id=tree_id, config=self.application.config, fmeta_tree=fmeta_tree)
        return ds

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
        self.cache_registry_db = CacheRegistry(self.main_registry_path)

        self.local_disk_cache = None
        self.gdrive_cache = None

    def load_all_caches(self):
        """Should be called during startup. Loop over all caches and load/merge them into a
        single large in-memory cache"""
        self.local_disk_cache = LocalDiskMasterCache(self.application)
        self.gdrive_cache = GDriveMasterCache(self.application)

        exisiting_caches = self.cache_registry_db.get_cache_info()
        for existing in exisiting_caches:
            if existing.cache_type == CACHE_TYPE_LOCAL_DISK:
                self.load_local_disk_cache(existing)
            elif existing.cache_type == CACHE_TYPE_GDRIVE:
                self.load_gdrive_cache(existing)
            else:
                raise RuntimeError(f'Unrecognized value for cache_type: {existing.cache_type}')

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
