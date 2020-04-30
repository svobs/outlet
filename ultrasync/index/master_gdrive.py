
# ⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛
# CLASS GDriveMasterCache
# ⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟
import logging

from stopwatch import Stopwatch

from constants import OBJ_TYPE_GDRIVE, GDRIVE_PREFIX, ROOT
from gdrive.gdrive_tree_loader import GDriveTreeLoader
from index.cache_manager import PersistedCacheInfo
from index.meta_store.gdrive import GDriveMS
from index.two_level_dict import FullPathBeforeIdDict, Md5BeforeIdDict
from model.gdrive import GDriveMeta
from ui.actions import ID_GLOBAL_CACHE

logger = logging.getLogger(__name__)


class GDriveMasterCache:
    def __init__(self, application):
        self.application = application
        self.full_path_dict = FullPathBeforeIdDict()
        self.md5_dict = Md5BeforeIdDict()
        self.meta_master = GDriveMeta()

    def init_subtree_gdrive_cache(self, info: PersistedCacheInfo, tree_id: str):
        self.load_gdrive_cache(info, tree_id)

    def load_gdrive_cache(self, info: PersistedCacheInfo, tree_id: str) -> GDriveMeta:
        """Loads an EXISTING GDrive cache from disk and updates the in-memory cache from it"""
        stopwatch_total = Stopwatch()

        cache_path = info.cache_info.cache_location
        tree_builder = GDriveTreeLoader(config=self.application.config, cache_path=cache_path, tree_id=tree_id)

        meta = GDriveMeta()
        tree_builder.load_from_cache(meta)
        self._update_in_memory_cache(meta)
        logger.info(f'GDrive cache for {info.cache_info.subtree_root} loaded in: {stopwatch_total}')

        info.is_loaded = True
        return meta

    def get_metastore_for_subtree(self, subtree_path, tree_id):
        cache_man = self.application.cache_manager
        # TODO: currently we will just load the root and use that.
        #       But in the future we should do on-demand retrieval of subtrees
        cache_info = cache_man.get_or_create_cache_info_entry(OBJ_TYPE_GDRIVE, ROOT)

        if not cache_info.is_loaded:
            # Load from disk
            # TODO: this will fail if the cache does not exist. Need the above!
            gdrive_meta = self.load_gdrive_cache(cache_info, tree_id)
            self.meta_master = gdrive_meta
        return GDriveMS(tree_id, self.application.config, self.meta_master, ROOT)

    def download_all_gdrive_meta(self, tree_id):
        cache_info = self.application.cache_manager.get_or_create_cache_info_entry(OBJ_TYPE_GDRIVE, ROOT)
        cache_path = cache_info.cache_info.cache_location
        tree_builder = GDriveTreeLoader(config=self.application.config, cache_path=cache_path, tree_id=tree_id)
        meta = tree_builder.load_all(invalidate_cache=False)
        self.meta_master = meta
        logger.info('Replaced entire GDrive in-memory cache with downloaded meta')

    def _update_in_memory_cache(self, meta):
        # TODO
        pass

