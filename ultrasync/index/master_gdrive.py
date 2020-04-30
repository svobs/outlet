
# ⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛
# CLASS GDriveMasterCache
# ⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟
import logging

from stopwatch import Stopwatch

from constants import CACHE_TYPE_GDRIVE, GDRIVE_PREFIX, ROOT
from gdrive.gdrive_tree_loader import GDriveTreeLoader
from index.cache_manager import PersistedCacheInfo
from index.two_level_dict import FullPathDict, Md5BeforePathDict
from model.gdrive import GDriveMeta
from ui.actions import ID_GLOBAL_CACHE

logger = logging.getLogger(__name__)


class GDriveMasterCache:
    def __init__(self, application):
        self.application = application
        self.full_path_dict = FullPathDict()
        self.meta_master = GDriveMeta()

    def init_subtree_gdrive_cache(self, info: PersistedCacheInfo):
        stopwatch_total = Stopwatch()

        cache_path = info.cache_info.cache_location
        tree_builder = GDriveTreeLoader(config=self.application.config, cache_path=cache_path, tree_id=ID_GLOBAL_CACHE)

        meta = GDriveMeta()
        tree_builder.load_from_cache(meta)
        self._update_in_memory_cache(meta)
        logger.info(f'GDrive cache for {info.cache_info.subtree_root} loaded in: {stopwatch_total}')

        info.is_loaded = True

    def get_metastore_for_subtree(self, subtree_path, tree_id):
        pass
        # TODO!

    def download_all_gdrive_meta(self, tree_id):
        cache_info = self.application.cache_manager.get_or_create_cache_info_entry(CACHE_TYPE_GDRIVE, ROOT)
        cache_path = cache_info.cache_info.cache_location
        tree_builder = GDriveTreeLoader(config=self.application.config, cache_path=cache_path, tree_id=tree_id)
        meta = tree_builder.load_all(invalidate_cache=False)
        self.meta_master = meta
        logger.info('Replaced entire GDrive in-memory cache with downloaded meta')

    def _update_in_memory_cache(self, meta):
        # TODO
        pass

