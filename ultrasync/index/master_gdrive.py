
# ⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛
# CLASS GDriveMasterCache
# ⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟
import file_util
from constants import CACHE_TYPE_GDRIVE, GDRIVE_PREFIX, ROOT
from gdrive.gdrive_tree_loader import GDriveTreeLoader
from index.cache_manager import PersistedCacheInfo
from index.two_level_dict import FullPathDict, Md5BeforePathDict


class GDriveMasterCache:
    def __init__(self, application):
        self.application = application
        self.full_path_dict = FullPathDict()
        self.md5_dict = Md5BeforePathDict()

    def init_subtree_gdrive_cache(self, info: PersistedCacheInfo):
        # TODO
        pass

    def get_metastore_for_subtree(self, subtree_path, tree_id):
        pass
        # TODO!

    def download_all_gdrive_meta(self, tree_id):
        cache_info = self.application.cache_manager.get_or_create_cache_info_entry(CACHE_TYPE_GDRIVE, ROOT)
        cache_path = cache_info.cache_info.cache_location
        tree_builder = GDriveTreeLoader(config=self.application.config, cache_path=cache_path, tree_id=tree_id)
        return tree_builder.load_all(invalidate_cache=False)
