import logging
from queue import Queue
from typing import Optional

from pydispatch import dispatcher
from stopwatch import Stopwatch

from constants import NOT_TRASHED, OBJ_TYPE_GDRIVE, ROOT
from gdrive.gdrive_tree_loader import GDriveTreeLoader
from index.cache_manager import PersistedCacheInfo
from index.meta_store.gdrive import GDriveMS
from index.two_level_dict import FullPathBeforeIdDict, Md5BeforeIdDict
from model.gdrive_meta import GDriveMeta
from model.goog_node import GoogFolder
from ui import actions

logger = logging.getLogger(__name__)


# CLASS GDriveMasterCache
# ⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟


class GDriveMasterCache:
    """Singleton in-memory cache for Google Drive"""
    def __init__(self, application):
        self.application = application
        self.full_path_dict = FullPathBeforeIdDict()
        self.md5_dict = Md5BeforeIdDict()
        self.meta_master = None

    def init_subtree_gdrive_cache(self, info: PersistedCacheInfo, tree_id: str):
        self._load_gdrive_cache(info, tree_id)

    def _load_gdrive_cache(self, info: PersistedCacheInfo, tree_id: str) -> GDriveMeta:
        """Loads an EXISTING GDrive cache from disk and updates the in-memory cache from it"""
        status = f'Loading meta for "{info.cache_info.subtree_root}" from cache: "{info.cache_info.cache_location}"'
        logger.debug(status)
        dispatcher.send(actions.SET_PROGRESS_TEXT, sender=tree_id, msg=status)

        stopwatch_total = Stopwatch()

        cache_path = info.cache_info.cache_location
        tree_builder = GDriveTreeLoader(config=self.application.config, cache_path=cache_path, tree_id=tree_id)

        meta = GDriveMeta()
        tree_builder.load_from_cache(meta)
        self._update_in_memory_cache(meta)
        logger.info(f'GDrive cache for {info.cache_info.subtree_root} loaded in: {stopwatch_total}')

        info.is_loaded = True
        self.meta_master = meta  # TODO
        return meta

    def _slice_off_subtree_from_master(self, subtree_root_id) -> GDriveMeta:
        subtree_meta = GDriveMeta()

        root = self.meta_master.get_for_id(subtree_root_id)
        if not root:
            raise RuntimeError(f'Root not found: {subtree_root_id}')

        q = Queue()
        q.put(root)

        count_trashed = 0
        count_total = 0

        while not q.empty():
            item: GoogFolder = q.get()
            if item.trashed == NOT_TRASHED:
                subtree_meta.add_item(item)
            else:
                count_trashed += 1
            count_total += 1

            child_list = self.meta_master.get_children(item.id)
            if child_list:
                for child in child_list:
                    q.put(child)

        md5_count = subtree_meta.md5_dict.total_entries
        logger.debug(f'Sliced off subtree with {(count_total - count_trashed)} items (+{count_trashed} trashed), {md5_count} MD5s')
        return subtree_meta

    def get_metastore_for_subtree(self, subtree_root_id, tree_id):
        logger.debug(f'Getting metastore for subtree: "{subtree_root_id}"')
        cache_man = self.application.cache_manager
        # TODO: currently we will just load the root and use that.
        #       But in the future we should do on-demand retrieval of subtrees
        cache_info = cache_man.get_or_create_cache_info_entry(OBJ_TYPE_GDRIVE, ROOT)
        if not cache_info.is_loaded:
            # Load from disk
            # TODO: this will fail if the cache does not exist. Need the above!
            self._load_gdrive_cache(cache_info, tree_id)

        if subtree_root_id == ROOT:
            # Special case. GDrive does not have a single root (it treats shared drives as roots, for example).
            # We'll use this special token to represent "everything"
            gdrive_meta = self.meta_master
        else:
            gdrive_meta = self._slice_off_subtree_from_master(subtree_root_id)
        return GDriveMS(tree_id, self.application.config, gdrive_meta, ROOT)

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

    def get_path_for_id(self, goog_id: str) -> Optional[str]:
        if not self.meta_master:
            logger.warning('Cannot look up item: caches have not been loaded!')
            return None

        item = self.meta_master.get_for_id(goog_id)
        if not item:
            raise RuntimeError(f'Item not found: id={goog_id}')
        path = ''
        while True:
            path = '/' + item.name + path
            parents = item.parents
            if not parents:
                return path
            elif len(parents > 1):
                logger.debug(f'Multiple parents found for {item.id} ("{item.name}"). Picking the first one.')
                item = self.meta_master.get_for_id(parents[0])
