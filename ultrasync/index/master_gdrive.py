import logging
from typing import List

from pydispatch import dispatcher

from constants import OBJ_TYPE_GDRIVE, ROOT
from gdrive.gdrive_tree_loader import GDriveTreeLoader
from index.cache_manager import PersistedCacheInfo
from index.two_level_dict import FullPathBeforeUidDict, Md5BeforeUidDict
from model import display_id
from model.display_id import GDriveIdentifier, Identifier
from model.gdrive_subtree import GDriveSubtree
from model.gdrive_whole_tree import GDriveTree, GDriveWholeTree
from model.goog_node import GoogNode
from stopwatch_sec import Stopwatch
from ui import actions

logger = logging.getLogger(__name__)

"""
# CLASS GDriveMasterCache
# ⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟

Some notes:
- Need to create a store which can keep track of whether each parent has all children. If not we
will have to make a request to retrieve all nodes with 'X' as parent and update the store before
returning

- GoogRemote >= GoogDiskStores >= GoogInMemoryStore >= DisplayStore

- GoogDiskCache should try to download all dirs & files ASAP. But in the meantime, download level by level

- Every time you expand a node, you should call to sync it from the GoogStore.
- Every time you retrieve new data from G, you must perform sanity checks on it
"""


class GDriveMasterCache:
    """Singleton in-memory cache for Google Drive"""
    def __init__(self, application):
        self.application = application
        self.full_path_dict = FullPathBeforeUidDict()
        self.md5_dict = Md5BeforeUidDict()
        self.meta_master: GDriveWholeTree = None

    def init_subtree_gdrive_cache(self, info: PersistedCacheInfo, tree_id: str):
        self._load_gdrive_cache(info, tree_id)

    def _load_gdrive_cache(self, cache_info: PersistedCacheInfo, tree_id: str):
        """Loads an EXISTING GDrive cache from disk and updates the in-memory cache from it"""
        if not self.application.cache_manager.enable_load_from_disk:
            logger.debug('Skipping cache load because cache.enable_cache_load is False')
            return None
        status = f'Loading meta for "{cache_info.subtree_root.full_path}" from cache: "{cache_info.cache_location}"'
        logger.debug(status)
        dispatcher.send(actions.SET_PROGRESS_TEXT, sender=tree_id, msg=status)

        stopwatch_total = Stopwatch()

        cache_path = cache_info.cache_location
        tree_loader = GDriveTreeLoader(config=self.application.config, cache_path=cache_path, tree_id=tree_id)

        meta = GDriveWholeTree()
        tree_loader.load_from_cache(meta)
        self._update_in_memory_cache(meta)

        actions.get_dispatcher().send(actions.SET_PROGRESS_TEXT, sender=tree_id, tx_id=None, msg=f'Calculating paths for GDrive nodes...')

        # Fill in full paths
        # Needs to be done AFTER all the nodes in the tree have been downloaded
        full_path_stopwatch = Stopwatch()
        for item in meta.id_dict.values():
            meta.get_full_paths_for_item(item)

        logger.debug(f'{full_path_stopwatch} Full paths calculated for {len(meta.id_dict)} items')

        logger.info(f'{stopwatch_total} GDrive cache for {cache_info.subtree_root.full_path} loaded')

        cache_info.is_loaded = True
        self.meta_master = meta

    def _slice_off_subtree_from_master(self, subtree_root: GDriveIdentifier, tree_id: str) -> GDriveSubtree:
        root: GoogNode = self.meta_master.get_item_for_id(subtree_root.uid)
        if not root:
            return None

        subtree_meta = GDriveSubtree(whole_tree=self.meta_master, root_node=root)

        return subtree_meta

    def load_subtree(self, subtree_root: GDriveIdentifier, tree_id: str) -> GDriveTree:
        if subtree_root == ROOT:
            subtree_root = GDriveTree.get_root_constant_identifier()
        logger.debug(f'Getting meta for subtree: "{subtree_root}"')
        cache_man = self.application.cache_manager
        # TODO: currently we will just load the root and use that.
        #       But in the future we should do on-demand retrieval of subtrees
        root = display_id.for_values(OBJ_TYPE_GDRIVE, ROOT, ROOT)
        cache_info = cache_man.get_or_create_cache_info_entry(root)
        if not cache_info.is_loaded:
            # Load from disk
            # TODO: this will fail if the cache does not exist. Need the above!
            logger.debug(f'Cache is not loaded: {cache_info.cache_location}')
            self._load_gdrive_cache(cache_info, tree_id)

        if subtree_root.uid == ROOT:
            # Special case. GDrive does not have a single root (it treats shared drives as roots, for example).
            # We'll use this special token to represent "everything"
            gdrive_meta = self.meta_master
        else:
            slice_timer = Stopwatch()
            gdrive_meta = self._slice_off_subtree_from_master(subtree_root, tree_id)
            if gdrive_meta:
                logger.debug(f'{slice_timer} Sliced off {gdrive_meta}')
            else:
                raise RuntimeError(f'Cannot load subtree because it does not exist: "{subtree_root}"')
        return gdrive_meta

    def download_all_gdrive_meta(self, tree_id):
        root_identifier = GDriveIdentifier(uid=ROOT, full_path=ROOT)
        cache_info = self.application.cache_manager.get_or_create_cache_info_entry(root_identifier)
        cache_path = cache_info.cache_location
        tree_loader = GDriveTreeLoader(config=self.application.config, cache_path=cache_path, tree_id=tree_id)
        self.meta_master = tree_loader.load_all(invalidate_cache=False)
        logger.info('Replaced entire GDrive in-memory cache with downloaded meta')

    def _update_in_memory_cache(self, meta):
        # TODO
        pass

    def get_all_for_path(self, path: str) -> List[Identifier]:
        return self.meta_master.get_all_ids_for_path(path)
