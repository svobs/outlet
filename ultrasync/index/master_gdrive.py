import logging
from queue import Queue
from typing import List, Optional

from pydispatch import dispatcher

from model import display_id
from model.display_id import GDriveIdentifier, Identifier
from model.gdrive_tree import GDriveSubtree, GDriveTree, GDriveWholeTree
from stopwatch_sec import Stopwatch

from constants import NOT_TRASHED, OBJ_TYPE_GDRIVE, ROOT
from gdrive.gdrive_tree_loader import GDriveTreeLoader
from index.cache_manager import PersistedCacheInfo
from index.meta_store.gdrive import GDriveMS
from index.two_level_dict import FullPathBeforeUidDict, Md5BeforeUidDict
from model.goog_node import GoogFolder, GoogNode
from ui import actions
from ui.tree.meta_store import DummyMS

logger = logging.getLogger(__name__)


# CLASS GDriveMasterCache
# ⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟


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
        tree_builder = GDriveTreeLoader(config=self.application.config, cache_path=cache_path, tree_id=tree_id)

        meta = GDriveWholeTree()  # TODO
        tree_builder.load_from_cache(meta)
        self._update_in_memory_cache(meta)
        logger.info(f'{stopwatch_total} GDrive cache for {cache_info.subtree_root.full_path} loaded')

        cache_info.is_loaded = True
        self.meta_master = meta  # TODO

    def _slice_off_subtree_from_master(self, subtree_root: GDriveIdentifier, tree_id: str) -> GDriveSubtree:
        subtree_meta = GDriveSubtree(subtree_root)

        root: GoogNode = self.meta_master.get_for_id(subtree_root.uid)
        if not root:
            return None

        q = Queue()
        q.put(root)

        count_trashed = 0
        count_total = 0

        while not q.empty():
            item: GoogNode = q.get()
            # Filter out trashed items:
            if item.trashed == NOT_TRASHED:
                subtree_meta.add_item(item)
            else:
                count_trashed += 1
            count_total += 1

            child_list = self.meta_master.get_children(item.uid)
            if child_list:
                for child in child_list:
                    q.put(child)

        if logger.isEnabledFor(logging.DEBUG):
            subtree_meta.validate()

        # Calculate full paths
        # Needs to be done AFTER all the nodes in the tree have been downloaded
        for item in subtree_meta.id_dict.values():
            full_path = subtree_meta.get_full_path_for_item(item)

            item.identifier.full_path = full_path

        return subtree_meta

    def get_metastore_for_subtree(self, subtree_root: GDriveIdentifier, tree_id: str):
        if subtree_root == ROOT:
            subtree_root = GDriveTree.get_root_identifier()
        logger.debug(f'Getting metastore for subtree: "{subtree_root}"')
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
            # TODO: this will not work
            return GDriveMS(tree_id, self.application.config, gdrive_meta, subtree_root)
        else:
            slice_timer = Stopwatch()
            gdrive_meta = self._slice_off_subtree_from_master(subtree_root, tree_id)
            if gdrive_meta:
                logger.debug(f'{slice_timer} Sliced off {gdrive_meta}')
            else:
                logger.info(f'Cannot load meta for subtree because it does not exist: "{subtree_root}". '
                            f'Returning an empty metastore')
                return DummyMS(tree_id, self.application.config, subtree_root)
        return GDriveMS(tree_id, self.application.config, gdrive_meta, subtree_root)

    def download_all_gdrive_meta(self, tree_id):
        root_identifier = GDriveIdentifier(uid=ROOT, full_path=ROOT)
        cache_info = self.application.cache_manager.get_or_create_cache_info_entry(root_identifier)
        cache_path = cache_info.cache_info.cache_location
        tree_builder = GDriveTreeLoader(config=self.application.config, cache_path=cache_path, tree_id=tree_id)
        self.meta_master = tree_builder.load_all(invalidate_cache=False)
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
                logger.debug(f'Mapped ID "{goog_id}" to path "{path}"')
                return path
            elif len(parents) > 1:
                logger.warning(f'Multiple parents found for {item.uid} ("{item.name}"). Picking the first one.')
                # pass through
            item = self.meta_master.get_for_id(parents[0])

    def get_all_for_path(self, path: str) -> List[Identifier]:
        return self.meta_master.get_all_ids_for_path(path)
