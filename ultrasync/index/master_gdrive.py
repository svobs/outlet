import logging
from typing import List, Optional

from pydispatch import dispatcher

from constants import ROOT_PATH
from gdrive.gdrive_tree_loader import GDriveTreeLoader
from index import uid_generator
from index.cache_manager import PersistedCacheInfo
from index.error import CacheNotLoadedError, GDriveItemNotFoundError
from index.sqlite.gdrive_db import GDriveDatabase
from index.two_level_dict import FullPathBeforeUidDict, Md5BeforeUidDict
from model.display_node import DisplayNode
from model.gdrive_subtree import GDriveSubtree
from model.gdrive_whole_tree import GDriveWholeTree
from model.goog_node import GoogNode
from model.node_identifier import GDriveIdentifier, NodeIdentifier, NodeIdentifierFactory
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
        self.meta_master: Optional[GDriveWholeTree] = None

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
        tree_loader = GDriveTreeLoader(application=self.application, cache_path=cache_path, tree_id=tree_id)

        meta: GDriveWholeTree = tree_loader.load_all(invalidate_cache=False)

        actions.get_dispatcher().send(actions.SET_PROGRESS_TEXT, sender=tree_id, msg=f'Calculating paths for GDrive nodes...')

        logger.info(f'{stopwatch_total} GDrive cache for {cache_info.subtree_root.full_path} loaded')

        cache_info.is_loaded = True
        self.meta_master = meta

    def _load_gdrive_subtree_stats(self, subtree_meta: GDriveSubtree, tree_id: str):
        subtree_meta.refresh_stats()

        actions.set_status(sender=tree_id, status_msg=subtree_meta.get_summary())

    def _slice_off_subtree_from_master(self, subtree_root: GDriveIdentifier, tree_id: str) -> GDriveSubtree:
        if not subtree_root.uid:
            # TODO: raise something
            return None
        root: GoogNode = self.meta_master.get_item_for_id(subtree_root.uid)
        if not root:
            return None

        subtree_meta: GDriveSubtree = GDriveSubtree(whole_tree=self.meta_master, root_node=root)

        self.application.task_runner.enqueue(self._load_gdrive_subtree_stats, subtree_meta, tree_id)

        return subtree_meta

    def load_subtree(self, subtree_root: GDriveIdentifier, tree_id: str) -> GDriveSubtree:
        if subtree_root.full_path == ROOT_PATH or subtree_root.uid == uid_generator.ROOT_UID:
            subtree_root = NodeIdentifierFactory.get_gdrive_root_constant_identifier()
        logger.debug(f'Getting meta for subtree: "{subtree_root}"')
        cache_man = self.application.cache_manager
        # TODO: currently we will just load the root and use that.
        #       But in the future we should do on-demand retrieval of subtrees
        root = NodeIdentifierFactory.get_gdrive_root_constant_identifier()
        cache_info = cache_man.get_or_create_cache_info_entry(root)
        if not cache_info.is_loaded:
            # Load from disk
            # TODO: this will fail if the cache does not exist. Need the above!
            logger.debug(f'Cache is not loaded: {cache_info.cache_location}')
            self._load_gdrive_cache(cache_info, tree_id)

        if subtree_root.uid == uid_generator.ROOT_UID:
            # Special case. GDrive does not have a single root (it treats shared drives as roots, for example).
            # We'll use this special token to represent "everything"
            gdrive_meta = self.meta_master
        else:
            slice_timer = Stopwatch()
            gdrive_meta = self._slice_off_subtree_from_master(subtree_root, tree_id)
            if gdrive_meta:
                logger.debug(f'{slice_timer} Sliced off {gdrive_meta}')
            else:
                raise GDriveItemNotFoundError(node_identifier=subtree_root,
                                              msg=f'Cannot load subtree because it does not exist: "{subtree_root}"',
                                              offending_path=subtree_root.full_path)
        return gdrive_meta

    def add_node(self, node: DisplayNode):
        # try to prevent cache corruption by doing some sanity checks
        if not node:
            raise RuntimeError(f'No node supplied!')
        if not node.uid:
            raise RuntimeError(f'Node is missing UID: {node}')
        if not isinstance(node, GoogNode):
            raise RuntimeError(f'Unrecognized node type: {node}')
        if not node.goog_id:
            raise RuntimeError(f'Node is missing Google ID: {node}')
        if not node.parent_uids:
            # Adding a new root is currently not allowed (which is fine because there should be no way to do this
            # via the UI)
            raise RuntimeError(f'Node is missing parent UIDs: {node}')

        # Prepare data for insertion to disk cache:
        if not self.meta_master:
            # TODO: give more thought to lifecycle
            raise RuntimeError('GDriveWholeTree not loaded!')
        parent_goog_ids = self.meta_master.resolve_uids_to_goog_ids(node.parent_uids)
        parent_mappings = []
        assert len(node.parent_uids) == len(parent_goog_ids)
        for parent_uid, parent_goog_id in zip(node.parent_uids, parent_goog_ids):
            parent_mappings.append((node.uid, parent_uid, parent_goog_id, node.sync_ts))
        node_tuple = node.to_tuple()

        # Open master database...
        root = NodeIdentifierFactory.get_gdrive_root_constant_identifier()
        cache_info = self.application.cache_manager.get_or_create_cache_info_entry(root)
        cache_path = cache_info.cache_location

        # Write new values:
        with GDriveDatabase(cache_path) as cache:
            logger.debug(f'Writing id-parent mappings to the GDrive master cache: {parent_mappings}')
            cache.insert_id_parent_mappings(parent_mappings, commit=False)
            if node.is_dir():
                logger.debug(f'Writing folder node to the GDrive master cache: {node}')
                cache.insert_gdrive_dirs([node_tuple])
            else:
                logger.debug(f'Writing file node to the GDrive master cache: {node}')
                cache.insert_gdrive_files([node_tuple])

        # Finally, update in-memory cache (tree):
        self.meta_master.add_item(node)

    def download_all_gdrive_meta(self, tree_id):
        root_identifier = NodeIdentifierFactory.get_gdrive_root_constant_identifier()
        cache_info = self.application.cache_manager.get_or_create_cache_info_entry(root_identifier)
        cache_path = cache_info.cache_location
        tree_loader = GDriveTreeLoader(application=self.application, cache_path=cache_path, tree_id=tree_id)
        self.meta_master = tree_loader.load_all(invalidate_cache=False)
        logger.info('Replaced entire GDrive in-memory cache with downloaded meta')

    def get_all_for_path(self, path: str) -> List[NodeIdentifier]:
        if not self.meta_master:
            raise CacheNotLoadedError()
        return self.meta_master.get_all_ids_for_path(path)
