import logging
import os
from queue import Queue
from typing import List, Optional

import treelib
from pydispatch import dispatcher

import file_util
from constants import OBJ_TYPE_LOCAL_DISK, ROOT
from fmeta.fmeta_tree_loader import TreeMetaScanner
from index.cache_manager import PersistedCacheInfo
from index.meta_store.local import LocalDiskSubtreeMS
from index.sqlite.fmeta_db import FMetaDatabase
from index.two_level_dict import FullPathDict, Md5BeforePathDict, ParentPathBeforeFileNameDict, Sha256BeforePathDict
from model.display_id import Identifier, LocalFsIdentifier
from model.fmeta import FMeta
from model.fmeta_tree import FMetaTree
from model.planning_node import PlanningNode
from stopwatch_sec import Stopwatch
from ui import actions
from ui.tree.meta_store import BaseMetaStore, DummyMS

logger = logging.getLogger(__name__)


# ⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛
# CLASS LocalDiskMasterCache
# ⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟


class LocalDiskMasterCache:
    def __init__(self, application):
        """Singleton in-memory cache for local filesystem"""
        self.application = application
        self.use_md5 = application.config.get('cache.enable_md5_lookup')
        if self.use_md5:
            self.md5_dict = Md5BeforePathDict()
        else:
            self.md5_dict = None

        self.use_sha256 = application.config.get('cache.enable_sha256_lookup')
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

    def _get_subtree_from_memory_only(self, subtree_path: Identifier):
        logger.debug(f'Getting items from in-memory cache for subtree: {subtree_path}')
        fmeta_tree = FMetaTree(root_path=subtree_path.full_path)
        count_dirs = 0
        count_added_from_cache = 0

        # Loop over all the descendants dirs, and add all of the files in each:
        q = Queue()
        q.put(subtree_path.full_path)
        while not q.empty():
            dir_path = q.get()
            count_dirs += 1
            files_in_dir = self.parent_path_dict.get(dir_path)
            for file_name, fmeta in files_in_dir.items():
                fmeta_tree.add_item(fmeta)
                count_added_from_cache += 1
            if self.dir_tree.get_node(dir_path):
                for child_dir in self.dir_tree.children(dir_path):
                    q.put(child_dir.identifier)

        logger.debug(f'Got {count_added_from_cache} items from in-memory cache (from {count_dirs} dirs)')
        return fmeta_tree

    # Load/save on-disk cache:

    def _load_subtree_disk_cache(self, cache_info: PersistedCacheInfo, tree_id) -> Optional[FMetaTree]:
        if not self.application.cache_manager.enable_load_from_disk:
            logger.debug('Skipping cache load because enable_load_from_disk is False')
            return None

        # Load cache from file, and update with any local FS changes found:
        with FMetaDatabase(cache_info.cache_info.cache_location) as fmeta_disk_cache:
            if not fmeta_disk_cache.has_local_files():
                logger.debug('No meta found in cache')
                return None

            status = f'Loading meta for "{cache_info.cache_info.subtree_root}" from cache: "{cache_info.cache_info.cache_location}"'
            logger.debug(status)
            dispatcher.send(actions.SET_PROGRESS_TEXT, sender=tree_id, msg=status)

            fmeta_tree = FMetaTree(cache_info.cache_info.subtree_root.full_path)

            db_file_changes: List[FMeta] = fmeta_disk_cache.get_local_files()
            if len(db_file_changes) == 0:
                logger.debug('No data found in disk cache')

            count_from_disk = 0
            for change in db_file_changes:
                existing = fmeta_tree.get_for_path(change.full_path)
                # Overwrite older changes for the same path:
                if existing is None:
                    fmeta_tree.add_item(change)
                    count_from_disk += 1
                elif existing.sync_ts < change.sync_ts:
                    fmeta_tree.add_item(change)

            logger.debug(f'Reduced {str(len(db_file_changes))} disk cache entries into {str(count_from_disk)} unique entries')
            logger.debug(fmeta_tree.get_stats_string())

            cache_info.is_loaded = True
            return fmeta_tree

    def _save_subtree_disk_cache(self, fmeta_tree: FMetaTree):
        # Get existing cache location if available. We will overwrite it.
        cache_info = self.application.cache_manager.get_or_create_cache_info_entry(fmeta_tree.identifier)
        to_insert = fmeta_tree.get_all()

        stopwatch_write_cache = Stopwatch()
        with FMetaDatabase(cache_info.cache_info.cache_location) as fmeta_disk_cache:
            # Update cache:
            fmeta_disk_cache.insert_local_files(to_insert, overwrite=True)

        logger.info(f'Wrote {str(len(to_insert))} FMetas to "{cache_info.cache_info.cache_location}" in {stopwatch_write_cache}')

    # Load/save on-disk cache:

    def init_subtree_localfs_cache(self, cache_info: PersistedCacheInfo, tree_id):
        """Called at startup to handle a single subtree cache from a local fs"""

        if not os.path.exists(cache_info.cache_info.subtree_root.full_path):
            logger.info(f'Subtree not found; assuming it is a removable drive: "{cache_info.cache_info.subtree_root}"')
            cache_info.needs_refresh = True
            return

        self._load_subtree(cache_info, tree_id)

    def get_metastore_for_subtree(self, subtree_root: LocalFsIdentifier, tree_id: str) -> BaseMetaStore:
        if not os.path.exists(subtree_root.full_path):
            logger.info(f'Cannot load meta for subtree because it does not exist: "{subtree_root.full_path}". Returning an empty metastore')
            return DummyMS(tree_id, self.application.config, subtree_root)

        cache_man = self.application.cache_manager
        cache_info = cache_man.get_or_create_cache_info_entry(subtree_root)
        assert cache_info is not None

        fmeta_tree = self._load_subtree(cache_info, tree_id)

        ds = LocalDiskSubtreeMS(tree_id=tree_id, config=self.application.config, fmeta_tree=fmeta_tree)
        return ds

    def _load_subtree(self, cache_info: PersistedCacheInfo, tree_id):
        assert cache_info
        stopwatch_total = Stopwatch()
        has_data_to_store_in_memory = False
        fmeta_tree = None

        if cache_info.is_loaded:
            fmeta_tree = self._get_subtree_from_memory_only(cache_info.cache_info.subtree_root)
        else:
            # Load from disk
            fmeta_tree = self._load_subtree_disk_cache(cache_info, tree_id)
            if fmeta_tree:
                has_data_to_store_in_memory = True
            else:
                fmeta_tree = FMetaTree(cache_info.cache_info.subtree_root.full_path)

        if cache_info.is_loaded and not \
                self.application.cache_manager.sync_from_local_disk_on_cache_load:
            logger.debug('Skipping filesystem sync because it is disabled for cache loads')
        elif cache_info.is_loaded and not cache_info.needs_refresh:
            logger.debug(f'Skipping filesystem sync because the cache is still fresh for path: {cache_info.cache_info.subtree_root}')
        else:
            # 2. Update from the file system, and optionally save any changes back to cache:
            fmeta_tree = self._resync_with_file_system(fmeta_tree, tree_id)
            cache_info.needs_refresh = False
            has_data_to_store_in_memory = True

        # Save the updates back to in-memory cache:
        if has_data_to_store_in_memory:
            self._update_in_memory_cache(fmeta_tree)

        logger.info(f'LocalFS cache for {cache_info.cache_info.subtree_root} loaded in: {stopwatch_total}')

        # Display summary of tree in the status area (if any)
        status = fmeta_tree.get_summary()
        actions.set_status(sender=tree_id, status_msg=status)

        return fmeta_tree

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

    def _update_in_memory_cache(self, fresh_tree):
        for item in fresh_tree.get_all():
            if not isinstance(item, PlanningNode):  # Planning nodes should not be cached, and should remain in their trees
                self.add_or_update_item(item)
                # FIXME: need to enable track changes, and handle deletes, etc
                # FIXME FIXME FIXME

        logger.debug(f'Updated in-memory cache: {self.get_summary()}')

    def _resync_with_file_system(self, stale_tree: FMetaTree, tree_id: str):
        # Scan directory tree and update where needed.
        logger.debug(f'Scanning filesystem subtree: {stale_tree.root_path}')
        scanner = TreeMetaScanner(root_path=stale_tree.root_path, stale_tree=stale_tree, tree_id=tree_id, track_changes=False)
        scanner.scan()
        fresh_tree = scanner.fresh_tree

        # Save the updates back to local disk cache:
        cache_man = self.application.cache_manager
        if cache_man.enable_save_to_disk:
            self._save_subtree_disk_cache(fresh_tree)

        return fresh_tree
