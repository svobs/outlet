import logging
import os
from queue import Queue

import treelib

import file_util
from index.meta_store.local import LocalDiskSubtreeMS
from index.two_level_dict import FullPathDict, Md5BeforePathDict, ParentPathBeforeFileNameDict, Sha256BeforePathDict
from constants import ROOT
from model.fmeta import FMeta
from model.fmeta_tree import FMetaTree

logger = logging.getLogger(__name__)


# ⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛
# CLASS LocalDiskMasterCache
# ⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟


class LocalDiskMasterCache:
    def __init__(self, application):
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

    def _get_subtree_from_memory_only(self, subtree_path):
        logger.debug(f'Getting items from in-memory cache for subtree: {subtree_path}')
        fmeta_tree = FMetaTree(root_path=subtree_path)
        count_dirs = 0
        count_added_from_cache = 0

        # Loop over all the descendants dirs, and add all of the files in each:
        q = Queue()
        q.put(subtree_path)
        while not q.empty():
            dir_path = q.get()
            count_dirs += 1
            files_in_dir = self.parent_path_dict.get(dir_path)
            for file_name, fmeta in files_in_dir.items():
                fmeta_tree.add(fmeta)
                count_added_from_cache += 1
            if self.dir_tree.get_node(dir_path):
                for child_dir in self.dir_tree.children(dir_path):
                    q.put(child_dir.identifier)

        logger.debug(f'Got {count_added_from_cache} items from in-memory cache (from {count_dirs} dirs)')
        return fmeta_tree

    def get_metastore_for_subtree(self, subtree_path: str, tree_id: str) -> LocalDiskSubtreeMS:
        if not os.path.exists(subtree_path):
            raise RuntimeError(f'Cannot load meta for subtree because it does not exist: {subtree_path}')

        cache_man = self.application.cache_manager
        cache_info = cache_man.persisted_cache_info.get(subtree_path)
        if cache_info and not cache_info.is_loaded:
            # Load from disk
            fmeta_tree = cache_man.load_local_disk_cache(cache_info.cache_info)
            cache_info.is_loaded = True
        else:
            # Load as many items as possible from the in-memory cache.
            fmeta_tree = self._get_subtree_from_memory_only(subtree_path)

        sync_to_fs = True
        if cache_info:
            if not self.application.cache_manager.sync_from_local_disk_on_cache_load:
                logger.debug('Skipping file system sync because it is disabled for cache loads')
                sync_to_fs = False
            elif not cache_info.needs_refresh:
                logger.debug(f'Skipping filesystem sync because the cache is still fresh for path: {subtree_path}')
                sync_to_fs = False
        if sync_to_fs:
            # Sync from disk, and save to disk cache again (if configured) and update in-memory-store:
            fmeta_tree = self.application.cache_manager.refresh_from_local_fs(fmeta_tree, tree_id)

        ds = LocalDiskSubtreeMS(tree_id=tree_id, config=self.application.config, fmeta_tree=fmeta_tree)
        return ds

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
