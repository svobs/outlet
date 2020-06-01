import errno
import logging
import os
import threading
import time
from typing import List, Optional, Tuple

from pydispatch import dispatcher

import file_util
from constants import CACHE_LOAD_TIMEOUT_SEC, MAIN_REGISTRY_FILE_NAME, TREE_TYPE_GDRIVE, TREE_TYPE_LOCAL_DISK
from file_util import get_resource_path
from index.cache_info import CacheInfoEntry, PersistedCacheInfo
from index.master_gdrive import GDriveMasterCache
from index.master_local import LocalDiskMasterCache
from index.sqlite.cache_registry_db import CacheRegistry
from index.two_level_dict import TwoLevelDict
from index.uid_generator import UID
from model.category import Category
from model.display_node import DisplayNode
from model.fmeta_tree import FMetaTree
from model.gdrive_whole_tree import GDriveWholeTree
from model.node_identifier import GDriveIdentifier, LocalFsIdentifier, NodeIdentifier, NodeIdentifierFactory
from model.null_subtree import NullSubtree
from model.subtree_snapshot import SubtreeSnapshot
from stopwatch_sec import Stopwatch
from ui import actions
from ui.actions import ID_GLOBAL_CACHE

logger = logging.getLogger(__name__)

CFG_ENABLE_LOAD_FROM_DISK = 'cache.enable_cache_load'


def _ensure_cache_dir_path(config):
    cache_dir_path = get_resource_path(config.get('cache.cache_dir_path'))
    if not os.path.exists(cache_dir_path):
        logger.info(f'Cache directory does not exist; attempting to create: "{cache_dir_path}"')
    os.makedirs(name=cache_dir_path, exist_ok=True)
    return cache_dir_path


#    CLASS CacheInfoByType
# ⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟


class CacheInfoByType(TwoLevelDict):
    """Holds CacheInfo objects"""
    def __init__(self):
        super().__init__(lambda x: x.subtree_root.tree_type, lambda x: x.subtree_root.full_path, lambda x, y: True)


#    CLASS CacheManager
# ⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟

# -> only the "rea1" nodes should go in the cache. Other nodes (e.g. 'planning nodes') should not
class CacheManager:
    def __init__(self, application):
        self.application = application

        self.cache_dir_path = _ensure_cache_dir_path(self.application.config)
        self.main_registry_path = os.path.join(self.cache_dir_path, MAIN_REGISTRY_FILE_NAME)

        self.caches_by_type: CacheInfoByType = CacheInfoByType()

        self.enable_load_from_disk = application.config.get(CFG_ENABLE_LOAD_FROM_DISK)
        self.enable_save_to_disk = application.config.get('cache.enable_cache_save')
        self.load_all_caches_on_startup = application.config.get('cache.load_all_caches_on_startup')
        self.sync_from_local_disk_on_cache_load = application.config.get('cache.sync_from_local_disk_on_cache_load')
        self.reload_tree_on_root_path_update = application.config.get('cache.load_cache_when_tree_root_selected')

        if not self.load_all_caches_on_startup:
            logger.info('Configured not to fetch all caches on startup; will lazy load instead')

        self._local_disk_cache = None
        self._gdrive_cache = None

        # Create an Event object.
        self.all_caches_loaded = threading.Event()

    def load_all_caches(self, sender):
        """Should be called during startup. Loop over all caches and load/merge them into a
        single large in-memory cache"""
        logger.debug(f'Received signal: "{actions.LOAD_ALL_CACHES}"')
        if self._local_disk_cache:
            logger.info(f'Caches already loaded. Ignoring signal from {sender}.')
            return

        logger.debug(f'CacheManager.load_all_caches() initiated by {sender}')
        logger.debug(f'Sending START_PROGRESS_INDETERMINATE for ID: {ID_GLOBAL_CACHE}')
        stopwatch = Stopwatch()
        dispatcher.send(actions.START_PROGRESS_INDETERMINATE, sender=ID_GLOBAL_CACHE)

        try:
            self._local_disk_cache = LocalDiskMasterCache(self.application)
            self._gdrive_cache = GDriveMasterCache(self.application)

            # First put into map, to eliminate possible duplicates
            caches_from_registry: List[CacheInfoEntry] = self._get_cache_info_from_registry()
            unique_cache_count = 0
            skipped_count = 0
            for cache_from_registry in caches_from_registry:
                info: PersistedCacheInfo = PersistedCacheInfo(cache_from_registry)
                if not os.path.exists(info.cache_location):
                    logger.info(f'Skipping non-existent cache info entry: {info.cache_location} (for subtree: {info.subtree_root})')
                    skipped_count += 1
                    continue
                existing = self.caches_by_type.get_single(info.subtree_root.tree_type, info.subtree_root.full_path)
                if existing:
                    if info.sync_ts < existing.sync_ts:
                        logger.info(f'Skipping duplicate cache info entry: {existing.subtree_root}')
                        continue
                    else:
                        logger.info(f'Overwriting older duplicate cache info entry: {existing.subtree_root}')

                    skipped_count += 1
                else:
                    unique_cache_count += 1

                # Special handling to local-type caches: ignore any UID we find in the registry and bring it in line with what's in memory:
                if info.subtree_root.tree_type == TREE_TYPE_LOCAL_DISK:
                    new_uid = self._local_disk_cache.get_uid_for_path(info.subtree_root.full_path)
                    info.subtree_root.uid = new_uid

                self.caches_by_type.put(info)

            if skipped_count > 0:
                caches = self.caches_by_type.get_all()
                self._overwrite_all_caches_in_registry(caches)

            if self.application.cache_manager.enable_load_from_disk and self.load_all_caches_on_startup:
                # MUST read GDrive first, because currently we assign incrementing integer UIDs for local files dynamically,
                # and we won't know which are reserved until we have read in all the existing GDrive caches
                existing_caches = list(self.caches_by_type.get_second_dict(TREE_TYPE_GDRIVE).values())
                assert len(existing_caches) <= 1

                consolidated_local_caches, registry_needs_update = self._consolidate_local_caches()
                existing_caches += consolidated_local_caches

                if registry_needs_update and self.enable_save_to_disk:
                    self._overwrite_all_caches_in_registry(existing_caches)

                for cache_num, existing_disk_cache in enumerate(existing_caches):
                    try:
                        info = PersistedCacheInfo(existing_disk_cache)
                        self.caches_by_type.put(info)
                        logger.info(f'Init cache {(cache_num + 1)}/{len(existing_caches)}: id={existing_disk_cache.subtree_root}')
                        self._init_existing_cache(info)
                    except Exception:
                        logger.exception(f'Failed to load cache: {existing_disk_cache.cache_location}')
                logger.info(f'{stopwatch} Load All Caches complete')
            else:
                logger.info(f'{stopwatch} Found {unique_cache_count} existing caches but configured not to load on startup')
        finally:
            dispatcher.send(actions.STOP_PROGRESS, sender=ID_GLOBAL_CACHE)
            self.all_caches_loaded.set()
            dispatcher.send(signal=actions.LOAD_ALL_CACHES_DONE, sender=ID_GLOBAL_CACHE)

    def _consolidate_local_caches(self) -> Tuple[List[PersistedCacheInfo], bool]:
        local_caches: List[PersistedCacheInfo] = list(self.caches_by_type.get_second_dict(TREE_TYPE_LOCAL_DISK).values())

        supertree_sets: List[Tuple[PersistedCacheInfo, PersistedCacheInfo]] = []

        for cache in local_caches:
            for other_cache in local_caches:
                if other_cache.subtree_root.full_path.startswith(cache.subtree_root.full_path) and \
                        not cache.subtree_root.full_path == other_cache.subtree_root.full_path:
                    # cache is a super-tree of other_cache
                    supertree_sets.append((cache, other_cache))

        for supertree_cache, subtree_cache in supertree_sets:
            local_caches.remove(subtree_cache)

            if supertree_cache.sync_ts > subtree_cache.sync_ts:
                logger.info(f'Cache for supertree (root={supertree_cache.subtree_root.full_path}, ts={supertree_cache.sync_ts}) is newer '
                            f'than for subtree (root={subtree_cache.subtree_root.full_path}, ts={subtree_cache.sync_ts}): it will be deleted')
                file_util.delete_file(subtree_cache.cache_location)
            else:
                logger.info(f'Cache for subtree (root={subtree_cache.subtree_root.full_path}, ts={subtree_cache.sync_ts}) is newer '
                            f'than for supertree (root={supertree_cache.subtree_root.full_path}, ts={supertree_cache.sync_ts}): it will be merged '
                            f'into supertree')

                # 1. Load super-tree into memory
                super_tree: FMetaTree = self._local_disk_cache.load_subtree_from_disk(supertree_cache, ID_GLOBAL_CACHE)
                supertree_paths: List[str] = list(map(lambda x: x.full_path, super_tree.get_all()))
                subtree_root: str = subtree_cache.subtree_root.full_path
                # 2. Discard all paths from super-tree which fall under sub-tree:
                for supertree_path in supertree_paths:
                    if supertree_path.startswith(subtree_root):
                        super_tree.remove(full_path=supertree_path)

                # 3. Load sub-tree into memory
                sub_tree = self._local_disk_cache.load_subtree_from_disk(subtree_cache, ID_GLOBAL_CACHE)
                # 4. Add contents of sub-tree into super-tree:
                for item in sub_tree.get_all():
                    super_tree.add_item(item)
                sub_tree = None
                # 5. We already loaded it into memory; add it to the in-memory cache:
                self._local_disk_cache.load_subtree(supertree_cache, ID_GLOBAL_CACHE, super_tree)
                if self.enable_save_to_disk:
                    self._local_disk_cache.save_subtree_disk_cache(super_tree)
                else:
                    logger.debug('Skipping cache update because save to disk is disabled')

        registry_needs_update = len(supertree_sets) > 0
        return local_caches, registry_needs_update

    def _overwrite_all_caches_in_registry(self, cache_info_list: List[CacheInfoEntry]):
        logger.info(f'Overwriting all cache entries in registry with {len(cache_info_list)} entries')
        with CacheRegistry(self.main_registry_path, self.application.node_identifier_factory) as cache_registry_db:
            cache_registry_db.insert_cache_info(cache_info_list, append=False, overwrite=True)

    def _get_cache_info_from_registry(self) -> List[CacheInfoEntry]:
        with CacheRegistry(self.main_registry_path, self.application.node_identifier_factory) as cache_registry_db:
            if cache_registry_db.has_cache_info():
                exisiting_caches = cache_registry_db.get_cache_info()
                logger.debug(f'Found {len(exisiting_caches)} caches listed in registry')
                return exisiting_caches
            else:
                logger.debug('Registry has no caches listed')
                return []

    def _init_existing_cache(self, existing_disk_cache: PersistedCacheInfo):
        cache_type = existing_disk_cache.subtree_root.tree_type
        if cache_type != TREE_TYPE_LOCAL_DISK and cache_type != TREE_TYPE_GDRIVE:
            raise RuntimeError(f'Unrecognized tree type: {cache_type}')

        if cache_type == TREE_TYPE_LOCAL_DISK:
            if not os.path.exists(existing_disk_cache.subtree_root.full_path):
                logger.info(f'Subtree not found; will defer loading: "{existing_disk_cache.subtree_root}"')
                existing_disk_cache.needs_refresh = True
            else:
                self._load_local_subtree(existing_disk_cache.subtree_root, ID_GLOBAL_CACHE)
        elif cache_type == TREE_TYPE_GDRIVE:
            self._gdrive_cache.load_gdrive_cache(existing_disk_cache, ID_GLOBAL_CACHE)

    def load_subtree(self, node_identifier: NodeIdentifier, tree_id: str) -> SubtreeSnapshot:
        """
        Performs a read-through retreival of all the FMetas in the given subtree
        on the local filesystem.
        """

        dispatcher.send(signal=actions.LOAD_TREE_STARTED, sender=tree_id)

        if node_identifier.tree_type == TREE_TYPE_LOCAL_DISK:
            return self._load_local_subtree(node_identifier, tree_id)
        elif node_identifier.tree_type == TREE_TYPE_GDRIVE:
            assert self._gdrive_cache
            return self._gdrive_cache.load_gdrive_subtree(node_identifier, tree_id)
        else:
            raise RuntimeError(f'Unrecognized tree type: {node_identifier.tree_type}')

    def _find_existing_supertree_for_subtree(self, subtree_root: NodeIdentifier, tree_id: str):
        existing_caches: List[PersistedCacheInfo] = list(self.caches_by_type.get_second_dict(subtree_root.tree_type).values())

        for existing_cache in existing_caches:
            if existing_cache.subtree_root.full_path.startswith(subtree_root.full_path):
                if existing_cache.subtree_root.full_path == subtree_root.full_path:
                    # Exact match exists: just return from here and allow exact match logic to work
                    return None
                else:
                    return existing_cache
        # Nothing in the cache contains subtree
        return None

    def _load_local_subtree(self, subtree_root: NodeIdentifier, tree_id) -> SubtreeSnapshot:
        """
        Performs a read-through retreival of all the FMetas in the given subtree
        on the local filesystem.
        """
        if not os.path.exists(subtree_root.full_path):
            logger.info(f'Cannot load meta for subtree because it does not exist: "{subtree_root.full_path}"')
            return NullSubtree(subtree_root)

        # If we have already loaded this subtree as part of a larger cache, use that:
        supertree = self._find_existing_supertree_for_subtree(subtree_root, tree_id)
        if supertree:
            logger.debug(f'Subtree ({subtree_root.full_path}) is part of existing cached supertree ({supertree.subtree_root.full_path})')
            if not supertree.is_loaded:
                # load into in-memory cache
                self._local_disk_cache.load_subtree(supertree, tree_id)
            return self._local_disk_cache.get_subtree_from_memory_only(subtree_root)

        # no supertree found in cache. use exact match logic:
        cache_info = self.get_or_create_cache_info_entry(subtree_root)
        assert cache_info is not None

        assert self._local_disk_cache
        return self._local_disk_cache.load_subtree(cache_info, tree_id)

    def get_gdrive_whole_tree(self, tree_id) -> GDriveWholeTree:
        """Will load if necessary"""
        root_identifier: NodeIdentifier = NodeIdentifierFactory.get_gdrive_root_constant_identifier()
        return self._gdrive_cache.load_gdrive_subtree(root_identifier, tree_id)

    def build_fmeta(self, full_path: str, category=Category.NA):
        return self._local_disk_cache.build_fmeta(full_path, category)

    def add_or_update_node(self, node: DisplayNode):
        tree_type = node.node_identifier.tree_type
        if tree_type == TREE_TYPE_GDRIVE:
            self._gdrive_cache.add_or_update_goog_node(node)
        elif tree_type == TREE_TYPE_LOCAL_DISK:
            self._local_disk_cache.add_or_update_fmeta(node)
        else:
            raise RuntimeError(f'Unrecognized tree type ({tree_type}) for node {node}')

    def remove_node(self, node: DisplayNode, to_trash):
        tree_type = node.node_identifier.tree_type
        if tree_type == TREE_TYPE_GDRIVE:
            self._gdrive_cache.remove_goog_node(node, to_trash)
        elif tree_type == TREE_TYPE_LOCAL_DISK:
            self._local_disk_cache.remove_fmeta(node, to_trash)
        else:
            raise RuntimeError(f'Unrecognized tree type ({tree_type}) for node {node}')

    def download_all_gdrive_meta(self, tree_id):
        return self._gdrive_cache.download_all_gdrive_meta(tree_id)

    def get_cache_info_entry(self, subtree_root: NodeIdentifier) -> PersistedCacheInfo:
        return self.caches_by_type.get_single(subtree_root.tree_type, subtree_root.full_path)

    def get_or_create_cache_info_entry(self, subtree_root: NodeIdentifier) -> PersistedCacheInfo:
        existing = self.get_cache_info_entry(subtree_root)
        if existing:
            logger.debug(f'Found existing cache for type={subtree_root.tree_type} subtree="{subtree_root.full_path}"')
            return existing
        else:
            logger.debug(f'No existing cache found for type={subtree_root.tree_type} subtree="{subtree_root.full_path}"')

        if subtree_root.tree_type == TREE_TYPE_LOCAL_DISK:
            prefix = 'LO'
        elif subtree_root.tree_type == TREE_TYPE_GDRIVE:
            prefix = 'GD'
        else:
            raise RuntimeError(f'Unrecognized tree type: {subtree_root.tree_type}')

        mangled_file_name = prefix + subtree_root.full_path.replace('/', '_') + '.db'
        cache_location = os.path.join(self.cache_dir_path, mangled_file_name)
        now_ms = int(time.time())
        db_entry = CacheInfoEntry(cache_location=cache_location,
                                  subtree_root=subtree_root, sync_ts=now_ms,
                                  is_complete=True)

        with CacheRegistry(self.main_registry_path, self.application.node_identifier_factory) as cache_registry_db:
            logger.info(f'Inserting new cache info into registry: {subtree_root}')
            cache_registry_db.insert_cache_info(db_entry, append=True, overwrite=False)

        cache_info = PersistedCacheInfo(db_entry)

        # Save reference in memory
        self.caches_by_type.put(cache_info)

        return cache_info

    def resolve_path(self, full_path: str = None, node_identifier: Optional[NodeIdentifier] = None) -> List[NodeIdentifier]:
        """Resolves the given path into either a local file, a set of Google Drive matches, or raises a GDriveItemNotFoundError"""
        if not node_identifier:
            node_identifier = self.application.node_identifier_factory.for_values(full_path=full_path)
        if node_identifier.tree_type == TREE_TYPE_GDRIVE:
            # Need to wait until all caches are loaded:
            if not self.all_caches_loaded.wait(CACHE_LOAD_TIMEOUT_SEC):
                logger.error('Timed out waiting for all caches to load!')

            return self._gdrive_cache.get_all_for_path(node_identifier.full_path)
        else:
            if not os.path.exists(full_path):
                raise FileNotFoundError(errno.ENOENT, os.strerror(errno.ENOENT), full_path)
            uid = self.get_uid_for_path(full_path)
            return [LocalFsIdentifier(full_path=full_path, uid=uid)]

    def get_uid_for_path(self, path: str) -> UID:
        return self._local_disk_cache.get_uid_for_path(path)

    def get_for_local_path(self, path: str):
        uid = self.get_uid_for_path(path)
        return self._local_disk_cache.get_item(uid)

    def get_children(self, parent_identifier: NodeIdentifier):
        if parent_identifier.tree_type == TREE_TYPE_GDRIVE:
            return self._gdrive_cache.get_children(parent_identifier)
        elif parent_identifier.tree_type == TREE_TYPE_LOCAL_DISK:
            return self._local_disk_cache.get_children(parent_identifier)
        else:
            raise RuntimeError(f'Unknown tree type: {parent_identifier.tree_type} for {parent_identifier}')

    def get_parent_for_item(self, item: DisplayNode):
        if item.node_identifier.tree_type == TREE_TYPE_GDRIVE:
            return self._gdrive_cache.get_parent_for_item(item)
        elif item.node_identifier.tree_type == TREE_TYPE_LOCAL_DISK:
            return self._local_disk_cache.get_parent_for_item(item)
        else:
            raise RuntimeError(f'Unknown tree type: {item.node_identifier.tree_type} for {item}')
