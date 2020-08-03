import errno
import logging
import os
import threading
import time
from typing import Dict, Iterable, List, Optional, Tuple

from pydispatch import dispatcher

from ui.tree.controller import TreePanelController
from util import file_util
from model.change_action import ChangeAction
from constants import CACHE_LOAD_TIMEOUT_SEC, MAIN_REGISTRY_FILE_NAME, TREE_TYPE_GDRIVE, TREE_TYPE_LOCAL_DISK
from util.file_util import get_resource_path
from index.cache_info import CacheInfoEntry, PersistedCacheInfo
from index.change_ledger import ChangeLedger
from index.master_gdrive import GDriveMasterCache
from index.master_local import LocalDiskMasterCache
from index.sqlite.cache_registry_db import CacheRegistry
from index.two_level_dict import TwoLevelDict
from index.uid.uid import UID
from model.node.display_node import DisplayNode
from model.node.local_disk_node import LocalFileNode
from model.gdrive_whole_tree import GDriveWholeTree
from model.node.gdrive_node import GDriveNode
from model.node_identifier import LocalFsIdentifier, NodeIdentifier
from model.node_identifier_factory import NodeIdentifierFactory
from model.display_tree.display_tree import DisplayTree
from util.stopwatch_sec import Stopwatch
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


# CLASS CacheInfoByType
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼


class CacheInfoByType(TwoLevelDict):
    """Holds PersistedCacheInfo objects"""
    def __init__(self):
        super().__init__(lambda x: x.subtree_root.tree_type, lambda x: x.subtree_root.full_path, lambda x, y: True)


# CLASS CacheManager
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼

class CacheManager:
    """
    This is the central source of truth for the application (or attempts to be as much as possible).
    """
    def __init__(self, application):
        self.application = application

        self.cache_dir_path = _ensure_cache_dir_path(self.application.config)
        self.main_registry_path = os.path.join(self.cache_dir_path, MAIN_REGISTRY_FILE_NAME)

        self.caches_by_type: CacheInfoByType = CacheInfoByType()

        self.enable_load_from_disk = application.config.get(CFG_ENABLE_LOAD_FROM_DISK)
        self.enable_save_to_disk = application.config.get('cache.enable_cache_save')
        self.load_all_caches_on_startup = application.config.get('cache.load_all_caches_on_startup')
        self.load_caches_for_displayed_trees_at_startup = application.config.get('cache.load_caches_for_displayed_trees_on_startup')
        self.sync_from_local_disk_on_cache_load = application.config.get('cache.sync_from_local_disk_on_cache_load')
        self.reload_tree_on_root_path_update = application.config.get('cache.load_cache_when_tree_root_selected')
        self.cancel_all_pending_changes_on_startup = application.config.get('cache.cancel_all_pending_changes_on_startup')

        # FIXME: put all planning nodes into their own cache (using Google and Local table structures). Doesn't make sense to store them
        # alongside regular objects

        if not self.load_all_caches_on_startup:
            logger.info('Configured not to fetch all caches on startup; will lazy load instead')

        self._local_disk_cache = None
        """Sub-module of Cache Manager which manages local disk caches"""

        self._gdrive_cache = None
        """Sub-module of Cache Manager which manages Google Drive caches"""

        self._change_ledger = None
        """Sub-module of Cache Manager which manages commands which have yet to execute"""

        self._tree_controllers: Dict[str, TreePanelController] = {}
        """Keep track of live UI tree controllers, so that we can look them up by ID (e.g. for use in automated testing)"""

        # Create Event objects to wait for init and cache loads.
        self._init_done = threading.Event()

        # (Optional) variables needed for producer/consumer behavior if Load All Caches needed
        self._load_all_caches_done = threading.Event()
        self._all_caches_loading = False
        self._all_caches_loaded = False

    # Startup loading/maintenance
    # ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼

    def init(self, sender):
        """Should be called during startup. Loop over all caches and load/merge them into a
        single large in-memory cache"""
        if self._local_disk_cache:
            logger.info(f'Caches already loaded. Ignoring signal from {sender}.')
            return

        logger.debug(f'CacheManager.init() initiated by {sender}')
        logger.debug(f'Sending START_PROGRESS_INDETERMINATE for ID: {ID_GLOBAL_CACHE}')
        stopwatch = Stopwatch()
        dispatcher.send(actions.START_PROGRESS_INDETERMINATE, sender=ID_GLOBAL_CACHE)

        try:
            # Init sub-modules:
            self._local_disk_cache = LocalDiskMasterCache(self.application)
            self._gdrive_cache = GDriveMasterCache(self.application)
            self._change_ledger = ChangeLedger(self.application)

            # Load registry. Do validation along the way
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
                # TODO: delete this once we are stable enough to assume this won't be necessary. It doesn't really buy us anything
                # (except to tell us we did something wrong)
                if info.subtree_root.tree_type == TREE_TYPE_LOCAL_DISK:
                    new_uid = self._local_disk_cache.get_uid_for_path(info.subtree_root.full_path, info.subtree_root.uid)
                    if new_uid != info.subtree_root.uid:
                        logger.warning(f'Requested UID "{info.subtree_root.uid}" is invalid for given path "{info.subtree_root.full_path}";'
                                       f' changing it to "{new_uid}"')
                    info.subtree_root.uid = new_uid

                # Put into map to eliminate possible duplicates
                self.caches_by_type.put(info)

            # Write back to cache if we need to clean things up:
            if skipped_count > 0:
                caches = self.caches_by_type.get_all()
                self._overwrite_all_caches_in_registry(caches)

            # Now load all caches (if configured):
            if self.application.cache_manager.enable_load_from_disk and self.load_all_caches_on_startup:
                self.load_all_caches()
            else:
                logger.info(f'{stopwatch} Found {unique_cache_count} existing caches but configured not to load on startup')

            # Finally, add any queued changes (asynchronously)
            self.application.executor.submit_async_task(self._change_ledger.load_pending_changes)

        finally:
            dispatcher.send(actions.STOP_PROGRESS, sender=ID_GLOBAL_CACHE)
            self._init_done.set()
            logger.debug('CacheManager init done')
            dispatcher.send(signal=actions.LOAD_ALL_CACHES_DONE, sender=ID_GLOBAL_CACHE)

    def _wait_for_init_done(self):
        if not self._init_done.is_set():
            logger.info('Waiting for CacheManager init to complete')
        if not self._init_done.wait(CACHE_LOAD_TIMEOUT_SEC):
            logger.error('Timed out waiting for CacheManager init!')

    def load_all_caches(self):
        """Load ALL the caches into memory. This is needed in certain circumstances, such as when a UID is being derefernced but we
        don't know which cache it belongs to."""
        if not self.application.cache_manager.enable_load_from_disk:
            raise RuntimeError('Cannot load all caches; loading caches from disk is disabled in config!')

        if self._all_caches_loading:
            logger.info('Waiting for all caches to load')
            # Wait for the other thread to complete. (With no timeout, it will never return):
            if not self._load_all_caches_done.wait(CACHE_LOAD_TIMEOUT_SEC):
                logger.error('Timed out waiting for all caches to load!')
        if self._load_all_caches_done.is_set():
            # Other thread completed
            return

        self._all_caches_loading = True
        logger.info('Loading all caches from disk')

        stopwatch = Stopwatch()

        # MUST read GDrive first, because currently we assign incrementing integer UIDs for local files dynamically,
        # and we won't know which are reserved until we have read in all the existing GDrive caches
        existing_caches: List[PersistedCacheInfo] = list(self.caches_by_type.get_second_dict(TREE_TYPE_GDRIVE).values())
        assert len(existing_caches) <= 1

        local_caches: List[PersistedCacheInfo] = list(self.caches_by_type.get_second_dict(TREE_TYPE_LOCAL_DISK).values())
        consolidated_local_caches, registry_needs_update = self._local_disk_cache.consolidate_local_caches(local_caches, ID_GLOBAL_CACHE)
        existing_caches += consolidated_local_caches

        if registry_needs_update and self.enable_save_to_disk:
            self._overwrite_all_caches_in_registry(existing_caches)
            logger.debug(f'Overwriting in-memory list ({len(self.caches_by_type)}) with {len(existing_caches)} entries')
            self.caches_by_type.clear()
            for cache in existing_caches:
                self.caches_by_type.put(cache)

        for cache_num, existing_disk_cache in enumerate(existing_caches):
            try:
                self.caches_by_type.put(existing_disk_cache)
                logger.info(f'Init cache {(cache_num + 1)}/{len(existing_caches)}: id={existing_disk_cache.subtree_root}')
                if existing_disk_cache.is_loaded:
                    logger.debug('Cache is already loaded; skipping')
                else:
                    self._init_existing_cache(existing_disk_cache)
            except Exception:
                logger.exception(f'Failed to load cache: {existing_disk_cache.cache_location}')

        logger.info(f'{stopwatch} Load All Caches complete')
        self._all_caches_loading = False
        self._load_all_caches_done.set()

    def _overwrite_all_caches_in_registry(self, cache_info_list: List[CacheInfoEntry]):
        logger.info(f'Overwriting all cache entries in persisted registry with {len(cache_info_list)} entries')
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
                self._local_disk_cache.load_local_subtree(existing_disk_cache.subtree_root, ID_GLOBAL_CACHE)
        elif cache_type == TREE_TYPE_GDRIVE:
            self._gdrive_cache.load_gdrive_cache(existing_disk_cache, ID_GLOBAL_CACHE)

    # Subtree-level stuff
    # ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼

    def load_subtree(self, node_identifier: NodeIdentifier, tree_id: str) -> DisplayTree:
        """
        Performs a read-through retreival of all the FMetas in the given subtree
        on the local filesystem.
        """
        logger.debug(f'Got request to load subtree: {node_identifier}')

        if not file_util.is_normalized(node_identifier.full_path):
            node_identifier.full_path = file_util.normalize_path(node_identifier.full_path)
            logger.debug(f'Normalized path: {node_identifier.full_path}')

        dispatcher.send(signal=actions.LOAD_SUBTREE_STARTED, sender=tree_id)

        if node_identifier.tree_type == TREE_TYPE_LOCAL_DISK:
            assert self._local_disk_cache
            subtree = self._local_disk_cache.load_local_subtree(node_identifier, tree_id)
        elif node_identifier.tree_type == TREE_TYPE_GDRIVE:
            assert self._gdrive_cache
            subtree = self._gdrive_cache.load_gdrive_subtree(node_identifier, tree_id)
        else:
            raise RuntimeError(f'Unrecognized tree type: {node_identifier.tree_type}')

        return subtree

    def find_existing_supertree_for_subtree(self, subtree_root: NodeIdentifier, tree_id: str) -> Optional[PersistedCacheInfo]:
        existing_caches: List[PersistedCacheInfo] = list(self.caches_by_type.get_second_dict(subtree_root.tree_type).values())

        for existing_cache in existing_caches:
            # Is existing_cache an ancestor of target tree?
            if subtree_root.full_path.startswith(existing_cache.subtree_root.full_path):
                if existing_cache.subtree_root.full_path == subtree_root.full_path:
                    # Exact match exists: just return from here and allow exact match logic to work
                    return None
                else:
                    return existing_cache
        # Nothing in the cache contains subtree
        return None

    def get_cache_info_entry(self, subtree_root: NodeIdentifier) -> PersistedCacheInfo:
        return self.caches_by_type.get_single(subtree_root.tree_type, subtree_root.full_path)

    def get_or_create_cache_info_entry(self, subtree_root: NodeIdentifier) -> PersistedCacheInfo:
        existing = self.get_cache_info_entry(subtree_root)
        if existing:
            logger.debug(f'Found existing cache entry for type={subtree_root.tree_type} subtree="{subtree_root.full_path}"')
            return existing
        else:
            logger.debug(f'No existing cache entry found for type={subtree_root.tree_type} subtree="{subtree_root.full_path}"')

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

    # Individual item cache updates
    # ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼

    def add_or_update_node(self, node: DisplayNode):
        tree_type = node.node_identifier.tree_type
        if tree_type == TREE_TYPE_GDRIVE:
            self._gdrive_cache.add_or_update_goog_node(node)
        elif tree_type == TREE_TYPE_LOCAL_DISK:
            self._local_disk_cache.add_or_update_node(node)
        else:
            raise RuntimeError(f'Unrecognized tree type ({tree_type}) for node {node}')

    def remove_node(self, node: DisplayNode, to_trash):
        tree_type = node.node_identifier.tree_type
        if tree_type == TREE_TYPE_GDRIVE:
            self._gdrive_cache.remove_goog_node(node, to_trash)
        elif tree_type == TREE_TYPE_LOCAL_DISK:
            self._local_disk_cache.remove_node(node, to_trash)
        else:
            raise RuntimeError(f'Unrecognized tree type ({tree_type}) for node {node}')

    # Tree controller tracking/lookup
    # ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼

    def register_tree_controller(self, controller: TreePanelController):
        self._tree_controllers[controller.tree_id] = controller

    def unregister_tree_controller(self, controller: TreePanelController):
        self._tree_controllers.pop(controller.tree_id)

    def get_tree_controller(self, tree_id: str) -> Optional[TreePanelController]:
        return self._tree_controllers.get(tree_id, None)

    # Various public methods
    # ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼

    def get_last_pending_change_for_node(self, node_uid: UID) -> Optional[ChangeAction]:
        return self._change_ledger.get_last_pending_change_for_node(node_uid)

    def enqueue_change_list(self, change_list: Iterable[ChangeAction]):
        """Attempt to add the given ChangeActions to the execution tree. No need to worry whether some changes overlap or are redundant;
         the ChangeLedger will sort that out - although it will raise an error if it finds incompatible changes such as adding to a tree
         that is scheduled for deletion."""
        self._change_ledger.append_new_pending_changes(change_list)

    def get_next_command(self):
        # blocks !
        self._wait_for_init_done()
        # also blocks !
        return self._change_ledger.get_next_command()

    def download_all_gdrive_meta(self, tree_id):
        return self._gdrive_cache.download_all_gdrive_meta(tree_id)

    def get_gdrive_whole_tree(self, tree_id) -> GDriveWholeTree:
        """Will load if necessary"""
        root_identifier: NodeIdentifier = NodeIdentifierFactory.get_gdrive_root_constant_identifier()
        return self._gdrive_cache.load_gdrive_subtree(root_identifier, tree_id)

    def build_local_file_node(self, full_path: str, staging_path=None) -> Optional[LocalFileNode]:
        return self._local_disk_cache.build_local_file_node(full_path, staging_path)

    def resolve_path(self, full_path: str = None, node_identifier: Optional[NodeIdentifier] = None) -> List[NodeIdentifier]:
        """Resolves the given path into either a local file, a set of Google Drive matches, or raises a GDriveItemNotFoundError"""
        full_path = file_util.normalize_path(full_path)
        if not node_identifier:
            node_identifier = self.application.node_identifier_factory.for_values(full_path=full_path)
        if node_identifier.tree_type == TREE_TYPE_GDRIVE:
            # Need to wait until all caches are loaded:
            self._wait_for_init_done()

            return self._gdrive_cache.get_all_for_path(node_identifier.full_path)
        else:
            if not os.path.exists(full_path):
                raise FileNotFoundError(errno.ENOENT, os.strerror(errno.ENOENT), full_path)
            uid = self.get_uid_for_path(full_path)
            return [LocalFsIdentifier(full_path=full_path, uid=uid)]

    def get_goog_ids_for_uids(self, uids: List[UID]) -> List[str]:
        return self._gdrive_cache.get_goog_ids_for_uids(uids)

    def get_uid_for_path(self, path: str, uid_suggestion: Optional[UID] = None) -> UID:
        """Deterministically gets or creates a UID corresponding to the given path string"""
        return self._local_disk_cache.get_uid_for_path(path, uid_suggestion)

    def get_uid_list_for_goog_id_list(self, goog_id_list: List[str]) -> List[UID]:
        return self._gdrive_cache.get_uid_list_for_goog_id_list(goog_id_list)

    def get_uid_for_goog_id(self, goog_id: str, uid_suggestion: Optional[UID] = None) -> UID:
        """Deterministically gets or creates a UID corresponding to the given goog_id"""
        return self._gdrive_cache.get_uid_for_goog_id(goog_id, uid_suggestion)

    def get_node_for_local_path(self, path: str) -> DisplayNode:
        uid = self.get_uid_for_path(path)
        return self._local_disk_cache.get_item(uid)

    def get_goog_node_for_name_and_parent_uid(self, name: str, parent_uid: UID) -> Optional[GDriveNode]:
        """Returns the first GDrive node found with the given name and parent.
        This roughly matches the logic used to search for an item in Google Drive when we are unsure about its goog_id."""
        return self._gdrive_cache.get_item_for_name_and_parent_uid(name, parent_uid)

    def get_item_for_goog_id(self, goog_id: str) -> UID:
        return self._gdrive_cache.get_item_for_goog_id(goog_id)

    def get_item_for_uid(self, uid: UID, tree_type: int = None):
        if tree_type:
            if tree_type == TREE_TYPE_LOCAL_DISK:
                return self._local_disk_cache.get_item(uid)
            elif tree_type == TREE_TYPE_GDRIVE:
                return self._gdrive_cache.get_item_for_uid(uid)
            else:
                raise RuntimeError(f'Bad tree type: {tree_type}')

        # no tree type provided? -> just try all trees:
        item = self._local_disk_cache.get_item(uid)
        if not item:
            item = self._gdrive_cache.get_item_for_uid(uid)
        return item

    def get_item_for_uid_and_tree_type(self, uid: UID, tree_type):
        if tree_type == TREE_TYPE_GDRIVE:
            return self._gdrive_cache.get_item_for_uid(uid)
        elif tree_type == TREE_TYPE_LOCAL_DISK:
            return self._local_disk_cache.get_item(uid)
        else:
            raise RuntimeError(f'Unknown tree type: {tree_type} for UID {uid}')

    def get_children(self, node: DisplayNode):
        tree_type: int = node.node_identifier.tree_type
        if tree_type == TREE_TYPE_GDRIVE:
            return self._gdrive_cache.get_children(node)
        elif tree_type == TREE_TYPE_LOCAL_DISK:
            return self._local_disk_cache.get_children(node)
        else:
            raise RuntimeError(f'Unknown tree type: {tree_type} for {node.node_identifier}')

    def get_parent_for_item(self, item: DisplayNode):
        if item.node_identifier.tree_type == TREE_TYPE_GDRIVE:
            return self._gdrive_cache.get_parent_for_item(item)
        elif item.node_identifier.tree_type == TREE_TYPE_LOCAL_DISK:
            return self._local_disk_cache.get_parent_for_item(item)
        else:
            raise RuntimeError(f'Unknown tree type: {item.node_identifier.tree_type} for {item}')

    def get_all_files_and_dirs_for_subtree(self, subtree_root: NodeIdentifier) -> Tuple[List[DisplayNode], List[DisplayNode]]:
        if subtree_root.tree_type == TREE_TYPE_GDRIVE:
            return self._gdrive_cache.get_all_goog_files_and_folders_for_subtree(subtree_root)
        elif subtree_root.tree_type == TREE_TYPE_LOCAL_DISK:
            return self._local_disk_cache.dir_tree.get_all_files_and_dirs_for_subtree(subtree_root)
        else:
            raise RuntimeError(f'Unknown tree type: {subtree_root.tree_type} for {subtree_root}')