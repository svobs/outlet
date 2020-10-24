import errno
import logging
import os
import pathlib
import threading
import time
from typing import Dict, Iterable, List, Optional, Tuple

from pydispatch import dispatcher

from command.cmd_interface import Command, CommandResult
from constants import CACHE_LOAD_TIMEOUT_SEC, GDRIVE_INDEX_FILE_NAME, INDEX_FILE_SUFFIX, MAIN_REGISTRY_FILE_NAME, NULL_UID, TREE_TYPE_GDRIVE, \
    TREE_TYPE_LOCAL_DISK
from error import CacheNotLoadedError, GDriveItemNotFoundError
from model.cache_info import CacheInfoEntry, PersistedCacheInfo
from model.display_tree.display_tree import DisplayTree
from model.display_tree.gdrive import GDriveDisplayTree
from model.node.node import Node, HasParentList
from model.node.gdrive_node import GDriveNode
from model.node.local_disk_node import LocalDirNode, LocalFileNode
from model.node_identifier import LocalNodeIdentifier, NodeIdentifier
from model.node_identifier_factory import NodeIdentifierFactory
from model.op import Op
from model.uid import UID
from store.gdrive.master_gdrive import GDriveMasterStore
from store.gdrive.master_gdrive_op_load import GDriveDiskLoadOp
from store.live_monitor import LiveMonitor
from store.local.master_local import LocalDiskMasterStore
from store.op.op_ledger import OpLedger
from store.sqlite.cache_registry_db import CacheRegistry
from ui import actions
from ui.actions import ID_GLOBAL_CACHE
from ui.tree.controller import TreePanelController
from util import file_util
from util.file_util import get_resource_path
from util.has_lifecycle import HasLifecycle
from util.stopwatch_sec import Stopwatch
from util.two_level_dict import TwoLevelDict

logger = logging.getLogger(__name__)

CFG_ENABLE_LOAD_FROM_DISK = 'cache.enable_cache_load'


def ensure_cache_dir_path(config):
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
        super().__init__(lambda x: x.subtree_root.tree_type, lambda x: x.subtree_root.get_single_path(), lambda x, y: True)


# CLASS CacheManager
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼

class CacheManager(HasLifecycle):
    """
    This is the central source of truth for the app (or attempts to be as much as possible).
    """
    def __init__(self, app):
        HasLifecycle.__init__(self)
        self.app = app

        self.cache_dir_path = ensure_cache_dir_path(self.app.config)
        self.main_registry_path = os.path.join(self.cache_dir_path, MAIN_REGISTRY_FILE_NAME)

        self.caches_by_type: CacheInfoByType = CacheInfoByType()

        self.enable_load_from_disk = app.config.get(CFG_ENABLE_LOAD_FROM_DISK)
        self.enable_save_to_disk = app.config.get('cache.enable_cache_save')
        self.load_all_caches_on_startup = app.config.get('cache.load_all_caches_on_startup')
        self.load_caches_for_displayed_trees_at_startup = app.config.get('cache.load_caches_for_displayed_trees_on_startup')
        self.sync_from_local_disk_on_cache_load = app.config.get('cache.sync_from_local_disk_on_cache_load')
        self.reload_tree_on_root_path_update = app.config.get('cache.load_cache_when_tree_root_selected')
        self.cancel_all_pending_ops_on_startup = app.config.get('cache.cancel_all_pending_ops_on_startup')
        self._is_live_capture_enabled = app.config.get('cache.live_capture_enabled')

        if not self.load_all_caches_on_startup:
            logger.info('Configured not to fetch all caches on startup; will lazy load instead')

        self._master_local = None
        """Sub-module of Cache Manager which manages local disk caches"""

        self._master_gdrive = None
        """Sub-module of Cache Manager which manages Google Drive caches"""

        self._op_ledger = None
        """Sub-module of Cache Manager which manages commands which have yet to execute"""

        self._live_monitor = None
        """Sub-module of Cache Manager which, for displayed trees, provides [close to] real-time notifications for changes
         which originated from outside this app"""

        self._tree_controllers: Dict[str, TreePanelController] = {}
        """Keep track of live UI tree controllers, so that we can look them up by ID (e.g. for use in automated testing)"""

        # Create Event objects to optionally wait for lifecycle events
        self._load_registry_done: threading.Event = threading.Event()

        self._startup_done: threading.Event = threading.Event()

        # (Optional) variables needed for producer/consumer behavior if Load All Caches needed
        self.load_all_caches_done: threading.Event = threading.Event()
        self._load_all_caches_in_process: bool = False

        dispatcher.connect(signal=actions.START_CACHEMAN, receiver=self._on_start_cacheman_requested)

    def shutdown(self):
        logger.debug('CacheManager.shutdown() entered')
        HasLifecycle.shutdown(self)

        try:
            if self._op_ledger:
                self._op_ledger.shutdown()
                self._op_ledger = None
        except NameError:
            pass

        try:
            if self._tree_controllers:
                for controller in list(self._tree_controllers.values()):
                    controller.destroy()
                self._tree_controllers.clear()
        except NameError:
            pass

        # Do this after destroying controllers, for a more orderly shutdown:
        try:
            if self._live_monitor:
                self._live_monitor.shutdown()
                self._live_monitor = None
        except NameError:
            pass

        try:
            if self._master_local:
                self._master_local.shutdown()
                self._master_local = None
        except NameError:
            pass

    # Startup loading/maintenance
    # ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼

    def _on_start_cacheman_requested(self, sender):
        if self._master_local:
            logger.info(f'Caches already loaded. Ignoring signal from {sender}.')
            return

        logger.debug(f'CacheManager.start() initiated by {sender}')
        self.start()

    def start(self):
        """Should be called during startup. Loop over all caches and load/merge them into a
        single large in-memory cache"""
        HasLifecycle.start(self)

        logger.debug(f'Sending START_PROGRESS_INDETERMINATE for ID: {ID_GLOBAL_CACHE}')
        stopwatch = Stopwatch()
        dispatcher.send(actions.START_PROGRESS_INDETERMINATE, sender=ID_GLOBAL_CACHE)

        try:
            # Init sub-modules:
            self._master_local = LocalDiskMasterStore(self.app)
            self._master_local.start()
            self._master_gdrive = GDriveMasterStore(self.app)
            self._master_gdrive.start()
            self._op_ledger = OpLedger(self.app)
            self._op_ledger.start()
            self._live_monitor = LiveMonitor(self.app)
            self._live_monitor.start()

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
                existing = self.caches_by_type.get_single(info.subtree_root.tree_type, info.subtree_root.get_single_path())
                if existing:
                    if info.sync_ts < existing.sync_ts:
                        logger.info(f'Skipping duplicate cache info entry: {existing.subtree_root}')
                        continue
                    else:
                        logger.info(f'Overwriting older duplicate cache info entry: {existing.subtree_root}')

                    skipped_count += 1
                else:
                    unique_cache_count += 1

                # There are up to 4 locations where the subtree root can be stored
                if info.subtree_root.tree_type == TREE_TYPE_LOCAL_DISK:
                    uid_from_mem = self._master_local.get_uid_for_path(info.subtree_root.get_single_path(), info.subtree_root.uid)
                    if uid_from_mem != info.subtree_root.uid:
                        raise RuntimeError(f'Subtree root UID from diskstore registry ({info.subtree_root.uid}) does not match UID '
                                           f'from memstore ({uid_from_mem}) for path="{info.subtree_root.get_single_path()}"')

                # Put into map to eliminate possible duplicates
                self.caches_by_type.put(info)

            # Write back to cache if we need to clean things up:
            if skipped_count > 0:
                caches = self.caches_by_type.get_all()
                self._overwrite_all_caches_in_registry(caches)

            self._load_registry_done.set()
            dispatcher.send(signal=actions.LOAD_REGISTRY_DONE, sender=ID_GLOBAL_CACHE)

            # Now load all caches (if configured):
            if self.enable_load_from_disk and self.load_all_caches_on_startup:
                self.load_all_caches()
            else:
                logger.info(f'{stopwatch} Found {unique_cache_count} existing caches but configured not to load on startup')

            # Finally, add any queued changes (asynchronously)
            self.app.executor.submit_async_task(self._op_ledger.load_pending_ops)

        finally:
            dispatcher.send(actions.STOP_PROGRESS, sender=ID_GLOBAL_CACHE)
            self._startup_done.set()
            logger.debug('CacheManager init done')
            dispatcher.send(signal=actions.START_CACHEMAN_DONE, sender=ID_GLOBAL_CACHE)

    def wait_for_load_registry_done(self, fail_on_timeout: bool = True):
        if not self._load_registry_done.wait(CACHE_LOAD_TIMEOUT_SEC):
            if fail_on_timeout:
                raise RuntimeError('Timed out waiting for CacheManager to finish loading the registry!')
            else:
                logger.error('Timed out waiting for CacheManager to finish loading the registry!')

    def wait_for_startup_done(self):
        if not self._startup_done.is_set():
            logger.info('Waiting for CacheManager startup to complete')
        if not self._startup_done.wait(CACHE_LOAD_TIMEOUT_SEC):
            logger.error('Timed out waiting for CacheManager startup!')

    def load_all_caches(self):
        """Load ALL the caches into memory. This is needed in certain circumstances, such as when a UID is being derefernced but we
        don't know which cache it belongs to."""
        if not self.enable_load_from_disk:
            raise RuntimeError('Cannot load all caches; loading caches from disk is disabled in config!')

        if self._load_all_caches_in_process:
            logger.info('Waiting for all caches to finish loading in other thread')
            # Wait for the other thread to complete. (With no timeout, it will never return):
            if not self.load_all_caches_done.wait(CACHE_LOAD_TIMEOUT_SEC):
                logger.error('Timed out waiting for all caches to load!')
        if self.load_all_caches_done.is_set():
            # Other thread completed
            return

        self._load_all_caches_in_process = True
        logger.info('Loading all caches from disk')

        stopwatch = Stopwatch()

        # MUST read GDrive first, because currently we assign incrementing integer UIDs for local files dynamically,
        # and we won't know which are reserved until we have read in all the existing GDrive caches
        existing_caches: List[PersistedCacheInfo] = list(self.caches_by_type.get_second_dict(TREE_TYPE_GDRIVE).values())
        assert len(existing_caches) <= 1, f'Expected at most 1 GDrive cache in registry but found: {len(existing_caches)}'

        local_caches: List[PersistedCacheInfo] = list(self.caches_by_type.get_second_dict(TREE_TYPE_LOCAL_DISK).values())
        registry_needs_update = self._master_local.consolidate_local_caches(local_caches, ID_GLOBAL_CACHE)
        existing_caches += local_caches

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
                self._init_existing_cache(existing_disk_cache)
            except RuntimeError:
                logger.exception(f'Failed to load cache: {existing_disk_cache.cache_location}')

        logger.info(f'{stopwatch} Load All Caches complete')
        self._load_all_caches_in_process = False
        self.load_all_caches_done.set()

    def _overwrite_all_caches_in_registry(self, cache_info_list: List[CacheInfoEntry]):
        logger.info(f'Overwriting all cache entries in persisted registry with {len(cache_info_list)} entries')
        with CacheRegistry(self.main_registry_path, self.app.node_identifier_factory) as cache_registry_db:
            cache_registry_db.insert_cache_info(cache_info_list, append=False, overwrite=True)

    def _get_cache_info_from_registry(self) -> List[CacheInfoEntry]:
        with CacheRegistry(self.main_registry_path, self.app.node_identifier_factory) as cache_registry_db:
            if cache_registry_db.has_cache_info():
                exisiting_caches = cache_registry_db.get_cache_info()
                logger.debug(f'Found {len(exisiting_caches)} caches listed in registry')
                return exisiting_caches
            else:
                logger.debug('Registry has no caches listed')
                return []

    def _init_existing_cache(self, existing_disk_cache: PersistedCacheInfo):
        if existing_disk_cache.is_loaded:
            logger.debug('Cache is already loaded; skipping')
            return

        cache_type = existing_disk_cache.subtree_root.tree_type
        if cache_type != TREE_TYPE_LOCAL_DISK and cache_type != TREE_TYPE_GDRIVE:
            raise RuntimeError(f'Unrecognized tree type: {cache_type}')

        if cache_type == TREE_TYPE_LOCAL_DISK:
            if not os.path.exists(existing_disk_cache.subtree_root.get_single_path()):
                logger.info(f'Subtree not found; will defer loading: "{existing_disk_cache.subtree_root}"')
                existing_disk_cache.needs_refresh = True
            else:
                self._master_local.get_display_tree(existing_disk_cache.subtree_root, ID_GLOBAL_CACHE)
        elif cache_type == TREE_TYPE_GDRIVE:
            assert existing_disk_cache.subtree_root == NodeIdentifierFactory.get_gdrive_root_constant_identifier(), \
                f'Expected GDrive root ({NodeIdentifierFactory.get_gdrive_root_constant_identifier()}) but found: {existing_disk_cache.subtree_root}'
            self._master_gdrive.get_synced_master_tree(tree_id=ID_GLOBAL_CACHE)

    # Subtree-level stuff
    # ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼

    def load_subtree(self, node_identifier: NodeIdentifier, tree_id: str) -> DisplayTree:
        """
        Performs a read-through retreival of all the nodes in the given subtree.
        """
        logger.debug(f'Got request to load subtree: {node_identifier}')

        node_identifier.normalize_paths()

        dispatcher.send(signal=actions.LOAD_SUBTREE_STARTED, sender=tree_id)

        if self._is_live_capture_enabled:
            self._live_monitor.start_or_update_capture(node_identifier, tree_id)

        if node_identifier.tree_type == TREE_TYPE_LOCAL_DISK:
            assert self._master_local
            subtree = self._master_local.get_display_tree(node_identifier, tree_id)
        elif node_identifier.tree_type == TREE_TYPE_GDRIVE:
            assert self._master_gdrive
            subtree = self._master_gdrive.get_display_tree(node_identifier, tree_id=tree_id)
        else:
            raise RuntimeError(f'Unrecognized tree type: {node_identifier.tree_type}')

        return subtree

    def find_existing_cache_info_for_local_subtree(self, full_path: str) -> Optional[PersistedCacheInfo]:
        existing_caches: List[PersistedCacheInfo] = list(self.caches_by_type.get_second_dict(TREE_TYPE_LOCAL_DISK).values())

        for existing_cache in existing_caches:
            # Is existing_cache an ancestor of target tree?
            if full_path.startswith(existing_cache.subtree_root.get_single_path()):
                return existing_cache
        # Nothing in the cache contains subtree
        return None

    def get_cache_info_entry(self, subtree_root: NodeIdentifier) -> PersistedCacheInfo:
        return self.caches_by_type.get_single(subtree_root.tree_type, subtree_root.get_single_path())

    def get_or_create_cache_info_entry(self, subtree_root: NodeIdentifier) -> PersistedCacheInfo:
        existing = self.get_cache_info_entry(subtree_root)
        if existing:
            logger.debug(f'Found existing cache entry for subtree: {subtree_root}')
            return existing
        else:
            logger.debug(f'No existing cache entry found for subtree: {subtree_root}')

        if subtree_root.tree_type == TREE_TYPE_LOCAL_DISK:
            unique_path = subtree_root.get_single_path().replace('/', '_')
            file_name = f'LO_{unique_path}.{INDEX_FILE_SUFFIX}'
        elif subtree_root.tree_type == TREE_TYPE_GDRIVE:
            file_name = GDRIVE_INDEX_FILE_NAME
        else:
            raise RuntimeError(f'Unrecognized tree type: {subtree_root.tree_type}')

        cache_location = os.path.join(self.cache_dir_path, file_name)
        now_ms = int(time.time())
        db_entry = CacheInfoEntry(cache_location=cache_location,
                                  subtree_root=subtree_root, sync_ts=now_ms,
                                  is_complete=True)

        with CacheRegistry(self.main_registry_path, self.app.node_identifier_factory) as cache_registry_db:
            logger.info(f'Inserting new cache info into registry: {subtree_root}')
            cache_registry_db.insert_cache_info(db_entry, append=True, overwrite=False)

        cache_info = PersistedCacheInfo(db_entry)

        # Save reference in memory
        self.caches_by_type.put(cache_info)

        return cache_info

    # Main cache CRUD
    # ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼

    def upsert_single_node(self, node: Node):
        tree_type = node.node_identifier.tree_type
        if tree_type == TREE_TYPE_GDRIVE:
            self._master_gdrive.upsert_single_node(node)
        elif tree_type == TREE_TYPE_LOCAL_DISK:
            self._master_local.upsert_single_node(node)
        else:
            raise RuntimeError(f'Unrecognized tree type ({tree_type}) for node {node}')

    def update_single_node(self, node: Node):
        """Simliar to upsert, but fails silently if node does not already exist in caches. Useful for things such as asynch MD5 filling"""
        tree_type = node.node_identifier.tree_type
        if tree_type == TREE_TYPE_GDRIVE:
            self._master_gdrive.update_single_node(node)
        elif tree_type == TREE_TYPE_LOCAL_DISK:
            self._master_local.update_single_node(node)
        else:
            raise RuntimeError(f'Unrecognized tree type ({tree_type}) for node {node}')

    def remove_subtree(self, node: Node, to_trash: bool):
        tree_type = node.node_identifier.tree_type
        if tree_type == TREE_TYPE_GDRIVE:
            self._master_gdrive.remove_subtree(node, to_trash)
        elif tree_type == TREE_TYPE_LOCAL_DISK:
            self._master_local.remove_subtree(node, to_trash)
        else:
            raise RuntimeError(f'Unrecognized tree type ({tree_type}) for node {node}')

    def move_local_subtree(self, src_full_path: str, dst_full_path: str, is_from_watchdog=False):
        self._master_local.move_local_subtree(src_full_path, dst_full_path, is_from_watchdog)

    def remove_node(self, node: Node, to_trash):
        tree_type = node.node_identifier.tree_type
        if tree_type == TREE_TYPE_GDRIVE:
            self._master_gdrive.remove_single_node(node, to_trash)
        elif tree_type == TREE_TYPE_LOCAL_DISK:
            self._master_local.remove_single_node(node, to_trash)
        else:
            raise RuntimeError(f'Unrecognized tree type ({tree_type}) for node {node}')

    # Tree controller tracking/lookup
    # ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼

    def register_tree_controller(self, controller: TreePanelController):
        logger.debug(f'[{controller.tree_id}] Registering controller')
        self._tree_controllers[controller.tree_id] = controller

    def unregister_tree_controller(self, controller: TreePanelController):
        logger.debug(f'[{controller.tree_id}] Unregistering controller')
        popped_con = self._tree_controllers.pop(controller.tree_id, None)
        if popped_con:
            if self._is_live_capture_enabled and self._live_monitor:
                self._live_monitor.stop_capture(controller.tree_id)
        else:
            logger.debug(f'Could not unregister TreeController; it was not found: {controller.tree_id}')

    def get_tree_controller(self, tree_id: str) -> Optional[TreePanelController]:
        return self._tree_controllers.get(tree_id, None)

    # Various public methods
    # ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼

    def show_tree(self, subtree_root: LocalNodeIdentifier) -> str:
        if subtree_root.tree_type == TREE_TYPE_LOCAL_DISK:
            return self._master_local.show_tree(subtree_root)
        elif subtree_root.tree_type == TREE_TYPE_GDRIVE:
            raise
        else:
            assert False

    def execute_gdrive_load_op(self, op: GDriveDiskLoadOp):
        self._master_gdrive.execute_load_op(op)

    def update_from(self, cmd_result: CommandResult):
        """Updates the in-memory cache, on-disk cache, and UI with the nodes from the given CommandResult"""
        # TODO: refactor so that we can attempt to create (close to) an atomic operation which combines GDrive and Local functionality

        if cmd_result.nodes_to_upsert:
            logger.debug(f'Upserted {len(cmd_result.nodes_to_upsert)} nodes: notifying cacheman')
            for upsert_node in cmd_result.nodes_to_upsert:
                self.upsert_single_node(upsert_node)

        if cmd_result.nodes_to_delete:
            try:
                to_trash = cmd_result.to_trash
            except AttributeError:
                to_trash = False

            logger.debug(f'Deleted {len(cmd_result.nodes_to_delete)} nodes: notifying cacheman')
            for deleted_node in cmd_result.nodes_to_delete:
                self.remove_node(deleted_node, to_trash)

    def refresh_subtree(self, node: Node, tree_id: str):
        """Called asynchronously via actions.REFRESH_SUBTREE"""
        logger.debug(f'[{tree_id}] Refreshing subtree: {node}')
        if node.get_tree_type() == TREE_TYPE_LOCAL_DISK:
            self._master_local.refresh_subtree(node, tree_id)
        elif node.get_tree_type() == TREE_TYPE_GDRIVE:
            self._master_gdrive.refresh_subtree(node, tree_id)
        else:
            assert False

    def refresh_stats(self, subtree_root_node: Node, tree_id: str):
        """Does not send signals. The caller is responsible for sending REFRESH_SUBTREE_STATS_DONE and SET_STATUS themselves"""
        logger.debug(f'[{tree_id}] Refreshing stats for subtree: {subtree_root_node}')
        if subtree_root_node.get_tree_type() == TREE_TYPE_LOCAL_DISK:
            self._master_local.refresh_subtree_stats(subtree_root_node, tree_id)
        elif subtree_root_node.get_tree_type() == TREE_TYPE_GDRIVE:
            self._master_gdrive.refresh_subtree_stats(subtree_root_node, tree_id)
        else:
            assert False

    def get_last_pending_op_for_node(self, node_uid: UID) -> Optional[Op]:
        return self._op_ledger.get_last_pending_op_for_node(node_uid)

    def enqueue_op_list(self, op_list: Iterable[Op]):
        """Attempt to add the given Ops to the execution tree. No need to worry whether some changes overlap or are redundant;
         the OpLedger will sort that out - although it will raise an error if it finds incompatible changes such as adding to a tree
         that is scheduled for deletion."""
        self._op_ledger.append_new_pending_ops(op_list)

    def get_next_command(self) -> Optional[Command]:
        # blocks !
        self.wait_for_startup_done()
        # also blocks !
        return self._op_ledger.get_next_command()

    def download_all_gdrive_meta(self, tree_id: str):
        """Wipes any existing disk cache and replaces it with a complete fresh download from the GDrive servers."""
        self._master_gdrive.get_synced_master_tree(invalidate_cache=True, tree_id=tree_id)

    def get_synced_gdrive_master_tree(self, tree_id: str) -> GDriveDisplayTree:
        """Will load from disk and sync latest changes from GDrive server before returning."""
        return self._master_gdrive.get_synced_master_tree(tree_id=tree_id)

    def build_local_file_node(self, full_path: str, staging_path=None, must_scan_signature=False) -> Optional[LocalFileNode]:
        return self._master_local.build_local_file_node(full_path, staging_path, must_scan_signature)

    def build_local_dir_node(self, full_path: str) -> LocalDirNode:
        return self._master_local.build_local_dir_node(full_path)

    def resolve_root_from_path(self, full_path: str) -> Tuple[NodeIdentifier, Exception]:
        """Resolves the given path into either a local file, a set of Google Drive matches, or generates a GDriveItemNotFoundError,
        and returns a tuple of both"""
        try:
            full_path = file_util.normalize_path(full_path)
            node_identifier = self.app.node_identifier_factory.for_values(path_list=full_path)
            if node_identifier.tree_type == TREE_TYPE_GDRIVE:
                # Need to wait until all caches are loaded:
                self.wait_for_startup_done()

                identifiers = self._master_gdrive.get_identifier_list_for_full_path_list(node_identifier.full_path)
            else:
                if not os.path.exists(full_path):
                    raise FileNotFoundError(errno.ENOENT, os.strerror(errno.ENOENT), full_path)
                uid = self.get_uid_for_path(full_path)
                identifiers = [LocalNodeIdentifier(uid=uid, path_list=full_path)]

            assert len(identifiers) > 0, f'Got no identifiers (not even NULL) for path: {full_path}'
            new_root: NodeIdentifier = identifiers[0]
            err = None
        except GDriveItemNotFoundError as ginf:
            new_root = ginf.node_identifier
            err = ginf
        except FileNotFoundError as fnf:
            new_root = self.app.node_identifier_factory.for_values(full_path=full_path)
            err = fnf
        except CacheNotLoadedError as cnlf:
            err = cnlf
            new_root = self.app.node_identifier_factory.for_values(full_path=full_path, uid=NULL_UID)

        return new_root, err

    def get_goog_id_for_parent(self, node: GDriveNode) -> str:
        """Fails if there is not exactly 1 parent"""
        parent_uids: List[UID] = node.get_parent_uids()
        if len(parent_uids) != 1:
            raise RuntimeError(f'Only one parent is allowed but node has {len(parent_uids)} parents: {node}')

        # This will raise an exception if it cannot resolve:
        parent_goog_ids: List[str] = self.get_goog_id_list_for_uid_list(parent_uids, fail_if_missing=True)

        parent_goog_id: str = parent_goog_ids[0]
        return parent_goog_id

    def get_goog_id_list_for_uid_list(self, uids: List[UID], fail_if_missing: bool = True) -> List[str]:
        return self._master_gdrive.get_goog_id_list_for_uid_list(uids, fail_if_missing=fail_if_missing)

    def get_uid_for_path(self, full_path: str, uid_suggestion: Optional[UID] = None) -> UID:
        """Deterministically gets or creates a UID corresponding to the given path string"""
        assert full_path
        return self._master_local.get_uid_for_path(full_path, uid_suggestion)

    def read_single_node_from_disk_for_path(self, full_path: str, tree_type: int) -> Node:
        if not file_util.is_normalized(full_path):
            full_path = file_util.normalize_path(full_path)
            logger.debug(f'Normalized path: {full_path}')

        if tree_type == TREE_TYPE_LOCAL_DISK:
            assert self._master_local
            node = self._master_local.load_node_for_path(full_path)
            return node
        elif tree_type == TREE_TYPE_GDRIVE:
            assert self._master_gdrive
            # TODO
            raise RuntimeError(f'Not yet supported: {tree_type}')
        else:
            raise RuntimeError(f'Unrecognized tree type: {tree_type}')

    def get_uid_list_for_goog_id_list(self, goog_id_list: List[str]) -> List[UID]:
        return self._master_gdrive.get_uid_list_for_goog_id_list(goog_id_list)

    def get_uid_for_goog_id(self, goog_id: str, uid_suggestion: Optional[UID] = None) -> UID:
        """Deterministically gets or creates a UID corresponding to the given goog_id"""
        if not goog_id:
            raise RuntimeError('get_uid_for_goog_id(): no goog_id specified!')
        return self._master_gdrive.get_uid_for_goog_id(goog_id, uid_suggestion)

    def get_node_for_local_path(self, full_path: str) -> Node:
        uid = self.get_uid_for_path(full_path)
        return self._master_local.get_node_for_uid(uid)

    def get_goog_node_for_name_and_parent_uid(self, name: str, parent_uid: UID) -> Optional[GDriveNode]:
        """Returns the first GDrive node found with the given name and parent.
        This roughly matches the logic used to search for an node in Google Drive when we are unsure about its goog_id."""
        return self._master_gdrive.get_node_for_name_and_parent_uid(name, parent_uid)

    def get_node_for_goog_id(self, goog_id: str) -> UID:
        return self._master_gdrive.get_node_for_domain_id(goog_id)

    def get_node_for_uid(self, uid: UID, tree_type: int = None):
        if tree_type:
            if tree_type == TREE_TYPE_LOCAL_DISK:
                return self._master_local.get_node_for_uid(uid)
            elif tree_type == TREE_TYPE_GDRIVE:
                return self._master_gdrive.get_node_for_uid(uid)
            else:
                raise RuntimeError(f'Unknown tree type: {tree_type} for UID {uid}')

        # no tree type provided? -> just try all trees:
        node = self._master_local.get_node_for_uid(uid)
        if not node:
            node = self._master_gdrive.get_node_for_uid(uid)
        return node

    def get_children(self, node: Node):
        tree_type: int = node.node_identifier.tree_type
        if tree_type == TREE_TYPE_GDRIVE:
            return self._master_gdrive.get_children(node)
        elif tree_type == TREE_TYPE_LOCAL_DISK:
            return self._master_local.get_children(node)
        else:
            raise RuntimeError(f'Unknown tree type: {tree_type} for {node.node_identifier}')

    def get_parent_uid_for_node(self, node: Node) -> UID:
        """Derives the UID for the parent of the given node"""
        if isinstance(node, HasParentList):
            assert isinstance(node, GDriveNode) and node.node_identifier.tree_type == TREE_TYPE_GDRIVE, f'Node: {node}'
            parent_uids = node.get_parent_uids()
            assert len(parent_uids) == 1, f'Expected exactly one parent_uid for node: {node}'
            return parent_uids[0]
        else:
            assert node.node_identifier.tree_type == TREE_TYPE_LOCAL_DISK, f'Node: {node}'
            parent_path = str(pathlib.Path(node.full_path).parent)
            return self.get_uid_for_path(parent_path)

    def get_parent_for_node(self, node: Node, required_subtree_path: str = None):
        if node.node_identifier.tree_type == TREE_TYPE_GDRIVE:
            return self._master_gdrive.get_parent_for_node(node, required_subtree_path)
        elif node.node_identifier.tree_type == TREE_TYPE_LOCAL_DISK:
            return self._master_local.get_parent_for_node(node, required_subtree_path)
        else:
            raise RuntimeError(f'Unknown tree type: {node.node_identifier.tree_type} for {node}')

    def get_all_files_and_dirs_for_subtree(self, subtree_root: NodeIdentifier) -> Tuple[List[Node], List[Node]]:
        if subtree_root.tree_type == TREE_TYPE_GDRIVE:
            return self._master_gdrive.get_all_gdrive_files_and_folders_for_subtree(subtree_root)
        elif subtree_root.tree_type == TREE_TYPE_LOCAL_DISK:
            assert isinstance(subtree_root, LocalNodeIdentifier)
            return self._master_local.get_all_files_and_dirs_for_subtree(subtree_root)
        else:
            raise RuntimeError(f'Unknown tree type: {subtree_root.tree_type} for {subtree_root}')

    def get_gdrive_user_for_permission_id(self, permission_id: str):
        return self._master_gdrive.get_gdrive_user_for_permission_id(permission_id)

    def create_gdrive_user(self, user):
        return self._master_gdrive.create_gdrive_user(user)

    def get_or_create_gdrive_mime_type(self, mime_type_string: str):
        return self._master_gdrive.get_or_create_gdrive_mime_type(mime_type_string)

    def delete_all_gdrive_meta(self):
        return self._master_gdrive.delete_all_gdrive_meta()

    def apply_gdrive_changes(self, gdrive_change_list):
        self._master_gdrive.apply_gdrive_changes(gdrive_change_list)

    def get_gdrive_client(self):
        return self._master_gdrive.gdrive_client
