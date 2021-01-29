import logging
import os
import pathlib
import threading
from collections import deque
from typing import Deque, Dict, Iterable, List, Optional, Tuple

from pydispatch import dispatcher

import util.format
from backend.executor.command.cmd_interface import Command
from backend.executor.user_op.op_ledger import OpLedger
from backend.store.gdrive.master_gdrive import GDriveMasterStore
from backend.store.sqlite.cache_registry_db import CacheRegistry
from backend.store.tree.change_tree import ChangeTree
from constants import CACHE_LOAD_TIMEOUT_SEC, CFG_ENABLE_LOAD_FROM_DISK, GDRIVE_INDEX_FILE_NAME, GDRIVE_ROOT_UID, IconId, INDEX_FILE_SUFFIX, \
    MAIN_REGISTRY_FILE_NAME, OPS_FILE_NAME, ROOT_PATH, \
    SUPER_DEBUG, TREE_TYPE_GDRIVE, \
    TREE_TYPE_LOCAL_DISK, TreeDisplayMode, UID_PATH_FILE_NAME
from backend.diff.change_maker import ChangeMaker
from error import InvalidOperationError
from model.cache_info import CacheInfoEntry, PersistedCacheInfo
from model.display_tree.build_struct import DisplayTreeRequest
from model.display_tree.display_tree import DisplayTreeUiState
from model.display_tree.filter_criteria import FilterCriteria
from model.node.gdrive_node import GDriveNode
from model.node.local_disk_node import LocalDirNode, LocalFileNode
from model.node.node import Node, SPIDNodePair
from model.node.trait import HasDirectoryStats
from model.node_identifier import GDriveIdentifier, LocalNodeIdentifier, NodeIdentifier, SinglePathNodeIdentifier
from model.node_identifier_factory import NodeIdentifierFactory
from model.uid import UID
from model.user_op import UserOp, UserOpType
from backend.store.gdrive.master_gdrive_op_load import GDriveDiskLoadOp
from backend.store.local.master_local import LocalDiskMasterStore
from backend.store.tree.active_tree_manager import ActiveTreeManager
from backend.store.tree.active_tree_meta import ActiveDisplayTreeMeta
from backend.store.tree.load_request_thread import LoadRequest, LoadRequestThread
from backend.store.uid.uid_mapper import UidChangeTreeMapper
from signal_constants import ID_GDRIVE_DIR_SELECT, ID_GLOBAL_CACHE, Signal
from util import file_util, time_util
from util.ensure import ensure_list
from util.file_util import get_resource_path
from util.has_lifecycle import HasLifecycle
from util.stopwatch_sec import Stopwatch
from util.two_level_dict import TwoLevelDict

logger = logging.getLogger(__name__)


def ensure_cache_dir_path(backend):
    cache_dir_path = get_resource_path(backend.get_config('cache.cache_dir_path'))
    if not os.path.exists(cache_dir_path):
        logger.info(f'Cache directory does not exist; attempting to create: "{cache_dir_path}"')
    os.makedirs(name=cache_dir_path, exist_ok=True)
    return cache_dir_path


class CacheInfoByType(TwoLevelDict):
    """
    ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
    CLASS CacheInfoByType

    Holds PersistedCacheInfo objects
    ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼
    """

    def __init__(self):
        super().__init__(lambda x: x.subtree_root.tree_type, lambda x: x.subtree_root.get_path_list()[0], lambda x, y: True)


class CacheManager(HasLifecycle):
    """
    ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
    CLASS CacheManager

    This is the central source of truth for the backend (or attempts to be as much as possible).
    ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼
    """

    def __init__(self, backend):
        HasLifecycle.__init__(self)
        self.backend = backend

        self.cache_dir_path = ensure_cache_dir_path(self.backend)
        self.main_registry_path = os.path.join(self.cache_dir_path, MAIN_REGISTRY_FILE_NAME)

        self.change_tree_uid_mapper = UidChangeTreeMapper(self.backend)

        self._caches_by_type: CacheInfoByType = CacheInfoByType()

        self.enable_load_from_disk = backend.get_config(CFG_ENABLE_LOAD_FROM_DISK)
        self.enable_save_to_disk = backend.get_config('cache.enable_cache_save')
        self.load_all_caches_on_startup = backend.get_config('cache.load_all_caches_on_startup')
        self.load_caches_for_displayed_trees_at_startup = backend.get_config('cache.load_caches_for_displayed_trees_on_startup')
        self.sync_from_local_disk_on_cache_load = backend.get_config('cache.sync_from_local_disk_on_cache_load')
        self.sync_from_gdrive_on_cache_load = backend.get_config('cache.sync_from_gdrive_on_cache_load')
        self.reload_tree_on_root_path_update = backend.get_config('cache.load_cache_when_tree_root_selected')
        self.cancel_all_pending_ops_on_startup = backend.get_config('cache.cancel_all_pending_ops_on_startup')

        if not self.load_all_caches_on_startup:
            logger.info('Configured not to fetch all caches on startup; will lazy load instead')

        self._load_request_thread = LoadRequestThread(backend=backend, cacheman=self)

        # Instantiate but do not start submodules yet, to avoid entangled dependencies:

        self._active_tree_manager = ActiveTreeManager(self.backend)

        uid_path_cache_path = os.path.join(self.cache_dir_path, UID_PATH_FILE_NAME)
        self._master_local: LocalDiskMasterStore = LocalDiskMasterStore(self.backend, uid_path_cache_path)
        """Sub-module of Cache Manager which manages local disk caches"""

        self._master_gdrive: GDriveMasterStore = GDriveMasterStore(self.backend)
        """Sub-module of Cache Manager which manages Google Drive caches"""

        op_db_path = os.path.join(self.cache_dir_path, OPS_FILE_NAME)
        self._op_ledger: OpLedger = OpLedger(self.backend, op_db_path)
        """Sub-module of Cache Manager which manages commands which have yet to execute"""

        # Create Event objects to optionally wait for lifecycle events
        self._load_registry_done: threading.Event = threading.Event()

        self._startup_done: threading.Event = threading.Event()

        # (Optional) variables needed for producer/consumer behavior if Load All Caches needed
        self._load_all_caches_done: threading.Event = threading.Event()
        self._load_all_caches_in_process: bool = False

        self.connect_dispatch_listener(signal=Signal.START_CACHEMAN, receiver=self._on_start_cacheman_requested)
        self.connect_dispatch_listener(signal=Signal.COMMAND_COMPLETE, receiver=self._on_command_completed)

    def shutdown(self):
        logger.debug('CacheManager.shutdown() entered')
        HasLifecycle.shutdown(self)

        try:
            if self._op_ledger:
                self._op_ledger.shutdown()
                self._op_ledger = None
        except NameError or AttributeError:
            pass

        try:
            if self._master_local:
                self._master_local.shutdown()
                self._master_local = None
        except NameError or AttributeError:
            pass

        try:
            if self._master_gdrive:
                self._master_gdrive.shutdown()
                self._master_gdrive = None
        except NameError or AttributeError:
            pass

        try:
            if self._load_request_thread:
                self._load_request_thread.shutdown()
                self._load_request_thread = None
        except NameError or AttributeError:
            pass

        try:
            if self._active_tree_manager:
                self._active_tree_manager.shutdown()
                self._active_tree_manager = None
        except NameError or AttributeError:
            pass

    # Startup loading/maintenance
    # ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼

    def start(self):
        """Should be called during startup. Loop over all caches and load/merge them into a
        single large in-memory cache"""
        logger.debug(f'Starting CacheManager')
        HasLifecycle.start(self)

        logger.debug(f'Sending START_PROGRESS_INDETERMINATE for ID: {ID_GLOBAL_CACHE}')
        dispatcher.send(Signal.START_PROGRESS_INDETERMINATE, sender=ID_GLOBAL_CACHE)

        try:
            # Load registry first. Do validation along the way
            self._load_registry()

            # Start sub-modules:
            self._active_tree_manager.start()
            self._master_local.start()
            self._master_gdrive.start()
            self._op_ledger.start()

            self._load_request_thread.start()

            # Now load all caches (if configured):
            if self.enable_load_from_disk and self.load_all_caches_on_startup:
                # TODO: make this into async task
                self._load_all_caches()
            else:
                logger.info(f'Configured not to load on startup')

            # Finally, add or cancel any queued changes (asynchronously)
            if self.cancel_all_pending_ops_on_startup:
                logger.debug(f'User configuration specifies cancelling all pending ops on startup')
                pending_ops_task = self._op_ledger.cancel_pending_ops_from_disk
            else:
                pending_ops_task = self._op_ledger.resume_pending_ops_from_disk
            self.backend.executor.submit_async_task(pending_ops_task)

        finally:
            dispatcher.send(Signal.STOP_PROGRESS, sender=ID_GLOBAL_CACHE)
            self._startup_done.set()
            logger.info('CacheManager startup done')
            dispatcher.send(signal=Signal.START_CACHEMAN_DONE, sender=ID_GLOBAL_CACHE)

    def _on_start_cacheman_requested(self, sender):
        if self._startup_done.is_set():
            logger.info(f'Caches already loaded. Ignoring signal from {sender}.')
            return

        logger.debug(f'CacheManager.start() initiated by {sender}')
        self.start()

    def _load_registry(self):
        stopwatch = Stopwatch()
        caches_from_registry: List[CacheInfoEntry] = self._get_cache_info_list_from_registry()
        unique_cache_count = 0
        skipped_count = 0
        for cache_from_registry in caches_from_registry:
            info: PersistedCacheInfo = PersistedCacheInfo(cache_from_registry)
            if not os.path.exists(info.cache_location):
                logger.info(f'Skipping non-existent cache info entry: {info.cache_location} (for subtree: {info.subtree_root})')
                skipped_count += 1
                continue
            existing = self._caches_by_type.get_single(info.subtree_root.tree_type, info.subtree_root.get_path_list()[0])
            if existing:
                if info.sync_ts < existing.sync_ts:
                    logger.info(f'Skipping duplicate cache info entry: {existing.subtree_root}')
                    continue
                else:
                    logger.info(f'Overwriting older duplicate cache info entry: {existing.subtree_root}')

                skipped_count += 1
            else:
                unique_cache_count += 1

            # Put into map to eliminate possible duplicates
            self._caches_by_type.put_item(info)

        # Write back to cache if we need to clean things up:
        if skipped_count > 0:
            caches = self._caches_by_type.get_all()
            self._overwrite_all_caches_in_registry(caches)

        self._load_registry_done.set()
        dispatcher.send(signal=Signal.LOAD_REGISTRY_DONE, sender=ID_GLOBAL_CACHE)

        logger.info(f'{stopwatch} Found {unique_cache_count} existing caches (+ {skipped_count} skipped)')

    def wait_for_load_registry_done(self, fail_on_timeout: bool = True):
        if self._load_registry_done.is_set():
            return

        logger.debug(f'Waiting for Load Registry to complete')
        if not self._load_registry_done.wait(CACHE_LOAD_TIMEOUT_SEC):
            if fail_on_timeout:
                raise RuntimeError('Timed out waiting for CacheManager to finish loading the registry!')
            else:
                logger.error('Timed out waiting for CacheManager to finish loading the registry!')
        logger.debug(f'Load Registry completed')

    def wait_for_startup_done(self):
        if not self._startup_done.is_set():
            logger.debug('Waiting for CacheManager startup to complete')
        if not self._startup_done.wait(CACHE_LOAD_TIMEOUT_SEC):
            logger.error('Timed out waiting for CacheManager startup!')

    def _load_all_caches(self):
        """Load ALL the caches into memory. This is needed in certain circumstances, such as when a UID is being derefernced but we
        don't know which cache it belongs to."""
        if not self.enable_load_from_disk:
            raise RuntimeError('Cannot load all caches; loading caches from disk is disabled in config!')

        if self._load_all_caches_in_process:
            logger.info('Waiting for all caches to finish loading in other thread')
            # Wait for the other thread to complete. (With no timeout, it will never return):
            if not self._load_all_caches_done.wait(CACHE_LOAD_TIMEOUT_SEC):
                logger.error('Timed out waiting for all caches to load!')
        if self._load_all_caches_done.is_set():
            # Other thread completed
            return

        self._load_all_caches_in_process = True
        logger.info('Loading all caches from disk')

        stopwatch = Stopwatch()

        # MUST read GDrive first, because currently we assign incrementing integer UIDs for local files dynamically,
        # and we won't know which are reserved until we have read in all the existing GDrive caches
        existing_caches: List[PersistedCacheInfo] = list(self._caches_by_type.get_second_dict(TREE_TYPE_GDRIVE).values())
        assert len(existing_caches) <= 1, f'Expected at most 1 GDrive cache in registry but found: {len(existing_caches)}'

        local_caches: List[PersistedCacheInfo] = list(self._caches_by_type.get_second_dict(TREE_TYPE_LOCAL_DISK).values())
        registry_needs_update = self._master_local.consolidate_local_caches(local_caches, ID_GLOBAL_CACHE)
        existing_caches += local_caches

        if registry_needs_update and self.enable_save_to_disk:
            self._overwrite_all_caches_in_registry(existing_caches)
            logger.debug(f'Overwriting in-memory list ({len(self._caches_by_type)}) with {len(existing_caches)} entries')
            self._caches_by_type.clear()
            for cache in existing_caches:
                self._caches_by_type.put_item(cache)

        for cache_num, existing_disk_cache in enumerate(existing_caches):
            try:
                self._caches_by_type.put_item(existing_disk_cache)
                logger.info(f'Init cache {(cache_num + 1)}/{len(existing_caches)}: id={existing_disk_cache.subtree_root}')
                self._init_existing_cache(existing_disk_cache)
            except RuntimeError:
                logger.exception(f'Failed to load cache: {existing_disk_cache.cache_location}')

        logger.info(f'{stopwatch} Load All Caches complete')
        self._load_all_caches_in_process = False
        self._load_all_caches_done.set()

    def _get_cache_info_list_from_registry(self) -> List[CacheInfoEntry]:
        with CacheRegistry(self.main_registry_path, self.backend.node_identifier_factory) as db:
            if db.has_cache_info():
                exisiting_caches = db.get_cache_info()
                logger.debug(f'Found {len(exisiting_caches)} caches listed in registry')
            else:
                logger.debug('Registry has no caches listed')
                db.create_cache_registry_if_not_exist()
                exisiting_caches = []

        return exisiting_caches

    def _overwrite_all_caches_in_registry(self, cache_info_list: List[CacheInfoEntry]):
        logger.info(f'Overwriting all cache entries in persisted registry with {len(cache_info_list)} entries')
        with CacheRegistry(self.main_registry_path, self.backend.node_identifier_factory) as db:
            db.insert_cache_info(cache_info_list, append=False, overwrite=True)

    def _init_existing_cache(self, existing_disk_cache: PersistedCacheInfo):
        if existing_disk_cache.is_loaded:
            logger.debug('Cache is already loaded; skipping')
            return

        cache_type = existing_disk_cache.subtree_root.tree_type
        if cache_type != TREE_TYPE_LOCAL_DISK and cache_type != TREE_TYPE_GDRIVE:
            raise RuntimeError(f'Unrecognized tree type: {cache_type}')

        if cache_type == TREE_TYPE_LOCAL_DISK:
            if not os.path.exists(existing_disk_cache.subtree_root.get_path_list()[0]):
                logger.info(f'Subtree not found; will defer loading: "{existing_disk_cache.subtree_root}"')
                existing_disk_cache.needs_refresh = True
            else:
                assert isinstance(existing_disk_cache.subtree_root, LocalNodeIdentifier)
                self._master_local.load_subtree(existing_disk_cache.subtree_root, ID_GLOBAL_CACHE)
        elif cache_type == TREE_TYPE_GDRIVE:
            assert existing_disk_cache.subtree_root == NodeIdentifierFactory.get_root_constant_gdrive_identifier(), \
                f'Expected GDrive root ({NodeIdentifierFactory.get_root_constant_gdrive_identifier()}) but found: {existing_disk_cache.subtree_root}'
            self._master_gdrive.load_and_sync_master_tree()

    # Action listener callbacks
    # ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼

    def _on_command_completed(self, sender, command: Command):
        """Updates the in-memory cache, on-disk cache, and UI with the nodes from the given UserOpResult"""
        logger.debug(f'Received signal: "{Signal.COMMAND_COMPLETE.name}"')
        result = command.op.result

        # TODO: refactor so that we can attempt to create (close to) an atomic operation which combines GDrive and Local functionality

        if result.nodes_to_upsert:
            logger.debug(f'Cmd resulted in {len(result.nodes_to_upsert)} nodes to upsert')
            for node_to_upsert in result.nodes_to_upsert:
                self.upsert_single_node(node_to_upsert)

        if result.nodes_to_delete:
            # TODO: to_trash?

            logger.debug(f'Cmd resulted in {len(result.nodes_to_delete)} nodes to delete')
            for deleted_node in result.nodes_to_delete:
                self.remove_node(deleted_node, to_trash=False)

    def enqueue_refresh_subtree_task(self, node_identifier: NodeIdentifier, tree_id: str):
        logger.info(f'Enqueuing task to refresh subtree at {node_identifier}')
        self.backend.executor.submit_async_task(self._refresh_subtree, node_identifier, tree_id)

    def enqueue_refresh_subtree_stats_task(self, root_uid: UID, tree_id: str):
        logger.info(f'[{tree_id}] Enqueuing task to refresh stats')
        self.backend.executor.submit_async_task(self._refresh_stats, root_uid, tree_id)

    # DisplayTree stuff
    # ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼

    def enqueue_load_tree_task(self, tree_id: str, send_signals: bool):
        logger.debug(f'[{tree_id}] Enqueueing subtree load task')
        self._load_request_thread.enqueue(LoadRequest(tree_id=tree_id, send_signals=send_signals))

    def load_data_for_display_tree(self, load_request: LoadRequest):
        """Executed asyncly via the LoadRequestThread."""
        tree_id: str = load_request.tree_id
        logger.debug(f'Loading data for display tree: {tree_id}')
        display_tree_meta: ActiveDisplayTreeMeta = self.get_active_display_tree_meta(tree_id)
        if not display_tree_meta:
            logger.info(f'Display tree is no longer tracked; discarding data load: {tree_id}')
            return

        if load_request.send_signals:
            # This will be carried across gRPC if needed
            dispatcher.send(signal=Signal.LOAD_SUBTREE_STARTED, sender=tree_id)

        if not display_tree_meta.is_first_order():
            logger.info(f'DisplayTree is higher-order and thus is already loaded: {tree_id}')
        elif display_tree_meta.root_exists:
            spid = display_tree_meta.root_sn.spid
            if spid.tree_type == TREE_TYPE_LOCAL_DISK:
                self._master_local.load_subtree(spid, tree_id)
            elif spid.tree_type == TREE_TYPE_GDRIVE:
                assert self._master_gdrive
                if tree_id == ID_GDRIVE_DIR_SELECT:
                    # special handling for dir select dialog: make sure we are fully synced first
                    self._master_gdrive.load_and_sync_master_tree()
                else:
                    self._master_gdrive.load_subtree(spid, tree_id=tree_id)
            else:
                raise RuntimeError(f'Unrecognized tree type: {spid.tree_type}')

        if load_request.send_signals:
            # Notify UI that we are done. For gRPC backend, this will be received by the server stub and relayed to the client:
            dispatcher.send(signal=Signal.LOAD_SUBTREE_DONE, sender=tree_id)

    def register_change_tree(self, change_display_tree: ChangeTree, src_tree_id: str):
        self._active_tree_manager.register_change_tree(change_display_tree, src_tree_id)

    def request_display_tree_ui_state(self, request: DisplayTreeRequest) -> Optional[DisplayTreeUiState]:
        return self._active_tree_manager.request_display_tree_ui_state(request)

    def get_active_display_tree_meta(self, tree_id) -> ActiveDisplayTreeMeta:
        return self._active_tree_manager.get_active_display_tree_meta(tree_id)

    def get_filter_criteria(self, tree_id: str) -> FilterCriteria:
        return self._active_tree_manager.get_filter_criteria(tree_id)

    def update_filter_criteria(self, tree_id: str, filter_criteria: FilterCriteria):
        self._active_tree_manager.update_filter_criteria(tree_id, filter_criteria)

    def is_manual_load_required(self, spid: SinglePathNodeIdentifier, is_startup: bool) -> bool:
        cache_info = self.get_cache_info_for_subtree(spid)
        if cache_info:
            if cache_info.is_loaded:
                # Already loaded!
                return False

        if is_startup and self.load_all_caches_on_startup or self.load_caches_for_displayed_trees_at_startup:
            # We are still starting up but will auto-load this tree soon:
            return False

        if not is_startup and self.reload_tree_on_root_path_update:
            return False
        return True

    # PersistedCacheInfo stuff
    # ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼

    def get_cache_info_for_subtree(self, subtree_root: SinglePathNodeIdentifier, create_if_not_found: bool = False) \
            -> Optional[PersistedCacheInfo]:
        """Finds the cache which contains the given subtree, if there is one.
        If create_if_not_found==True, then it will create & register a new cache and return that.
        If create_if_not_found==False, then it will return None if no associated cache could be found."""

        self.wait_for_load_registry_done()

        if subtree_root.tree_type == TREE_TYPE_GDRIVE:
            # there is only 1 GDrive cache:
            cache_info = self._caches_by_type.get_single(TREE_TYPE_GDRIVE, ROOT_PATH)
        elif subtree_root.tree_type == TREE_TYPE_LOCAL_DISK:
            cache_info = self.find_existing_cache_info_for_local_subtree(subtree_root.get_single_path())
        else:
            raise RuntimeError(f'Unrecognized tree type: {subtree_root.tree_type}')

        if not cache_info and create_if_not_found:
            cache_info = self._create_new_cache_info(subtree_root)

        return cache_info

    def find_existing_cache_info_for_local_subtree(self, full_path: str) -> Optional[PersistedCacheInfo]:
        # Wait for registry to finish loading before attempting to read dict. Shouldn't take long.
        self.wait_for_load_registry_done()

        existing_caches: List[PersistedCacheInfo] = list(self._caches_by_type.get_second_dict(TREE_TYPE_LOCAL_DISK).values())

        for existing_cache in existing_caches:
            # Is existing_cache an ancestor of target tree?
            if full_path.startswith(existing_cache.subtree_root.get_path_list()[0]):
                return existing_cache
        # Nothing in the cache contains subtree
        return None

    def get_or_create_cache_info_for_gdrive(self) -> PersistedCacheInfo:
        master_tree_root = NodeIdentifierFactory.get_root_constant_gdrive_spid()
        return self.get_cache_info_for_subtree(master_tree_root, create_if_not_found=True)

    def get_or_create_cache_info_entry(self, subtree_root: SinglePathNodeIdentifier) -> PersistedCacheInfo:
        """DEPRECATED: use get_cache_info_for_subtree() instead since it includes support for subtrees"""
        return self.get_cache_info_for_subtree(subtree_root, create_if_not_found=True)

    def ensure_loaded(self, node_list: List[Node]):
        """Ensures that all the necessary caches are loaded for all of the given nodes"""
        needed_cache_dict: Dict[str, PersistedCacheInfo] = {}

        needs_gdrive: bool = False
        for node in node_list:
            if node.get_tree_type() == TREE_TYPE_GDRIVE:
                needs_gdrive = True
            else:
                cache: Optional[PersistedCacheInfo] = self.find_existing_cache_info_for_local_subtree(node.get_single_path())
                if cache:
                    needed_cache_dict[cache.subtree_root.get_single_path()] = cache
                else:
                    raise RuntimeError(f'Could not find a cache file for planning node: {node}')

        if needs_gdrive:
            self._master_gdrive.load_and_sync_master_tree()

        for cache in needed_cache_dict.values():
            if not cache.is_loaded:
                if not os.path.exists(cache.subtree_root.get_single_path()):
                    raise RuntimeError(f'Could not load planning node(s): cache subtree does not exist: {cache.subtree_root.get_single_path()}')
                else:
                    assert isinstance(cache.subtree_root, LocalNodeIdentifier)
                    self._master_local.load_subtree(cache.subtree_root, ID_GLOBAL_CACHE)

    def _create_new_cache_info(self, subtree_root: SinglePathNodeIdentifier) -> PersistedCacheInfo:
        if subtree_root.tree_type == TREE_TYPE_LOCAL_DISK:
            unique_path = subtree_root.get_single_path().replace('/', '_')
            file_name = f'LO_{unique_path}.{INDEX_FILE_SUFFIX}'
            parent_path = self.derive_parent_path(subtree_root.get_single_path())
        elif subtree_root.tree_type == TREE_TYPE_GDRIVE:
            file_name = GDRIVE_INDEX_FILE_NAME
        else:
            raise RuntimeError(f'Unrecognized tree type: {subtree_root.tree_type}')

        cache_location = os.path.join(self.cache_dir_path, file_name)
        sync_ts = time_util.now_sec()
        db_entry = CacheInfoEntry(cache_location=cache_location,
                                  subtree_root=subtree_root, sync_ts=sync_ts,
                                  is_complete=True)

        with CacheRegistry(self.main_registry_path, self.backend.node_identifier_factory) as db:
            logger.info(f'Inserting new cache info into registry: {subtree_root}')
            db.insert_cache_info(db_entry, append=True, overwrite=False)

        cache_info = PersistedCacheInfo(db_entry)

        # Save reference in memory
        self._caches_by_type.put_item(cache_info)

        return cache_info

    # Main cache CRUD
    # ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼

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

    def delete_subtree(self, node_uid_list: List[UID]):
        logger.debug(f'Setting up recursive delete operations for {len(node_uid_list)} nodes')

        # don't worry about overlapping trees; the cacheman will sort everything out
        batch_uid = self.backend.uid_generator.next_uid()
        op_list = []
        for uid_to_delete in node_uid_list:
            node_to_delete = self.get_node_for_uid(uid_to_delete)
            if not node_to_delete:
                logger.error(f'delete_subtree(): could not find node with UID {uid_to_delete}; skipping')
                continue

            if node_to_delete.is_dir():
                # Expand dir nodes. ChangeManager will not remove non-empty dirs
                expanded_node_list = self._get_subtree_for_node(node_to_delete)
                for node in expanded_node_list:
                    # somewhere in this returned list is the subtree root. Need to check so we don't include a duplicate:
                    if node.uid != node_to_delete.uid:
                        op_list.append(UserOp(op_uid=self.backend.uid_generator.next_uid(), batch_uid=batch_uid,
                                              op_type=UserOpType.RM, src_node=node))

            op_list.append(UserOp(op_uid=self.backend.uid_generator.next_uid(), batch_uid=batch_uid,
                                  op_type=UserOpType.RM, src_node=node_to_delete))

        self.enqueue_op_list(op_list)

    def _get_subtree_for_node(self, subtree_root: Node) -> List[Node]:
        subtree_files, subtree_dirs = self.get_all_files_and_dirs_for_subtree(subtree_root.node_identifier)
        return subtree_files + subtree_dirs

    def remove_subtree(self, node: Node, to_trash: bool):
        """TODO: when is this called? Only for tests?"""
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

    # Getters: Nodes and node identifiers
    # ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼

    def get_uid_for_local_path(self, full_path: str, uid_suggestion: Optional[UID] = None, override_load_check: bool = False) -> UID:
        """Deterministically gets or creates a UID corresponding to the given path string"""
        assert full_path and isinstance(full_path, str)
        return self._master_local.get_uid_for_path(full_path, uid_suggestion, override_load_check)

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

    def get_uid_for_change_tree_node(self, tree_type: int, single_path: Optional[str], op: Optional[UserOpType]) -> UID:
        return self.change_tree_uid_mapper.get_uid_for(tree_type, single_path, op)

    def get_uid_list_for_goog_id_list(self, goog_id_list: List[str]) -> List[UID]:
        return self._master_gdrive.get_uid_list_for_goog_id_list(goog_id_list)

    def get_uid_for_goog_id(self, goog_id: str, uid_suggestion: Optional[UID] = None) -> UID:
        """Deterministically gets or creates a UID corresponding to the given goog_id"""
        if not goog_id:
            raise RuntimeError('get_uid_for_goog_id(): no goog_id specified!')
        return self._master_gdrive.get_uid_for_goog_id(goog_id, uid_suggestion)

    def get_node_for_local_path(self, full_path: str) -> Optional[Node]:
        if not full_path:
            raise RuntimeError('get_node_for_local_path(): full_path not specified!')
        uid = self.get_uid_for_local_path(full_path)
        return self._master_local.get_node_for_uid(uid)

    def get_goog_node_for_name_and_parent_uid(self, name: str, parent_uid: UID) -> Optional[GDriveNode]:
        """Returns the first GDrive node found with the given name and parent.
        This roughly matches the logic used to search for an node in Google Drive when we are unsure about its goog_id."""
        return self._master_gdrive.get_node_for_name_and_parent_uid(name, parent_uid)

    def get_node_for_goog_id(self, goog_id: str) -> GDriveNode:
        return self._master_gdrive.get_node_for_domain_id(goog_id)

    def get_node_for_node_identifier(self, node_identifer: NodeIdentifier) -> Optional[Node]:
        return self.get_node_for_uid(node_identifer.uid, node_identifer.tree_type)

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

    def get_node_list_for_path_list(self, path_list: List[str], tree_type: int) -> List[Node]:
        """Because of GDrive, we cannot guarantee that a single path will have only one node, or a single node will have only one path."""
        path_list = ensure_list(path_list)
        if tree_type == TREE_TYPE_GDRIVE:
            return self._master_gdrive.get_node_list_for_path_list(path_list)
        elif tree_type == TREE_TYPE_LOCAL_DISK:
            return self._master_local.get_node_list_for_path_list(path_list)
        else:
            raise RuntimeError(f'Unknown tree type: {tree_type}')

    def get_gdrive_identifier_list_for_full_path_list(self, path_list: List[str], error_if_not_found: bool = False) -> List[NodeIdentifier]:
        return self._master_gdrive.get_identifier_list_for_full_path_list(path_list, error_if_not_found)

    def get_children(self, node: Node, tree_id: str):
        if SUPER_DEBUG:
            logger.debug(f'Entered get_children() for tree_id={tree_id}, node = {node}')

        display_tree = self.get_active_display_tree_meta(tree_id)
        if not display_tree:
            raise RuntimeError(f'DisplayTree not registered: {tree_id}')
        # Is change tree? Follow separate code path:
        if display_tree.state.tree_display_mode == TreeDisplayMode.CHANGES_ONE_TREE_PER_CATEGORY:
            return display_tree.change_tree.get_children(node)
        else:
            logger.debug(f'Found active display tree for {tree_id} with TreeDisplayMode: {display_tree.state.tree_display_mode.name}')
        filter_state = display_tree.filter_state

        tree_type: int = node.node_identifier.tree_type
        if tree_type == TREE_TYPE_GDRIVE:
            child_list = self._master_gdrive.get_children(node, filter_state)
        elif tree_type == TREE_TYPE_LOCAL_DISK:
            child_list = self._master_local.get_children(node, filter_state)
        else:
            raise RuntimeError(f'Unknown tree type: {tree_type} for {node.node_identifier}')

        for child in child_list:
            self._update_node_icon(child)

        if SUPER_DEBUG:
            logger.debug(f'[{tree_id}] Returning {len(child_list)} children for node: {node}')
        return child_list

    def _update_node_icon(self, node: Node):
        icon: Optional[IconId] = self._op_ledger.get_icon_for_node(node.uid)
        node.set_icon(icon)

    @staticmethod
    def derive_parent_path(child_path) -> Optional[str]:
        if child_path == '/':
            return None
        return str(pathlib.Path(child_path).parent)

    def get_parent_list_for_node(self, node: Node) -> List[Node]:
        if node.node_identifier.tree_type == TREE_TYPE_GDRIVE:
            return self._master_gdrive.get_parent_list_for_node(node)
        elif node.node_identifier.tree_type == TREE_TYPE_LOCAL_DISK:
            return self._master_local.get_parent_list_for_node(node)
        else:
            raise RuntimeError(f'Unknown tree type: {node.node_identifier.tree_type} for {node}')

    def _find_parent_matching_path(self, child_node: Node, parent_path: str) -> Optional[Node]:
        """Note: this can return multiple results if two parents with the same name and path contain the same child
        (cough, cough, GDrive, cough). Although possible, I cannot think of a valid reason for that scenario."""
        logger.debug(f'Looking for parent with path "{parent_path}" (child: {child_node.node_identifier})')
        filtered_list: List[Node] = []
        if parent_path == ROOT_PATH:
            return None

        parent_node_list: List[Node] = self.get_parent_list_for_node(child_node)
        for node in parent_node_list:
            if node.node_identifier.has_path(parent_path):
                filtered_list.append(node)

        # FIXME: audit tree to prevent this case
        # FIXME: submit to adjudicator
        if not filtered_list:
            return None
        if len(filtered_list) > 1:
            raise RuntimeError(f'Expected exactly 1 but found {len(filtered_list)} parents which matched parent path "{parent_path}"')
        logger.debug(f'Matched path "{parent_path}" with node {filtered_list[0]}')
        return filtered_list[0]

    def get_parent_sn_for_sn(self, sn: SPIDNodePair) -> Optional[SPIDNodePair]:
        """Given a single SPIDNodePair, we should be able to guarantee that we get no more than 1 SPIDNodePair as its parent.
        Having more than one path indicates that not just the node, but also any of its ancestors has multiple parents."""
        path_list = sn.node.get_path_list()
        parent_path: str = self.derive_parent_path(sn.spid.get_single_path())
        if len(path_list) == 1 and path_list[0] == sn.spid.get_single_path():
            # only one parent -> easy
            if sn.spid.tree_type == TREE_TYPE_GDRIVE:
                parent_list: List[Node] = self._master_gdrive.get_parent_list_for_node(sn.node)
                if parent_list:
                    if len(parent_list) > 1:
                        raise RuntimeError(f'Expected exactly 1 but found {len(parent_list)} parents for node: "{sn.node}"')
                    parent_node = parent_list[0]
                else:
                    return None
            elif sn.spid.tree_type == TREE_TYPE_LOCAL_DISK:
                parent_node = self._master_local.get_single_parent_for_node(sn.node)
            else:
                raise RuntimeError(f'Unknown tree type: {sn.spid.tree_type} for {sn.node}')
        else:
            parent_node = self._find_parent_matching_path(sn.node, parent_path)
        if not parent_node:
            return None
        parent_spid = SinglePathNodeIdentifier(parent_node.uid, parent_path, parent_node.get_tree_type())
        return SPIDNodePair(parent_spid, parent_node)

    def get_ancestor_list_for_spid(self, spid: SinglePathNodeIdentifier, stop_at_path: Optional[str] = None) -> Deque[Node]:

        ancestor_deque: Deque[Node] = deque()
        ancestor: Node = self.get_node_for_uid(spid.uid)
        if not ancestor:
            logger.warning(f'get_ancestor_list_for_spid(): Node not found: {spid}')
            return ancestor_deque

        parent_path: str = spid.get_single_path()  # not actually parent's path until it enters loop

        while True:
            parent_path = self.derive_parent_path(parent_path)
            if parent_path == stop_at_path:
                return ancestor_deque

            ancestor: Node = self._find_parent_matching_path(ancestor, parent_path)
            if ancestor:
                ancestor_deque.appendleft(ancestor)
            else:
                return ancestor_deque

    def get_all_files_and_dirs_for_subtree(self, subtree_root: NodeIdentifier) -> Tuple[List[Node], List[Node]]:
        if subtree_root.tree_type == TREE_TYPE_GDRIVE:
            assert isinstance(subtree_root, GDriveIdentifier)
            return self._master_gdrive.get_all_gdrive_files_and_folders_for_subtree(subtree_root)
        elif subtree_root.tree_type == TREE_TYPE_LOCAL_DISK:
            assert isinstance(subtree_root, LocalNodeIdentifier)
            return self._master_local.get_all_files_and_dirs_for_subtree(subtree_root)
        else:
            raise RuntimeError(f'Unknown tree type: {subtree_root.tree_type} for {subtree_root}')

    # GDrive-specific
    # ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼

    def get_gdrive_user_for_permission_id(self, permission_id: str):
        return self._master_gdrive.get_gdrive_user_for_permission_id(permission_id)

    def create_gdrive_user(self, user):
        return self._master_gdrive.create_gdrive_user(user)

    def get_or_create_gdrive_mime_type(self, mime_type_string: str):
        return self._master_gdrive.get_or_create_gdrive_mime_type(mime_type_string)

    def delete_all_gdrive_data(self):
        return self._master_gdrive.delete_all_gdrive_data()

    def apply_gdrive_changes(self, gdrive_change_list):
        self._master_gdrive.apply_gdrive_changes(gdrive_change_list)

    def get_gdrive_client(self):
        return self._master_gdrive.gdrive_client

    # Drag & drop
    # ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼

    def drop_dragged_nodes(self, src_tree_id: str, src_sn_list: List[SPIDNodePair], is_into: bool, dst_tree_id: str, dst_sn: SPIDNodePair):
        logger.info(f'Got drop of {len(src_sn_list)} nodes from "{src_tree_id}" -> "{dst_tree_id}" is_into={is_into}')

        if not is_into or (dst_sn and not dst_sn.node.is_dir()):
            # cannot drop into a file; just use parent in this case
            dst_sn = self.get_parent_sn_for_sn(dst_sn)

        if not dst_sn:
            logger.error(f'[{dst_tree_id}] Cancelling drop: no parent node for dropped location!')
        elif dst_tree_id == src_tree_id and self._is_dropping_on_itself(dst_sn, src_sn_list, dst_tree_id):
            logger.debug(f'[{dst_tree_id}] Cancelling drop: nodes were dropped in same location in the tree')
        else:
            logger.debug(f'[{dst_tree_id}] Dropping into dest: {dst_sn.spid}')
            # "Left tree" here is the source tree, and "right tree" is the dst tree:
            src_tree: ActiveDisplayTreeMeta = self.get_active_display_tree_meta(src_tree_id)
            dst_tree: ActiveDisplayTreeMeta = self.get_active_display_tree_meta(dst_tree_id)
            if not src_tree:
                logger.error(f'Aborting drop: could not find src tree: "{src_tree_id}"')
                return
            if not dst_tree:
                logger.error(f'Aborting drop: could not find dst tree: "{dst_tree_id}"')
                return

            change_maker = ChangeMaker(backend=self.backend, left_tree_root_sn=src_tree.root_sn, right_tree_root_sn=dst_tree.root_sn)
            # So far we only support COPY.
            change_maker.copy_nodes_left_to_right(src_sn_list, dst_sn, UserOpType.CP)
            # This should fire listeners which ultimately populate the tree:
            op_list: Iterable[UserOp] = change_maker.right_side.change_tree.get_ops()
            self.enqueue_op_list(op_list)

    @staticmethod
    def _is_dropping_on_itself(dst_sn: SPIDNodePair, sn_list: List[SPIDNodePair], dst_tree_id: str):
        for sn in sn_list:
            logger.debug(f'[{dst_tree_id}] DestNode="{dst_sn.spid}", DroppedNode="{sn.node}"')
            if dst_sn.node.is_parent_of(sn.node):
                return True
        return False

    # Various public methods
    # ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼

    def show_tree(self, subtree_root: LocalNodeIdentifier) -> str:
        if subtree_root.tree_type == TREE_TYPE_LOCAL_DISK:
            return self._master_local.show_tree(subtree_root)
        elif subtree_root.tree_type == TREE_TYPE_GDRIVE:
            return self._master_gdrive.show_tree(subtree_root)
        else:
            assert False

    def download_file_from_gdrive(self, node_uid: UID, requestor_id: str):
        self._master_gdrive.download_file_from_gdrive(node_uid, requestor_id)

    def execute_gdrive_load_op(self, op: GDriveDiskLoadOp):
        self._master_gdrive.execute_load_op(op)

    def _refresh_subtree(self, node_identifier: NodeIdentifier, tree_id: str):
        """Called asynchronously via task executor"""
        logger.debug(f'[{tree_id}] Refreshing subtree: {node_identifier}')
        if node_identifier.tree_type == TREE_TYPE_LOCAL_DISK:
            self._master_local.refresh_subtree(node_identifier, tree_id)
        elif node_identifier.tree_type == TREE_TYPE_GDRIVE:
            self._master_gdrive.refresh_subtree(node_identifier, tree_id)
        else:
            assert False

    def _refresh_stats(self, subtree_root_uid: UID, tree_id: str):
        """Called async via task exec (cacheman.enqueue_refresh_subtree_stats_task()) """

        tree_meta = self._active_tree_manager.get_active_display_tree_meta(tree_id)
        if tree_meta.change_tree:
            logger.debug(f'Tree "{tree_id}" is a ChangeTree: it will provide the stats')
            tree_meta.change_tree.refresh_stats()
            subtree_root_node: Node = tree_meta.change_tree.get_root_node()
        else:
            # get up-to-date object:
            subtree_root_node: Node = self.get_node_for_uid(subtree_root_uid)

            logger.debug(f'[{tree_id}] Refreshing stats for subtree: {subtree_root_node}')

            if subtree_root_node.get_tree_type() == TREE_TYPE_LOCAL_DISK:
                self._master_local.refresh_subtree_stats(subtree_root_node, tree_id)
            elif subtree_root_node.get_tree_type() == TREE_TYPE_GDRIVE:
                self._master_gdrive.refresh_subtree_stats(subtree_root_node, tree_id)
            else:
                assert False

        dispatcher.send(signal=Signal.REFRESH_SUBTREE_STATS_DONE, sender=tree_id)
        summary_msg: str = self._get_tree_summary(tree_id, subtree_root_node)
        dispatcher.send(signal=Signal.SET_STATUS, sender=tree_id, status_msg=summary_msg)

    def _get_tree_summary(self, tree_id: str, root_node: Node):
        tree_meta = self._active_tree_manager.get_active_display_tree_meta(tree_id)
        if tree_meta.change_tree:
            logger.debug(f'Tree "{tree_id}" is a ChangeTree: it will provide the summary')
            return tree_meta.change_tree.get_summary()

        if not root_node:
            logger.debug(f'No summary (tree does not exist)')
            return 'Tree does not exist'
        elif not root_node.is_stats_loaded():
            logger.debug(f'No summary (stats not loaded): {root_node.node_identifier}')
            return 'Loading stats...'
        else:
            if root_node.get_tree_type() == TREE_TYPE_GDRIVE:
                if root_node.uid == GDRIVE_ROOT_UID:
                    logger.debug('Generating summary for whole GDrive master tree')
                    return self._master_gdrive.get_whole_tree_summary()
                else:
                    logger.debug(f'Generating summary for GDrive tree: {root_node.node_identifier}')
                    assert isinstance(root_node, HasDirectoryStats)
                    size_hf = util.format.humanfriendlier_size(root_node.get_size_bytes())
                    trashed_size_hf = util.format.humanfriendlier_size(root_node.trashed_bytes)
                    return f'{size_hf} total in {root_node.file_count:n} nodes (including {trashed_size_hf} in ' \
                           f'{root_node.trashed_file_count:n} trashed)'
            else:
                assert root_node.get_tree_type() == TREE_TYPE_LOCAL_DISK
                logger.debug(f'Generating summary for LocalDisk tree: {root_node.node_identifier}')
                size_hf = util.format.humanfriendlier_size(root_node.get_size_bytes())
                return f'{size_hf} total in {root_node.file_count:n} files and {root_node.dir_count:n} dirs'

    def get_last_pending_op_for_node(self, node_uid: UID) -> Optional[UserOp]:
        return self._op_ledger.get_last_pending_op_for_node(node_uid)

    def enqueue_op_list(self, op_list: Iterable[UserOp]):
        """Attempt to add the given Ops to the execution tree. No need to worry whether some changes overlap or are redundant;
         the OpLedger will sort that out - although it will raise an error if it finds incompatible changes such as adding to a tree
         that is scheduled for deletion."""
        self._op_ledger.append_new_pending_op_batch(op_list)

    def get_next_command(self) -> Optional[Command]:
        # blocks !
        self.wait_for_startup_done()
        # also blocks !
        return self._op_ledger.get_next_command()

    def sync_and_get_gdrive_master_tree(self, tree_id: str):
        """Will load from disk and sync latest changes from GDrive server before returning."""
        self._master_gdrive.load_and_sync_master_tree()

    def build_local_file_node(self, full_path: str, staging_path=None, must_scan_signature=False) -> Optional[LocalFileNode]:
        return self._master_local.build_local_file_node(full_path, staging_path, must_scan_signature)

    def build_local_dir_node(self, full_path: str, is_live: bool = True) -> LocalDirNode:
        return self._master_local.build_local_dir_node(full_path, is_live)

    def read_single_node_from_disk_for_local_path(self, full_path: str) -> Node:
        """Consults disk cache directly, skipping memory cache"""
        if not file_util.is_normalized(full_path):
            full_path = file_util.normalize_path(full_path)
            logger.debug(f'Normalized path: {full_path}')

        assert self._master_local
        node = self._master_local.load_single_node_for_path(full_path)
        return node

    def read_single_node_from_disk_for_uid(self, uid: UID, tree_type: int) -> Optional[Node]:
        """Consults disk cache directly, skipping memory cache"""
        if tree_type == TREE_TYPE_LOCAL_DISK:
            raise InvalidOperationError(f'read_single_node_from_disk_for_uid(): not yet supported: {tree_type}')
        elif tree_type == TREE_TYPE_GDRIVE:
            return self._master_gdrive.read_single_node_from_disk_for_uid(uid)
        else:
            raise RuntimeError(f'Unknown tree type: {tree_type} for UID {uid}')
