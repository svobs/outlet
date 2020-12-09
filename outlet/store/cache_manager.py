import errno
import logging
import os
import pathlib
import threading
import time
from collections import deque
from typing import Deque, Dict, Iterable, List, Optional, Tuple

from pydispatch import dispatcher

import util.format
from command.cmd_interface import Command
from constants import CACHE_LOAD_TIMEOUT_SEC, GDRIVE_INDEX_FILE_NAME, GDRIVE_ROOT_UID, IconId, INDEX_FILE_SUFFIX, MAIN_REGISTRY_FILE_NAME, NULL_UID, \
    OPS_FILE_NAME, ROOT_PATH, \
    SUPER_DEBUG, SUPER_ROOT_UID, TREE_TYPE_GDRIVE, \
    TREE_TYPE_LOCAL_DISK
from diff.change_maker import ChangeMaker
from error import CacheNotLoadedError, GDriveItemNotFoundError, InvalidOperationError
from model.cache_info import CacheInfoEntry, PersistedCacheInfo
from model.display_tree.display_tree import DisplayTreeUiState
from model.gdrive_whole_tree import GDriveWholeTree
from model.node.gdrive_node import GDriveNode
from model.node.local_disk_node import LocalDirNode, LocalFileNode, LocalNode
from model.node.node import HasChildStats, HasParentList, Node, SPIDNodePair
from model.node_identifier import GDriveIdentifier, LocalNodeIdentifier, NodeIdentifier, SinglePathNodeIdentifier
from model.node_identifier_factory import NodeIdentifierFactory
from model.uid import UID
from model.user_op import UserOp, UserOpStatus, UserOpType
from store.gdrive.master_gdrive import GDriveMasterStore
from store.gdrive.master_gdrive_op_load import GDriveDiskLoadOp
from store.live_monitor import LiveMonitor
from store.local.master_local import LocalDiskMasterStore
from store.sqlite.cache_registry_db import CacheRegistry
from store.user_op.op_ledger import OpLedger
from ui.signal import ID_GDRIVE_DIR_SELECT, ID_GLOBAL_CACHE, Signal
from ui.tree.filter_criteria import FilterCriteria
from ui.tree.root_path_config import RootPathConfigPersister
from util import file_util
from util.ensure import ensure_list
from util.file_util import get_resource_path
from util.has_lifecycle import HasLifecycle
from util.qthread import QThread
from util.root_path_meta import RootPathMeta
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


class ActiveDisplayTreeMeta:
    """
    ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
    CLASS ActiveDisplayTreeMeta

    For internal use by CacheManager.
    ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼
    """

    def __init__(self, backend, tree_id: str, root_sn: SPIDNodePair, root_exists: bool = True, offending_path: Optional[str] = None):
        self.tree_id: str = tree_id
        self.root_sn: SPIDNodePair = root_sn
        self.root_exists: bool = root_exists
        self.offending_path: Optional[str] = offending_path
        self.root_path_config_persister: Optional[RootPathConfigPersister] = None


class CacheInfoByType(TwoLevelDict):
    """
    ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
    CLASS CacheInfoByType

    Holds PersistedCacheInfo objects
    ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼
    """

    def __init__(self):
        super().__init__(lambda x: x.subtree_root.tree_type, lambda x: x.subtree_root.get_path_list()[0], lambda x, y: True)


class LoadRequest:
    """
    ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
    CLASS LoadRequest

    Object encapsulating the information for a single load request on the LoadRequestThread
    ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼
    """

    def __init__(self, tree_id: str, send_signals: bool):
        self.tree_id = tree_id
        self.send_signals = send_signals


class LoadRequestThread(QThread):
    """
    ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
    CLASS LoadRequestThread

    Hasher thread which churns through signature queue and sends updates to cacheman
    ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼
    """

    def __init__(self, backend, cacheman):
        QThread.__init__(self, name='LoadRequestThread', initial_sleep_sec=0.0)
        self.backend = backend
        self.cacheman = cacheman

    def on_thread_start(self):
        # Wait for CacheMan to finish starting up so as not to deprive it of resources:
        logger.debug(f'[{self.name}] Waiting for CacheMan startup to complete...')
        self.cacheman.wait_for_startup_done()

    def process_single_item(self, load_request: LoadRequest):
        logger.debug(f'[{self.name}] Submitting load request for tree_id: {load_request.tree_id}, send_signals={load_request.send_signals}')
        self.backend.executor.submit_async_task(self.cacheman.load_data_for_display_tree, load_request)


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

        self.cache_dir_path = ensure_cache_dir_path(self.backend.config)
        self.main_registry_path = os.path.join(self.cache_dir_path, MAIN_REGISTRY_FILE_NAME)

        self.caches_by_type: CacheInfoByType = CacheInfoByType()

        self.enable_load_from_disk = backend.config.get(CFG_ENABLE_LOAD_FROM_DISK)
        self.enable_save_to_disk = backend.config.get('cache.enable_cache_save')
        self.load_all_caches_on_startup = backend.config.get('cache.load_all_caches_on_startup')
        self.load_caches_for_displayed_trees_at_startup = backend.config.get('cache.load_caches_for_displayed_trees_on_startup')
        self.sync_from_local_disk_on_cache_load = backend.config.get('cache.sync_from_local_disk_on_cache_load')
        self.reload_tree_on_root_path_update = backend.config.get('cache.load_cache_when_tree_root_selected')
        self.cancel_all_pending_ops_on_startup = backend.config.get('cache.cancel_all_pending_ops_on_startup')
        self._is_live_capture_enabled = backend.config.get('cache.live_capture_enabled')

        if not self.load_all_caches_on_startup:
            logger.info('Configured not to fetch all caches on startup; will lazy load instead')

        self._load_request_thread = LoadRequestThread(backend=backend, cacheman=self)

        # Instantiate but do not start submodules yet, to avoid entangled dependencies:

        self._master_local: LocalDiskMasterStore = LocalDiskMasterStore(self.backend)
        """Sub-module of Cache Manager which manages local disk caches"""

        self._master_gdrive: GDriveMasterStore = GDriveMasterStore(self.backend)
        """Sub-module of Cache Manager which manages Google Drive caches"""

        op_db_path = os.path.join(self.cache_dir_path, OPS_FILE_NAME)
        self._op_ledger: OpLedger = OpLedger(self.backend, op_db_path)
        """Sub-module of Cache Manager which manages commands which have yet to execute"""

        self._live_monitor: LiveMonitor = LiveMonitor(self.backend)
        """Sub-module of Cache Manager which, for displayed trees, provides [close to] real-time notifications for changes
         which originated from outside this backend"""

        self._display_tree_dict: Dict[str, ActiveDisplayTreeMeta] = {}
        """Keeps track of which display trees are currently being used in the UI"""

        # Create Event objects to optionally wait for lifecycle events
        self._load_registry_done: threading.Event = threading.Event()

        self._startup_done: threading.Event = threading.Event()

        # (Optional) variables needed for producer/consumer behavior if Load All Caches needed
        self.load_all_caches_done: threading.Event = threading.Event()
        self._load_all_caches_in_process: bool = False

        self.connect_dispatch_listener(signal=Signal.START_CACHEMAN, receiver=self._on_start_cacheman_requested)
        self.connect_dispatch_listener(signal=Signal.COMMAND_COMPLETE, receiver=self._on_command_completed)
        self.connect_dispatch_listener(signal=Signal.DEREGISTER_DISPLAY_TREE, receiver=self._deregister_display_tree)

        self.connect_dispatch_listener(signal=Signal.GDRIVE_RELOADED, receiver=self._on_gdrive_whole_tree_reloaded)

    def shutdown(self):
        logger.debug('CacheManager.shutdown() entered')
        HasLifecycle.shutdown(self)

        try:
            if self._op_ledger:
                self._op_ledger.shutdown()
                self._op_ledger = None
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

        try:
            if self._master_gdrive:
                self._master_gdrive.shutdown()
                self._master_gdrive = None
        except NameError:
            pass

        try:
            if self._load_request_thread:
                self._load_request_thread.shutdown()
                self._load_request_thread = None
        except NameError:
            pass

    # Startup loading/maintenance
    # ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼

    def _on_start_cacheman_requested(self, sender):
        if self._startup_done.is_set():
            logger.info(f'Caches already loaded. Ignoring signal from {sender}.')
            return

        logger.debug(f'CacheManager.start() initiated by {sender}')
        self.start()

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
            self._master_local.start()
            self._master_gdrive.start()
            self._op_ledger.start()
            self._live_monitor.start()

            self._load_request_thread.start()

            # Now load all caches (if configured):
            if self.enable_load_from_disk and self.load_all_caches_on_startup:
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
            existing = self.caches_by_type.get_single(info.subtree_root.tree_type, info.subtree_root.get_path_list()[0])
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
            self.caches_by_type.put_item(info)

        # Write back to cache if we need to clean things up:
        if skipped_count > 0:
            caches = self.caches_by_type.get_all()
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
                self.caches_by_type.put_item(cache)

        for cache_num, existing_disk_cache in enumerate(existing_caches):
            try:
                self.caches_by_type.put_item(existing_disk_cache)
                logger.info(f'Init cache {(cache_num + 1)}/{len(existing_caches)}: id={existing_disk_cache.subtree_root}')
                self._init_existing_cache(existing_disk_cache)
            except RuntimeError:
                logger.exception(f'Failed to load cache: {existing_disk_cache.cache_location}')

        logger.info(f'{stopwatch} Load All Caches complete')
        self._load_all_caches_in_process = False
        self.load_all_caches_done.set()

    def _overwrite_all_caches_in_registry(self, cache_info_list: List[CacheInfoEntry]):
        logger.info(f'Overwriting all cache entries in persisted registry with {len(cache_info_list)} entries')
        with CacheRegistry(self.main_registry_path, self.backend.node_identifier_factory) as cache_registry_db:
            cache_registry_db.insert_cache_info(cache_info_list, append=False, overwrite=True)

    def _get_cache_info_list_from_registry(self) -> List[CacheInfoEntry]:
        with CacheRegistry(self.main_registry_path, self.backend.node_identifier_factory) as cache_registry_db:
            if cache_registry_db.has_cache_info():
                exisiting_caches = cache_registry_db.get_cache_info()
                logger.debug(f'Found {len(exisiting_caches)} caches listed in registry')
            else:
                logger.debug('Registry has no caches listed')
                exisiting_caches = []

        for cache_info in exisiting_caches:
            if cache_info.subtree_root.tree_type == TREE_TYPE_LOCAL_DISK:
                # Make UIDMapper aware of these new UID<->path mappings:
                cached_uid = self.get_uid_for_local_path(cache_info.subtree_root.get_single_path(), uid_suggestion=cache_info.subtree_root.uid,
                                                         override_load_check=True)
                if cached_uid != cache_info.subtree_root.uid:
                    logger.error(f'UID from registry ({cache_info.subtree_root.uid}) does not match cached UID ({cached_uid})! Will use cached UID.')
                    cache_info.subtree_root.uid = cached_uid

                # TODO: test what will happen to parent of '/'
                parent_path = self._derive_parent_path(cache_info.subtree_root.get_single_path())
                cached_uid = self.get_uid_for_local_path(parent_path, uid_suggestion=cache_info.subtree_root_parent_uid,
                                                         override_load_check=True)
                if cached_uid != cache_info.subtree_root_parent_uid:
                    logger.error(f'Parent UID from registry ({cache_info}) does not match cached parent UID ({cached_uid})'
                                 f' of parent! Will use cached UID.')
                    cache_info.subtree_root_parent_uid = cached_uid
        return exisiting_caches

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
                self._master_local.get_display_tree(existing_disk_cache.subtree_root, ID_GLOBAL_CACHE)
        elif cache_type == TREE_TYPE_GDRIVE:
            assert existing_disk_cache.subtree_root == NodeIdentifierFactory.get_gdrive_root_constant_identifier(), \
                f'Expected GDrive root ({NodeIdentifierFactory.get_gdrive_root_constant_identifier()}) but found: {existing_disk_cache.subtree_root}'
            self._master_gdrive.get_synced_master_tree()

    # Action listener callbacks
    # ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼

    def _on_command_completed(self, sender, command: Command):
        """Updates the in-memory cache, on-disk cache, and UI with the nodes from the given UserOpResult"""
        logger.debug(f'Received signal: "{Signal.COMMAND_COMPLETE}"')
        result = command.op.result

        # TODO: refactor so that we can attempt to create (close to) an atomic operation which combines GDrive and Local functionality

        if result.nodes_to_upsert:
            logger.debug(f'Cmd resulted in {len(result.nodes_to_upsert)} nodes to upsert')
            for upsert_node in result.nodes_to_upsert:
                self.upsert_single_node(upsert_node)

        if result.nodes_to_delete:
            # TODO: to_trash?

            logger.debug(f'Cmd resulted in {len(result.nodes_to_delete)} nodes to delete')
            for deleted_node in result.nodes_to_delete:
                self.remove_node(deleted_node, to_trash=False)

    def _deregister_display_tree(self, sender: str):
        logger.debug(f'[{sender}] Received signal: "{Signal.DEREGISTER_DISPLAY_TREE}"')
        display_tree = self._display_tree_dict.pop(sender, None)
        if display_tree:
            logger.debug(f'[{sender}] Display tree de-registered in backend')
        else:
            logger.debug(f'[{sender}] Could not deregister display tree in backend: it was not found')

        # Also stop live capture, if any
        if self._is_live_capture_enabled and self._live_monitor:
            self._live_monitor.stop_capture(sender)

    def enqueue_refresh_subtree_task(self, node_identifier: NodeIdentifier, tree_id: str):
        logger.info(f'Enqueuing task to refresh subtree at {node_identifier}')
        self.backend.executor.submit_async_task(self._refresh_subtree, node_identifier, tree_id)

    def enqueue_refresh_subtree_stats_task(self, root_uid: UID, tree_id: str):
        logger.info(f'[{tree_id}] Enqueuing task to refresh stats')
        self.backend.executor.submit_async_task(self._refresh_stats, root_uid, tree_id)

    def _on_gdrive_whole_tree_reloaded(self, sender: str):
        # If GDrive was reloaded, our previous selection was almost certainly invalid. Just reset all open GDrive trees to GDrive root.
        logger.info(f'Received signal: "{Signal.GDRIVE_RELOADED}"')

        tree_id_list: List[str] = []
        for tree_meta in self._display_tree_dict.values():
            if tree_meta.root_sn.spid.tree_type == TREE_TYPE_GDRIVE:
                tree_id_list.append(tree_meta.tree_id)

        gdrive_root_spid = NodeIdentifierFactory.get_gdrive_root_constant_single_path_identifier()
        for tree_id in tree_id_list:
            logger.info(f'[{tree_id}] Resetting path to GDrive root')
            self.request_display_tree_ui_state(tree_id, spid=gdrive_root_spid)

    # Subtree-level stuff
    # ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼

    def _is_manual_load_required(self, spid: SinglePathNodeIdentifier, is_startup: bool) -> bool:
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

    def _convert_display_tree_meta_to_ui_state(self, display_tree_meta, is_startup: bool) -> DisplayTreeUiState:

        state = DisplayTreeUiState(display_tree_meta.tree_id, display_tree_meta.root_sn, display_tree_meta.root_exists,
                                   display_tree_meta.offending_path)
        if self._is_manual_load_required(display_tree_meta.root_sn.spid, is_startup):
            state.needs_manual_load = True
        logger.debug(f'[{display_tree_meta.tree_id}] NeedsManualLoad = {state.needs_manual_load}')
        return state

    def request_display_tree_ui_state(self, tree_id: str, return_async: bool, user_path: str = None,
                                      spid: Optional[SinglePathNodeIdentifier] = None, is_startup: bool = False) -> Optional[DisplayTreeUiState]:
        logger.debug(f'[{tree_id}] Got request to load display tree (user_path={user_path}, spid: {spid}, is_startup={is_startup})')

        root_path_persister = None

        # Make RootPathMeta object. If neither SPID nor user_path supplied, read from config
        if user_path:
            root_path_meta: RootPathMeta = self._resolve_root_meta_from_path(user_path)
            spid = root_path_meta.root_spid
        elif spid:
            assert isinstance(spid, SinglePathNodeIdentifier), f'Expected SinglePathNodeIdentifier but got {type(spid)}'
            # params -> root_path_meta
            spid.normalize_paths()
            root_path_meta = RootPathMeta(spid, True)
        elif is_startup:
            # root_path_meta -> params
            root_path_persister = RootPathConfigPersister(backend=self.backend, tree_id=tree_id)
            root_path_meta = root_path_persister.read_from_config()
            spid = root_path_meta.root_spid
            if not spid:
                raise RuntimeError(f"Unable to read valid root from config for: '{tree_id}'")
        else:
            raise RuntimeError('Invalid args supplied to get_display_tree_ui_state()!')

        display_tree_meta: ActiveDisplayTreeMeta = self.get_active_display_tree_meta(tree_id)
        if display_tree_meta:
            if display_tree_meta.root_sn.spid == root_path_meta.root_spid:
                # Requested the existing tree and root? Just return that:
                logger.debug(f'Display tree already registered with given root; returning existing')
                return self._return_display_tree_ui_state(display_tree_meta, is_startup, return_async)

            if display_tree_meta.root_path_config_persister:
                # If we started from a persister, continue persisting:
                root_path_persister = display_tree_meta.root_path_config_persister

        # Try to retrieve the root node from the cache:
        if spid.tree_type == TREE_TYPE_LOCAL_DISK:
            node: Node = self._read_single_node_from_disk_for_local_path(spid.get_single_path())
            if node:
                if spid.uid != node.uid:
                    logger.warning(f'UID requested ({spid.uid}) does not match UID from cache ({node.uid}); will use value from cache')
                spid = node.node_identifier
                root_path_meta.root_spid = spid

            if os.path.exists(spid.get_single_path()):
                # Override in case something changed since the last shutdown
                root_path_meta.root_exists = True
            else:
                root_path_meta.root_exists = False
            root_path_meta.offending_path = None
        elif spid.tree_type == TREE_TYPE_GDRIVE:
            if spid.uid == GDRIVE_ROOT_UID:
                node: Node = GDriveWholeTree.get_super_root()
            else:
                node: Node = self.read_single_node_from_disk_for_uid(spid.uid, TREE_TYPE_GDRIVE)
            root_path_meta.root_exists = node is not None
            root_path_meta.offending_path = None
        else:
            raise RuntimeError(f'Unrecognized tree type: {spid.tree_type}')

        # Now that we have the root, we have all the info needed to assemble the ActiveDisplayTreeMeta from the RootPathMeta.
        root_sn = SPIDNodePair(spid, node)
        # easier to just create a whole new object rather than update old one:
        display_tree_meta: ActiveDisplayTreeMeta = ActiveDisplayTreeMeta(self.backend, tree_id, root_sn,
                                                                         root_path_meta.root_exists, root_path_meta.offending_path)
        if root_path_persister:
            # Write updates to config if applicable
            root_path_persister.write_to_config(root_path_meta)

            # Retain the persister for next time:
            display_tree_meta.root_path_config_persister = root_path_persister

        self._display_tree_dict[tree_id] = display_tree_meta

        # Update monitoring state
        if self._is_live_capture_enabled:
            if display_tree_meta.root_exists:
                self._live_monitor.start_or_update_capture(display_tree_meta.root_sn.spid, tree_id)
            else:
                self._live_monitor.stop_capture(tree_id)

        return self._return_display_tree_ui_state(display_tree_meta, is_startup, return_async)

    def _return_display_tree_ui_state(self, display_tree_meta, is_startup: bool, return_async: bool):
        state = self._convert_display_tree_meta_to_ui_state(display_tree_meta, is_startup)
        assert state.tree_id and state.root_sn and state.root_sn.spid, f'Bad DisplayTreeUiState: {state}'

        # Kick off data load task, if needed
        if not state.needs_manual_load:
            self.enqueue_load_subtree_task(state.tree_id, send_signals=False)
        else:
            logger.debug(f'[{state.tree_id}] Tree needs manual load; skipping subtree load task')

        if return_async:
            # notify clients asynchronously
            tree = state.to_display_tree(self.backend)
            logger.debug(f'[{state.tree_id}] Firing signal: {Signal.DISPLAY_TREE_CHANGED}')
            dispatcher.send(Signal.DISPLAY_TREE_CHANGED, sender=state.tree_id, tree=tree)
            return None
        else:
            logger.debug(f'[{state.tree_id}] Returning display tree synchronously because return_async=False: {state}')
            return state

    def _resolve_root_meta_from_path(self, full_path: str) -> RootPathMeta:
        """Resolves the given path into either a local file, a set of Google Drive matches, or generates a GDriveItemNotFoundError,
        and returns a tuple of both"""
        logger.debug(f'resolve_root_from_path() called with path="{full_path}"')
        try:
            full_path = file_util.normalize_path(full_path)
            node_identifier: NodeIdentifier = self.backend.node_identifier_factory.for_values(path_list=full_path)
            if node_identifier.tree_type == TREE_TYPE_GDRIVE:
                # Need to wait until all caches are loaded:
                self.wait_for_startup_done()
                # this will load the GDrive master tree if needed:
                identifier_list = self._master_gdrive.get_identifier_list_for_full_path_list(node_identifier.get_path_list(), error_if_not_found=True)
            else:  # LocalNode
                if not os.path.exists(full_path):
                    raise FileNotFoundError(errno.ENOENT, os.strerror(errno.ENOENT), full_path)
                uid = self.get_uid_for_local_path(full_path)
                identifier_list = [LocalNodeIdentifier(uid=uid, path_list=full_path)]

            assert len(identifier_list) > 0, f'Got no identifiers for path but no error was raised: {full_path}'
            logger.debug(f'resolve_root_from_path(): got identifier_list={identifier_list}"')
            if len(identifier_list) > 1:
                # Create the appropriate
                candidate_list = []
                for identifier in identifier_list:
                    if identifier.tree_type == TREE_TYPE_GDRIVE:
                        path_to_find = NodeIdentifierFactory.strip_gdrive(full_path)
                    else:
                        path_to_find = full_path

                    if path_to_find in identifier.get_path_list():
                        candidate_list.append(identifier)
                if len(candidate_list) != 1:
                    raise RuntimeError(f'Serious error: found multiple identifiers with same path ({full_path}): {candidate_list}')
                new_root_spid: SinglePathNodeIdentifier = candidate_list[0]
            else:
                new_root_spid = identifier_list[0]

            if len(new_root_spid.get_path_list()) > 0:
                # must have single path
                if new_root_spid.tree_type == TREE_TYPE_GDRIVE:
                    full_path = NodeIdentifierFactory.strip_gdrive(full_path)
                new_root_spid = SinglePathNodeIdentifier(uid=new_root_spid.uid, path_list=full_path, tree_type=new_root_spid.tree_type)

            root_path_meta = RootPathMeta(new_root_spid, root_exists=True)
        except GDriveItemNotFoundError as ginf:
            root_path_meta = RootPathMeta(ginf.node_identifier, root_exists=False)
            root_path_meta.offending_path = ginf.offending_path
        except FileNotFoundError as fnf:
            root = self.backend.node_identifier_factory.for_values(path_list=full_path, must_be_single_path=True)
            root_path_meta = RootPathMeta(root, root_exists=False)
        except CacheNotLoadedError as cnlf:
            root = self.backend.node_identifier_factory.for_values(path_list=full_path, uid=NULL_UID, must_be_single_path=True)
            root_path_meta = RootPathMeta(root, root_exists=False)

        logger.debug(f'resolve_root_from_path(): returning new_root={root_path_meta}"')
        return root_path_meta

    def enqueue_load_subtree_task(self, tree_id: str, send_signals: bool):
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

        if display_tree_meta.root_exists:
            spid = display_tree_meta.root_sn.spid
            if spid.tree_type == TREE_TYPE_LOCAL_DISK:
                self._master_local.get_display_tree(spid, tree_id)
            elif spid.tree_type == TREE_TYPE_GDRIVE:
                assert self._master_gdrive
                if tree_id == ID_GDRIVE_DIR_SELECT:
                    # special handling for dir select dialog: make sure we are fully synced first
                    self._master_gdrive.get_synced_master_tree()
                else:
                    self._master_gdrive.get_display_tree(spid, tree_id=tree_id)
            else:
                raise RuntimeError(f'Unrecognized tree type: {spid.tree_type}')

        if load_request.send_signals:
            # Notify UI that we are done. For gRPC backend, this will be received by the server stub and relayed to the client:
            dispatcher.send(signal=Signal.LOAD_SUBTREE_DONE, sender=tree_id)

    def get_active_display_tree_meta(self, tree_id) -> ActiveDisplayTreeMeta:
        return self._display_tree_dict.get(tree_id, None)

    def get_cache_info_for_subtree(self, subtree_root: SinglePathNodeIdentifier, create_if_not_found: bool = False) \
            -> Optional[PersistedCacheInfo]:
        """Finds the cache which contains the given subtree, if there is one.
        If create_if_not_found==True, then it will create & register a new cache and return that.
        If create_if_not_found==False, then it will return None if no associated cache could be found."""

        self.wait_for_load_registry_done()

        if subtree_root.tree_type == TREE_TYPE_GDRIVE:
            # there is only 1 GDrive cache:
            cache_info = self.caches_by_type.get_single(TREE_TYPE_GDRIVE, ROOT_PATH)
        elif subtree_root.tree_type == TREE_TYPE_LOCAL_DISK:
            cache_info = self.find_existing_cache_info_for_local_subtree(subtree_root.get_single_path())
        else:
            raise RuntimeError(f'Unrecognized tree type: {subtree_root.tree_type}')

        if not cache_info and create_if_not_found:
            cache_info = self._create_new_cache_info(subtree_root)

        return cache_info

    def get_or_create_cache_info_for_gdrive(self) -> PersistedCacheInfo:
        master_tree_root = NodeIdentifierFactory.get_gdrive_root_constant_single_path_identifier()
        return self.backend.cacheman.get_or_create_cache_info_entry(master_tree_root)

    def find_existing_cache_info_for_local_subtree(self, full_path: str) -> Optional[PersistedCacheInfo]:
        # Wait for registry to finish loading before attempting to read dict. Shouldn't take long.
        self.wait_for_load_registry_done()

        existing_caches: List[PersistedCacheInfo] = list(self.caches_by_type.get_second_dict(TREE_TYPE_LOCAL_DISK).values())

        for existing_cache in existing_caches:
            # Is existing_cache an ancestor of target tree?
            if full_path.startswith(existing_cache.subtree_root.get_path_list()[0]):
                return existing_cache
        # Nothing in the cache contains subtree
        return None

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
            self._master_gdrive.get_synced_master_tree()

        for cache in needed_cache_dict.values():
            if not cache.is_loaded:
                if not os.path.exists(cache.subtree_root.get_single_path()):
                    raise RuntimeError(f'Could not load planning node(s): cache subtree does not exist: {cache.subtree_root.get_single_path()}')
                else:
                    assert isinstance(cache.subtree_root, LocalNodeIdentifier)
                    self._master_local.get_display_tree(cache.subtree_root, ID_GLOBAL_CACHE)

    def get_or_create_cache_info_entry(self, subtree_root: SinglePathNodeIdentifier) -> PersistedCacheInfo:
        """DEPRECATED: use get_cache_info_for_subtree() instead since it includes support for subtrees"""
        self.wait_for_load_registry_done()

        existing = self.caches_by_type.get_single(subtree_root.tree_type, subtree_root.get_single_path())
        if existing:
            logger.debug(f'Found existing cache entry for subtree: {subtree_root}')
            return existing
        else:
            logger.debug(f'No existing cache entry found for subtree: {subtree_root}')

        return self._create_new_cache_info(subtree_root)

    def _create_new_cache_info(self, subtree_root: SinglePathNodeIdentifier) -> PersistedCacheInfo:
        if subtree_root.tree_type == TREE_TYPE_LOCAL_DISK:
            unique_path = subtree_root.get_single_path().replace('/', '_')
            file_name = f'LO_{unique_path}.{INDEX_FILE_SUFFIX}'
            parent_path = self._derive_parent_path(subtree_root.get_single_path())
            subtree_root_parent_uid = self.get_uid_for_local_path(parent_path, override_load_check=True)
        elif subtree_root.tree_type == TREE_TYPE_GDRIVE:
            file_name = GDRIVE_INDEX_FILE_NAME
            subtree_root_parent_uid = SUPER_ROOT_UID
        else:
            raise RuntimeError(f'Unrecognized tree type: {subtree_root.tree_type}')

        cache_location = os.path.join(self.cache_dir_path, file_name)
        now_ms = int(time.time())
        db_entry = CacheInfoEntry(cache_location=cache_location,
                                  subtree_root=subtree_root, subtree_root_parent_uid=subtree_root_parent_uid, sync_ts=now_ms,
                                  is_complete=True)

        with CacheRegistry(self.main_registry_path, self.backend.node_identifier_factory) as cache_registry_db:
            logger.info(f'Inserting new cache info into registry: {subtree_root}')
            cache_registry_db.insert_cache_info(db_entry, append=True, overwrite=False)

        cache_info = PersistedCacheInfo(db_entry)

        # Save reference in memory
        self.caches_by_type.put_item(cache_info)

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

    # Various public methods
    # ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼

    def show_tree(self, subtree_root: LocalNodeIdentifier) -> str:
        if subtree_root.tree_type == TREE_TYPE_LOCAL_DISK:
            return self._master_local.show_tree(subtree_root)
        elif subtree_root.tree_type == TREE_TYPE_GDRIVE:
            return self._master_gdrive.show_tree(subtree_root)
        else:
            assert False

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
        summary_msg: str = self._get_tree_summary(subtree_root_node)
        dispatcher.send(signal=Signal.SET_STATUS, sender=tree_id, status_msg=summary_msg)

    def _get_tree_summary(self, root_node: Node):
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
                    assert isinstance(root_node, HasChildStats)
                    logger.debug(f'Generating summary for GDrive tree: {root_node.node_identifier}')
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

    def get_synced_gdrive_master_tree(self, tree_id: str):
        """Will load from disk and sync latest changes from GDrive server before returning."""
        self._master_gdrive.get_synced_master_tree()

    def build_local_file_node(self, full_path: str, staging_path=None, must_scan_signature=False) -> Optional[LocalFileNode]:
        return self._master_local.build_local_file_node(full_path, staging_path, must_scan_signature)

    def build_local_dir_node(self, full_path: str, is_live: bool = True) -> LocalDirNode:
        return self._master_local.build_local_dir_node(full_path, is_live)

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

    def get_uid_for_local_path(self, full_path: str, uid_suggestion: Optional[UID] = None, override_load_check: bool = False) -> UID:
        """Deterministically gets or creates a UID corresponding to the given path string"""
        assert full_path and isinstance(full_path, str)
        return self._master_local.get_uid_for_path(full_path, uid_suggestion, override_load_check)

    def _read_single_node_from_disk_for_local_path(self, full_path: str) -> Node:
        if not file_util.is_normalized(full_path):
            full_path = file_util.normalize_path(full_path)
            logger.debug(f'Normalized path: {full_path}')

        assert self._master_local
        node = self._master_local.load_single_node_for_path(full_path)
        return node

    def read_single_node_from_disk_for_uid(self, uid: UID, tree_type: int) -> Optional[Node]:
        if tree_type == TREE_TYPE_LOCAL_DISK:
            raise InvalidOperationError(f'read_single_node_from_disk_for_uid(): not yet supported: {tree_type}')
        elif tree_type == TREE_TYPE_GDRIVE:
            return self._master_gdrive.read_single_node_from_disk_for_uid(uid)
        else:
            raise RuntimeError(f'Unknown tree type: {tree_type} for UID {uid}')

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

    def get_children(self, node: Node, filter_criteria: FilterCriteria = None):
        tree_type: int = node.node_identifier.tree_type
        if tree_type == TREE_TYPE_GDRIVE:
            child_list = self._master_gdrive.get_children(node, filter_criteria)
        elif tree_type == TREE_TYPE_LOCAL_DISK:
            child_list = self._master_local.get_children(node, filter_criteria)
        else:
            raise RuntimeError(f'Unknown tree type: {tree_type} for {node.node_identifier}')

        for child in child_list:
            self._update_node_icon(child)

        return child_list

    def _update_node_icon(self, node: Node):
        op: Optional[UserOp] = self.get_last_pending_op_for_node(node.uid)
        if op and not op.is_completed():
            icon: Optional[IconId] = op.get_icon_for_node(node.uid)
            if SUPER_DEBUG:
                logger.debug(f'Node {node.uid} belongs to pending op ({op.op_uid}): {op.op_type.name}): returning icon="{icon.name}"')
        else:
            icon = None
        node.set_icon(icon)

    @staticmethod
    def _derive_parent_path(child_path) -> Optional[str]:
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

    def get_parent_uid_list_for_node(self, node: Node, whitelist_subtree_path: str = None) -> List[UID]:
        """Derives the UID for the parent of the given node. If whitelist_subtree_path is provided, filter the results by subtree"""
        if isinstance(node, HasParentList):
            assert isinstance(node, GDriveNode) and node.get_tree_type() == TREE_TYPE_GDRIVE, f'Node: {node}'
            if whitelist_subtree_path:
                filtered_parent_uid_list = []
                for parent_uid in node.get_parent_uids():
                    parent_node = self.get_node_for_uid(parent_uid, node.get_tree_type())
                    # if SUPER_DEBUG:
                    #     logger.debug(f'get_parent_uid_list_for_node(): Checking parent_node {parent_node} against path "{whitelist_subtree_path}"')
                    if parent_node and parent_node.node_identifier.has_path_in_subtree(whitelist_subtree_path):
                        filtered_parent_uid_list.append(parent_node.uid)
                    # elif SUPER_DEBUG:
                    #     logger.debug(f'get_parent_uid_list_for_node(): Filtered out parent {parent_uid}')
                # if SUPER_DEBUG:
                #     logger.debug(f'get_parent_uid_list_for_node(): Returning parent list: {filtered_parent_uid_list}')
                return filtered_parent_uid_list

            return node.get_parent_uids()
        else:
            assert isinstance(node, LocalNode) and node.node_identifier.tree_type == TREE_TYPE_LOCAL_DISK, f'Node: {node}'
            parent_path: str = node.derive_parent_path()
            uid: UID = self.get_uid_for_local_path(parent_path)
            assert uid
            if not whitelist_subtree_path or parent_path.startswith(whitelist_subtree_path):
                return [uid]
            return []

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
        parent_path: str = self._derive_parent_path(sn.spid.get_single_path())
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

    def get_ancestor_list_for_single_path_identifier(self, single_path_node_identifier: SinglePathNodeIdentifier,
                                                     stop_at_path: Optional[str] = None) -> Deque[Node]:
        ancestor_deque: Deque[Node] = deque()
        ancestor: Node = self.get_node_for_uid(single_path_node_identifier.uid)
        if not ancestor:
            logger.warning(f'get_ancestor_list_for_single_path_identifier(): Node not found: {single_path_node_identifier}')
            return ancestor_deque

        parent_path: str = single_path_node_identifier.get_single_path()  # not actually parent's path until it enters loop

        while True:
            parent_path = self._derive_parent_path(parent_path)
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
            logger.debug(f'[{dst_tree_id}]Dropping into dest: {dst_sn.spid}')
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
