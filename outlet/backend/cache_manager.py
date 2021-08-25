import logging
import os
import pathlib
import threading
import uuid
from collections import deque
from typing import Deque, Dict, Iterable, List, Optional, Set, Tuple

from pydispatch import dispatcher

from backend.diff.change_maker import ChangeMaker
from backend.display_tree.active_tree_manager import ActiveTreeManager
from backend.display_tree.active_tree_meta import ActiveDisplayTreeMeta
from backend.display_tree.change_tree import ChangeTree
from backend.display_tree.row_state_tracking import RowStateTracking
from backend.executor.central import ExecPriority
from backend.executor.command.cmd_interface import Command
from backend.executor.user_op.op_manager import OpManager
from backend.sqlite.cache_registry_db import CacheRegistry
from backend.tree_store.gdrive.master_gdrive import GDriveMasterStore
from backend.tree_store.gdrive.master_gdrive_op_load import GDriveDiskLoadOp
from backend.tree_store.local.master_local import LocalDiskMasterStore
from backend.tree_store.local.sig_calc_thread import SigCalcBatchingThread
from backend.tree_store.tree_store_interface import TreeStore
from backend.uid.uid_mapper import UidGoogIdMapper, UidPathMapper
from constants import CACHE_LOAD_TIMEOUT_SEC, GDRIVE_INDEX_FILE_NAME, GDRIVE_ROOT_UID, IconId, INDEX_FILE_SUFFIX, \
    MAIN_REGISTRY_FILE_NAME, NULL_UID, OPS_FILE_NAME, ROOT_PATH, \
    SUPER_DEBUG_ENABLED, SUPER_ROOT_DEVICE_UID, TRACE_ENABLED, TreeDisplayMode, TreeID, TreeLoadState, TreeType, UID_GOOG_ID_FILE_NAME, \
    UID_PATH_FILE_NAME
from error import CacheNotFoundError, CacheNotLoadedError, ResultsExceededError
from model.cache_info import CacheInfoEntry, PersistedCacheInfo
from model.device import Device
from model.display_tree.build_struct import DisplayTreeRequest, RowsOfInterest
from model.display_tree.display_tree import DisplayTree, DisplayTreeUiState
from model.display_tree.filter_criteria import FilterCriteria
from model.display_tree.summary import TreeSummarizer
from model.node.gdrive_node import GDriveNode
from model.node.local_disk_node import LocalDirNode, LocalFileNode
from model.node.node import Node, SPIDNodePair
from model.node_identifier import GUID, LocalNodeIdentifier, NodeIdentifier, SinglePathNodeIdentifier
from model.uid import UID
from model.user_op import UserOp, UserOpType
from signal_constants import ID_GDRIVE_DIR_SELECT, ID_GLOBAL_CACHE, Signal
from util import file_util, time_util
from util.ensure import ensure_list
from util.file_util import get_resource_path
from util.has_lifecycle import HasLifecycle
from util.stopwatch_sec import Stopwatch
from util.task_runner import Task
from util.two_level_dict import TwoLevelDict

logger = logging.getLogger(__name__)

DEVICE_UUID_CONFIG_KEY = 'agent.local_disk.device_uuid'


def ensure_cache_dir_path(backend):
    cache_dir_path = get_resource_path(backend.get_config('cache.cache_dir_path'))
    if not os.path.exists(cache_dir_path):
        logger.info(f'Cache directory does not exist; attempting to create: "{cache_dir_path}"')
    os.makedirs(name=cache_dir_path, exist_ok=True)
    return cache_dir_path


class CacheInfoByDeviceUid(TwoLevelDict):
    """
    ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
    CLASS CacheInfoByDeviceUid

    Holds PersistedCacheInfo objects
    ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼
    """

    def __init__(self):
        super().__init__(lambda x: x.subtree_root.device_uid, lambda x: x.subtree_root.get_path_list()[0], lambda x, y: True)


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

        self._device_uuid: str = self.get_or_set_local_device_uuid()

        self._store_dict: Dict[UID, TreeStore] = {}

        self._this_disk_local_store: Optional[LocalDiskMasterStore] = None
        """Convenience pointer, for the disk on which the backend is running. Many operations (such as monitoring) can only be done for this store"""

        self._cache_info_dict: CacheInfoByDeviceUid = CacheInfoByDeviceUid()

        self.load_all_caches_on_startup = backend.get_config('cache.load_all_caches_on_startup')
        self.load_caches_for_displayed_trees_at_startup = backend.get_config('cache.load_caches_for_displayed_trees_on_startup')
        self.sync_from_local_disk_on_cache_load = backend.get_config('cache.local_disk.sync_from_local_disk_on_cache_load')
        self.sync_from_gdrive_on_cache_load = backend.get_config('cache.sync_from_gdrive_on_cache_load')
        self.reload_tree_on_root_path_update = backend.get_config('cache.load_cache_when_tree_root_selected')
        self.cancel_all_pending_ops_on_startup = backend.get_config('cache.cancel_all_pending_ops_on_startup')
        self.lazy_load_local_file_signatures: bool = backend.get_config('cache.local_disk.signatures.lazy_load')
        logger.debug(f'lazy_load_local_file_signatures = {self.lazy_load_local_file_signatures}')

        if not self.load_all_caches_on_startup:
            logger.info('Configured not to fetch all caches on startup; will lazy load instead')

        # Instantiate but do not start submodules yet, to avoid entangled dependencies:

        self._active_tree_manager = ActiveTreeManager(self.backend)
        self._row_state_tracking = RowStateTracking(self.backend, self._active_tree_manager)

        uid_path_cache_path = os.path.join(self.cache_dir_path, UID_PATH_FILE_NAME)
        self._uid_path_mapper = UidPathMapper(backend, uid_path_cache_path)
        """Officially, we allow for different devices to have different UIDs for a given path. But in practice, given a single agent, 
        all devices under its ownership will share the same UID-Path mapper, which means that the will all map the same UIDs to the same paths."""

        uid_goog_id_cache_path = os.path.join(self.cache_dir_path, UID_GOOG_ID_FILE_NAME)
        self._uid_goog_id_mapper = UidGoogIdMapper(backend, uid_goog_id_cache_path)
        """Same deal with GoogID mapper. We init it here, so that we can load in all the cached GoogIDs ASAP"""

        op_db_path = os.path.join(self.cache_dir_path, OPS_FILE_NAME)
        self._op_ledger: OpManager = OpManager(self.backend, op_db_path)
        """Sub-module of Cache Manager which manages commands which have yet to execute"""

        self._local_disk_sig_calc_thread: Optional[SigCalcBatchingThread] = None

        # Create Event objects to optionally wait for lifecycle events
        self._load_registry_done: threading.Event = threading.Event()

        self._startup_done: threading.Event = threading.Event()

        # (Optional) variables needed for producer/consumer behavior if Load All Caches needed
        self._load_all_caches_done: threading.Event = threading.Event()
        self._load_all_caches_in_process: bool = False

        self._cached_device_list: List[Device] = []

        self.connect_dispatch_listener(signal=Signal.COMMAND_COMPLETE, receiver=self._on_command_completed)

    def shutdown(self):
        logger.debug('CacheManager.shutdown() entered')
        HasLifecycle.shutdown(self)

        try:
            if self._uid_path_mapper:
                self._uid_path_mapper.shutdown()
        except (AttributeError, NameError):
            pass

        try:
            if self._uid_goog_id_mapper:
                self._uid_goog_id_mapper.shutdown()
        except (AttributeError, NameError):
            pass

        try:
            if self._local_disk_sig_calc_thread:
                self._local_disk_sig_calc_thread.shutdown()
        except (AttributeError, NameError):
            pass

        try:
            if self._op_ledger:
                self._op_ledger.shutdown()
                self._op_ledger = None
        except (AttributeError, NameError):
            pass

        try:
            if self._store_dict:
                for store in self._store_dict.values():
                    store.shutdown()
                self._store_dict = None
        except (AttributeError, NameError):
            pass

        try:
            if self._active_tree_manager:
                self._active_tree_manager.shutdown()
                self._active_tree_manager = None
        except (AttributeError, NameError):
            pass

    # Startup loading/maintenance
    # ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼

    def start(self):
        """Should be called during startup. Loop over all caches and load/merge them into a
        single large in-memory cache"""
        if self._startup_done.is_set():
            logger.info(f'Caches already loaded. Ignoring start request.')
            return

        logger.debug(f'Starting CacheManager')
        HasLifecycle.start(self)

        logger.debug(f'Sending START_PROGRESS_INDETERMINATE for ID: {ID_GLOBAL_CACHE}')
        dispatcher.send(Signal.START_PROGRESS_INDETERMINATE, sender=ID_GLOBAL_CACHE)

        try:
            # Get paths loaded ASAP, so we won't worry about creating duplicate paths or UIDs for them
            self._uid_path_mapper.start()
            # same deal with GoogIDs
            self._uid_goog_id_mapper.start()

            self._init_store_dict()

            # Load registry first. Do validation along the way
            self._load_registry()

            # Start sub-modules:
            self._active_tree_manager.start()
            for store in self._store_dict.values():
                store.start()
            self._op_ledger.start()

            if self._this_disk_local_store and self.lazy_load_local_file_signatures:
                self._local_disk_sig_calc_thread = SigCalcBatchingThread(self.backend, self._this_disk_local_store.device_uid)
                self._local_disk_sig_calc_thread.start()

            # Now load all caches (if configured):
            if self.load_all_caches_on_startup:
                load_all_caches_sw = Stopwatch()

                def notify_load_all_done(this_task):
                    self._load_all_caches_in_process = False
                    self._load_all_caches_done.set()
                    logger.info(f'{load_all_caches_sw} Done loading all caches.')

                load_all_caches_task = Task(ExecPriority.P3_BACKGROUND_CACHE_LOAD, self._load_all_caches_start)
                load_all_caches_task.add_next_task(notify_load_all_done)
                self.backend.executor.submit_async_task(load_all_caches_task)
            else:
                logger.info(f'Configured not to load on startup')

            # Finally, add or cancel any queued changes (asynchronously)
            if self.cancel_all_pending_ops_on_startup:
                logger.debug(f'User configuration specifies cancelling all pending ops on startup')
                pending_ops_task = self._op_ledger.cancel_pending_ops_from_disk
            else:
                pending_ops_task = self._op_ledger.resume_pending_ops_from_disk
            # This is a lower priority, so will not execute until after caches are all loaded
            self.backend.executor.submit_async_task(Task(ExecPriority.P7_USER_OP_EXECUTION, pending_ops_task))

        finally:
            dispatcher.send(Signal.STOP_PROGRESS, sender=ID_GLOBAL_CACHE)
            self._startup_done.set()
            logger.info('CacheManager startup done')

    def get_or_set_local_device_uuid(self) -> str:
        file_path: str = file_util.get_resource_path(self.backend.get_config('agent.local_disk.device_id_file_path'))
        if os.path.exists(file_path):
            with open(file_path, 'r') as reader:
                device_uuid = reader.readline().strip()
        else:
            device_uuid = str(uuid.uuid4())
            with open(file_path, 'w') as reader:
                reader.write(device_uuid)
                reader.write('\n')
                reader.flush()
        logger.debug(f'LocalDisk device UUID is: {device_uuid}')
        return device_uuid

    # TODO: when do we create a new device?
    def upsert_device(self, device: Device):
        with CacheRegistry(self.main_registry_path, self.backend.node_identifier_factory) as db:
            db.upsert_device(device)
            logger.debug(f'Upserted device to DB: {device}')

        dispatcher.send(signal=Signal.DEVICE_UPSERTED, device=device)

    def get_device_list(self) -> List[Device]:
        # Cache this for better performance.
        # TODO: Need to update the cached list on change
        if not self._cached_device_list:
            device_list = list(filter(lambda x: x.uid != SUPER_ROOT_DEVICE_UID, self._read_device_list()))
            logger.debug(f'Device list: {device_list}')
            self._cached_device_list = device_list

        return self._cached_device_list

    def _read_device_list(self) -> List[Device]:
        with CacheRegistry(self.main_registry_path, self.backend.node_identifier_factory) as db:
            return db.get_device_list()

    def _write_new_device(self, device: Device):
        with CacheRegistry(self.main_registry_path, self.backend.node_identifier_factory) as db:
            db.insert_device(device)
            logger.debug(f'Wrote new device to DB: {device}')

    # TODO: add this to backend API
    def get_tree_type_for_device_uid(self, device_uid: UID) -> TreeType:
        if device_uid == SUPER_ROOT_DEVICE_UID:
            return TreeType.MIXED

        for device in self.get_device_list():
            if device.uid == device_uid:
                return device.tree_type

        raise RuntimeError(f'Could not find device with UID: {device_uid}')

    def _init_store_dict(self):
        logger.debug('Init store dict')
        has_super_root = False
        # TODO: add true support for multiple GDrives
        master_gdrive = None
        for device in self._read_device_list():
            if device.tree_type == TreeType.MIXED:
                if device.uid != SUPER_ROOT_DEVICE_UID:
                    raise RuntimeError(f'Invalid device_uid: {device.uid} (expected {SUPER_ROOT_DEVICE_UID}) for super-root device {device}')
                has_super_root = True
            elif device.tree_type == TreeType.LOCAL_DISK:
                store = LocalDiskMasterStore(self.backend, self._uid_path_mapper, device)

                if device.long_device_id == self._device_uuid:
                    self._this_disk_local_store = store

                self._store_dict[device.uid] = store
            elif device.tree_type == TreeType.GDRIVE:
                store = GDriveMasterStore(self.backend, self._uid_goog_id_mapper, device)
                master_gdrive = store

                self._store_dict[device.uid] = store
            else:
                raise RuntimeError(f'Invalid tree type: {device.tree_type} for device {device}')

        if has_super_root:
            logger.debug(f'Found super-root in registry')
        else:
            # Need to create new device for this disk (first run)
            logger.debug(f'Writing super-root device to registry')
            device = Device(SUPER_ROOT_DEVICE_UID, "ROOT", TreeType.MIXED, "Super Root")
            self._write_new_device(device)

        if self._this_disk_local_store:
            logger.info(f'Found this_local_disk in registry with UID {self._this_disk_local_store.device_uid}')
        else:
            # Need to create new device for this disk (first run)
            device = Device(NULL_UID, self._device_uuid, TreeType.LOCAL_DISK, "My Local Disk")
            self._write_new_device(device)
            store = LocalDiskMasterStore(self.backend, self._uid_path_mapper, device)
            self._store_dict[device.uid] = store
            self._this_disk_local_store = store

        # TODO: add true support for multiple GDrives
        if master_gdrive:
            logger.info(f'Found master_gdrive in registry with device UID {master_gdrive.device_uid}')
        else:
            device = Device(NULL_UID, 'GDriveTODO', TreeType.GDRIVE, "My Google Drive")
            self._write_new_device(device)
            store = GDriveMasterStore(self.backend, device)
            self._store_dict[device.uid] = store

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
            existing = self._cache_info_dict.get_single(info.subtree_root.device_uid, info.subtree_root.get_single_path())
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
            self._cache_info_dict.put_item(info)

        # Write back to cache if we need to clean things up:
        if skipped_count > 0:
            self.write_cache_registry_updates_to_disk()
            caches = self._cache_info_dict.get_all()
            self._overwrite_all_caches_in_registry(caches)

        self._load_registry_done.set()
        logger.info(f'{stopwatch} Found {unique_cache_count} existing caches (+ {skipped_count} skipped)')

    def write_cache_registry_updates_to_disk(self):
        """Overrites all entries in the CacheInfoRegistry with the entries in memory"""
        caches = self._cache_info_dict.get_all()
        self._overwrite_all_caches_in_registry(caches)

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

    def _get_store_for_device_uid(self, device_uid: UID) -> TreeStore:
        assert device_uid, f'get_store_for_device_uid(): device_uid not specified!'
        store: TreeStore = self._store_dict.get(device_uid, None)
        if not store:
            raise RuntimeError(f'get_tree_type(): no store found for device_uid: {device_uid}')
        return store

    def _load_all_caches_start(self, this_task: Task):
        """Load ALL the caches into memory. This is needed in certain circumstances, such as when a UID is being derefernced but we
        don't know which cache it belongs to."""

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

        class ConsolidateCachesTask:
            def __init__(self):
                self.existing_cache_list: List[PersistedCacheInfo] = []
                self.registry_needs_update: bool = False

        state = ConsolidateCachesTask()

        def _consolidate_local_caches_for_all_devices(_consolidate_all_device_caches_task: Task):
            for device_uid, second_dict in self._cache_info_dict.get_first_dict().items():

                device_cache_list: List[PersistedCacheInfo] = list(second_dict.values())
                if SUPER_DEBUG_ENABLED:
                    logger.debug(f'Examining device_uid={device_uid} for consolidation: it has {len(device_cache_list)} caches')
                if device_cache_list:
                    store: TreeStore = self._store_dict[device_uid]
                    if store.device.tree_type == TreeType.LOCAL_DISK:
                        assert isinstance(store, LocalDiskMasterStore)

                        # Add as child task, so that it executes prior to _update_registry()
                        child_task_single_device = _consolidate_all_device_caches_task.create_child_task(
                            store.consolidate_local_caches, device_cache_list, state)
                        self.backend.executor.submit_async_task(child_task_single_device)
                    else:
                        # this is otherwise done by consolidate_local_caches()
                        state.existing_cache_list += device_cache_list  # do this for all devices

            # TODO: idea: detect bad shutdown, and if it's bad, check max UIDs of all caches

        child_task_consolidate_all = this_task.create_child_task(_consolidate_local_caches_for_all_devices)
        self.backend.executor.submit_async_task(child_task_consolidate_all)

        def _update_registry_and_launch_load_tasks(_this_task: Task):
            if state.registry_needs_update:
                self._overwrite_all_caches_in_registry(state.existing_cache_list)
                logger.debug(f'Overwriting in-memory list ({len(self._cache_info_dict)}) with {len(state.existing_cache_list)} entries')
                self._cache_info_dict.clear()
                for cache in state.existing_cache_list:
                    self._cache_info_dict.put_item(cache)

            for cache_num, existing_disk_cache in enumerate(state.existing_cache_list):
                self._cache_info_dict.put_item(existing_disk_cache)

                cache_num_plus_1 = cache_num + 1
                cache_count = len(state.existing_cache_list)

                # Load each cache as a separate child task.
                child_task_single_load = _this_task.create_child_task(self._init_existing_cache, existing_disk_cache, cache_num_plus_1, cache_count)
                self.backend.executor.submit_async_task(child_task_single_load)

        child_task = this_task.create_child_task(_update_registry_and_launch_load_tasks)
        self.backend.executor.submit_async_task(child_task)

    def _get_cache_info_list_from_registry(self) -> List[CacheInfoEntry]:
        with CacheRegistry(self.main_registry_path, self.backend.node_identifier_factory) as db:
            if db.has_cache_info():
                exisiting_cache_list = db.get_cache_info_list()
                logger.debug(f'Found {len(exisiting_cache_list)} caches listed in registry')
            else:
                logger.debug('Registry has no caches listed')
                db.create_cache_registry_if_not_exist()
                exisiting_cache_list = []

        return exisiting_cache_list

    def _overwrite_all_caches_in_registry(self, cache_info_list: List[CacheInfoEntry]):
        logger.info(f'Overwriting all cache entries in persisted registry with {len(cache_info_list)} entries')
        with CacheRegistry(self.main_registry_path, self.backend.node_identifier_factory) as db:
            db.insert_cache_info(cache_info_list, append=False, overwrite=True)

    def _init_existing_cache(self, this_task: Task, existing_disk_cache: PersistedCacheInfo, cache_num: int, total_cache_count: int):
        try:
            logger.info(f'Init cache {cache_num}/{total_cache_count}: id={existing_disk_cache.subtree_root}')

            stopwatch = Stopwatch()

            if existing_disk_cache.is_loaded:
                logger.debug('Cache is already loaded; skipping')
                return

            device_uid = existing_disk_cache.subtree_root.device_uid
            store: TreeStore = self._store_dict[device_uid]
            tree_type = store.device.tree_type

            if tree_type == TreeType.LOCAL_DISK:
                if not os.path.exists(existing_disk_cache.subtree_root.get_path_list()[0]):
                    logger.info(f'Subtree not found; will defer loading: "{existing_disk_cache.subtree_root}"')
                    existing_disk_cache.needs_refresh = True
                else:
                    assert isinstance(existing_disk_cache.subtree_root, LocalNodeIdentifier)
                    store.load_subtree(this_task, existing_disk_cache.subtree_root, ID_GLOBAL_CACHE)
            elif tree_type == TreeType.GDRIVE:
                assert existing_disk_cache.subtree_root == self.backend.node_identifier_factory.get_root_constant_gdrive_spid(device_uid), \
                    f'Expected GDrive root ({self.backend.node_identifier_factory.get_root_constant_gdrive_spid(device_uid)}) ' \
                    f'but found: {existing_disk_cache.subtree_root}'
                assert isinstance(store, GDriveMasterStore)
                store.load_and_sync_master_tree(this_task)
            else:
                assert False

            logger.info(f'{stopwatch} Done loading cache: {cache_num}/{total_cache_count}: id={existing_disk_cache.subtree_root}')
        except RuntimeError:
            logger.exception(f'Failed to load cache: {existing_disk_cache.cache_location}')

    # SignalDispatcher callbacks
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

        if result.nodes_to_remove:
            logger.debug(f'Cmd resulted in {len(result.nodes_to_remove)} nodes to remove')
            for removed_node in result.nodes_to_remove:
                self.remove_node(removed_node, to_trash=False)

        self._op_ledger.finish_command(command)

    # DisplayTree stuff
    # ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼

    def start_subtree_load(self, tree_id: TreeID, send_signals: bool):
        """Called from backend.start_subtree_load(). See load_data_for_display_tree() below."""
        self.wait_for_startup_done()

        logger.debug(f'[{tree_id}] Enqueueing subtree load task')
        self.backend.executor.submit_async_task(Task(ExecPriority.P1_USER_LOAD, self.load_data_for_display_tree, tree_id, send_signals))

    def load_data_for_display_tree(self, this_task: Task, tree_id: TreeID, send_signals: bool):
        """
        TREE LOAD SEQUENCE:
        - Client requests display tree: see request_display_tree()
        - TreeState changes to: NOT_LOADED
        - Client calls start_subtree_load()
        - TreeState changes to: LOAD_STARTED
        - FE can now request unfiltered dirs, but filter controls are grayed out
        - BE now loads the tree, either (a) from cache, all at once, if it exists, or (b) layer by layer, BFS style,
          in discrete chunks based on directory.
          But also allows for the user to expand a dir, and gives higher priority to load that directory in that case
        - Finally all directories are loaded. We can now calculate stats and push those out
        - TreeState: COMPLETELY_LOADED
        - Calculate MD5s for all items, if local drive
        """
        logger.debug(f'[{tree_id}] Loading data for display tree (send_signals={send_signals})')
        tree_meta: ActiveDisplayTreeMeta = self.get_active_display_tree_meta(tree_id)
        if not tree_meta:
            logger.info(f'[{tree_id}] Display tree is no longer tracked; discarding data load')
            return

        # Load and bring up-to-date expanded & selected rows:
        self._row_state_tracking.load_rows_of_interest(tree_id)

        if tree_meta.is_first_order():  # i.e. not ChangeTree

            # Update monitoring state.
            # This should be started AT THE SAME TIME as tree load start, and its operations will be queued until after load completed
            self._active_tree_manager.update_live_capture(tree_meta.root_exists, tree_meta.root_sn.spid, tree_id)

            # fall through

        # Transition: Load State = LOAD_STARTED
        tree_meta.load_state = TreeLoadState.LOAD_STARTED
        if send_signals:
            # This will be carried across gRPC if needed
            # Clients will immediately begin to request rows of interest & populating their trees via get_child_list()
            logger.debug(f'[{tree_id}] Sending signal {Signal.TREE_LOAD_STATE_UPDATED.name} with state={TreeLoadState.LOAD_STARTED.name})')
            dispatcher.send(signal=Signal.TREE_LOAD_STATE_UPDATED, sender=tree_id, tree_load_state=TreeLoadState.LOAD_STARTED, status_msg='Loading...')

        # Full cache load. Both first-order & higher-order trees do this:
        self.backend.executor.submit_async_task(Task(ExecPriority.P3_BACKGROUND_CACHE_LOAD, self._load_cache_for_subtree, tree_meta, send_signals))

    def is_cache_loaded_for(self, spid: SinglePathNodeIdentifier) -> bool:
        # this will return False if either a cache exists but is not loaded, or no cache yet exists:
        return self._get_store_for_device_uid(spid.device_uid).is_cache_loaded_for(spid)

    def _load_cache_for_subtree(self, this_task: Task, tree_meta: ActiveDisplayTreeMeta, send_signals: bool):
        """Note: this method is the "owner" of this_task"""

        if tree_meta.is_first_order():  # i.e. not ChangeTree
            # Load meta for all nodes:
            spid = tree_meta.root_sn.spid
            store = self._get_store_for_device_uid(spid.device_uid)

            if tree_meta.tree_id == ID_GDRIVE_DIR_SELECT:
                # special handling for dir select dialog: make sure we are fully synced first
                assert isinstance(store, GDriveMasterStore)
                store.load_and_sync_master_tree(this_task)
            else:
                # make sure cache is loaded for relevant subtree:
                store.load_subtree(this_task, spid, tree_meta.tree_id)

            def _populate_filter_for_subtree(_this_task):
                if tree_meta.state.root_exists:
                    # get up-to-date root node:
                    subtree_root_node: Optional[Node] = self.get_node_for_uid(spid.node_uid)
                    if not subtree_root_node:
                        raise RuntimeError(f'Could not find node in cache with identifier: {spid} (tree_id={tree_meta.tree_id})')

                    store.populate_filter(tree_meta.filter_state)

            # Let _pre_post_load() be called when any subtasks are done
            this_task.add_next_task(_populate_filter_for_subtree)

        else:
            # ChangeTree: should already be loaded into memory, except for FilterState
            assert not tree_meta.is_first_order()
            if tree_meta.filter_state.has_criteria():
                tree_meta.filter_state.ensure_cache_populated(tree_meta.change_tree)

        def _repopulate_dir_stats_and_finish(_this_task):
            tree_meta.load_state = TreeLoadState.COMPLETELY_LOADED  # do this first to avoid race condition in ActiveTreeManager
            self.repopulate_dir_stats_for_tree(tree_meta)
            if send_signals:
                # Transition: Load State = COMPLETELY_LOADED
                # Notify UI that we are done. For gRPC backend, this will be received by the server stub and relayed to the client:
                logger.debug(f'[{tree_meta.tree_id}] Sending signal {Signal.TREE_LOAD_STATE_UPDATED.name} with'
                             f' tree_load_state={TreeLoadState.COMPLETELY_LOADED.name} status_msg="{tree_meta.summary_msg}"')
                dispatcher.send(signal=Signal.TREE_LOAD_STATE_UPDATED, sender=tree_meta.tree_id, tree_load_state=TreeLoadState.COMPLETELY_LOADED,
                                status_msg=tree_meta.summary_msg)

        this_task.add_next_task(_repopulate_dir_stats_and_finish)

    def repopulate_dir_stats_for_tree(self, tree_meta: ActiveDisplayTreeMeta):
        """
        BE-internal. NOT A CLIENT API
        """
        if tree_meta.root_exists:
            if tree_meta.is_first_order():
                # Load meta for all nodes:
                spid = tree_meta.root_sn.spid
                store = self._get_store_for_device_uid(spid.device_uid)

                # Calculate stats for all dir nodes:
                logger.debug(f'[{tree_meta.tree_id}] Refreshing stats for subtree: {tree_meta.root_sn.spid}')
                tree_meta.dir_stats_unfiltered_by_uid = store.generate_dir_stats(tree_meta.root_sn.node, tree_meta.tree_id)
                tree_meta.dir_stats_unfiltered_by_guid = {}  # just to be sure we don't have old data
            else:
                # ChangeTree
                assert not tree_meta.is_first_order()
                logger.debug(f'[{tree_meta.tree_id}] Tree is a ChangeTree; loading its dir stats')
                tree_meta.dir_stats_unfiltered_by_guid = tree_meta.change_tree.generate_dir_stats()
                tree_meta.dir_stats_unfiltered_by_uid = {}
        else:
            logger.debug(f'[{tree_meta.tree_id}] No DirStats generated: tree does not exist')

        # Now that we have all the stats, we can calculate the summary:
        tree_meta.summary_msg = TreeSummarizer.build_tree_summary(tree_meta, self.get_device_list())
        logger.debug(f'[{tree_meta.tree_id}] New summary: "{tree_meta.summary_msg}"')

    def request_display_tree(self, request: DisplayTreeRequest) -> Optional[DisplayTreeUiState]:
        """The FE needs to first call this to ensure the given tree_id has a ActiveDisplayTreeMeta loaded into memory.
        Afterwards, the FE should call backend.start_subtree_load(), which will call enqueue_load_tree_task(),
        which will then asynchronously call load_data_for_display_tree()"""
        self.wait_for_startup_done()

        return self._active_tree_manager.request_display_tree(request)

    def register_change_tree(self, change_display_tree: ChangeTree, src_tree_id: TreeID) -> DisplayTree:
        """Kinda similar to request_display_tree(), but for change trees"""
        return self._active_tree_manager.register_change_tree(change_display_tree, src_tree_id)

    def get_active_display_tree_meta(self, tree_id) -> ActiveDisplayTreeMeta:
        """Gets an existing ActiveDisplayTreeMeta. The FE should not call this directly."""
        return self._active_tree_manager.get_active_display_tree_meta(tree_id)

    # used by the filter panel:
    def get_filter_criteria(self, tree_id: TreeID) -> Optional[FilterCriteria]:
        return self._active_tree_manager.get_filter_criteria(tree_id)

    # used by the filter panel:
    def update_filter_criteria(self, tree_id: TreeID, filter_criteria: FilterCriteria):
        self._active_tree_manager.update_filter_criteria(tree_id, filter_criteria)

    def is_manual_load_required(self, spid: SinglePathNodeIdentifier, is_startup: bool) -> bool:
        # make sure to create it if not found:
        cache_info = self.get_cache_info_for_subtree(spid, create_if_not_found=True)
        if cache_info.is_loaded:
            # Already loaded!
            return False

        if is_startup and self.load_all_caches_on_startup or self.load_caches_for_displayed_trees_at_startup:
            # We are still starting up but will auto-load this tree soon:
            return False

        if not is_startup and self.reload_tree_on_root_path_update:
            return False
        return True

    def enqueue_refresh_subtree_task(self, node_identifier: NodeIdentifier, tree_id: TreeID):
        logger.info(f'Enqueuing task to refresh subtree at {node_identifier}')
        # TODO: split this in P1_USER_LOAD and LOAD_1 tasks. Need to cross-reference each dir with the visible dirs indicated by tree_id's metadata
        self.backend.executor.submit_async_task(Task(ExecPriority.P1_USER_LOAD, self._refresh_subtree, node_identifier, tree_id))

    # PersistedCacheInfo stuff
    # ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼

    def get_cache_info_for_subtree(self, subtree_root: SinglePathNodeIdentifier, create_if_not_found: bool = False) \
            -> Optional[PersistedCacheInfo]:
        """Finds the cache which contains the given subtree, if there is one.
        If create_if_not_found==True, then it will create & register a new cache and return that.
        If create_if_not_found==False, then it will raise CacheNotFoundError if no associated cache could be found.
        Note that this will also occur if a cache file was deleted, because such caches are detected and purged from the registry
        at startup."""

        self.wait_for_load_registry_done()

        if subtree_root.tree_type == TreeType.GDRIVE:
            # there is only 1 GDrive cache per GDrive account:
            cache_info = self._cache_info_dict.get_single(subtree_root.device_uid, ROOT_PATH)
        elif subtree_root.tree_type == TreeType.LOCAL_DISK:
            cache_info = self.find_existing_cache_info_for_local_subtree(subtree_root.device_uid, subtree_root.get_single_path())
        else:
            raise RuntimeError(f'Unrecognized tree type: {subtree_root.tree_type}')

        if not cache_info:
            if create_if_not_found:
                cache_info = self._create_new_cache_info(subtree_root)
            else:
                raise CacheNotFoundError(f'Could not find cache_info in memory for: {subtree_root} (and create_if_not_found=false)')

        return cache_info

    def find_existing_cache_info_for_local_subtree(self, device_uid: UID, full_path: str) -> Optional[PersistedCacheInfo]:
        # Wait for registry to finish loading before attempting to read dict. Shouldn't take long.
        self.wait_for_load_registry_done()

        for existing_cache in list(self._cache_info_dict.get_second_dict(device_uid).values()):
            # Is existing_cache an ancestor of target tree?
            if full_path.startswith(existing_cache.subtree_root.get_path_list()[0]):
                return existing_cache
        # Nothing in the cache contains subtree
        return None

    def ensure_loaded(self, this_task: Task, node_list: List[Node]):
        """Ensures that all the necessary caches are loaded for all of the given nodes.
        We launch separate executor tasks for each cache load that we require."""

        # use dict and set here to root out duplicate caches:
        needed_localdisk_cache_dict: Dict[str, PersistedCacheInfo] = {}  # cache location -> PersistedCacheInfo
        needed_gdrive_device_uid_set: Set[UID] = set()

        for node in node_list:
            if node.tree_type == TreeType.GDRIVE:
                needed_gdrive_device_uid_set.add(node.device_uid)
            else:
                assert node.tree_type == TreeType.LOCAL_DISK, f'Not a LocalDisk node: {node}'
                cache: Optional[PersistedCacheInfo] = self.find_existing_cache_info_for_local_subtree(node.device_uid, node.get_single_path())
                if cache:
                    needed_localdisk_cache_dict[cache.cache_location] = cache
                else:
                    raise RuntimeError(f'Could not find a cache file for planning node: {node}')

        # GDrive
        for gdrive_device_uid in needed_gdrive_device_uid_set:
            store = self._get_store_for_device_uid(gdrive_device_uid)
            assert isinstance(store, GDriveMasterStore)
            cache_load_task = this_task.create_child_task(store.load_and_sync_master_tree)
            cache_load_task.priority = ExecPriority.P3_BACKGROUND_CACHE_LOAD  # yes, override parent priority
            self.backend.executor.submit_async_task(cache_load_task)

        # LocalDisk:
        for cache in needed_localdisk_cache_dict.values():
            # load each cache one by one
            if not cache.is_loaded:
                if not os.path.exists(cache.subtree_root.get_single_path()):
                    raise RuntimeError(f'Could not load cache: dir does not exist: {cache.subtree_root.get_single_path()}')
                else:
                    assert isinstance(cache.subtree_root, LocalNodeIdentifier)
                    store = self._get_store_for_device_uid(cache.subtree_root.device_uid)
                    cache_load_task = this_task.create_child_task(store.load_subtree, cache.subtree_root, ID_GLOBAL_CACHE)
                    cache_load_task.priority = ExecPriority.P3_BACKGROUND_CACHE_LOAD  # yes, override parent priority
                    self.backend.executor.submit_async_task(cache_load_task)

    def _create_new_cache_info(self, subtree_root: SinglePathNodeIdentifier) -> PersistedCacheInfo:
        if not subtree_root.is_spid():
            raise RuntimeError(f'Internal error: not a SPID: {subtree_root}')

        if subtree_root.tree_type == TreeType.LOCAL_DISK:
            unique_path = subtree_root.get_single_path().replace('/', '_')
            file_name = f'{subtree_root.device_uid}_LO_{unique_path}.{INDEX_FILE_SUFFIX}'
        elif subtree_root.tree_type == TreeType.GDRIVE:
            file_name = f'{subtree_root.device_uid}_{GDRIVE_INDEX_FILE_NAME}'
        else:
            raise RuntimeError(f'Unrecognized tree type: {subtree_root.tree_type}')

        cache_location = os.path.join(self.cache_dir_path, file_name)
        sync_ts = time_util.now_sec()
        db_entry = CacheInfoEntry(cache_location=cache_location,
                                  subtree_root=subtree_root, sync_ts=sync_ts,
                                  is_complete=False)

        with CacheRegistry(self.main_registry_path, self.backend.node_identifier_factory) as db:
            logger.info(f'Inserting new cache info into registry: {subtree_root}')
            db.insert_cache_info(db_entry, append=True, overwrite=False)

        cache_info = PersistedCacheInfo(db_entry)

        # Save reference in memory
        self._cache_info_dict.put_item(cache_info)

        return cache_info

    # Main cache CRUD
    # ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼

    def upsert_single_node(self, node: Node) -> Node:
        return self._get_store_for_device_uid(node.device_uid).upsert_single_node(node)

    def update_single_node(self, node: Node) -> Node:
        """Simliar to upsert, but fails silently if node does not already exist in caches. Useful for things such as asynch MD5 filling"""
        return self._get_store_for_device_uid(node.device_uid).update_single_node(node)

    def delete_subtree(self, device_uid: UID, node_uid_list: List[UID]):
        logger.debug(f'Setting up recursive delete operations for {len(node_uid_list)} nodes')

        # don't worry about overlapping trees; the cacheman will sort everything out
        batch_uid = self.backend.uid_generator.next_uid()
        op_list = []
        for uid_to_delete in node_uid_list:
            node_to_delete = self.get_node_for_uid(uid_to_delete, device_uid)
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
        """NOTE: this is only called for tests currently."""
        self._get_store_for_device_uid(node.device_uid).remove_subtree(node, to_trash)

    def move_local_subtree(self, this_task: Task, src_full_path: str, dst_full_path: str) -> Optional[Tuple]:
        return self._this_disk_local_store.move_local_subtree(this_task, src_full_path, dst_full_path)

    def remove_node(self, node: Node, to_trash):
        self._get_store_for_device_uid(node.device_uid).remove_single_node(node, to_trash)

    # Getters: Nodes and node identifiers
    # ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼

    def get_path_for_uid(self, uid: UID) -> str:
        # Throws exception if no path found
        return self._uid_path_mapper.get_path_for_uid(uid)

    def get_uid_for_local_path(self, full_path: str, uid_suggestion: Optional[UID] = None) -> UID:
        """Deterministically gets or creates a UID corresponding to the given path string"""
        assert full_path and isinstance(full_path, str), f'full_path value is invalid: {full_path}'
        return self._uid_path_mapper.get_uid_for_path(full_path, uid_suggestion)

    def get_node_for_node_identifier(self, node_identifer: NodeIdentifier) -> Optional[Node]:
        return self.get_node_for_uid(node_identifer.node_uid, node_identifer.device_uid)

    def get_node_for_uid(self, uid: UID, device_uid: Optional[UID] = None):
        if device_uid:
            return self._get_store_for_device_uid(device_uid).get_node_for_uid(uid)

        for store in self._store_dict.values():
            node = store.get_node_for_uid(uid)
            if node:
                return node

        return None

    def get_node_list_for_path_list(self, path_list: List[str], device_uid: UID) -> List[Node]:
        """Because of GDrive, we cannot guarantee that a single path will have only one node, or a single node will have only one path."""
        path_list = ensure_list(path_list)
        return self._get_store_for_device_uid(device_uid).get_node_list_for_path_list(path_list)

    def get_child_list(self, parent_spid: SinglePathNodeIdentifier, tree_id: TreeID, is_expanding_parent: bool = False, max_results: int = 0) \
            -> List[SPIDNodePair]:
        if SUPER_DEBUG_ENABLED:
            logger.debug(f'[{tree_id}] Entered get_child_list() for parent_spid={parent_spid} (is_expanding_parent={is_expanding_parent})')
        if not parent_spid:
            raise RuntimeError('get_child_list(): parent_spid not provided!')
        if not isinstance(parent_spid, SinglePathNodeIdentifier):
            raise RuntimeError(f'get_child_list(): not a SPID (type={type(parent_spid)}): {parent_spid}')

        tree_meta: ActiveDisplayTreeMeta = self.get_active_display_tree_meta(tree_id)
        if not tree_meta:
            raise RuntimeError(f'get_child_list(): DisplayTree not registered: {tree_id}')

        if is_expanding_parent:
            self._row_state_tracking.add_expanded_row(parent_spid.guid, tree_id)

        if tree_meta.state.tree_display_mode == TreeDisplayMode.CHANGES_ONE_TREE_PER_CATEGORY:
            # Change trees have their own storage of nodes (not in master caches)
            if tree_meta.filter_state and tree_meta.filter_state.has_criteria():
                child_list = tree_meta.filter_state.get_filtered_child_list(parent_spid, tree_meta.change_tree)
            else:
                child_list = tree_meta.change_tree.get_child_list_for_spid(parent_spid)
        else:
            filter_state = tree_meta.filter_state

            device_uid: UID = parent_spid.device_uid
            child_list = self._get_store_for_device_uid(device_uid).get_child_list_for_spid(parent_spid, filter_state)

        if max_results and (len(child_list) > max_results):
            raise ResultsExceededError(len(child_list))

        self._copy_dir_stats_into_sn_list(child_list, tree_meta)

        # The node icon is also a global change:
        for child_sn in child_list:
            self.update_node_icon(child_sn.node)

        if SUPER_DEBUG_ENABLED:
            logger.debug(f'[{tree_id}] Returning {len(child_list)} children for node: {parent_spid}')
        return child_list

    @staticmethod
    def _copy_dir_stats_into_sn_list(sn_list: List[SPIDNodePair], tree_meta: ActiveDisplayTreeMeta):
        # Fill in dir_stats. For now, we always display the unfiltered stats, even if we are applying a filter in the UI.
        # This is both more useful to the user, and less of a headache, because the stats are relevant across all views in the UI.
        if tree_meta.dir_stats_unfiltered_by_guid:
            uses_uid_key = False
            dir_stats_dict = tree_meta.dir_stats_unfiltered_by_guid
        else:
            # this will only happen for first-order trees pulling directly from the cache:
            uses_uid_key = True
            dir_stats_dict = tree_meta.dir_stats_unfiltered_by_uid
        for sn in sn_list:
            if sn.node.is_dir():
                if uses_uid_key:
                    key = sn.spid.node_uid
                else:
                    key = sn.spid.guid
                sn.node.dir_stats = dir_stats_dict.get(key, None)

    def set_selected_rows(self, tree_id: TreeID, selected: Set[GUID]):
        """Saves the selected rows from the UI for the given tree"""
        self._row_state_tracking.set_selected_rows(tree_id, selected)

    def remove_expanded_row(self, row_guid: GUID, tree_id: TreeID):
        """AKA collapsing a row on the frontend"""
        self._row_state_tracking.remove_expanded_row(row_guid, tree_id)

    def get_rows_of_interest(self, tree_id: TreeID) -> RowsOfInterest:
        return self._row_state_tracking.get_rows_of_interest(tree_id)

    def update_node_icon(self, node: Node):
        icon_id: Optional[IconId] = self._op_ledger.get_icon_for_node(node.device_uid, node.uid)
        if TRACE_ENABLED:
            logger.debug(f'Setting custom icon for node {node.device_uid}:{node.uid} to {"None" if not icon_id else icon_id.name}')
        node.set_icon(icon_id)

    @staticmethod
    def derive_parent_path(child_path) -> Optional[str]:
        if child_path == '/':
            return None
        return str(pathlib.Path(child_path).parent)

    def get_parent_list_for_node(self, node: Node) -> List[Node]:
        return self._get_store_for_device_uid(node.device_uid).get_parent_list_for_node(node)

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
            parent_list: List[Node] = self._get_store_for_device_uid(sn.spid.device_uid).get_parent_list_for_node(sn.node)
            if parent_list:
                if len(parent_list) > 1:
                    raise RuntimeError(f'Expected exactly 1 but found {len(parent_list)} parents for node: "{sn.node}"')
                parent_node = parent_list[0]
            else:
                return None
        else:
            parent_node = self._find_parent_matching_path(sn.node, parent_path)
        if not parent_node:
            return None
        parent_spid = self.backend.node_identifier_factory.for_values(uid=parent_node.uid, device_uid=parent_node.device_uid,
                                                                      tree_type=parent_node.tree_type, path_list=parent_path,
                                                                      must_be_single_path=True)
        return SPIDNodePair(parent_spid, parent_node)

    def get_ancestor_list_for_spid(self, spid: SinglePathNodeIdentifier, stop_at_path: Optional[str] = None) -> Deque[SPIDNodePair]:
        if not spid:
            raise RuntimeError('get_ancestor_list_for_spid(): SPID not provided!')
        if not isinstance(spid, SinglePathNodeIdentifier):
            raise RuntimeError(f'get_ancestor_list_for_spid(): not a SPID (type={type(spid)}): {spid}')

        ancestor_deque: Deque[SPIDNodePair] = deque()
        ancestor_node: Node = self.get_node_for_uid(spid.node_uid)
        if not ancestor_node:
            logger.debug(f'get_ancestor_list_for_spid(): Node not found: {spid}')
            return ancestor_deque

        ancestor_sn = SPIDNodePair(spid, ancestor_node)

        while True:
            parent_path = ancestor_sn.spid.get_single_path()
            if parent_path == stop_at_path:
                return ancestor_deque

            ancestor_sn = self.get_parent_sn_for_sn(ancestor_sn)

            if ancestor_sn:
                ancestor_deque.appendleft(ancestor_sn)
            else:
                return ancestor_deque

    def get_all_files_and_dirs_for_subtree(self, subtree_root: NodeIdentifier) -> Tuple[List[Node], List[Node]]:
        return self._get_store_for_device_uid(subtree_root.device_uid).get_all_files_and_dirs_for_subtree(subtree_root)

    def make_spid_for(self, node_uid: UID, device_uid: UID, full_path: str) -> SinglePathNodeIdentifier:
        return self.backend.node_identifier_factory.for_values(uid=node_uid, device_uid=device_uid, path_list=full_path, must_be_single_path=True)

    def get_sn_for(self, node_uid: UID, device_uid: UID, full_path: str) -> Optional[SPIDNodePair]:
        node = self._get_store_for_device_uid(device_uid).get_node_for_uid(node_uid)
        if not node:
            return None

        spid = self.backend.node_identifier_factory.for_values(uid=node_uid, device_uid=device_uid, tree_type=node.tree_type,
                                                               path_list=full_path, must_be_single_path=True)

        return SPIDNodePair(spid, node)

    def get_parent_for_sn(self, sn: SPIDNodePair) -> Optional[SPIDNodePair]:
        return self._get_store_for_device_uid(sn.spid.device_uid).get_parent_for_sn(sn)

    # GDrive-specific
    # ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼

    def get_parent_goog_id_list(self, node: GDriveNode) -> List[str]:
        parent_uid_list: List[UID] = node.get_parent_uids()

        # special case for GDrive super-root: no goog_id
        if len(parent_uid_list) == 1 and parent_uid_list[0] == GDRIVE_ROOT_UID:
            return []

        # This will raise an exception if it cannot resolve:
        return self.get_goog_id_list_for_uid_list(node.device_uid, parent_uid_list)

    def _get_gdrive_store_for_device_uid(self, device_uid: UID) -> GDriveMasterStore:
        store = self._get_store_for_device_uid(device_uid)
        assert isinstance(store, GDriveMasterStore), f'Expected GDriveMasterStore: {type(store)}'
        return store

    def get_gdrive_client(self, device_uid: UID):
        return self._get_gdrive_store_for_device_uid(device_uid).get_gdrive_client()

    def get_goog_id_list_for_uid_list(self, device_uid: UID, uids: List[UID], fail_if_missing: bool = True) -> List[str]:
        return self._get_gdrive_store_for_device_uid(device_uid).get_goog_id_list_for_uid_list(uids, fail_if_missing=fail_if_missing)

    def get_uid_list_for_goog_id_list(self, device_uid: UID, goog_id_list: List[str]) -> List[UID]:
        return self._get_gdrive_store_for_device_uid(device_uid).get_uid_list_for_goog_id_list(goog_id_list)

    def get_uid_for_goog_id(self, device_uid: UID, goog_id: str, uid_suggestion: Optional[UID] = None) -> UID:
        """Deterministically gets or creates a UID corresponding to the given goog_id"""
        if not goog_id:
            raise RuntimeError('get_uid_for_goog_id(): no goog_id specified!')
        return self._get_gdrive_store_for_device_uid(device_uid).get_uid_for_goog_id(goog_id, uid_suggestion)

    def get_gdrive_identifier_list_for_full_path_list(self, device_uid: UID, path_list: List[str], error_if_not_found: bool = False) \
            -> List[NodeIdentifier]:
        store = self._get_gdrive_store_for_device_uid(device_uid)
        return store.get_identifier_list_for_full_path_list(path_list, error_if_not_found)

    def delete_all_gdrive_data(self, device_uid: UID):
        self._get_gdrive_store_for_device_uid(device_uid).delete_all_gdrive_data()

    def execute_gdrive_load_op(self, device_uid: UID, op: GDriveDiskLoadOp):
        self._get_gdrive_store_for_device_uid(device_uid).execute_load_op(op)

    def download_file_from_gdrive(self, device_uid: UID, node_uid: UID, requestor_id: str):
        gdrive_store = self._get_gdrive_store_for_device_uid(device_uid)

        # Launch as task with high priority:
        download_file_from_gdrive_task = Task(ExecPriority.P1_USER_LOAD, gdrive_store.download_file_from_gdrive, node_uid, requestor_id)
        self.backend.executor.submit_async_task(download_file_from_gdrive_task)

    # This local disk-specific
    # ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼

    def get_node_for_local_path(self, full_path: str) -> Optional[Node]:
        """This will consult both the in-memory and disk caches"""
        if not full_path:
            raise RuntimeError('get_node_for_local_path(): full_path not specified!')
        return self._this_disk_local_store.read_single_node_for_path(full_path)

    # Drag & drop
    # ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼

    def drop_dragged_nodes(self, src_tree_id: TreeID, src_guid_list: List[GUID], is_into: bool, dst_tree_id: TreeID, dst_guid: GUID):
        logger.info(f'Got drop of {len(src_guid_list)} nodes from "{src_tree_id}" -> "{dst_tree_id}" dst_guid={dst_guid} is_into={is_into}')

        src_tree: ActiveDisplayTreeMeta = self.get_active_display_tree_meta(src_tree_id)
        dst_tree: ActiveDisplayTreeMeta = self.get_active_display_tree_meta(dst_tree_id)
        if not src_tree:
            logger.error(f'Aborting drop: could not find src tree: "{src_tree_id}"')
            return
        if not dst_tree:
            logger.error(f'Aborting drop: could not find dst tree: "{dst_tree_id}"')
            return

        src_sn_list = self.get_sn_list_for_guid_list(src_guid_list, src_tree_id)

        dst_sn: SPIDNodePair = self.get_sn_for_guid(dst_guid, dst_tree_id)

        if not is_into or (dst_sn and not dst_sn.node.is_dir()):
            # cannot drop into a file; just use parent in this case
            dst_sn = self.get_parent_sn_for_sn(dst_sn)

        if not dst_guid:
            logger.error(f'[{dst_tree_id}] Cancelling drop: no dst given for dropped location!')
        elif self._is_dropping_on_self(src_sn_list, dst_sn, dst_tree_id):
            logger.debug(f'[{dst_tree_id}] Cancelling drop: nodes were dropped in same location in the tree')
        else:
            logger.debug(f'[{dst_tree_id}] Dropping into dest: {dst_sn.spid}')
            # "Left tree" here is the source tree, and "right tree" is the dst tree:
            change_maker = ChangeMaker(backend=self.backend, left_tree_root_sn=src_tree.root_sn, right_tree_root_sn=dst_tree.root_sn,
                                       tree_id_left_src=src_tree_id, tree_id_right_src=dst_tree_id)
            # So far we only support COPY.
            change_maker.copy_nodes_left_to_right(src_sn_list, dst_sn, UserOpType.CP)
            # This should fire listeners which ultimately populate the tree:
            op_list: Iterable[UserOp] = change_maker.right_side.change_tree.get_ops()
            self.enqueue_op_list(op_list)

    def _is_dropping_on_self(self, src_sn_list: List[SPIDNodePair], dst_sn: SPIDNodePair, dst_tree_id: TreeID):
        dst_ancestor_list = self.get_ancestor_list_for_spid(dst_sn.spid)

        for src_sn in src_sn_list:
            logger.debug(f'[{dst_tree_id}] DestNode="{dst_sn.spid}", DroppedNode="{src_sn.node}"')

            # Same node onto itself?
            if dst_sn.node.node_identifier == src_sn.node.node_identifier:
                return True

            # Dropping into its parent (essentially a no-op)
            if dst_sn.node.is_parent_of(src_sn.node):
                return True

            # Dropping an ancestor onto its descendant:
            for dst_ancestor in dst_ancestor_list:
                if src_sn.node.node_identifier == dst_ancestor.node.node_identifier:
                    logger.debug(f'[{dst_tree_id}] Source node ({src_sn.spid}) is ancestor of dest ({dst_sn.spid}): no bueno')
                    return True

        return False

    def get_sn_for_guid(self, guid: GUID, tree_id: Optional[TreeID] = None) -> Optional[SPIDNodePair]:
        if tree_id:
            sn_list = self.get_sn_list_for_guid_list(guid_list=[guid], tree_id=tree_id)
            if sn_list:
                return sn_list[0]
            return None

        spid = self.backend.node_identifier_factory.from_guid(guid)
        return self.get_sn_for(node_uid=spid.node_uid, device_uid=spid.device_uid, full_path=spid.get_single_path())

    def get_sn_list_for_guid_list(self, guid_list: List[GUID], tree_id: TreeID) -> List[SPIDNodePair]:
        sn_list = []
        tree_meta: ActiveDisplayTreeMeta = self.get_active_display_tree_meta(tree_id)
        if not tree_meta:
            logger.error(f'Could not find tree: "{tree_id}"')
            return sn_list

        if tree_meta.change_tree:
            for guid in guid_list:
                sn = tree_meta.change_tree.get_sn_for_guid(guid)
                if sn:
                    sn_list.append(sn)
                else:
                    logger.error(f'[{tree_id}] Could not find node for GUID (skipping): "{guid}"')
        else:
            for guid in guid_list:
                spid = self.backend.node_identifier_factory.from_guid(guid)
                sn = self.get_sn_for(node_uid=spid.node_uid, device_uid=spid.device_uid, full_path=spid.get_single_path())
                if sn:
                    sn_list.append(sn)
                else:
                    logger.error(f'[{tree_id}] Could not build SN for GUID (skipping): "{guid}"')

        return sn_list

    # Various public methods
    # ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼

    def show_tree(self, subtree_root: NodeIdentifier) -> str:
        return self._get_store_for_device_uid(subtree_root.device_uid).show_tree(subtree_root)

    def _refresh_subtree(self, this_task: Task, node_identifier: NodeIdentifier, tree_id: TreeID):
        """Called asynchronously via task executor"""
        logger.debug(f'[{tree_id}] Refreshing subtree: {node_identifier}')
        self._get_store_for_device_uid(node_identifier.device_uid).refresh_subtree(this_task, node_identifier, tree_id)

    def get_last_pending_op_for_node(self, device_uid: UID, node_uid: UID) -> Optional[UserOp]:
        return self._op_ledger.get_last_pending_op_for_node(device_uid, node_uid)

    def enqueue_op_list(self, op_list: Iterable[UserOp]):
        """Attempt to add the given Ops to the execution tree. No need to worry whether some changes overlap or are redundant;
         the OpManager will sort that out - although it will raise an error if it finds incompatible changes such as adding to a tree
         that is scheduled for deletion."""
        self._op_ledger.append_new_pending_op_batch(op_list)

    def get_next_command(self) -> Optional[Command]:
        # blocks !
        self.wait_for_startup_done()
        # also blocks !
        return self._op_ledger.get_next_command()

    def get_next_command_nowait(self) -> Optional[Command]:
        # blocks !
        self.wait_for_startup_done()

        return self._op_ledger.get_next_command_nowait()

    def get_pending_op_count(self) -> int:
        return self._op_ledger.get_pending_op_count()

    def build_local_file_node(self, full_path: str, staging_path=None, must_scan_signature=False) -> Optional[LocalFileNode]:
        return self._this_disk_local_store.build_local_file_node(full_path, staging_path, must_scan_signature)

    def build_local_dir_node(self, full_path: str, is_live: bool = True, all_children_fetched: bool = False) -> LocalDirNode:
        return self._this_disk_local_store.build_local_dir_node(full_path, is_live, all_children_fetched=all_children_fetched)

    def build_gdrive_root_node(self, device_uid: UID, sync_ts: Optional[int] = None) -> GDriveNode:
        store = self._get_gdrive_store_for_device_uid(device_uid)
        return store.build_gdrive_root_node(sync_ts=sync_ts)

    def read_single_node(self, spid: SinglePathNodeIdentifier) -> Optional[Node]:
        store = self._get_store_for_device_uid(spid.device_uid)

        # Use in-memory cache if available:
        try:
            node: Node = store.get_node_for_uid(spid.node_uid)
            if node:
                return node
        except CacheNotLoadedError:
            pass

        # else read from disk
        if spid.tree_type == TreeType.LOCAL_DISK:
            full_path = spid.get_single_path()
            if not file_util.is_normalized(full_path):
                full_path = file_util.normalize_path(full_path)
                logger.debug(f'Normalized path: {full_path}')

            assert isinstance(store, LocalDiskMasterStore)
            node = store.read_single_node_for_path(full_path)
            if node:
                return node
            else:
                if store.device.uid == self._this_disk_local_store.device_uid:
                    # Local disk? Scan and add to cache
                    if os.path.isdir(full_path):
                        node = self.build_local_dir_node(full_path)
                    else:
                        node = self.build_local_file_node(full_path)
                    self.upsert_single_node(node)
                    return node
                else:
                    # out of luck
                    return None

        elif spid.tree_type == TreeType.GDRIVE:
            assert isinstance(store, GDriveMasterStore)
            node = store.read_single_node_from_disk_for_uid(spid.node_uid)
            if node:
                return node
            else:
                # try to recover the goog_id from the UID database
                goog_id = store.get_goog_id_for_uid(spid.node_uid)
                node: Optional[GDriveNode] = store.gdrive_client.get_existing_node_by_id(goog_id)
                if node:
                    return node
                else:
                    return None
        else:
            raise RuntimeError(f'Unrecognized tree type: {spid.tree_type}')
