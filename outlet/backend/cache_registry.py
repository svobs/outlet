import logging
import logging
import os
import subprocess
import threading
import uuid
from typing import Dict, List, Optional, Set

from pydispatch import dispatcher

from backend.executor.central import ExecPriority
from backend.sqlite.cache_registry_db import CacheRegistryDatabase
from backend.tree_store.gdrive.master_gdrive import GDriveMasterStore
from backend.tree_store.local.master_local import LocalDiskMasterStore
from backend.tree_store.tree_store_interface import TreeStore
from backend.uid.uid_mapper import UidGoogIdMapper, UidPathMapper
from constants import CACHE_LOAD_TIMEOUT_SEC, GDRIVE_INDEX_FILE_NAME, INDEX_FILE_SUFFIX, \
    IS_LINUX, IS_MACOS, IS_WINDOWS, MAIN_REGISTRY_FILE_NAME, NULL_UID, ROOT_PATH, \
    SUPER_DEBUG_ENABLED, SUPER_ROOT_DEVICE_UID, TRACE_ENABLED, TreeType, UID_GOOG_ID_FILE_NAME, \
    UID_PATH_FILE_NAME
from error import CacheNotFoundError
from model.cache_info import CacheInfoEntry, PersistedCacheInfo
from model.device import Device
from model.node.node import Node
from model.node_identifier import LocalNodeIdentifier, SinglePathNodeIdentifier
from model.uid import UID
from signal_constants import ID_GLOBAL_CACHE, Signal
from util import file_util, time_util
from util.has_lifecycle import HasLifecycle
from util.stopwatch_sec import Stopwatch
from util.task_runner import Task
from util.two_level_dict import TwoLevelDict

logger = logging.getLogger(__name__)


class CacheInfoByDeviceUid(TwoLevelDict):
    """
    ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
    CLASS CacheInfoByDeviceUid

    Holds PersistedCacheInfo objects
    ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼
    """

    def __init__(self):
        super().__init__(lambda x: x.subtree_root.device_uid, lambda x: x.subtree_root.get_path_list()[0], lambda x, y: True)


class CacheRegistry(HasLifecycle):
    """Internal to CacheManager."""

    def __init__(self, backend, cache_dir_path: str):
        HasLifecycle.__init__(self)
        self.backend = backend

        self.cache_dir_path = cache_dir_path
        self.main_registry_path = os.path.join(self.cache_dir_path, MAIN_REGISTRY_FILE_NAME)

        uid_path_cache_path = os.path.join(self.cache_dir_path, UID_PATH_FILE_NAME)
        self._uid_path_mapper = UidPathMapper(backend, uid_path_cache_path)
        """Officially, we allow for different devices to have different UIDs for a given path. But in practice, given a single agent, 
        all devices under its ownership will share the same UID-Path mapper, which means that the will all map the same UIDs to the same paths."""

        uid_goog_id_cache_path = os.path.join(self.cache_dir_path, UID_GOOG_ID_FILE_NAME)
        self._uid_goog_id_mapper = UidGoogIdMapper(backend, uid_goog_id_cache_path)
        """Same deal with GoogID mapper. We init it here, so that we can load in all the cached GoogIDs ASAP"""

        self._device_uuid: str = self._get_or_set_local_device_uuid()
        logger.debug(f'LocalDisk device UUID is: {self._device_uuid}')

        self._store_dict: Dict[UID, TreeStore] = {}

        self._this_disk_local_store: Optional[LocalDiskMasterStore] = None
        """Convenience pointer, for the disk on which the backend is running. Many operations (such as monitoring) can only be done for this store"""

        self._cache_info_dict: CacheInfoByDeviceUid = CacheInfoByDeviceUid()

        self._cached_device_list: List[Device] = []

        # (Optional) variables needed for producer/consumer behavior if Load All Caches needed
        self._load_all_caches_done: threading.Event = threading.Event()
        self._load_all_caches_in_process: bool = False

        # Create Event objects to optionally wait for lifecycle events
        self._load_registry_done: threading.Event = threading.Event()

    def start(self):
        # Get paths loaded ASAP, so we won't worry about creating duplicate paths or UIDs for them
        self._uid_path_mapper.start()
        # same deal with GoogIDs
        self._uid_goog_id_mapper.start()

        self._init_store_dict()

        # Load registry first. Do validation along the way
        self._load_registry()

        for store in self._store_dict.values():
            store.start()

        # Now load all caches (if configured):
        if self.backend.cacheman.load_all_caches_on_startup:
            load_all_caches_sw = Stopwatch()

            def notify_load_all_done(this_task):
                self._load_all_caches_in_process = False
                self._load_all_caches_done.set()
                logger.info(f'{load_all_caches_sw} Done loading all caches.')

            load_all_caches_task = Task(ExecPriority.P3_BACKGROUND_CACHE_LOAD, self._load_all_caches_start)
            load_all_caches_task.add_next_task(notify_load_all_done)
            self.backend.executor.submit_async_task(load_all_caches_task)
        else:
            logger.info('Configured not to load all caches on startup; will lazy load instead')

    def shutdown(self):
        logger.debug('CacheRegistryDatabase.shutdown() entered')

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
            if self._store_dict:
                for store in self._store_dict.values():
                    store.shutdown()
                self._store_dict = None
        except (AttributeError, NameError):
            pass

    # Init
    # ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼

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
            device = Device(NULL_UID, self._device_uuid, TreeType.LOCAL_DISK, "Local Disk")
            self._write_new_device(device)
            store = LocalDiskMasterStore(self.backend, self._uid_path_mapper, device)
            self._store_dict[device.uid] = store
            self._this_disk_local_store = store

        if master_gdrive:
            logger.info(f'Found master_gdrive in registry with device UID {master_gdrive.device_uid}')
        else:
            device = Device(NULL_UID, 'GDriveTODO', TreeType.GDRIVE, "My Google Drive")
            self._write_new_device(device)
            store = GDriveMasterStore(self.backend, device)
            self._store_dict[device.uid] = store

    def _load_registry(self):
        logger.debug('Loading registry')
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
            self.save_all_cache_info_to_disk()
            caches = self._cache_info_dict.get_all()
            self._overwrite_all_caches_in_registry(caches)

        self._load_registry_done.set()
        logger.info(f'{stopwatch} Done loading registry. Found {unique_cache_count} existing caches (+ {skipped_count} skipped)')

    def _wait_for_load_registry_done(self, fail_on_timeout: bool = True):
        if self._load_registry_done.is_set():
            return

        logger.debug(f'Waiting for Load Registry to complete')
        if not self._load_registry_done.wait(CACHE_LOAD_TIMEOUT_SEC):
            if fail_on_timeout:
                raise RuntimeError('Timed out waiting for CacheManager to finish loading the registry!')
            else:
                logger.error('Timed out waiting for CacheManager to finish loading the registry!')
        logger.debug(f'Load Registry completed')

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
        with CacheRegistryDatabase(self.main_registry_path, self.backend.node_identifier_factory) as db:
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
        with CacheRegistryDatabase(self.main_registry_path, self.backend.node_identifier_factory) as db:
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

            logger.info(f'{stopwatch} Init cache done: {cache_num}/{total_cache_count}: id={existing_disk_cache.subtree_root}')
        except RuntimeError:
            logger.exception(f'Failed to load cache: {existing_disk_cache.cache_location}')

    # Device stuff
    # ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼

    def _get_or_set_local_device_uuid(self) -> str:
        if IS_MACOS:
            logger.debug(f'_get_or_set_local_device_uuid(): looking for MacOS volume UUID...')
            entry_list = subprocess.check_output('/usr/sbin/diskutil info /'.split(' ')).decode().split('\n')[1:]
            prefix = 'Volume UUID:'
            for entry in entry_list:
                entry = entry.strip()
                if TRACE_ENABLED:
                    logger.debug(f'Checking diskutil entry: {entry}')
                if entry.startswith(prefix):
                    return entry.removeprefix(prefix).strip().lower()

            raise RuntimeError('Could not find Volume UUID for local MacOS device!')
        elif IS_LINUX:
            # logger.debug(f'_get_or_set_local_device_uuid(): looking for Linux volume UUID...')
            # TODO: https://stackoverflow.com/questions/4193514/how-to-get-hard-disk-serial-number-using-python
            pass
        elif IS_WINDOWS:
            logger.debug(f'_get_or_set_local_device_uuid(): looking for Windows disk serial number...')
            serial_list = subprocess.check_output('wmic diskdrive get SerialNumber'.split(' ')).decode().split('\n')[1:]
            serial_list = [s.strip() for s in serial_list if s.strip()]
            for entry in serial_list:
                entry = entry.strip()
                # TODO
                logger.info(f'SERIAL: {entry}')
        else:
            raise RuntimeError('Unknown local device!')

        # FIXME: use serial number instead. Ditch this whole file.
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
        return device_uuid

    # TODO: when do we create a new device?
    def upsert_device(self, device: Device):
        with CacheRegistryDatabase(self.main_registry_path, self.backend.node_identifier_factory) as db:
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
        with CacheRegistryDatabase(self.main_registry_path, self.backend.node_identifier_factory) as db:
            return db.get_device_list()

    def _write_new_device(self, device: Device):
        with CacheRegistryDatabase(self.main_registry_path, self.backend.node_identifier_factory) as db:
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

    # PersistedCacheInfo stuff
    # ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼

    def get_cache_info_for_subtree(self, subtree_root: SinglePathNodeIdentifier, create_if_not_found: bool = False) \
            -> Optional[PersistedCacheInfo]:
        """Finds the cache which contains the given subtree, if there is one.
        If create_if_not_found==True, then it will create & register a new cache and return that.
        If create_if_not_found==False, then it will raise CacheNotFoundError if no associated cache could be found.
        Note that this will also occur if a cache file was deleted, because such caches are detected and purged from the registry
        at startup."""

        self._wait_for_load_registry_done()

        if subtree_root.tree_type == TreeType.GDRIVE:
            # there is only 1 GDrive cache per GDrive account:
            cache_info = self._cache_info_dict.get_single(subtree_root.device_uid, ROOT_PATH)
        elif subtree_root.tree_type == TreeType.LOCAL_DISK:
            cache_info = self.get_existing_cache_info_for_local_path(subtree_root.device_uid, subtree_root.get_single_path())
        else:
            raise RuntimeError(f'Unrecognized tree type: {subtree_root.tree_type}')

        if not cache_info:
            if create_if_not_found:
                cache_info = self._create_new_cache_info(subtree_root)
            else:
                raise CacheNotFoundError(f'Could not find cache_info in memory for: {subtree_root} (and create_if_not_found=false)')

        return cache_info

    def get_existing_cache_info_for_local_path(self, device_uid: UID, full_path: str) -> Optional[PersistedCacheInfo]:
        # Wait for registry to finish loading before attempting to read dict. Shouldn't take long.
        self._wait_for_load_registry_done()

        for existing_cache in list(self._cache_info_dict.get_second_dict(device_uid).values()):
            # Is existing_cache an ancestor of target tree?
            if full_path.startswith(existing_cache.subtree_root.get_path_list()[0]):
                return existing_cache
        # Nothing in the cache contains subtree
        return None

    def save_all_cache_info_to_disk(self):
        """Overrites all entries in the CacheInfoRegistry with the entries in memory"""
        caches = self._cache_info_dict.get_all()
        self._overwrite_all_caches_in_registry(caches)

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

        with CacheRegistryDatabase(self.main_registry_path, self.backend.node_identifier_factory) as db:
            logger.info(f'Inserting new cache info into registry: {subtree_root}')
            db.insert_cache_info(db_entry, append=True, overwrite=False)

        cache_info = PersistedCacheInfo(db_entry)

        # Save reference in memory
        self._cache_info_dict.put_item(cache_info)

        return cache_info

    def ensure_cache_loaded_for_node_list(self, this_task: Task, node_list: List[Node]):
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
                cache: Optional[PersistedCacheInfo] = self.get_existing_cache_info_for_local_path(node.device_uid,
                                                                                                                      node.get_single_path())
                if cache:
                    needed_localdisk_cache_dict[cache.cache_location] = cache
                else:
                    raise RuntimeError(f'Could not find a cache file for planning node: {node}')

        # GDrive
        for gdrive_device_uid in needed_gdrive_device_uid_set:
            store = self.get_store_for_device_uid(gdrive_device_uid)
            assert isinstance(store, GDriveMasterStore)
            cache_load_task = this_task.create_child_task(store.load_and_sync_master_tree)
            assert cache_load_task.priority == ExecPriority.P3_BACKGROUND_CACHE_LOAD
            self.backend.executor.submit_async_task(cache_load_task)

        # LocalDisk:
        for cache in needed_localdisk_cache_dict.values():
            # load each cache one by one
            if not cache.is_loaded:
                if not os.path.exists(cache.subtree_root.get_single_path()):
                    raise RuntimeError(f'Could not load cache: dir does not exist: {cache.subtree_root.get_single_path()}')
                else:
                    assert isinstance(cache.subtree_root, LocalNodeIdentifier)
                    store = self.get_store_for_device_uid(cache.subtree_root.device_uid)
                    cache_load_task = this_task.create_child_task(store.load_subtree, cache.subtree_root, ID_GLOBAL_CACHE)
                    assert cache_load_task.priority == ExecPriority.P3_BACKGROUND_CACHE_LOAD
                    self.backend.executor.submit_async_task(cache_load_task)

    # TreeStore getters
    # ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼

    def get_store_for_device_uid(self, device_uid: UID) -> TreeStore:
        assert device_uid, f'get_store_for_device_uid(): device_uid not specified!'
        store: TreeStore = self._store_dict.get(device_uid, None)
        if not store:
            raise RuntimeError(f'get_tree_type(): no store found for device_uid: {device_uid}')
        return store

    def get_this_disk_local_store(self) -> Optional[LocalDiskMasterStore]:
        return self._this_disk_local_store

    # Path <-> PathUID
    # ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼

    def get_path_for_uid(self, uid: UID) -> str:
        """Throws exception if no path found"""
        return self._uid_path_mapper.get_path_for_uid(uid)

    def get_uid_for_path(self, full_path: str, uid_suggestion: Optional[UID] = None) -> UID:
        """Deterministically gets or creates a UID corresponding to the given path string"""
        assert full_path and isinstance(full_path, str), f'full_path value is invalid: {full_path}'
        return self._uid_path_mapper.get_uid_for_path(full_path, uid_suggestion)
