import functools
import logging
import os
import threading
from collections import deque
from typing import Deque, Dict, List, Optional, Tuple
from uuid import UUID

from pydispatch import dispatcher

from backend.display_tree.filter_state import FilterState
from backend.executor.central import ExecPriority
from backend.sqlite.gdrive_db import CurrentDownload
from backend.tree_store.gdrive.change_observer import GDriveChange, PagePersistingChangeObserver
from backend.tree_store.gdrive.gdrive_client import GDriveClient
from backend.tree_store.gdrive.gdrive_tree_loader import GDriveTreeLoader
from backend.tree_store.gdrive.master_gdrive_disk import GDriveDiskStore
from backend.tree_store.gdrive.master_gdrive_memory import GDriveMemoryStore
from backend.tree_store.gdrive.master_gdrive_op_load import GDriveDiskLoadOp
from backend.tree_store.gdrive.master_gdrive_op_write import BatchChangesOp, CreateUserOp, DeleteAllDataOp, DeleteSingleNodeOp, DeleteSubtreeOp, \
    GDriveWriteThroughOp, RefreshFolderOp, UpsertMimeTypeOp, UpsertSingleNodeOp
from backend.tree_store.tree_store_interface import TreeStore
from backend.uid.uid_mapper import UidGoogIdMapper
from constants import GDRIVE_DOWNLOAD_TYPE_CHANGES, GDRIVE_ME_USER_UID, GDRIVE_ROOT_UID, ROOT_PATH, \
    SUPER_DEBUG_ENABLED, \
    TRACE_ENABLED, TrashStatus, \
    TreeID
from error import CacheNotLoadedError, NodeNotPresentError
from global_actions import GlobalActions
from model.device import Device
from model.gdrive_meta import GDriveUser, MimeType
from model.node.directory_stats import DirectoryStats
from model.node.gdrive_node import GDriveFile, GDriveFolder, GDriveNode
from model.node.node import Node, SPIDNodePair
from model.node_identifier import GDriveIdentifier, GDriveSPID, NodeIdentifier, SinglePathNodeIdentifier
from model.uid import UID
from signal_constants import ID_GLOBAL_CACHE, Signal
from util import file_util, time_util
from util.stopwatch_sec import Stopwatch
from util.task_runner import Task

logger = logging.getLogger(__name__)


class RefreshSubtreeCompoundTask:
    """Creates a chain of tasks, each of which executes the refresh_next_folder() method"""
    def __init__(self, master_gdrive, subtree_root: GDriveIdentifier, tree_id: TreeID):
        self.master_gdrive = master_gdrive
        self.subtree_root: GDriveIdentifier = subtree_root
        self.stats_sw = Stopwatch()
        self.folders_to_process: Deque[GDriveFolder] = deque()
        self.count_folders: int = 0
        self.count_total: int = 0
        self.tree_id: TreeID = tree_id

    def refresh_next_folder(self, this_task: Task):
        if TRACE_ENABLED:
            logger.debug(f'Entered refresh_next_folder()')

        if len(self.folders_to_process) > 0:
            folder: GDriveFolder = self.folders_to_process.popleft()
            child_list: List[GDriveNode] = self.master_gdrive.fetch_and_merge_child_nodes_for_parent(folder)
            self.count_folders += 1
            self.count_total += len(child_list)

            for child in child_list:
                if child.is_dir():
                    assert isinstance(child, GDriveFolder)
                    self.folders_to_process.append(child)

            this_task.add_next_task(self.refresh_next_folder)
        else:
            logger.info(f'[{self.tree_id}] {self.stats_sw} Refresh subtree complete (SubtreeRoot={self.subtree_root} '
                        f'Folders={self.count_folders} Total={self.count_total})')


def ensure_locked(func):
    """DECORATOR: make sure _struct_lock is locked when executing func. If it is not currently locked,
    then lock it around the function.

    Although this is outside the GDriveMasterStore, it is actually part of it! Kind of a fudge to make "self" work
    """

    @functools.wraps(func)
    def wrapper(self, *args, **kwargs):
        if self._struct_lock.locked():
            return func(self, *args, **kwargs)
        else:
            with self._struct_lock:
                return func(self, *args, **kwargs)

    return wrapper


class GDriveMasterStore(TreeStore):
    """
    ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
    CLASS GDriveMasterStore

    Singleton in-memory cache for Google Drive

    Some notes:
    - Need to create a store which can keep track of whether each parent has all children. If not we
    will have to make a request to retrieve all nodes with 'X' as parent and update the store before
    returning

    - GoogRemote >= GoogDiskStores >= GoogInMemoryStore >= DisplayStore

    - GoogDiskCache should try to download all dirs & files ASAP. But in the meantime, download level by level

    - Every time you expand a node, you should call to sync it from the GoogStore.
    - Every time you retrieve new data from G, you must perform sanity checks on it

    # TODO: support drag & drop from GDrive to GDrive (e.g. "move" is really just changing parents)
    ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼
    """
    def __init__(self, backend, goog_id_mapper: UidGoogIdMapper, device: Device):
        TreeStore.__init__(self, device)
        self.backend = backend

        self._is_sync_in_progress: bool = False
        self._another_sync_requested = False  # this helps ensure that at most one sync is in progress and one sync is queued at any given time

        self._uid_mapper: UidGoogIdMapper = goog_id_mapper
        """Single source of UID<->GoogID mappings and UID assignments. Thread-safe."""

        self._struct_lock = threading.Lock()
        """Used to protect structures inside memstore"""
        self._memstore: GDriveMemoryStore = GDriveMemoryStore(backend, self._uid_mapper, device.uid)
        self._diskstore: GDriveDiskStore = GDriveDiskStore(backend, self._memstore, device.uid)
        self.gdrive_client: GDriveClient = GDriveClient(self.backend, gdrive_store=self, device_uid=self.device_uid, tree_id=ID_GLOBAL_CACHE)
        self.tree_loader = GDriveTreeLoader(backend=self.backend, diskstore=self._diskstore, gdrive_client=self.gdrive_client,
                                            device_uid=device.uid, tree_id=ID_GLOBAL_CACHE)

        self._load_master_cache_in_process_task_uuid: Optional[UUID] = None

        self.download_dir = file_util.get_resource_path(self.backend.get_config('agent.local_disk.download_dir'))

    def start(self):
        logger.debug(f'Starting GDriveMasterStore')
        TreeStore.start(self)
        self._diskstore.start()
        self.gdrive_client.start()
        self.connect_dispatch_listener(signal=Signal.SYNC_GDRIVE_CHANGES, receiver=self._on_gdrive_sync_changes_requested)

        if self._diskstore.needs_full_reload:
            self.download_all_gdrive_data()

    def shutdown(self):
        # disconnects all listeners:
        super(GDriveMasterStore, self).shutdown()

        try:
            if self.gdrive_client:
                self.gdrive_client.shutdown()
                self.gdrive_client = None
        except (AttributeError, NameError):
            pass

        try:
            self.backend = None
        except (AttributeError, NameError):
            pass

    def is_gdrive(self) -> bool:
        return True

    def get_gdrive_client(self) -> GDriveClient:
        return self.gdrive_client

    def execute_load_op(self, operation: GDriveDiskLoadOp):
        """Executes a single GDriveDiskLoadOp ({start}->disk->memory"""

        # 1. Load from disk store
        self._diskstore.execute_load_op(operation)

        # 2. Update memory store
        operation.update_memstore(self._memstore)

    @ensure_locked
    def _execute_write_op(self, operation: GDriveWriteThroughOp):
        """Executes a single GDriveWriteThroughOp ({start}->memory->disk->UI)"""

        # 1. Update memory store
        operation.update_memstore(self._memstore)

        # 2. Update disk store
        self._diskstore.execute_write_op(operation)

        # 3. Send signals
        operation.send_signals()

    # Tree-wide stuff
    # ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼

    def download_all_gdrive_data(self):
        """See below. Wipes any existing disk cache and replaces it with a complete fresh download from the GDrive servers."""
        self.backend.executor.submit_async_task(Task(ExecPriority.P3_BACKGROUND_CACHE_LOAD, self._download_all_gdrive_meta))

    def _download_all_gdrive_meta(self, this_task: Task):
        """See above. Executed by Task Runner. NOT UI thread"""
        logger.info(f'Downloading all GDrive meta (device_uid={self.device_uid})')
        try:
            """Wipes any existing disk cache and replaces it with a complete fresh download from the GDrive servers."""
            self.load_and_sync_master_tree(this_task, invalidate_cache=True)
        except Exception as err:
            logger.exception(err)
            GlobalActions.display_error_in_ui(ID_GLOBAL_CACHE, 'Download from GDrive failed due to unexpected error', repr(err))

    def load_and_sync_master_tree(self, this_task: Task, invalidate_cache: bool = False):
        """This will sync the latest changes as child tasks."""
        self._load_master_cache(this_task=this_task, sync_latest_changes=True, invalidate_cache=invalidate_cache)

    def _load_master_cache(self, this_task: Task, invalidate_cache: bool, sync_latest_changes: bool):
        """Loads an EXISTING GDrive cache from disk and updates the in-memory cache from it"""

        logger.debug(f'Entered _load_master_cache(): locked={self._struct_lock.locked()}, invalidate_cache={invalidate_cache}, '
                     f'sync_latest_changes={sync_latest_changes}')
        assert this_task.priority == ExecPriority.P3_BACKGROUND_CACHE_LOAD, f'Wrong priority for task: {this_task.priority}'

        if self._load_master_cache_in_process_task_uuid:
            if self.backend.executor.is_task_or_descendent_running(self._load_master_cache_in_process_task_uuid):
                logger.info(f'GDrive master cache is already loading (task {self._load_master_cache_in_process_task_uuid}); '
                            f'placing this task back in the queue')
                this_task.add_next_task(self._load_master_cache, invalidate_cache, sync_latest_changes)
                return
            else:
                logger.debug(f'_load_master_cache(): could not find task {self._load_master_cache_in_process_task_uuid}; assuming it has completed')
                self._load_master_cache_in_process_task_uuid = None

        try:
            self._load_master_cache_in_process_task_uuid = this_task.task_uuid
            if not self._is_sync_in_progress and sync_latest_changes:
                self._is_sync_in_progress = True
                self._another_sync_requested = False

            if not self._memstore.master_tree or invalidate_cache:
                # LOAD TREE
                if self._memstore.master_tree:
                    logger.debug(f'Master tree not loaded: will load tree into memory')
                else:
                    logger.debug(f'Master tree is loaded but invalidate_cache={invalidate_cache}')

                stopwatch_total = Stopwatch()

                def _after_tree_loaded(tree):
                    assert tree
                    self._memstore.master_tree = tree
                    logger.debug(f'GDrive master tree completely loaded; device_uid={self._memstore.master_tree.device_uid}')

                    logger.info(f'{stopwatch_total} GDrive master tree loaded')

                load_all_task = this_task.create_child_task(self.tree_loader.load_all, invalidate_cache, _after_tree_loaded)
                self.backend.executor.submit_async_task(load_all_task)
            else:
                logger.debug(f'Master tree already loaded, and invalidate_cache={invalidate_cache}')

            if sync_latest_changes:
                sync_changes_task = this_task.create_child_task(self._sync_latest_gdrive_changes)
                self.backend.executor.submit_async_task(sync_changes_task)

        except Exception:
            self._load_master_cache_in_process_task_uuid = None
            self._is_sync_in_progress = False
            raise

    @ensure_locked
    def _sync_latest_gdrive_changes(self, this_task: Task):
        logger.debug(f'Entered sync_latest_changes(): locked={self._struct_lock.locked()}')

        if not self._memstore.master_tree:
            logger.warning(f'GDrive master tree is not loaded! Aborting sync.')
            return

        changes_download: CurrentDownload = self._diskstore.get_current_download(GDRIVE_DOWNLOAD_TYPE_CHANGES)
        if not changes_download:
            raise RuntimeError(f'Download state not found for GDrive change log!')

        if not changes_download.page_token:
            # covering all our bases here in case we are recovering from corruption
            changes_download.page_token = self.gdrive_client.get_changes_start_token()

        observer: PagePersistingChangeObserver = PagePersistingChangeObserver(self, changes_download.page_token, parent_task=this_task)
        self.gdrive_client.get_changes_list(observer)

        if self._another_sync_requested:
            logger.debug(f'sync_latest_changes(): Another sync was requested. Recursing...')
            self._another_sync_requested = False
            this_task.add_next_task(self._sync_latest_gdrive_changes)

    # Action listener callbacks
    # ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼

    def _on_gdrive_sync_changes_requested(self, sender):
        """See below. This will load the GDrive tree (if it is not loaded already), then sync to the latest changes from GDrive.
        If a sync runs longer than the polling interval, then prevent buildup of multiple requests by just setting a polite flag which reqeuests
        a sync after the current one."""
        logger.debug(f'Received signal: "{Signal.SYNC_GDRIVE_CHANGES.name}" '
                     f'(is_sync_in_prgress={self._is_sync_in_progress}, another_sync_requested={self._another_sync_requested})')

        # Prevent possible buildup of requests
        if self._is_sync_in_progress:
            self._another_sync_requested = True
        else:
            self.backend.executor.submit_async_task(Task(ExecPriority.P3_BACKGROUND_CACHE_LOAD, self.load_and_sync_master_tree))

    # Subtree-level stuff
    # ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼

    def load_subtree(self, this_task: Task, subtree_root: SinglePathNodeIdentifier, tree_id: TreeID):
        self._load_master_cache(this_task=this_task, sync_latest_changes=self.backend.cacheman.sync_from_gdrive_on_cache_load, invalidate_cache=False)

    def is_cache_loaded_for(self, subtree_root: SinglePathNodeIdentifier) -> bool:
        # very easy: either our whole cache is loaded or it is not
        return self._memstore.master_tree is not None

    @ensure_locked
    def generate_dir_stats(self, subtree_root_node: GDriveFolder, tree_id: TreeID) -> Dict[UID, DirectoryStats]:
        if SUPER_DEBUG_ENABLED:
            logger.debug(f'Entered generate_dir_stats(): locked={self._struct_lock.locked()}')
        return self._memstore.master_tree.generate_dir_stats(tree_id=tree_id, subtree_root_node=subtree_root_node)

    def populate_filter(self, filter_state: FilterState):
        filter_state.ensure_cache_populated(self._memstore.master_tree)

    @ensure_locked
    def submit_batch_of_changes(self, subtree_root: GDriveIdentifier,  upsert_node_list: List[GDriveNode] = None,
                                remove_node_list: List[GDriveNode] = None):
        self._execute_write_op(BatchChangesOp(self.backend, upsert_node_list))
        self._execute_write_op(BatchChangesOp(self.backend, remove_node_list))

    def refresh_subtree(self, this_task: Task, subtree_root: GDriveIdentifier, tree_id: TreeID):
        # Call into client to get folder. Set has_all_children=False at first, then set to True when it's finished.
        logger.debug(f'[{tree_id}] Refresh requested. Querying GDrive for latest version of parent folder ({subtree_root})')

        refresh_task = RefreshSubtreeCompoundTask(self, subtree_root, tree_id)

        subtree_root_node = self.get_node_for_uid(subtree_root.node_uid)

        if not subtree_root_node:
            raise RuntimeError(f'Cannot refresh subtree for GDrive: could not find node in cache matching: {subtree_root}')

        if not subtree_root_node.goog_id:
            raise RuntimeError(f'Cannot refresh subtree for GDrive node: no goog_id for subtree root node: {subtree_root_node}')

        parent_node: Optional[GDriveNode] = self.gdrive_client.get_existing_node_by_id(subtree_root_node.goog_id)
        if not parent_node:
            # TODO: better handling
            raise RuntimeError(f'Node with goog_id "{subtree_root_node.goog_id}" was not found in Google Drive: {subtree_root_node}')
        if not parent_node.is_dir():
            self._execute_write_op(UpsertSingleNodeOp(parent_node))
            return

        assert isinstance(parent_node, GDriveFolder)
        parent_node.node_identifier.set_path_list(subtree_root.get_path_list())

        refresh_task.folders_to_process.append(parent_node)

        child_task = this_task.create_child_task(refresh_task.refresh_next_folder)
        self.backend.executor.submit_async_task(child_task)

    def fetch_and_merge_child_nodes_for_parent(self, folder: GDriveFolder) -> List[GDriveNode]:
        """Fetches all the children for the given GDriveFolder from the GDrive client, and merge the updated nodes into our cache"""
        logger.debug(f'Querying GDrive for children of folder ({folder})')
        child_list: List[GDriveNode] = self.gdrive_client.get_all_children_for_parent(folder.goog_id)
        # Derive paths with some cleverness:
        for child in child_list:
            child_path_list = []
            for path in folder.get_path_list():
                child_path_list.append(os.path.join(path, child.name))
            child.node_identifier.set_path_list(child_path_list)

        folder.all_children_fetched = True

        # This will write into the memory & disk caches, and notify FEs of updates
        self._execute_write_op(RefreshFolderOp(self.backend, folder, child_list))

        return child_list

    def show_tree(self, subtree_root: GDriveIdentifier) -> str:
        return self._memstore.master_tree.show_tree(subtree_root.node_uid)

    # Individual node cache updates
    # ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼

    @ensure_locked
    def upsert_single_node(self, node: GDriveNode) -> GDriveNode:
        if SUPER_DEBUG_ENABLED:
            logger.debug(f'Entered upsert_single_node(): locked={self._struct_lock.locked()}')

        logger.debug(f'Upserting GDrive node to caches: {node}')
        write_op = UpsertSingleNodeOp(node)
        self._execute_write_op(write_op)
        return write_op.node

    def update_single_node(self, node: GDriveNode) -> GDriveNode:
        if SUPER_DEBUG_ENABLED:
            logger.debug(f'Entered update_single_node(): locked={self._struct_lock.locked()}')
        write_op = UpsertSingleNodeOp(node, update_only=True)
        logger.debug(f'Updating GDrive node in caches: {node}')
        self._execute_write_op(write_op)

        return write_op.node

    def remove_subtree(self, subtree_root: GDriveNode, to_trash):
        assert isinstance(subtree_root, GDriveNode), f'For node: {subtree_root}'

        if subtree_root.is_dir():
            if SUPER_DEBUG_ENABLED:
                logger.debug(f'remove_subtree(): locked={self._struct_lock.locked()}')
            with self._struct_lock:
                subtree_nodes: List[GDriveNode] = self._memstore.master_tree.get_subtree_bfs(subtree_root.uid)
                logger.info(f'Removing subtree with {len(subtree_nodes)} nodes (to_trash={to_trash})')
                self._execute_write_op(DeleteSubtreeOp(subtree_root, node_list=subtree_nodes, to_trash=to_trash))
        else:
            logger.debug(f'Requested subtree is not a folder; calling remove_single_node()')
            self.remove_single_node(subtree_root, to_trash=to_trash)

    @ensure_locked
    def remove_single_node(self, node: GDriveNode, to_trash) -> Optional[GDriveNode]:
        logger.debug(f'Removing node from caches: {node}')

        if to_trash:
            if node.get_trashed_status().not_trashed():
                raise RuntimeError(f'Trying to trash Google node which is not marked as trashed: {node}')
            # this is actually an update
            return self.upsert_single_node(node)
        else:
            self._execute_write_op(DeleteSingleNodeOp(node, to_trash))
            return None

    # Various public methods
    # ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼

    def download_file_from_gdrive(self, this_task: Task, node_uid: UID, requestor_id: str):
        node: GDriveNode = self.get_node_for_uid(node_uid)
        if not node:
            raise RuntimeError(f'Could not download file from GDrive: node with UID not found: {node_uid}')

        os.makedirs(name=self.download_dir, exist_ok=True)
        dest_file = os.path.join(self.download_dir, node.name)

        try:
            self.gdrive_client.download_file(node.goog_id, dest_file)
            # notify async when done:
            dispatcher.send(signal=Signal.DOWNLOAD_FROM_GDRIVE_DONE, sender=requestor_id, filename=dest_file)
        except Exception as err:
            self.backend.report_error(ID_GLOBAL_CACHE, 'Download failed', repr(err))
            raise

    def apply_gdrive_changes(self, gdrive_change_list: List[GDriveChange], new_page_token: str):
        logger.debug(f'Applying {len(gdrive_change_list)} GDrive changes...')
        operation: BatchChangesOp = BatchChangesOp(self.backend, gdrive_change_list)

        try:
            self._execute_write_op(operation)

            # Update download token so we don't repeat work upon premature termination:

            if not new_page_token:
                # End of changes list: get new start token for next time, and so we don't submit our prev token again:
                new_page_token = self.gdrive_client.get_changes_start_token()

            if new_page_token:
                logger.debug(f'Updating changes download with token: {new_page_token}')
                self._diskstore.update_changes_download_start_token(new_page_token)
        except RuntimeError:
            logger.error(f'While executing GDrive change list: {gdrive_change_list}')
            raise

    def get_goog_id_list_for_uid_list(self, uid_list: List[UID], fail_if_missing: bool = True) -> List[str]:
        try:
            return self._memstore.master_tree.resolve_uids_to_goog_ids(uid_list, fail_if_missing=fail_if_missing)
        except RuntimeError:
            # Unresolved UIDs. This can happen when one cache's node refers to a parent which no longer exists...
            # TODO: let's make this even more robust by keeping track of tombstones
            logger.debug(f'Failed to find UIDs in master tree; assuming they were deleted. Trying uid_mapper...')
            goog_id_list: List[str] = []
            for uid in uid_list:
                goog_id = self._uid_mapper.get_goog_id_for_uid(uid)
                if goog_id:
                    goog_id_list.append(goog_id)
                else:
                    raise RuntimeError(f'Could not find goog_id for UID: {uid}')
            return goog_id_list

    def get_uid_list_for_goog_id_list(self, goog_ids: List[str]) -> List[UID]:
        uid_list = []
        for goog_id in goog_ids:
            uid_list.append(self._uid_mapper.get_uid_for_goog_id(goog_id))

        return uid_list

    def get_node_list_for_path_list(self, path_list: List[str]) -> List[GDriveNode]:
        return self._memstore.master_tree.get_node_list_for_path_list(path_list)

    def get_uid_for_domain_id(self, domain_id: str, uid_suggestion: Optional[UID] = None) -> UID:
        return self.get_uid_for_goog_id(domain_id, uid_suggestion)

    def get_uid_for_goog_id(self, goog_id: str, uid_suggestion: Optional[UID] = None) -> UID:
        return self._uid_mapper.get_uid_for_goog_id(goog_id, uid_suggestion)

    def get_node_for_goog_id(self, goog_id: str) -> Optional[GDriveNode]:
        uid = self._uid_mapper.get_uid_for_goog_id(goog_id)
        return self._memstore.master_tree.get_node_for_uid(uid)

    def get_node_for_domain_id(self, goog_id: str) -> Optional[GDriveNode]:
        return self.get_node_for_goog_id(goog_id)

    def get_node_for_uid(self, uid: UID) -> Optional[GDriveNode]:
        if not self._memstore.is_loaded():
            raise CacheNotLoadedError(f'Cannot retrieve node (UID={uid}(: GDrive cache not loaded!')
        return self._memstore.master_tree.get_node_for_uid(uid)

    def read_single_node_from_disk_for_uid(self, uid: UID) -> Optional[Node]:
        logger.debug(f'Loading single node for uid: {uid}')
        return self._diskstore.get_single_node_with_uid(uid)

    def build_gdrive_root_node(self, sync_ts: Optional[int] = None) -> GDriveFolder:
        # basically a fake / logical node which serves as the parent of My GDrive, shares, etc.
        node_identifier = self.backend.node_identifier_factory.get_root_constant_gdrive_identifier(self.device_uid)
        return GDriveFolder(node_identifier, None, ROOT_PATH, TrashStatus.NOT_TRASHED, None, None,
                            GDRIVE_ME_USER_UID, None, False, None, sync_ts=sync_ts, all_children_fetched=False)

    def get_goog_id_for_uid(self, uid: UID) -> Optional[str]:
        try:
            return self._uid_mapper.get_goog_id_for_uid(uid)
        except RuntimeError as e:
            logger.debug(f'get_goog_id_for_uid(): error getting value for UID {uid}: {e}')
            return None

    def to_sn(self, node: GDriveNode, single_path: str) -> SPIDNodePair:
        spid = self.backend.node_identifier_factory.for_values(uid=node.uid, device_uid=node.device_uid, tree_type=node.tree_type,
                                                               path_list=single_path, must_be_single_path=True)
        return SPIDNodePair(spid, node)

    def to_sn_from_node_and_parent_spid(self, node: GDriveNode, parent_spid: SinglePathNodeIdentifier) -> SPIDNodePair:
        # derive single child path from single parent path
        child_path: str = os.path.join(parent_spid.get_single_path(), node.name)
        assert child_path, f'derived child_path is empty for parent_spid: {parent_spid}'
        # Yuck...this is more expensive than preferred... at least there's no network call
        return self.to_sn(node, child_path)

    def get_child_list_for_spid(self, parent_spid: SinglePathNodeIdentifier, filter_state: Optional[FilterState]) -> List[SPIDNodePair]:
        """If the in-memory store is loaded, will return results from that.
        If it is not yet loaded, will try the disk store and return results from that.
        Failing both of those, will perform a read-through of the GDrive API and update the disk & memory cache before returning."""
        if TRACE_ENABLED:
            logger.debug(f'Entered get_child_list_for_spid(): spid={parent_spid} filter_state={filter_state} locked={self._struct_lock.locked()}')
        assert isinstance(parent_spid, GDriveSPID), f'Expected GDriveSPID but got: {type(parent_spid)}: {parent_spid}'

        # 0. Special case if filtered (in-memory cache MUST be loaded already):
        if filter_state and filter_state.has_criteria():
            if SUPER_DEBUG_ENABLED:
                logger.debug(f'get_child_list_for_spid(): getting child list from filter_state')
            if not self.is_cache_loaded_for(parent_spid):
                raise RuntimeError(f'Cannot load filtered child list: GDrive cache not yet loaded!')
            return filter_state.get_filtered_child_list(parent_spid, self._memstore.master_tree)

        # ------------------------------------------------------------------------------------
        # I. PARENT:
        parent_node = None
        # 1. Use in-memory cache if it exists:
        if self._memstore.is_loaded():
            parent_node = self._memstore.master_tree.get_node_for_uid(parent_spid.node_uid)

        # 2. Try disk cache if it exists:
        if not parent_node:
            if SUPER_DEBUG_ENABLED:
                logger.debug(f'get_child_list_for_spid(): parent node {parent_spid.node_uid} not found in memory cache; checking disk cache')
            parent_node = self._diskstore.get_single_node_with_uid(parent_spid.node_uid)

        # 3. Consult GDrive
        if not parent_node:
            if SUPER_DEBUG_ENABLED:
                logger.debug(f'get_child_list_for_spid(): parent node {parent_spid.node_uid} not found in disk cache; querying GDrive')
            goog_id = self._uid_mapper.get_goog_id_for_uid(parent_spid.node_uid)
            if not goog_id:
                # The UID mapper must at least know about the parent:
                raise RuntimeError(f'Could not get child list for node: could resolve node UID {parent_spid.node_uid} in caches')
            parent_node = self.gdrive_client.get_existing_node_by_id(goog_id=goog_id)

        if not parent_node:
            raise RuntimeError(f'Could not get child list for node: could not find node anywhere in caches '
                               f'and could not find node in Google Drive: {parent_spid}')

        if not parent_node.is_dir():
            logger.error(f'get_child_list_for_spid(): requested parent is not a dir: {parent_spid}')
            return []

        # ------------------------------------------------------------------------------------
        # II. CHILDREN:
        if parent_node.all_children_fetched:
            # 1. Use in-memory cache if it exists:
            if self._memstore.is_loaded():
                if SUPER_DEBUG_ENABLED:
                    logger.debug(f'get_child_list_for_spid(): getting child list from in-memory cache (parent_spid={parent_spid})')
                try:
                    return self._memstore.master_tree.get_child_list_for_spid(parent_spid)
                except NodeNotPresentError as e:
                    # In-memory cache miss. Try seeing if the relevant cache is loaded:
                    logger.debug(f'Could not find node in in-memory cache: {parent_spid}')
                    pass

            # 2. Try disk cache if it exists:
            if SUPER_DEBUG_ENABLED:
                logger.debug(f'get_child_list_for_spid(): getting child list from disk cache: {parent_node}')
            child_node_list: List[GDriveNode] = self._diskstore.get_child_list_for_parent_uid(parent_spid.node_uid)
            for child_node in child_node_list:
                # Need to fill in at least one path:
                child_node.node_identifier.add_path_if_missing(os.path.join(parent_spid.get_single_path(), child_node.name))
        else:
            # 3. Children not fetched. Must resort to a slow-ass GDrive API request:
            logger.warning(f'get_child_list_for_spid(): found node in cache but children not fetched: "{parent_spid}"; will query GDrive')

            if parent_node.uid == GDRIVE_ROOT_UID:
                # This appears to be a limitation of Google Drive
                raise RuntimeError(f'Cannot determine topmost nodes of Google Drive until entire tree has been downloaded!')
            child_node_list: List[GDriveNode] = self.fetch_and_merge_child_nodes_for_parent(parent_node)

        return [self.to_sn_from_node_and_parent_spid(child_node, parent_spid) for child_node in child_node_list]

    def get_parent_for_sn(self, sn: SPIDNodePair) -> Optional[SPIDNodePair]:
        return self._memstore.master_tree.get_parent_for_sn(sn)

    def get_parent_list_for_node(self, node: GDriveNode) -> List[GDriveNode]:
        if self._memstore.is_loaded():
            return self._memstore.master_tree.get_parent_list_for_node(node)
        else:
            logger.error(f'Failing for node: {node}')
            raise CacheNotLoadedError(f'get_parent_list_for_node(): GDrive cache not loaded yet!')

    def get_identifier_list_for_full_path_list(self, path_list: List[str], error_if_not_found: bool = False) -> List[NodeIdentifier]:
        if not self._memstore.is_loaded():
            # Just fail for now:
            raise RuntimeError(f'Cannot get path list ({path_list}): Google Drive tree is not yet loaded! (device_uid={self.device_uid})')
        return self._memstore.master_tree.get_identifier_list_for_path_list(path_list, error_if_not_found)

    def get_all_files_and_dirs_for_subtree(self, subtree_root: GDriveIdentifier) -> Tuple[List[GDriveFile], List[GDriveFolder]]:
        return self._memstore.master_tree.get_all_files_and_folders_for_subtree(subtree_root)

    def get_gdrive_user_for_permission_id(self, permission_id: str) -> GDriveUser:
        if TRACE_ENABLED:
            logger.debug(f'Entered get_gdrive_user_for_permission_id()')
        return self._memstore.get_gdrive_user_for_permission_id(permission_id)

    def get_gdrive_user_for_user_uid(self, uid: UID) -> GDriveUser:
        # if SUPER_DEBUG_ENABLED:
        #     logger.debug(f'Entered get_gdrive_user_for_user_uid()')
        return self._memstore.get_gdrive_user_for_user_uid(uid)

    def create_gdrive_user(self, user: GDriveUser):
        if TRACE_ENABLED:
            logger.debug(f'Entered create_gdrive_user(): locked={self._struct_lock.locked()}')
        op = CreateUserOp(user)
        self._execute_write_op(op)

    def get_or_create_gdrive_mime_type(self, mime_type_string: str) -> MimeType:
        # Note: this operation must be synchronous, so that it can return the MIME type
        if TRACE_ENABLED:
            logger.debug(f'Entered get_or_create_gdrive_mime_type(): locked={self._struct_lock.locked()}')

        op = UpsertMimeTypeOp(mime_type_string)
        self._execute_write_op(op)
        return op.mime_type

    def get_mime_type_for_uid(self, uid: UID) -> Optional[MimeType]:
        if SUPER_DEBUG_ENABLED:
            logger.debug(f'Entered get_mime_type_for_uid()')
        return self._memstore.get_mime_type_for_uid(uid)

    def delete_all_gdrive_data(self):
        if SUPER_DEBUG_ENABLED:
            logger.debug(f'Entered delete_all_gdrive_data(): locked={self._struct_lock.locked()}')
        self._execute_write_op(DeleteAllDataOp())
