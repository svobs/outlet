import collections
import logging
import os
import pathlib
import threading
from collections import deque
from typing import Callable, Deque, Dict, List, Optional, Set, Tuple

from pydispatch import dispatcher

from backend.cache_registry import CacheRegistry
from backend.diff.transfer_maker import TransferMaker
from backend.display_tree.active_tree_manager import ActiveTreeManager
from backend.display_tree.active_tree_meta import ActiveDisplayTreeMeta
from backend.display_tree.change_tree import ChangeTree
from backend.display_tree.row_state_tracking import RowStateTracking
from backend.executor.central import ExecPriority
from backend.executor.command.cmd_interface import Command
from backend.executor.user_op.op_manager import OpManager
from backend.tree_store.gdrive.master_gdrive import GDriveMasterStore
from backend.tree_store.gdrive.master_gdrive_op_load import GDriveDiskLoadOp
from backend.tree_store.local.sig_calc_thread import SigCalcBatchingThread
from constants import CACHE_LOAD_TIMEOUT_SEC, DirConflictPolicy, DragOperation, FileConflictPolicy, GDRIVE_ROOT_UID, IconId, \
    OPS_FILE_NAME, ROOT_PATH, \
    SUPER_DEBUG_ENABLED, TRACE_ENABLED, TreeDisplayMode, TreeID, TreeLoadState, TreeType
from error import ResultsExceededError
from model.cache_info import PersistedCacheInfo
from model.display_tree.build_struct import DisplayTreeRequest, RowsOfInterest
from model.display_tree.display_tree import DisplayTree
from model.display_tree.filter_criteria import FilterCriteria
from model.display_tree.summary import TreeSummarizer
from model.node.gdrive_node import GDriveNode
from model.node.local_disk_node import LocalDirNode, LocalFileNode
from model.node.node import Node, SPIDNodePair
from model.node_identifier import GUID, LocalNodeIdentifier, NodeIdentifier, SinglePathNodeIdentifier
from model.uid import UID
from model.user_op import UserOp, UserOpType
from signal_constants import ID_GDRIVE_DIR_SELECT, ID_GLOBAL_CACHE, Signal
from util import file_util
from util.ensure import ensure_list
from util.file_util import get_resource_path
from util.has_lifecycle import HasLifecycle
from util.task_runner import Task

logger = logging.getLogger(__name__)


def ensure_cache_dir_exists(backend):
    cache_dir_path = get_resource_path(backend.get_config('cache.cache_dir_path'))
    if not os.path.exists(cache_dir_path):
        logger.info(f'Cache directory does not exist; attempting to create: "{cache_dir_path}"')
    os.makedirs(name=cache_dir_path, exist_ok=True)
    return cache_dir_path


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

        self.cache_dir_path = ensure_cache_dir_exists(self.backend)
        
        self.load_all_caches_on_startup = backend.get_config('cache.load_all_caches_on_startup')

        self.load_caches_for_displayed_trees_at_startup = backend.get_config('cache.load_caches_for_displayed_trees_on_startup')
        self.sync_from_local_disk_on_cache_load = backend.get_config('cache.local_disk.sync_from_local_disk_on_cache_load')
        self.sync_from_gdrive_on_cache_load = backend.get_config('cache.sync_from_gdrive_on_cache_load')
        self.reload_tree_on_root_path_update = backend.get_config('cache.load_cache_when_tree_root_selected')
        self.cancel_all_pending_ops_on_startup = backend.get_config('cache.cancel_all_pending_ops_on_startup')
        self.lazy_load_local_file_signatures: bool = backend.get_config('cache.local_disk.signatures.lazy_load')
        logger.debug(f'lazy_load_local_file_signatures = {self.lazy_load_local_file_signatures}')

        if not self.sync_from_local_disk_on_cache_load:
            logger.warning('sync_from_local_disk_on_cache_load is set to false. This should only be set to false for internal testing!')

        # Instantiate but do not start submodules yet, to avoid entangled dependencies:

        self._cache_registry = CacheRegistry(backend, self.cache_dir_path)

        self._active_tree_manager = ActiveTreeManager(self.backend)
        self._row_state_tracking = RowStateTracking(self.backend, self._active_tree_manager)

        op_db_path = os.path.join(self.cache_dir_path, OPS_FILE_NAME)
        self._op_manager: OpManager = OpManager(self.backend, op_db_path)
        """Sub-module of Cache Manager which manages commands which have yet to execute"""

        self._local_disk_sig_calc_thread: Optional[SigCalcBatchingThread] = None

        self._startup_done: threading.Event = threading.Event()

        self.connect_dispatch_listener(signal=Signal.COMMAND_COMPLETE, receiver=self._on_command_completed)

    def shutdown(self):
        logger.debug('CacheManager.shutdown() entered')
        HasLifecycle.shutdown(self)

        try:
            if self._local_disk_sig_calc_thread:
                self._local_disk_sig_calc_thread.shutdown()
        except (AttributeError, NameError):
            pass

        try:
            if self._op_manager:
                self._op_manager.shutdown()
                self._op_manager = None
        except (AttributeError, NameError):
            pass

        try:
            if self._active_tree_manager:
                self._active_tree_manager.shutdown()
                self._active_tree_manager = None
        except (AttributeError, NameError):
            pass

        try:
            if self._cache_registry:
                self._cache_registry.shutdown()
                self._cache_registry = None
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
            # Load registry first. Do validation along the way
            self._cache_registry.start()

            # Start sub-modules:
            self._active_tree_manager.start()

            self._op_manager.start()

            local_store = self._cache_registry.get_this_disk_local_store()
            if local_store and self.lazy_load_local_file_signatures:
                self._local_disk_sig_calc_thread = SigCalcBatchingThread(self.backend, local_store.device_uid)
                self._local_disk_sig_calc_thread.start()

            # Finally, add or cancel any queued changes (asynchronously)
            if self.cancel_all_pending_ops_on_startup:
                logger.debug(f'User configuration specifies cancelling all pending ops on startup')
                pending_ops_func = self._op_manager.cancel_all_pending_ops
            else:
                pending_ops_func = self._op_manager.resume_pending_ops_from_disk
            # This is a lower priority, so will not execute until after caches are all loaded
            self.backend.executor.submit_async_task(Task(ExecPriority.P3_BACKGROUND_CACHE_LOAD, pending_ops_func))

        finally:
            dispatcher.send(Signal.STOP_PROGRESS, sender=ID_GLOBAL_CACHE)
            self._startup_done.set()
            logger.info('CacheManager startup done')

    def wait_for_startup_done(self):
        if not self._startup_done.is_set():
            logger.debug('Waiting for CacheManager startup to complete')
        if not self._startup_done.wait(CACHE_LOAD_TIMEOUT_SEC):
            logger.error('Timed out waiting for CacheManager startup!')

    # SignalDispatcher callbacks
    # ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼

    def _on_command_completed(self, sender, command: Command):
        """Updates the in-memory cache, on-disk cache, and UI with the nodes from the given UserOpResult"""
        logger.debug(f'Received signal: "{Signal.COMMAND_COMPLETE.name}"')
        result = command.op.result

        # FIXME: refactor all GDrive commands so that new UIDs are not assigned (and thus no NODE_REMOVED for the same node)

        # TODO: refactor so that we can attempt to create (close to) an atomic operation which combines GDrive and Local functionality

        if result.nodes_to_upsert:
            if SUPER_DEBUG_ENABLED:
                logger.debug(f'Cmd {command.__class__.__name__}:{command.uid} resulted in {len(result.nodes_to_upsert)} nodes to upsert: '
                             f'{result.nodes_to_upsert}')
            else:
                logger.debug(f'Cmd {command.__class__.__name__}:{command.uid} resulted in {len(result.nodes_to_upsert)} nodes to upsert')

            for node_to_upsert in result.nodes_to_upsert:
                self.upsert_single_node(node_to_upsert)

        if result.nodes_to_remove:
            if SUPER_DEBUG_ENABLED:
                logger.debug(f'Cmd {command.__class__.__name__}:{command.uid} resulted in {len(result.nodes_to_remove)} nodes to remove: '
                             f'{result.nodes_to_remove}')
            else:
                logger.debug(f'Cmd {command.__class__.__name__}:{command.uid} resulted in {len(result.nodes_to_remove)} nodes to remove')

            for removed_node in result.nodes_to_remove:
                self.remove_node(removed_node, to_trash=False)

        self._op_manager.finish_command(command)

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
            msg = 'Loading...'
            logger.debug(f'[{tree_id}] Sending signal {Signal.TREE_LOAD_STATE_UPDATED.name} with state={TreeLoadState.LOAD_STARTED.name} '
                         f' status_msg="{msg}"')
            dispatcher.send(signal=Signal.TREE_LOAD_STATE_UPDATED, sender=tree_id, tree_load_state=TreeLoadState.LOAD_STARTED, status_msg=msg,
                            dir_stats_dict_by_guid={}, dir_stats_dict_by_uid={})

        # Full cache load. Both first-order & higher-order trees do this:
        self.backend.executor.submit_async_task(Task(ExecPriority.P3_BACKGROUND_CACHE_LOAD, self._load_cache_for_subtree, tree_meta, send_signals))

    def is_cache_loaded_for(self, spid: SinglePathNodeIdentifier) -> bool:
        # this will return False if either a cache exists but is not loaded, or no cache yet exists:
        return self._cache_registry.get_store_for_device_uid(spid.device_uid).is_cache_loaded_for(spid)

    def _load_cache_for_subtree(self, this_task: Task, tree_meta: ActiveDisplayTreeMeta, send_signals: bool):
        """Note: this method is the "owner" of this_task"""

        if tree_meta.is_first_order():  # i.e. not ChangeTree
            # Load meta for all nodes:
            spid = tree_meta.root_sn.spid
            store = self._cache_registry.get_store_for_device_uid(spid.device_uid)

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
                    subtree_root_node: Optional[Node] = self.get_node_for_uid(spid.node_uid, spid.device_uid)
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

        this_task.add_next_task(self._repopulate_dir_stats_and_finish, tree_meta, send_signals)

    def _repopulate_dir_stats_and_finish(self, this_task, tree_meta, send_signals: bool):
        tree_meta.load_state = TreeLoadState.COMPLETELY_LOADED  # do this first to avoid race condition in ActiveTreeManager
        self.repopulate_dir_stats_for_tree(tree_meta)
        if send_signals:
            # Transition: Load State = COMPLETELY_LOADED
            # Notify UI that we are done. For gRPC backend, this will be received by the server stub and relayed to the client:
            logger.debug(f'[{tree_meta.tree_id}] Sending signal {Signal.TREE_LOAD_STATE_UPDATED.name} with'
                         f' tree_load_state={TreeLoadState.COMPLETELY_LOADED.name} status_msg="{tree_meta.summary_msg}"')
            dispatcher.send(signal=Signal.TREE_LOAD_STATE_UPDATED, sender=tree_meta.tree_id, tree_load_state=TreeLoadState.COMPLETELY_LOADED,
                            status_msg=tree_meta.summary_msg, dir_stats_dict_by_guid=tree_meta.dir_stats_unfiltered_by_guid,
                            dir_stats_dict_by_uid=tree_meta.dir_stats_unfiltered_by_uid)

    def repopulate_dir_stats_for_tree(self, tree_meta: ActiveDisplayTreeMeta):
        """
        BE-internal. NOT A CLIENT API
        """
        if tree_meta.root_exists:
            if tree_meta.is_first_order():
                # Load meta for all nodes:
                spid = tree_meta.root_sn.spid
                store = self._cache_registry.get_store_for_device_uid(spid.device_uid)

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

    def request_display_tree(self, request: DisplayTreeRequest) -> Optional[DisplayTree]:
        """The FE needs to first call this to ensure the given tree_id has a ActiveDisplayTreeMeta loaded into memory.
        Afterwards, the FE should call backend.start_subtree_load(), which will call enqueue_load_tree_task(),
        which will then asynchronously call load_data_for_display_tree()"""
        self.wait_for_startup_done()

        try:
            return self._active_tree_manager.request_display_tree(request)
        except RuntimeError as err:
            self.backend.report_exception(sender=ID_GLOBAL_CACHE, msg=f'Error requesting display tree "{request.tree_id}"', error=err)
            return None

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
        cache_info = self._cache_registry.get_cache_info_for_subtree(spid, create_if_not_found=True)
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

    def _refresh_subtree(self, this_task: Task, node_identifier: NodeIdentifier, tree_id: TreeID):
        """Called asynchronously via task executor"""
        logger.debug(f'[{tree_id}] Refreshing subtree: {node_identifier}')
        self._cache_registry.get_store_for_device_uid(node_identifier.device_uid).refresh_subtree(this_task, node_identifier, tree_id)

    def get_cache_info_for_subtree(self, subtree_root: SinglePathNodeIdentifier, create_if_not_found: bool = False) \
            -> Optional[PersistedCacheInfo]:
        return self._cache_registry.get_cache_info_for_subtree(subtree_root, create_if_not_found)

    def get_existing_cache_info_for_local_path(self, device_uid: UID, full_path: str) -> Optional[PersistedCacheInfo]:
        return self._cache_registry.get_existing_cache_info_for_local_path(device_uid, full_path)

    def save_all_cache_info_to_disk(self):
        self._cache_registry.save_all_cache_info_to_disk()

    def ensure_cache_loaded_for_node_list(self, this_task: Task, node_list: List[Node]):
        """Ensures that all the necessary caches are loaded for all of the given nodes.
        We launch separate executor tasks for each cache load that we require."""
        self._cache_registry.ensure_cache_loaded_for_node_list(this_task, node_list)

    # Main cache CRUD
    # ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼

    def upsert_single_node(self, node: Node) -> Node:
        return self._cache_registry.get_store_for_device_uid(node.device_uid).upsert_single_node(node)

    def update_single_node(self, node: Node) -> Node:
        """Simliar to upsert, but fails silently if node does not already exist in caches. Useful for things such as asynch MD5 filling"""
        return self._cache_registry.get_store_for_device_uid(node.device_uid).update_single_node(node)

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
                # Expand dir nodes. OpManager will not remove non-empty dirs
                expanded_node_list = self.get_subtree_bfs_node_list(node_to_delete.node_identifier)
                for node in expanded_node_list:
                    # somewhere in this returned list is the subtree root. Need to check so we don't include a duplicate:
                    if node.uid != node_to_delete.uid:
                        op_list.append(UserOp(op_uid=self.backend.uid_generator.next_uid(), batch_uid=batch_uid,
                                              op_type=UserOpType.RM, src_node=node))

            op_list.append(UserOp(op_uid=self.backend.uid_generator.next_uid(), batch_uid=batch_uid,
                                  op_type=UserOpType.RM, src_node=node_to_delete))

        self.enqueue_op_batch(op_list)

    def get_subtree_bfs_node_list(self, subtree_root: NodeIdentifier) -> List[Node]:
        return self._cache_registry.get_store_for_device_uid(subtree_root.device_uid).get_subtree_bfs_node_list(subtree_root)

    def get_subtree_bfs_sn_list(self, subtree_root_spid: SinglePathNodeIdentifier) -> List[SPIDNodePair]:
        return self._cache_registry.get_store_for_device_uid(subtree_root_spid.device_uid).get_subtree_bfs_sn_list()

    def remove_subtree(self, node: Node, to_trash: bool):
        """NOTE: this is only called for tests currently."""
        self._cache_registry.get_store_for_device_uid(node.device_uid).remove_subtree(node, to_trash)

    def remove_node(self, node: Node, to_trash):
        self._cache_registry.get_store_for_device_uid(node.device_uid).remove_single_node(node, to_trash)

    # Getters: Nodes and node identifiers
    # ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼

    def get_path_for_uid(self, uid: UID) -> str:
        # Throws exception if no path found
        return self._cache_registry.get_path_for_uid(uid)

    def get_uid_for_local_path(self, full_path: str, uid_suggestion: Optional[UID] = None) -> UID:
        return self._cache_registry.get_uid_for_path(full_path, uid_suggestion)

    def get_node_for_node_identifier(self, node_identifer: NodeIdentifier) -> Optional[Node]:
        return self.get_node_for_uid(node_identifer.node_uid, node_identifer.device_uid)

    def get_node_for_uid(self, uid: UID, device_uid: UID):
        assert device_uid, 'device_uid is required now!'
        return self._cache_registry.get_store_for_device_uid(device_uid).get_node_for_uid(uid)

    def get_node_list_for_path_list(self, path_list: List[str], device_uid: UID) -> List[Node]:
        """Because of GDrive, we cannot guarantee that a single path will have only one node, or a single node will have only one path."""
        path_list = ensure_list(path_list)
        return self._cache_registry.get_store_for_device_uid(device_uid).get_node_list_for_path_list(path_list)

    def get_child_list(self, parent_spid: SinglePathNodeIdentifier, tree_id: TreeID, is_expanding_parent: bool = False, use_filter: bool = False,
                       max_results: int = 0) -> List[SPIDNodePair]:
        """This method is a mess.
        Gets the children for the given SPID. This is intended for single-path identifier trees (i.e. DisplayTrees).
        Includes support for searching ChangeTrees (note that the tree_id param is required).
        If use_filter==True, will filter the results using the current FilterState for the tree, if any; if use_filter==False, will not filter.
        If max_results==0, unlimited nodes are returned. If nonzero and actual node count exceeds this, ResultsExceededError is raised."""
        if not tree_id:
            raise RuntimeError('get_child_list(): tree_id not provided!')
        if not parent_spid:
            raise RuntimeError('get_child_list(): parent_spid not provided!')
        if not isinstance(parent_spid, SinglePathNodeIdentifier):
            raise RuntimeError(f'get_child_list(): not a SPID (type={type(parent_spid)}): {parent_spid}')
        if TRACE_ENABLED:
            logger.debug(f'[{tree_id}] get_child_list() entered with parent_spid={parent_spid} is_expanding_parent={is_expanding_parent}')

        tree_meta: ActiveDisplayTreeMeta = self.get_active_display_tree_meta(tree_id)
        if not tree_meta:
            raise RuntimeError(f'get_child_list(): DisplayTree not registered: {tree_id}')

        if is_expanding_parent:
            self._row_state_tracking.add_expanded_row(parent_spid.guid, tree_id)

        if tree_meta.state.tree_display_mode == TreeDisplayMode.CHANGES_ONE_TREE_PER_CATEGORY:
            # Change trees have their own storage of nodes (not in master caches)
            if use_filter and tree_meta.filter_state and tree_meta.filter_state.has_criteria():
                child_list = tree_meta.filter_state.get_filtered_child_list(parent_spid, tree_meta.change_tree)
            else:
                child_list = tree_meta.change_tree.get_child_list_for_spid(parent_spid)

        else:
            # Regular tree
            filter_state = tree_meta.filter_state if use_filter else None
            device_uid: UID = parent_spid.device_uid
            child_list = self._cache_registry.get_store_for_device_uid(device_uid).get_child_list_for_spid(parent_spid, filter_state)

        if max_results and (len(child_list) > max_results):
            raise ResultsExceededError(len(child_list))

        self._copy_dir_stats_into_sn_list(child_list, tree_meta)

        if TRACE_ENABLED:
            logger.debug(f'[{tree_id}] get_child_list(): Returning {len(child_list)} children for node: {parent_spid}')
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

    def get_parent_list_for_node(self, node: Node) -> List[Node]:
        return self._cache_registry.get_store_for_device_uid(node.device_uid).get_parent_list_for_node(node)

    def get_parent_for_sn(self, sn: SPIDNodePair) -> Optional[SPIDNodePair]:
        return self._cache_registry.get_store_for_device_uid(sn.spid.device_uid).get_parent_for_sn(sn)

    def get_ancestor_list_for_spid(self, spid: SinglePathNodeIdentifier, stop_at_path: Optional[str] = None) -> Deque[SPIDNodePair]:
        if not spid:
            raise RuntimeError('get_ancestor_list_for_spid(): SPID not provided!')
        if not isinstance(spid, SinglePathNodeIdentifier):
            raise RuntimeError(f'get_ancestor_list_for_spid(): not a SPID (type={type(spid)}): {spid}')

        ancestor_deque: Deque[SPIDNodePair] = deque()
        ancestor_node: Node = self.get_node_for_uid(spid.node_uid, device_uid=spid.device_uid)
        if not ancestor_node:
            logger.debug(f'get_ancestor_list_for_spid(): Node not found: {spid}')
            return ancestor_deque

        ancestor_sn = SPIDNodePair(spid, ancestor_node)

        while True:
            parent_path = ancestor_sn.spid.get_single_path()
            if parent_path == stop_at_path:
                return ancestor_deque

            ancestor_sn = self.get_parent_for_sn(ancestor_sn)

            if ancestor_sn:
                ancestor_deque.appendleft(ancestor_sn)
            else:
                return ancestor_deque

    def get_all_files_and_dirs_for_subtree(self, subtree_root: NodeIdentifier) -> Tuple[List[Node], List[Node]]:
        return self._cache_registry.get_store_for_device_uid(subtree_root.device_uid).get_all_files_and_dirs_for_subtree(subtree_root)

    def make_spid_for(self, node_uid: UID, device_uid: UID, full_path: str) -> SinglePathNodeIdentifier:
        return self.backend.node_identifier_factory.for_values(uid=node_uid, device_uid=device_uid, path_list=full_path, must_be_single_path=True)

    def get_sn_for(self, node_uid: UID, device_uid: UID, full_path: str) -> Optional[SPIDNodePair]:
        assert node_uid and device_uid, f'node_uid={node_uid}, device_uid={device_uid}, full_path="{full_path}"'
        node = self._cache_registry.get_store_for_device_uid(device_uid).read_node_for_uid(node_uid)
        if not node:
            return None

        spid = self.backend.node_identifier_factory.for_values(uid=node_uid, device_uid=device_uid, tree_type=node.tree_type,
                                                               path_list=full_path, must_be_single_path=True)

        return SPIDNodePair(spid, node)

    def get_sn_for_guid(self, guid: GUID, tree_id: TreeID) -> Optional[SPIDNodePair]:
        """Unlike get_sn_for(), this will also examine change trees, but requires a tree_id"""
        sn_list = self.get_sn_list_for_guid_list(guid_list=[guid], tree_id=tree_id)
        if sn_list:
            return sn_list[0]
        return None

    def get_sn_list_for_guid_list(self, guid_list: List[GUID], tree_id: TreeID) -> List[SPIDNodePair]:
        """Unlike get_sn_for(), this will also examine change trees"""
        assert tree_id, 'tree_id is required!'

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
        store = self._cache_registry.get_store_for_device_uid(device_uid)
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

    def build_gdrive_root_node(self, device_uid: UID, sync_ts: Optional[int] = None) -> GDriveNode:
        store = self._get_gdrive_store_for_device_uid(device_uid)
        return store.build_gdrive_root_node(sync_ts=sync_ts)

    # This local disk-specific
    # ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼

    def move_local_subtree(self, this_task: Task, src_full_path: str, dst_full_path: str) -> Optional[Tuple]:
        return self._cache_registry.get_this_disk_local_store().move_local_subtree(this_task, src_full_path, dst_full_path)

    def get_node_for_local_path(self, full_path: str) -> Optional[Node]:
        """This will consult both the in-memory and disk caches.
        This is a convenience function which omits GDrive results because that would need to return a list
        (for that, see get_node_list_for_path_list().)"""
        if not full_path:
            raise RuntimeError('get_node_for_local_path(): full_path not specified!')
        return self._cache_registry.get_this_disk_local_store().read_node_for_path(full_path)

    def build_local_file_node(self, full_path: str, staging_path=None, must_scan_signature=False, is_live: bool = True) \
            -> Optional[LocalFileNode]:
        return self._cache_registry.get_this_disk_local_store().build_local_file_node(full_path, staging_path, must_scan_signature, is_live)

    def build_local_dir_node(self, full_path: str, is_live: bool = True, all_children_fetched: bool = False) -> LocalDirNode:
        return self._cache_registry.get_this_disk_local_store().build_local_dir_node(full_path, is_live, all_children_fetched=all_children_fetched)

    # Drag & drop
    # ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼

    def drop_dragged_nodes(self, src_tree_id: TreeID, src_guid_list: List[GUID], is_into: bool, dst_tree_id: TreeID, dst_guid: GUID,
                           drag_operation: DragOperation, dir_conflict_policy: DirConflictPolicy, file_conflict_policy: FileConflictPolicy) -> bool:
        assert drag_operation is not None and isinstance(drag_operation, DragOperation), f'Invalid drag operation: {drag_operation}'
        assert dir_conflict_policy is not None and isinstance(dir_conflict_policy, DirConflictPolicy), \
            f'Invalid dir_conflict_policy: {dir_conflict_policy}'
        assert file_conflict_policy is not None and isinstance(file_conflict_policy, FileConflictPolicy), \
            f'Invalid file_conflict_policy: {file_conflict_policy}'
        logger.info(f'Got drop: {drag_operation.name} {len(src_guid_list)} nodes from "{src_tree_id}" -> "{dst_tree_id}"'
                    f' dst_guid={dst_guid} is_into={is_into} dir_policy={dir_conflict_policy.name} file_policy={file_conflict_policy.name}')

        src_tree: ActiveDisplayTreeMeta = self.get_active_display_tree_meta(src_tree_id)
        dst_tree: ActiveDisplayTreeMeta = self.get_active_display_tree_meta(dst_tree_id)
        if not src_tree:
            logger.error(f'Aborting drop: could not find src tree: "{src_tree_id}"')
            return False
        if not dst_tree:
            logger.error(f'Aborting drop: could not find dst tree: "{dst_tree_id}"')
            return False

        if not src_tree.root_exists:
            logger.error(f'Aborting drop: src tree root does not exist: "{src_tree_id}"')
            return False
        if not dst_tree.root_exists:
            logger.error(f'Aborting drop: dst tree root does not exist: "{dst_tree_id}"')
            return False

        sn_src_list = self.get_sn_list_for_guid_list(src_guid_list, src_tree_id)
        if not sn_src_list:
            logger.error(f'Aborting drop: could not resolve GUIDs into any nodes: {src_guid_list}')
            return False

        sn_dst: SPIDNodePair = self.get_sn_for_guid(dst_guid, dst_tree_id)

        if not is_into or (sn_dst and not sn_dst.node.is_dir()):
            # cannot drop into a file; just use parent in this case
            sn_dst = self.get_parent_for_sn(sn_dst)

        if not dst_guid:
            logger.error(f'[{dst_tree_id}] Cancelling drop: no dst given for dropped location!')
            return False

        if self._is_dropping_on_self(sn_src_list, sn_dst, dst_tree_id):
            # don't allow this, even for copy. It's super annoying when an erroneous bump of the mouse results in a huge copy operation
            logger.info(f'[{dst_tree_id}] Cancelling drop: nodes were dropped in same location in the tree')
            return False

        # FIXME: add a wait so that the backend is done resuming pending ops, before continuing

        logger.debug(f'[{dst_tree_id}] Dropping into dest: {sn_dst.spid}')
        # "Left tree" here is the source tree, and "right tree" is the dst tree:
        transfer_maker = TransferMaker(backend=self.backend, left_tree_root_sn=src_tree.root_sn, right_tree_root_sn=dst_tree.root_sn,
                                       tree_id_left_src=src_tree_id, tree_id_right_src=dst_tree_id)

        if drag_operation == DragOperation.COPY or drag_operation == DragOperation.MOVE:
            transfer_maker.drag_and_drop(sn_src_list, sn_dst, drag_operation, dir_conflict_policy, file_conflict_policy)
        elif drag_operation == DragOperation.LINK:
            # TODO: link operation
            raise NotImplementedError('LINK drag operation is not yet supported!')
        else:
            raise RuntimeError(f'Unrecognized or unsupported drag operation: {drag_operation.name}')
        # This should fire listeners which ultimately populate the tree:
        op_list: List[UserOp] = transfer_maker.get_all_op_list()
        self.enqueue_op_batch(op_list)
        return True

    def _is_dropping_on_self(self, sn_src_list: List[SPIDNodePair], sn_dst: SPIDNodePair, dst_tree_id: TreeID):
        dst_ancestor_list = self.get_ancestor_list_for_spid(sn_dst.spid)

        for sn_src in sn_src_list:
            logger.debug(f'[{dst_tree_id}] DestNode="{sn_dst.spid}", DroppedNode="{sn_src.node}"')

            # Same node onto itself?
            if sn_dst.node.node_identifier == sn_src.node.node_identifier:
                return True

            # Dropping into its parent (essentially a no-op)
            if sn_dst.node.is_parent_of(sn_src.node):
                return True

            # Dropping an ancestor onto its descendant:
            for dst_ancestor in dst_ancestor_list:
                if sn_src.node.node_identifier == dst_ancestor.node.node_identifier:
                    logger.debug(f'[{dst_tree_id}] Source node ({sn_src.spid}) is ancestor of dest ({sn_dst.spid}): no bueno')
                    return True

        return False

    # Various public methods
    # ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼

    def visit_each_sn_in_subtree(self, tree_id: TreeID, subtree_root: SPIDNodePair, on_file_found: Callable[[SPIDNodePair], None]):
        """Note: here, param "tree_id" indicates which active tree from which to get child nodes from.
         This includes ChangeTrees, if tree_id resolves to a ChangeTree."""

        assert isinstance(subtree_root, Tuple), \
            f'Expected NamedTuple with SinglePathNodeIdentifier but got {type(subtree_root)}: {subtree_root}'
        queue: Deque[SPIDNodePair] = collections.deque()
        queue.append(subtree_root)

        count_total_nodes = 0
        count_file_nodes = 0

        while len(queue) > 0:
            sn: SPIDNodePair = queue.popleft()
            count_total_nodes += 1
            if not sn.node:
                raise RuntimeError(f'Node is null for: {sn.spid}')

            if sn.node.is_live():  # avoid pending op nodes
                if sn.node.is_dir():
                    child_sn_list = self.get_child_list(sn.spid, tree_id=tree_id)
                    if child_sn_list:
                        for child_sn in child_sn_list:
                            assert child_sn.spid.get_single_path() in child_sn.node.get_path_list(), \
                                f'Bad SPIDNodePair found in children of {sn}: Path from SPID ({child_sn.spid}) not found in node: {child_sn.node}'
                            queue.append(child_sn)
                else:
                    count_file_nodes += 1
                    on_file_found(sn)

        logger.debug(f'[{tree_id}] visit_each_sn_in_subtree(): Visited {count_file_nodes} file nodes out of {count_total_nodes} total nodes')

    def set_selected_rows(self, tree_id: TreeID, selected: Set[GUID]):
        """Saves the selected rows from the UI for the given tree"""
        self._row_state_tracking.set_selected_rows(tree_id, selected)

    def remove_expanded_row(self, row_guid: GUID, tree_id: TreeID):
        """AKA collapsing a row on the frontend"""
        self._row_state_tracking.remove_expanded_row(row_guid, tree_id)

    def get_rows_of_interest(self, tree_id: TreeID) -> RowsOfInterest:
        return self._row_state_tracking.get_rows_of_interest(tree_id)

    def update_node_icon(self, node: Node):
        """This is kind of a kludge, to make sure node icons are correct. Call this on all nodes we are sending to the client.
        Note: this should not be called for ChangeTree nodes. It will not consult a ChangeTree."""
        icon_id: Optional[IconId] = self._op_manager.get_icon_for_node(node.device_uid, node.uid)
        if SUPER_DEBUG_ENABLED:
            logger.debug(f'Setting custom icon for node {node.device_uid}:{node.uid} to {"None" if not icon_id else icon_id.name}')
        node.set_icon(icon_id)

    @staticmethod
    def derive_parent_path(child_path) -> Optional[str]:
        if child_path == '/':
            return None
        return str(pathlib.Path(child_path).parent)

    def submit_batch_of_changes(self, subtree_root: NodeIdentifier, upsert_node_list: List[Node] = None,
                                remove_node_list: List[Node] = None):
        return self._cache_registry.get_store_for_device_uid(subtree_root.device_uid).submit_batch_of_changes(subtree_root,
                                                                                                              upsert_node_list, remove_node_list)

    def get_device_list(self):
        return self._cache_registry.get_device_list()

    def show_tree(self, subtree_root: NodeIdentifier) -> str:
        return self._cache_registry.get_store_for_device_uid(subtree_root.device_uid).show_tree(subtree_root)

    def get_last_pending_op_for_node(self, device_uid: UID, node_uid: UID) -> Optional[UserOp]:
        return self._op_manager.get_last_pending_op_for_node(device_uid, node_uid)

    def enqueue_op_batch(self, op_list: List[UserOp]):
        """Attempt to add the given Ops to the execution tree. No need to worry whether some changes overlap or are redundant;
         the OpManager will sort that out - although it will raise an error if it finds incompatible changes such as adding to a tree
         that is scheduled for deletion."""
        try:
            self._op_manager.append_new_pending_op_batch(op_list)  # this now returns asynchronously
        except RuntimeError as err:
            self.backend.report_exception(sender=ID_GLOBAL_CACHE, msg=f'Failed to enqueue batch of operations', error=err)

    def get_next_command(self) -> Optional[Command]:
        # blocks !
        self.wait_for_startup_done()
        # also blocks !
        return self._op_manager.get_next_command()

    def get_next_command_nowait(self) -> Optional[Command]:
        # blocks !
        self.wait_for_startup_done()

        return self._op_manager.get_next_command_nowait()

    def get_pending_op_count(self) -> int:
        return self._op_manager.get_pending_op_count()

    # This is only called at startup (shh...)
    def read_node_for_spid(self, spid: SinglePathNodeIdentifier) -> Optional[Node]:
        # ensure all paths are normalized:
        path_list = spid.get_path_list()
        for index, full_path in enumerate(path_list):
            if not file_util.is_normalized(full_path):
                full_path = file_util.normalize_path(full_path)
                logger.debug(f'Normalized path: {full_path}')
                path_list[index] = full_path
        spid.set_path_list(path_list)

        return self.get_node_for_uid(spid.node_uid, spid.device_uid)
