import copy
import logging
import math
import os
import pathlib
import threading
from typing import Dict, List, Optional, Tuple

from backend.display_tree.filter_state import FilterState
from backend.executor.central import ExecPriority
from backend.sqlite.local_db import LocalDiskDatabase
from backend.tree_store.local import content_hasher
from backend.tree_store.local.local_disk_scanner import LocalDiskScanner
from backend.tree_store.local.local_disk_tree import LocalDiskTree
from backend.tree_store.local.master_local_disk import LocalDiskDiskStore
from backend.tree_store.local.master_local_write_op import BatchChangesOp, DeleteSingleNodeOp, DeleteSubtreeOp, LocalDiskMemoryStore, LocalSubtree, \
    LocalWriteThroughOp, RefreshDirEntriesOp, UpsertSingleNodeOp
from backend.tree_store.tree_store_interface import TreeStore
from backend.uid.uid_mapper import UidPathMapper
from constants import IS_MACOS, MAX_FS_LINK_DEPTH, SUPER_DEBUG_ENABLED, TRACE_ENABLED, TrashStatus, TreeID, TreeType
from error import CacheNotLoadedError, NodeNotPresentError
from model.cache_info import PersistedCacheInfo
from model.device import Device
from model.node.directory_stats import DirectoryStats
from model.node.local_disk_node import LocalDirNode, LocalFileNode, LocalNode
from model.node.node import SPIDNodePair
from model.node_identifier import LocalNodeIdentifier, SinglePathNodeIdentifier
from model.uid import UID
from signal_constants import ID_GLOBAL_CACHE, Signal
from util import file_util, time_util
from util.stopwatch_sec import Stopwatch
from util.task_runner import Task

logger = logging.getLogger(__name__)


class LocalDiskMasterStore(TreeStore):
    """
    ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
    CLASS LocalDiskMasterStore

    In-memory cache for local filesystem
    ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼
    """

    def __init__(self, backend, uid_path_mapper, device: Device):
        TreeStore.__init__(self, device)
        self.backend = backend
        self.uid_path_mapper: UidPathMapper = uid_path_mapper

        self._struct_lock = threading.Lock()
        self._memstore: LocalDiskMemoryStore = LocalDiskMemoryStore(backend, self.device.uid)
        self._diskstore: LocalDiskDiskStore = LocalDiskDiskStore(backend, self.device.uid)

        self.lazy_load_signatures: bool = backend.get_config('cache.lazy_load_local_file_signatures')
        logger.debug(f'lazy_load_signatures = {self.lazy_load_signatures}')

    def start(self):
        TreeStore.start(self)
        self._diskstore.start()

        if self.lazy_load_signatures:
            self.connect_dispatch_listener(signal=Signal.NODE_UPSERTED_IN_CACHE, receiver=self._on_node_upserted_in_cache)

    def shutdown(self):
        TreeStore.shutdown(self)
        try:
            self.backend = None
            self._memstore = None
            self._diskstore = None
        except (AttributeError, NameError):
            pass

    def is_gdrive(self) -> bool:
        return False

    def _execute_write_op(self, operation: LocalWriteThroughOp):
        if SUPER_DEBUG_ENABLED:
            logger.debug(f'Executing operation: {operation}')
        assert self._struct_lock.locked()

        # 1. Update memory
        operation.update_memstore(self._memstore)

        # 2. Update disk
        cacheman = self.backend.cacheman
        if cacheman.enable_save_to_disk:
            if SUPER_DEBUG_ENABLED:
                logger.debug(f'Updating diskstore for operation {operation}')
            self._diskstore.execute_op(operation)

        else:
            logger.debug(f'Save to disk is disabled: skipping save to disk for operation')

        # 3. Send signals
        if SUPER_DEBUG_ENABLED:
            logger.debug(f'Sending signals for operation {operation}')
        operation.send_signals()

    # Signature calculation
    # ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼

    def _on_node_upserted_in_cache(self, sender: str, node: LocalNode):
        if node.device_uid == self.device_uid and node.is_file() and not node.md5 and not node.sha256:
            assert isinstance(node, LocalFileNode)
            if SUPER_DEBUG_ENABLED:
                logger.debug(f'Enqueuing node for sig calc: {node.node_identifier}')
            self.backend.executor.submit_async_task(Task(ExecPriority.SIGNATURE_CALC, self.calculate_signature_for_local_node, node))

    def calculate_signature_for_local_node(self, this_task: Task, node: LocalFileNode):
        # Get up-to-date copy:
        node = self.backend.cacheman.get_node_for_uid(node.uid, node.device_uid)

        if node.md5 or node.sha256:
            # Other threads, e.g., CommandExecutor, can also fill this in asynchronously
            logger.debug(f'Node already has signature; skipping; {node}')
            return

        logger.debug(f'[SigCalc] Calculating signature for node: {node.node_identifier}')
        md5, sha256 = content_hasher.calculate_signatures(full_path=node.get_single_path())
        if not md5 and not sha256:
            logger.debug(f'[SigCalc] Failed to calculate signature for node {node.uid}: assuming it was deleted')
            return

        # Do not modify the original node, or cacheman will not detect that it has changed. Edit and submit a copy instead
        node_with_signature = copy.deepcopy(node)
        node_with_signature.md5 = md5
        node_with_signature.sha256 = sha256

        logger.debug(f'[SigCalc] Node {node_with_signature.node_identifier.guid} has MD5: {node_with_signature.md5}')

        # TODO: consider batching writes
        # Send back to ourselves to be re-stored in memory & disk caches:
        self.backend.cacheman.update_single_node(node_with_signature)

    # Disk access
    # ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼

    def _save_subtree_to_disk(self, cache_info: PersistedCacheInfo, tree_id):
        assert isinstance(cache_info.subtree_root, LocalNodeIdentifier)

        file_list, dir_list = self.get_all_files_and_dirs_for_subtree(cache_info.subtree_root)

        self._diskstore.save_subtree(cache_info, file_list, dir_list, tree_id)

    def overwrite_dir_entries_list(self, parent_full_path: str, child_list: List[LocalNode]):
        parent_dir: Optional[LocalNode] = self.get_node_for_uid(self.get_uid_for_path(parent_full_path))
        if not parent_dir or not parent_dir.is_dir():
            # Possibly just the cache for this one node is out-of-date. Let's bring it up to date.
            parent_dir = self.build_local_file_node(full_path=parent_full_path)
            if not parent_dir:
                logger.error(f'overwrite_dir_entries_list(): Parent dir not found! (path={parent_full_path}) Skipping...')
                # TODO: distinguish between removable volume dirs (volatile) & regular dirs which should be there

                # FIXME: if dir is not volatile, delete dir tree from cache
                return
            elif not parent_dir.is_dir():
                logger.warning(f'overwrite_dir_entries_list(): Parent dir is not a dir! (path={parent_full_path}) Will overwrite cache entries...')

                # FIXME: add functionality to overwrite dir node with file node
                raise NotImplementedError('Cannot yet overwrite dir node with file node!')

        parent_dir.all_children_fetched = True
        parent_spid: LocalNodeIdentifier = parent_dir.node_identifier

        existing_child_dict: Dict[UID, LocalNode] = {}
        existing_child_list: List[SPIDNodePair] = self._get_child_list_from_cache_for_spid(parent_spid)
        for existing_child in existing_child_list:
            # don't need SPIDs; just unwrap the node from the pair:
            existing_child_dict[existing_child.node.uid] = existing_child.node

        for new_child in child_list:
            existing_child_dict.pop(new_child.uid, None)

        remove_dir_list = []
        remove_file_list = []
        for node_to_remove in existing_child_dict.values():
            if node_to_remove.is_live():  # ignore nodes which are not live (i.e. pending op nodes)
                if node_to_remove.is_dir():
                    remove_dir_list.append(node_to_remove)
                else:
                    remove_file_list.append(node_to_remove)

        # Files can be removed in the main op, but dirs represent tress so we will create a DeleteSubtreeOp for each.
        remove_op_list = []
        for remove_dir in remove_dir_list:
            subtree_node_list = self._get_subtree_bfs(remove_dir.node_identifier)
            if subtree_node_list:
                logger.debug(f'Dir with {len(subtree_node_list)} nodes will be removed: {remove_dir.node_identifier}')
                remove_op_list.append(DeleteSubtreeOp(parent_spid, node_list=subtree_node_list))
            else:
                # should never happen
                logger.error(f'Dir not found in cache despite being listed somewhere: {remove_dir.node_identifier}')

        with self._struct_lock:
            self._execute_write_op(RefreshDirEntriesOp(parent_spid, upsert_node_list=child_list, remove_node_list=remove_file_list))
            for remove_op in remove_op_list:
                self._execute_write_op(remove_op)

    def _resync_with_file_system(self, this_task: Task, subtree_root: LocalNodeIdentifier, tree_id: TreeID):
        """Scan directory tree and update master tree where needed."""
        logger.debug(f'[{tree_id}] Scanning filesystem subtree: {subtree_root}')
        scanner = LocalDiskScanner(backend=self.backend, master_local=self, root_node_identifer=subtree_root, tree_id=tree_id)

        # Create child task. It will create next_task instances as it goes along, thus delaying execution of this_task's next_task
        child_task = Task(this_task.priority, scanner.start_recursive_scan)
        self.backend.executor.submit_async_task(child_task, parent_task=this_task)

    # LocalSubtree-level methods
    # ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼

    def show_tree(self, subtree_root: LocalNodeIdentifier) -> str:
        # logger.warning('LOCK ON!')
        with self._struct_lock:
            result = self._memstore.master_tree.show(nid=subtree_root.node_uid)
        # logger.warning('LOCK off')
        return result

    def load_subtree(self, this_task: Task, subtree_root: LocalNodeIdentifier, tree_id: TreeID):
        logger.debug(f'[{tree_id}] DisplayTree requested for root: {subtree_root}')
        self._load_subtree_from_disk_for_identifier(this_task, subtree_root, tree_id, force_rescan_disk=False)

    def is_cache_loaded_for(self, spid: LocalNodeIdentifier) -> bool:
        # If we have already loaded this subtree as part of a larger cache, use that:
        cache_info: PersistedCacheInfo = self.backend.cacheman.get_cache_info_for_subtree(spid, create_if_not_found=False)
        return cache_info and cache_info.is_loaded

    def _load_subtree_from_disk_for_identifier(self, this_task: Task, subtree_root: LocalNodeIdentifier, tree_id: TreeID,
                                               force_rescan_disk: bool = False):
        """
        Performs a read-through retrieval of all the LocalFileNodes in the given subtree on the local filesystem.
        Esentially just a wrapper for _load_subtree_from_disk() which first finds the appropriate cache for the given subtree.
        """

        if not os.path.exists(subtree_root.get_single_path()):
            logger.warning(f'Cannot load meta for subtree because it does not exist: "{subtree_root.get_single_path()}"')
            return

        self._ensure_uid_consistency(subtree_root)

        # If we have already loaded this subtree as part of a larger cache, use that:
        cache_man = self.backend.cacheman
        cache_info: PersistedCacheInfo = cache_man.get_cache_info_for_subtree(subtree_root, create_if_not_found=True)
        assert cache_info
        self._load_subtree_from_disk(this_task, cache_info, tree_id, subtree_root, force_rescan_disk)

    def _load_subtree_from_disk(self, this_task: Task, cache_info: PersistedCacheInfo, tree_id: TreeID,
                                requested_subtree_root: LocalNodeIdentifier = None, force_rescan_disk: bool = False) -> None:
        """Loads the appropriate cache from disk (if not already loaded into memory) for the given subtree root, and
        requested_subtree_root, if present, is a subset of the cache_info's subtree and it will be used. Otherwise cache_info's will be used"""
        assert cache_info
        assert isinstance(cache_info.subtree_root, SinglePathNodeIdentifier), f'Found instead: {type(cache_info.subtree_root)}'
        stopwatch_total = Stopwatch()

        # FIXME: don't forget consolidate_local_caches()

        self._ensure_uid_consistency(cache_info.subtree_root)

        if not requested_subtree_root:
            requested_subtree_root = cache_info.subtree_root
        else:
            self._ensure_uid_consistency(requested_subtree_root)

        was_loaded = True
        # LOAD into master tree. Only for first load!
        if not cache_info.is_loaded:
            was_loaded = False
            if self.backend.cacheman.enable_load_from_disk:
                tree = self._diskstore.load_subtree(cache_info, tree_id)
                if tree:
                    if TRACE_ENABLED:
                        logger.debug(f'[{tree_id}] Loaded cached tree: \n{tree.show()}')
                    # logger.warning('LOCK ON!')
                    with self._struct_lock:
                        self._memstore.master_tree.replace_subtree(tree)
                        logger.debug(f'[{tree_id}] Updated in-memory cache: tree_size={len(self._memstore.master_tree):n}')
                    # logger.warning('LOCK off')
            else:
                logger.debug(f'[{tree_id}] Skipping cache disk load because cache.enable_load_from_disk is false')

        # FS SYNC
        if force_rescan_disk or cache_info.needs_refresh or (not was_loaded and self.backend.cacheman.sync_from_local_disk_on_cache_load):
            logger.debug(f'[{tree_id}] Will resync with file system (is_loaded={cache_info.is_loaded}, sync_on_cache_load='
                         f'{self.backend.cacheman.sync_from_local_disk_on_cache_load}, needs_refresh={cache_info.needs_refresh}, '
                         f'force_rescan_disk={force_rescan_disk})')
            # Update from the file system, and optionally save any changes back to cache:
            self._resync_with_file_system(this_task, requested_subtree_root, tree_id)

            def _after_resync_complete(_this_task):
                # Need to save changes to CacheInfo, but we don't have an API for a single line. Just overwrite all for now - shouldn't hurt
                cache_info.is_complete = True
                self.backend.cacheman.write_cache_registry_updates_to_disk()
                if SUPER_DEBUG_ENABLED:
                    logger.debug(f'[{tree_id}] File system sync complete')
                # We can only mark this as 'done' (False) if the entire cache contents has been refreshed:
                if requested_subtree_root.node_uid == cache_info.subtree_root.node_uid:
                    cache_info.needs_refresh = False

            this_task.add_next_task(_after_resync_complete)
        elif not self.backend.cacheman.sync_from_local_disk_on_cache_load:
            logger.debug(f'[{tree_id}] Skipping filesystem sync because it is disabled for cache loads')
        elif not cache_info.needs_refresh:
            logger.debug(f'[{tree_id}] Skipping filesystem sync because the cache is still fresh for path: {cache_info.subtree_root}')

        def _after_load_complete(_this_task):
            """Finish up"""

            # SAVE
            if cache_info.needs_save:
                if not cache_info.is_loaded:
                    logger.warning(f'[{tree_id}] Skipping cache save: cache was never loaded!')
                elif self.backend.cacheman.enable_save_to_disk:
                    # Save the updates back to local disk cache:
                    self._save_subtree_to_disk(cache_info, tree_id)
                else:
                    logger.debug(f'[{tree_id}] Skipping cache save because it is disabled')
            elif SUPER_DEBUG_ENABLED:
                logger.debug(f'[{tree_id}] Skipping cache save: not needed')

            logger.info(f'[{tree_id}] {stopwatch_total} Load complete for {requested_subtree_root}')

        this_task.add_next_task(_after_load_complete)

    def _ensure_uid_consistency(self, subtree_root: SinglePathNodeIdentifier):
        """Since the UID of the subtree root node is stored in 3 different locations (registry, cache file, and memory),
        checks that at least registry & memory match. If UID is not in memory, guarantees that it will be stored with the value from registry.
        This method should only be called for the subtree root of display trees being loaded"""
        existing_uid = subtree_root.node_uid
        new_uid = self.get_uid_for_path(subtree_root.get_single_path(), existing_uid)
        if existing_uid != new_uid:
            logger.warning(f'Requested UID "{existing_uid}" is invalid for given path; changing it to "{new_uid}"')
        subtree_root.node_uid = new_uid

    def consolidate_local_caches(self, this_task: Task, local_caches: List[PersistedCacheInfo], tree_id) -> bool:
        supertree_sets: List[Tuple[PersistedCacheInfo, PersistedCacheInfo]] = []

        if not self.backend.cacheman.enable_save_to_disk:
            logger.debug(f'[{tree_id}] Will not consolidate caches; save to disk is disabled')
            return False

        for cache in local_caches:
            for other_cache in local_caches:
                if other_cache.subtree_root.get_single_path().startswith(cache.subtree_root.get_single_path()) and \
                        not cache.subtree_root.get_single_path() == other_cache.subtree_root.get_single_path():
                    # cache is a super-tree of other_cache
                    supertree_sets.append((cache, other_cache))

        for supertree_cache, subtree_cache in supertree_sets:
            local_caches.remove(subtree_cache)

            if supertree_cache.sync_ts > subtree_cache.sync_ts:
                logger.info(f'[{tree_id}] Cache for supertree (root={supertree_cache.subtree_root.get_single_path()}, ts={supertree_cache.sync_ts}) '
                            f'is newer than for subtree (root={subtree_cache.subtree_root.get_single_path()}, ts={subtree_cache.sync_ts}): '
                            f'it will be deleted')
                file_util.delete_file(subtree_cache.cache_location)
            else:
                logger.info(f'[{tree_id}] Cache for subtree (root={subtree_cache.subtree_root.get_single_path()}, ts={subtree_cache.sync_ts}) '
                            f'is newer than for supertree (root={supertree_cache.subtree_root.get_single_path()}, ts={supertree_cache.sync_ts}): '
                            f'it will be merged into supertree')

                # 1. Load super-tree into memory
                super_tree: LocalDiskTree = self._diskstore.load_subtree(supertree_cache, ID_GLOBAL_CACHE)
                # 2. Discard all paths from super-tree which fall under sub-tree:

                # 3. Load sub-tree into memory
                sub_tree: LocalDiskTree = self._diskstore.load_subtree(subtree_cache, ID_GLOBAL_CACHE)
                if sub_tree:
                    # 4. Add contents of sub-tree into super-tree:
                    super_tree.replace_subtree(sub_tree=sub_tree)

                # 5. We already loaded it into memory; add it to the in-memory cache:
                # logger.warning('LOCK ON!')
                with self._struct_lock:
                    self._memstore.master_tree.replace_subtree(super_tree)
                # logger.warning('LOCK off')

                # this will resync with file system and/or save if configured
                supertree_cache.needs_save = True
                self._load_subtree_from_disk(this_task, supertree_cache, tree_id)
                # Now it is safe to delete the subtree cache:
                file_util.delete_file(subtree_cache.cache_location)

        registry_needs_update = len(supertree_sets) > 0
        return registry_needs_update

    def refresh_subtree(self, this_task: Task, node_identifier: LocalNodeIdentifier, tree_id: TreeID):
        assert isinstance(node_identifier, LocalNodeIdentifier)
        self._load_subtree_from_disk_for_identifier(this_task, node_identifier, tree_id, force_rescan_disk=True)

    def generate_dir_stats(self, subtree_root_node: LocalNode, tree_id: TreeID) -> Dict[UID, DirectoryStats]:
        """Generate DirStatsDict for the given subtree, with no filter applied"""
        # logger.warning('LOCK ON!')
        with self._struct_lock:
            result = self._memstore.master_tree.generate_dir_stats(tree_id, subtree_root_node)
        # logger.warning('LOCK off')
        return result

    def populate_filter(self, filter_state: FilterState):
        filter_state.ensure_cache_populated(self._memstore.master_tree)

    # Cache CRUD operations
    # ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼

    def read_single_node_for_path(self, full_path: str) -> Optional[LocalNode]:
        """This actually reads directly from the disk cache"""
        logger.debug(f'Loading single node for path: "{full_path}"')
        cache_info: Optional[PersistedCacheInfo] = self.backend.cacheman.find_existing_cache_info_for_local_subtree(self.device.uid, full_path)
        if not cache_info:
            logger.debug(f'Could not find cache containing path: "{full_path}"')
            return None
        if cache_info.is_loaded:
            logger.debug(f'Cache is already loaded for subtree (will return node from memory): "{cache_info.subtree_root}"')
            return self.get_node_for_domain_id(full_path)
        with LocalDiskDatabase(cache_info.cache_location, self.backend, self.device.uid) as cache:
            return cache.get_file_or_dir_for_path(full_path)

    def upsert_single_node(self, node: LocalNode) -> LocalNode:
        if not node or node.tree_type != TreeType.LOCAL_DISK or node.device_uid != self.device.uid:
            raise RuntimeError(f'Cannot upsert node: invalid node provided: {node}')

        assert self.uid_path_mapper.get_uid_for_path(node.get_single_path(), node.uid) == node.uid, \
            f'Internal error while trying to upsert node to cache: UID did not match expected ' \
            f'({self.uid_path_mapper.get_uid_for_path(node.get_single_path(), node.uid)}); node={node}'

        write_op = UpsertSingleNodeOp(node)

        # logger.warning('LOCK ON!')
        with self._struct_lock:
            self._execute_write_op(write_op)
        # logger.warning('LOCK off')

        return write_op.node

    def update_single_node(self, node: LocalNode) -> LocalNode:
        if not node or node.tree_type != TreeType.LOCAL_DISK or node.device_uid != self.device.uid:
            raise RuntimeError(f'Cannot update node: invalid node provided: {node}')

        write_op = UpsertSingleNodeOp(node, update_only=True)

        # logger.warning('LOCK ON!')
        with self._struct_lock:
            self._execute_write_op(write_op)

        # logger.warning('LOCK off')

        return write_op.node

    def remove_single_node(self, node: LocalNode, to_trash=False):
        if not node or node.tree_type != TreeType.LOCAL_DISK or node.device_uid != self.device.uid:
            raise RuntimeError(f'Cannot remove node: invalid node provided: {node}')

        logger.debug(f'Removing node from caches (to_trash={to_trash}): {node}')

        # logger.warning('LOCK ON!')
        with self._struct_lock:
            self._execute_write_op(DeleteSingleNodeOp(node, to_trash=to_trash))

        # logger.warning('LOCK off')

    def _migrate_node(self, node: LocalNode, src_full_path: str, dst_full_path: str) -> LocalNode:
        new_node_full_path: str = file_util.change_path_to_new_root(node.get_single_path(), src_full_path, dst_full_path)
        new_node_uid: UID = self.get_uid_for_path(new_node_full_path)

        new_node = copy.deepcopy(node)
        new_node.set_node_identifier(LocalNodeIdentifier(uid=new_node_uid, device_uid=self.device.uid, full_path=new_node_full_path))
        return new_node

    def _add_to_expected_node_moves(self, src_node_list: List[LocalNode], dst_node_list: List[LocalNode]):
        first = True
        # Let's collate these two operations so that in case of failure, we have less inconsistent state
        for src_node, dst_node in zip(src_node_list, dst_node_list):
            logger.debug(f'Migrating copy of node {src_node.node_identifier} to {dst_node.node_identifier}')
            if first:
                # ignore subroot
                first = False
            else:
                self._memstore.expected_node_moves[src_node.get_single_path()] = dst_node.get_single_path()

    def move_local_subtree(self, src_full_path: str, dst_full_path: str, is_from_watchdog=False):
        # logger.warning('LOCK ON!')
        with self._struct_lock:
            # FIXME: refactor to put this in watchdog itself
            if is_from_watchdog:
                # See if we safely ignore this:
                expected_move_dst = self._memstore.expected_node_moves.pop(src_full_path, None)
                if expected_move_dst:
                    if expected_move_dst == dst_full_path:
                        logger.debug(f'Ignoring MV ("{src_full_path}" -> "{dst_full_path}") because it was already done')
                        return
                    else:
                        logger.error(f'MV ("{src_full_path}" -> "{dst_full_path}"): was expecting dst = "{expected_move_dst}"!')

            src_uid: UID = self.get_uid_for_path(src_full_path)
            src_node: LocalNode = self._memstore.master_tree.get_node_for_uid(src_uid)
            if src_node:
                src_nodes: List[LocalNode] = self._memstore.master_tree.get_subtree_bfs(src_node.uid)
                src_subtree: LocalSubtree = LocalSubtree(src_node.node_identifier, remove_node_list=[], upsert_node_list=src_nodes)
            else:
                logger.error(f'MV src node does not exist: UID={src_uid}, path={src_full_path}')
                return

            # Create up to 3 tree operations which should be executed in a single transaction if possible
            dst_uid: UID = self.get_uid_for_path(dst_full_path)
            dst_node_identifier: LocalNodeIdentifier = LocalNodeIdentifier(uid=dst_uid, device_uid=self.device.uid, full_path=dst_full_path)
            dst_subtree: LocalSubtree = LocalSubtree(dst_node_identifier, [], [])

            existing_dst_node: LocalNode = self._memstore.master_tree.get_node_for_uid(dst_uid)
            if existing_dst_node:
                logger.debug(f'Node already exists at MV dst; will remove: {existing_dst_node.node_identifier}')
                existing_dst_nodes: List[LocalNode] = self._memstore.master_tree.get_subtree_bfs(dst_uid)
                dst_subtree.remove_node_list = existing_dst_nodes

            if src_subtree:
                for src_node in src_subtree.remove_node_list:
                    dst_node = self._migrate_node(src_node, src_full_path, dst_full_path)
                    dst_subtree.upsert_node_list.append(dst_node)
            else:
                # Rescan dir in dst_full_path for nodes
                if os.path.isdir(dst_full_path):
                    # FIXME: this is broken!
                    fresh_tree: LocalDiskTree = self._resync_with_file_system(dst_node_identifier, ID_GLOBAL_CACHE)
                    for dst_node in fresh_tree.get_subtree_bfs():
                        dst_subtree.upsert_node_list.append(dst_node)
                    logger.debug(f'Added node list contains {len(dst_subtree.upsert_node_list)} nodes')
                else:
                    local_node: LocalFileNode = self.build_local_file_node(dst_full_path)
                    dst_subtree.upsert_node_list.append(local_node)

            subtree_list: List[LocalSubtree] = [src_subtree, dst_subtree]

            if is_from_watchdog:
                self._add_to_expected_node_moves(src_subtree.remove_node_list, dst_subtree.upsert_node_list)

            operation = BatchChangesOp(subtree_list=subtree_list)
            self._execute_write_op(operation)
        # logger.warning('LOCK off')

    def remove_subtree(self, subtree_root_node: LocalNode, to_trash: bool):
        """subtree_root can be either a file or dir"""
        logger.debug(f'Removing subtree_root from caches (to_trash={to_trash}): {subtree_root_node}')

        if to_trash:
            # TODO
            raise RuntimeError(f'Not supported: to_trash=true!')

        if not subtree_root_node.uid:
            raise RuntimeError(f'Cannot remove subtree_root from cache because it has no UID: {subtree_root_node}')

        if subtree_root_node.uid != self.get_uid_for_path(subtree_root_node.get_single_path()):
            raise RuntimeError(f'Internal error while trying to remove subtree_root ({subtree_root_node}): UID did not match expected '
                               f'({self.get_uid_for_path(subtree_root_node.get_single_path())})')

        # logger.warning('LOCK ON!')
        with self._struct_lock:
            subtree_nodes: List[LocalNode] = self._get_subtree_bfs(subtree_root_node.node_identifier)
            assert isinstance(subtree_root_node.node_identifier, LocalNodeIdentifier)
            operation: DeleteSubtreeOp = DeleteSubtreeOp(subtree_root_node.node_identifier, node_list=subtree_nodes)
            self._execute_write_op(operation)
        # logger.warning('LOCK off')

    # Various public getters
    # ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼

    def get_all_files_and_dirs_for_subtree(self, subtree_root: LocalNodeIdentifier) -> Tuple[List[LocalFileNode], List[LocalDirNode]]:
        if SUPER_DEBUG_ENABLED:
            logger.debug(f'Entered get_all_files_and_dirs_for_subtree(): locked={self._struct_lock.locked()}')
        # logger.warning('LOCK ON!')
        with self._struct_lock:
            result = self._memstore.master_tree.get_all_files_and_dirs_for_subtree(subtree_root)
        # logger.warning('LOCK off')
        return result

    def get_node_list_for_path_list(self, path_list: List[str]) -> List[LocalNode]:
        node_list: List[LocalNode] = []
        for full_path in path_list:
            node = self.get_node_for_domain_id(full_path)
            if node:
                node_list.append(node)
        return node_list

    def get_uid_for_domain_id(self, domain_id: str, uid_suggestion: Optional[UID] = None) -> UID:
        return self.get_uid_for_path(domain_id, uid_suggestion)

    def get_uid_for_path(self, full_path: str, uid_suggestion: Optional[UID] = None) -> UID:
        assert isinstance(full_path, str)
        return self.uid_path_mapper.get_uid_for_path(full_path, uid_suggestion)

    @staticmethod
    def _cache_exists(cache_info: PersistedCacheInfo):
        """Check for the uncommon case of a cache being listed but failing to be created"""
        return os.path.exists(cache_info.cache_location) and os.stat(cache_info.cache_location).st_size > 0

    def get_node_for_domain_id(self, domain_id: str) -> LocalNode:
        """AKA get_node_for_full_path()"""
        uid: UID = self.get_uid_for_domain_id(domain_id)
        return self.get_node_for_uid(uid)

    def _get_subtree_bfs(self, parent_spid: LocalNodeIdentifier) -> Optional[List[LocalNode]]:
        cache_info: Optional[PersistedCacheInfo] = \
            self.backend.cacheman.find_existing_cache_info_for_local_subtree(self.device.uid, parent_spid.get_single_path())
        if cache_info:
            if cache_info.is_loaded:
                return self._memstore.master_tree.get_subtree_bfs(parent_spid.node_uid)

            else:
                # FIXME: add support for recursively getting subtree from disk
                raise NotImplementedError('FIXME: add support for recursively getting subtree from disk')

        # both caches miss
        return None

    def _get_child_list_from_cache_for_spid(self, parent_spid: LocalNodeIdentifier) -> Optional[List[SPIDNodePair]]:
        """Searches in-memory cache, followed by disk cache, for children of the given SPID. Returns None if parent not found in either cache"""
        with self._struct_lock:

            # 1. Use in-memory cache if it exists:
            parent_node = self._memstore.master_tree.get_node_for_uid(parent_spid.node_uid)
            if parent_node and parent_node.is_dir() and parent_node.all_children_fetched:
                try:
                    return self._memstore.master_tree.get_child_list_for_spid(parent_spid)
                except NodeNotPresentError as e:
                    # In-memory cache miss. Try seeing if the relevant cache is loaded:
                    logger.debug(f'Could not find node in in-memory cache: {parent_spid}')
                    pass
            else:
                # in-memory cache miss
                logger.debug(f'In-memory cache miss (cache returned parent_node: {parent_node})')

            # 2. Read from disk cache if it exists:
            cache_info: Optional[PersistedCacheInfo] = \
                self.backend.cacheman.find_existing_cache_info_for_local_subtree(self.device.uid, parent_spid.get_single_path())
            if cache_info:
                if cache_info.is_loaded:
                    # something is probably wrong, or node truly doesn't exist
                    logger.warning(f'Could not find node in in-memory subtree but cache claims it is laoded: "{parent_spid}"')

                with LocalDiskDatabase(cache_info.cache_location, self.backend, self.device.uid) as cache:
                    parent_dir = cache.get_file_or_dir_for_uid(parent_spid.node_uid)
                    if parent_dir and parent_dir.is_dir() and parent_dir.all_children_fetched:
                        return [self.to_sn(x) for x in cache.get_child_list_for_node_uid(parent_spid.node_uid)]

        # both caches miss
        return None

    def get_child_list_for_spid(self, parent_spid: LocalNodeIdentifier, filter_state: FilterState) -> List[SPIDNodePair]:
        if SUPER_DEBUG_ENABLED:
            logger.debug(f'Entered get_child_list_for_spid(): spid={parent_spid} filter_state={filter_state} locked={self._struct_lock.locked()}')

        if filter_state and filter_state.has_criteria():
            # This only works if cache already loaded:
            if not self.is_cache_loaded_for(parent_spid):
                raise RuntimeError(f'Cannot load filtered child list: cache not yet loaded for: {parent_spid}')

            return filter_state.get_filtered_child_list(parent_spid, self._memstore.master_tree)

        child_list = self._get_child_list_from_cache_for_spid(parent_spid)
        if child_list is None:
            # 3. No cache hits. Must do a live scan:
            logger.debug(f'Could not find cache containing path: "{parent_spid.get_single_path()}"; will attempt a disk scan')
            return self._scan_and_cache_dir(parent_spid)
        else:
            return child_list

    def _scan_and_cache_dir(self, parent_spid: LocalNodeIdentifier) -> List[SPIDNodePair]:
        # Scan dir on disk (read-through)

        scanner = LocalDiskScanner(backend=self.backend, master_local=self, root_node_identifer=parent_spid, tree_id=None)
        # This may call overwrite_dir_entries_list()
        child_list = scanner.scan_single_dir(parent_spid.get_single_path())
        return [self.to_sn(x) for x in child_list]

    @staticmethod
    def to_sn(node) -> SPIDNodePair:
        # Trivial for LocalNodes
        return SPIDNodePair(node.node_identifier, node)

    def get_parent_for_sn(self, sn: SPIDNodePair) -> Optional[SPIDNodePair]:
        parent_node = self.get_single_parent_for_node(sn.node)
        if parent_node:
            return self.to_sn(parent_node)
        return None

    def get_node_for_uid(self, uid: UID) -> Optional[LocalNode]:
        if TRACE_ENABLED:
            logger.debug(f'Entered get_node_for_uid(): uid={uid} locked={self._struct_lock.locked()}')

        node = self._memstore.master_tree.get_node_for_uid(uid)
        if node:
            return node

        # if node not found, let's try to find the cache it belongs to:

        # throws exception if path not found:
        try:
            full_path = self.uid_path_mapper.get_path_for_uid(uid)
        except RuntimeError as err:
            logger.debug(f'Cannot retrieve node (UID={uid}): could not get full_path for node: {err}')
            return None

        cache_info: Optional[PersistedCacheInfo] = self.backend.cacheman.find_existing_cache_info_for_local_subtree(self.device.uid, full_path)
        if not cache_info:
            logger.debug(f'Could not find cache containing path: "{full_path}"')
            return None
        if cache_info.is_complete and not cache_info.is_loaded:
            raise CacheNotLoadedError(f'Cannot retrieve node (UID={uid}): LocalDisk cache not loaded!')

        # simply not found:
        return None

    def get_parent_list_for_node(self, node: LocalNode) -> List[LocalNode]:
        parent_node = self.get_single_parent_for_node(node)
        if parent_node:
            return [parent_node]
        return []

    def get_single_parent_for_node(self, node: LocalNode, required_subtree_path: str = None) -> Optional[LocalNode]:
        """LocalNodes are guaranteed to have at most 1 parent."""
        if TRACE_ENABLED:
            logger.debug(f'Entered get_single_parent_for_node({node.node_identifier}): locked={self._struct_lock.locked()}')
        try:
            # logger.warning('LOCK ON!')
            try:
                parent: LocalNode = self._memstore.master_tree.get_parent(node.identifier)
            except KeyError:
                # parent not found in tree... maybe we can derive it however
                parent_path: str = node.derive_parent_path()
                parent_uid: UID = self.get_uid_for_path(parent_path)
                parent = self._memstore.master_tree.get_node_for_uid(parent_uid)
                if not parent:
                    logger.debug(f'Parent not found for node ({node.uid})')
                    # logger.warning('LOCK off')
                    return None
                logger.debug(f'Parent not found for node ({node.uid}) but found parent at path: {parent.get_single_path()}')
            if not required_subtree_path or parent.get_single_path().startswith(required_subtree_path):
                # logger.warning('LOCK off')
                return parent
            # logger.warning('LOCK off')
            return None
        except NodeNotPresentError:
            return None
        except Exception:
            logger.error(f'Error getting parent for node: {node}, required_path: {required_subtree_path}')
            raise

    def build_local_dir_node(self, full_path: str, is_live: bool, all_children_fetched: bool) -> LocalDirNode:
        uid = self.get_uid_for_path(full_path)

        parent_path = str(pathlib.Path(full_path).parent)
        parent_uid: UID = self.get_uid_for_path(parent_path)
        return LocalDirNode(node_identifier=LocalNodeIdentifier(uid=uid, device_uid=self.device.uid, full_path=full_path), parent_uid=parent_uid,
                            trashed=TrashStatus.NOT_TRASHED, is_live=is_live, all_children_fetched=all_children_fetched)

    def build_local_file_node(self, full_path: str, staging_path: str = None, must_scan_signature=False) -> Optional[LocalFileNode]:
        uid = self.get_uid_for_path(full_path)

        parent_path = str(pathlib.Path(full_path).parent)
        parent_uid: UID = self.get_uid_for_path(parent_path)

        # Check for broken links:
        if os.path.islink(full_path):
            pointer = full_path
            # Links can be nested too (can there be cycles? Use max depth just in case):
            count_attempt = 0
            while os.path.islink(pointer):
                target = pathlib.Path(os.readlink(pointer)).resolve()
                logger.debug(f'Resolved link (depth {count_attempt}): "{pointer}" -> "{target}"')
                if not os.path.exists(target):
                    logger.warning(f'Broken link, skipping: "{pointer}" -> "{target}"')
                    if pointer != full_path:
                        logger.error(f'(original link: "{full_path}")')
                    return None
                count_attempt = count_attempt + 1
                if count_attempt == MAX_FS_LINK_DEPTH:
                    logger.error(f'Max link depth ({MAX_FS_LINK_DEPTH}) exceeded for: "{full_path}"')
                    return None
                pointer = target

        if self.lazy_load_signatures and not must_scan_signature:
            # Skip MD5 and set it NULL for now. Node will be added to content scanning queue when it is upserted into cache (above)
            md5 = None
            sha256 = None
        else:
            try:
                md5, sha256 = content_hasher.calculate_signatures(full_path, staging_path)
            except FileNotFoundError:
                # bad link
                return None

        # Get "now" in UNIX time:
        sync_ts = time_util.now_sec()

        if staging_path:
            path = staging_path
        else:
            path = full_path

        stat = os.stat(path)
        size_bytes = int(stat.st_size)

        modify_ts = int(stat.st_mtime * 1000)
        change_ts = int(stat.st_ctime * 1000)

        if IS_MACOS and staging_path:
            # MacOS has a bug where moving/copying a file will truncate its timestamps. We'll try to match its behavior.
            # See https://macperformanceguide.com/blog/2019/20190903_1600-macOS-truncates-file-dates.html
            modify_ts_mac = math.trunc(modify_ts / 1000) * 1000
            change_ts_mac = math.trunc(change_ts / 1000) * 1000
            if SUPER_DEBUG_ENABLED:
                logger.debug(f'MACOS: tweaked modify_ts ({modify_ts}->{modify_ts_mac}) & change_ts ({change_ts}->{change_ts_mac}) for '
                             f'{self.device.uid}:{uid}, "{full_path}"')
            modify_ts = modify_ts_mac
            change_ts = change_ts_mac

        assert modify_ts > 100000000000, f'modify_ts too small: {modify_ts} for path: {path}'
        assert change_ts > 100000000000, f'change_ts too small: {change_ts} for path: {path}'

        node_identifier = LocalNodeIdentifier(uid=uid, device_uid=self.device.uid, full_path=full_path)
        new_node = LocalFileNode(node_identifier, parent_uid, md5, sha256, size_bytes, sync_ts, modify_ts, change_ts, TrashStatus.NOT_TRASHED, True)

        if TRACE_ENABLED:
            logger.debug(f'Built: {new_node} with sync_ts: {sync_ts}')

        assert new_node.modify_ts == modify_ts

        return new_node
