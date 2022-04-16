import copy
import logging
import math
import os
import pathlib
from typing import Dict, List, Optional, Tuple

from pydispatch import dispatcher

from be.disp_tree.filter_state import FilterState
from be.tree_store.locald.ld_diskstore import LocalDiskDiskStore
from be.tree_store.locald.ld_fs_scanner import LocalDiskTreeScanner
from be.tree_store.locald.ld_tree import LocalDiskTree
from be.tree_store.locald.op_write import BatchChangesOp, DeleteSingleNodeOp, DeleteSubtreeOp, LocalDiskMemoryStore, LocalSubtree, \
    LocalWriteThroughOp, RefreshDirEntriesOp, UpsertSingleNodeOp
from be.tree_store.tree_store import TreeStore
from be.uid.uid_mapper import UidPathMapper
from constants import IS_MACOS, IS_WINDOWS, MAX_FS_LINK_DEPTH, ROOT_PATH, TrashStatus, TreeID, TreeType
from error import NodeNotPresentError
from logging_constants import SUPER_DEBUG_ENABLED, TRACE_ENABLED
from model.cache_info import PersistedCacheInfo
from model.device import Device
from model.node.dir_stats import DirStats
from model.node.locald_node import LocalDirNode, LocalFileNode, LocalNode
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

    Facade for in-memory & disk caches for a local filesystem.
    ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼
    """

    def __init__(self, backend, uid_path_mapper, device: Device):
        TreeStore.__init__(self, device)
        self.backend = backend
        self.uid_path_mapper: UidPathMapper = uid_path_mapper

        self._memstore: LocalDiskMemoryStore = LocalDiskMemoryStore(backend, self.device.uid)
        self._diskstore: LocalDiskDiskStore = LocalDiskDiskStore(backend, self.device.uid)

    def start(self):
        TreeStore.start(self)
        self._diskstore.start()

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
        # 3 stages. Failure at one stage cancels the stages after
        if SUPER_DEBUG_ENABLED:
            logger.debug(f'Executing operation: {operation}')

        # 1. Update memory
        operation.update_memstore(self._memstore)

        # 2. Update disk
        if SUPER_DEBUG_ENABLED:
            logger.debug(f'Updating diskstore for operation {operation}')
        self._diskstore.execute_op(operation)

        # 3. Send signals
        if SUPER_DEBUG_ENABLED:
            logger.debug(f'Sending signals for operation {operation}')
        operation.send_signals()

    # Disk access
    # ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼

    def _save_subtree_to_disk(self, cache_info: PersistedCacheInfo, tree_id):
        assert isinstance(cache_info.subtree_root, LocalNodeIdentifier)

        file_list, dir_list = self.get_all_files_and_dirs_for_subtree(cache_info.subtree_root)

        self._diskstore.save_subtree(cache_info, file_list, dir_list, tree_id)

    def submit_batch_of_changes(self, subtree_root: LocalNodeIdentifier,  upsert_node_list: List[LocalNode] = None,
                                remove_node_list: List[LocalNode] = None):
        self._execute_write_op(BatchChangesOp(subtree_root=subtree_root, upsert_node_list=upsert_node_list, remove_node_list=remove_node_list))

    def overwrite_dir_entries_list(self, parent_full_path: str, child_list: List[LocalNode]):
        logger.debug(f'overwrite_dir_entries_list() called with {len(child_list)} children & parent: "{parent_full_path}"')

        if not os.path.exists(parent_full_path):
            # TODO: distinguish between removable volume dirs (volatile) & regular dirs which should be there
            is_removable_volume = False
            if is_removable_volume:
                logger.debug(f'overwrite_dir_entries_list(): Dir not found but it is on removable volume; ignoring: "{parent_full_path}"')
            else:
                # delete subtree and return
                if child_list:
                    raise RuntimeError(f'Path does not exist but we think it has {len(child_list)} children: "{parent_full_path}"')

                cached_parent_node = self.read_node_for_path(parent_full_path)
                if cached_parent_node:
                    logger.info(f'overwrite_dir_entries_list(): given parent not found ("{parent_full_path}") Removing it from the cache.')
                    self.remove_subtree(cached_parent_node, to_trash=False)
                else:
                    logger.error(f'overwrite_dir_entries_list(): path does not exist and no record of it found: "{parent_full_path}"')
            return

        if os.path.isdir(parent_full_path):
            parent_dir = self.build_local_dir_node(full_path=parent_full_path, is_live=True, all_children_fetched=True)
        else:
            # Updated node is not a dir!
            parent_dir = self.build_local_file_node(full_path=parent_full_path)
            if child_list:
                assert not parent_dir.is_dir()
                raise RuntimeError(f'overwrite_dir_entries_list(): Parent (path={parent_full_path}) is not a dir, but we are asked to add '
                                   f'{len(child_list)} children to it!')

        parent_spid: LocalNodeIdentifier = parent_dir.node_identifier

        # Get children of target parent, and sort into dirs and non-dirs:
        # Files can be removed in the main RefreshDirEntriesOp op, but dirs represent trees, so we will create a DeleteSubtreeOp for each.
        file_node_remove_list = []
        existing_child_list = self._get_child_list_from_cache_for_spid(parent_spid, only_if_all_children_fetched=False)
        if existing_child_list:
            existing_child_dict: Dict[UID, LocalNode] = {}
            for existing_child_sn in existing_child_list:
                # don't need SPIDs; just unwrap the node from the pair:
                existing_child_dict[existing_child_sn.node.uid] = existing_child_sn.node

            for new_child in child_list:
                existing_child = existing_child_dict.pop(new_child.uid, None)
                # attempt to avoid signature recalculations by checking one against the other and merging old into new:
                if existing_child and existing_child.is_file() and new_child.is_file():
                    if TRACE_ENABLED:
                        logger.debug(f'overwrite_dir_entries_list(): checking signature for new child: {new_child}')
                    assert isinstance(new_child, LocalFileNode), f'Bad: {new_child}'
                    new_child.copy_signature_if_is_meta_equal(existing_child, self.backend.cacheman.is_seconds_precision_enough)

            dir_node_remove_list = []
            for node_to_remove in existing_child_dict.values():
                if node_to_remove.is_live():  # ignore nodes which are not live (i.e. pending op nodes)
                    if node_to_remove.is_dir():
                        dir_node_remove_list.append(node_to_remove)
                    else:
                        file_node_remove_list.append(node_to_remove)

            for dir_node in dir_node_remove_list:
                logger.debug(f'overwrite_dir_entries_list(): removing subtree: {dir_node.node_identifier}')
                self.remove_subtree(dir_node, to_trash=False)

        to_upsert_list = copy.copy(child_list)  # Do not modify the members of the caller list!
        to_upsert_list.append(parent_dir)  # Need to update all_children_fetched to True!
        self._execute_write_op(RefreshDirEntriesOp(parent_spid, upsert_node_list=to_upsert_list, remove_node_list=file_node_remove_list))

    def _resync_with_file_system(self, this_task: Task, subtree_root: LocalNodeIdentifier, tree_id: TreeID):
        """Scan directory tree and update master tree where needed."""
        logger.debug(f'[{tree_id}] Scanning filesystem subtree: {subtree_root}')
        scanner = LocalDiskTreeScanner(backend=self.backend, master_local=self, root_node_identifer=subtree_root, tree_id=tree_id)

        # Create child task. It will create next_task instances as it goes along, thus delaying execution of this_task's next_task
        child_task = this_task.create_child_task(scanner.start_tree_scan)
        self.backend.executor.submit_async_task(child_task)

    # LocalSubtree-level methods
    # ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼

    def show_tree(self, subtree_root: LocalNodeIdentifier) -> str:
        result = self._memstore.master_tree.show(nid=subtree_root.node_uid)
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

        if SUPER_DEBUG_ENABLED:
            logger.debug(f'[{tree_id}] Entered _load_subtree_from_disk(): cache_info={cache_info}, requested_subtree_root={requested_subtree_root}')

        stopwatch_total = Stopwatch()

        self._ensure_uid_consistency(cache_info.subtree_root)

        if not requested_subtree_root:
            requested_subtree_root = cache_info.subtree_root
        else:
            self._ensure_uid_consistency(requested_subtree_root)

        was_loaded = True
        # LOAD into master tree. Only for first load!
        if not cache_info.is_loaded:
            was_loaded = False
            tree = self._diskstore.load_subtree(cache_info, tree_id)
            if tree:
                if TRACE_ENABLED:
                    logger.debug(f'[{tree_id}] Loaded cached tree: \n{tree.show()}')

                self._memstore.master_tree.replace_subtree(tree)
                # Only set this once we are completely finished bringing the memstore up to date. Other tasks will depend on it
                # to choose whether to query memory or disk
                cache_info.is_loaded = True
                logger.debug(f'[{tree_id}] Updated memstore for device_uid={self.device_uid} from disk cache (subtree={cache_info.subtree_root}).'
                             f' Tree size is now: {len(self._memstore.master_tree):n} nodes')

        # FS SYNC
        did_rescan = False
        if force_rescan_disk or cache_info.needs_refresh or (not was_loaded and self.backend.cacheman.sync_from_local_disk_on_cache_load):
            logger.debug(f'[{tree_id}] Will resync with file system: is_loaded={cache_info.is_loaded}, sync_on_cache_load='
                         f'{self.backend.cacheman.sync_from_local_disk_on_cache_load}, needs_refresh={cache_info.needs_refresh}, '
                         f'force_rescan_disk={force_rescan_disk}')
            did_rescan = True
            # Update from the file system, and optionally save any changes back to cache:
            self._resync_with_file_system(this_task, requested_subtree_root, tree_id)

            def _after_resync_complete(_this_task):
                # Need to save changes to CacheInfo, but we don't have an API for a single line. Just overwrite all for now - shouldn't hurt
                cache_info.is_complete = True
                self.backend.cacheman.save_all_cache_info_to_disk()
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

        if not was_loaded and not did_rescan:
            logger.debug(f'[{tree_id}] Disk scan was skipped; checking cached nodes for missing signatures')
            # Check whether any nodes still need their signatures filled in, and enqueue them if so:
            for node in self._memstore.master_tree.get_subtree_bfs_node_list(requested_subtree_root.node_uid):
                if node.is_file() and not node.has_signature():
                    dispatcher.send(signal=Signal.NODE_NEEDS_SIG_CALC, sender=tree_id, node=node)

        def _after_load_complete(_this_task):
            """Finish up"""

            # SAVE
            if cache_info.needs_save:
                if not cache_info.is_loaded:
                    logger.warning(f'[{tree_id}] Skipping cache save: cache was never loaded!')
                else:
                    # Save the updates back to local disk cache:
                    self._save_subtree_to_disk(cache_info, tree_id)
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

    def consolidate_local_caches(self, this_task: Task, local_caches: List[PersistedCacheInfo], state):

        def _merge_two_caches(_this_task: Task, supertree_cache, subtree_cache):
            local_caches.remove(subtree_cache)

            if supertree_cache.sync_ts > subtree_cache.sync_ts:
                logger.info(f'Cache for supertree (root={supertree_cache.subtree_root.get_single_path()}, ts={supertree_cache.sync_ts}) '
                            f'is newer than for subtree (root={subtree_cache.subtree_root.get_single_path()}, ts={subtree_cache.sync_ts}): '
                            f'it will be deleted')
                file_util.delete_file(subtree_cache.cache_location)
            else:
                logger.info(f'Cache for subtree (root={subtree_cache.subtree_root.get_single_path()}, ts={subtree_cache.sync_ts}) '
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
                self._memstore.master_tree.replace_subtree(super_tree)
                super_tree.is_loaded = True

                # 6. This will resync with file system and re-save
                supertree_cache.needs_save = True
                load_subtree_child_task = _this_task.create_child_task(self._load_subtree_from_disk, supertree_cache, ID_GLOBAL_CACHE)
                self.backend.executor.submit_async_task(load_subtree_child_task)

                # 7. Now it is safe to delete the subtree cache:
                def _delete_cache_file(_this_task2: Task):
                    file_util.delete_file(subtree_cache.cache_location)

                delete_cache_file_child_task = _this_task.create_child_task(_delete_cache_file)
                self.backend.executor.submit_async_task(delete_cache_file_child_task)

        supertree_sets: List[Tuple[PersistedCacheInfo, PersistedCacheInfo]] = []

        for cache in local_caches:
            for other_cache in local_caches:
                if pathlib.PurePosixPath(other_cache.subtree_root.get_single_path()).is_relative_to(cache.subtree_root.get_single_path()) and \
                        not cache.subtree_root.get_single_path() == other_cache.subtree_root.get_single_path():
                    # cache is a super-tree of other_cache
                    logger.info(f'Cache {cache} represents a supertree of cache {other_cache}; will merge the latter into the former')
                    supertree_sets.append((cache, other_cache))

        for _supertree_cache, _subtree_cache in supertree_sets:
            merge_two_caches_child_task = this_task.create_child_task(_merge_two_caches, _supertree_cache, _subtree_cache)
            self.backend.executor.submit_async_task(merge_two_caches_child_task)

        def _finish_consolidation(_finally_task: Task):
            registry_needs_update = len(supertree_sets) > 0
            if registry_needs_update:
                state.registry_needs_update = True

            state.existing_cache_list += local_caches

        finally_child_task = this_task.create_child_task(_finish_consolidation)
        self.backend.executor.submit_async_task(finally_child_task)

    def refresh_subtree(self, this_task: Task, node_identifier: LocalNodeIdentifier, tree_id: TreeID):
        assert isinstance(node_identifier, LocalNodeIdentifier)
        self._load_subtree_from_disk_for_identifier(this_task, node_identifier, tree_id, force_rescan_disk=True)

    def generate_dir_stats(self, subtree_root_node: LocalNode, tree_id: TreeID) -> Dict[UID, DirStats]:
        """Generate DirStatsDict for the given subtree, with no filter applied"""
        return self._memstore.master_tree.generate_dir_stats(tree_id, subtree_root_node)

    def populate_filter(self, filter_state: FilterState):
        filter_state.ensure_cache_populated(self._memstore.master_tree)

    # Cache CRUD operations
    # ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼

    def read_node_for_path(self, full_path: str) -> Optional[LocalNode]:
        """This actually reads directly from the disk cache if needed"""
        if TRACE_ENABLED:
            logger.debug(f'read_node_for_path() entered: "{full_path}"')
        node_uid: UID = self.get_uid_for_domain_id(full_path)

        return self._read_single_node_for(node_uid, full_path)

    def read_node_for_uid(self, node_uid: UID) -> Optional[LocalNode]:
        """This actually reads directly from the disk cache if needed"""
        if TRACE_ENABLED:
            logger.debug(f'read_node_for_uid() entered: uid={node_uid}')
        full_path = self.get_path_for_uid(node_uid)

        return self._read_single_node_for(node_uid, full_path)

    def _read_single_node_for(self, node_uid: UID, full_path: str) -> Optional[LocalNode]:
        """Try in-memory cache first; if miss, try disk cache.
        Params node_uid and full_path MUST correspond, or we'll really be in trouble"""
        assert node_uid and full_path and self.get_path_for_uid(node_uid) == full_path, f'Invalid: node_uid={node_uid}, full_path={full_path}'

        # 1. Memory cache. This will also cover pending op nodes, which are not stored here but will be in memory:
        node = self._memstore.master_tree.get_node_for_uid(node_uid)
        if node:
            return node

        cache_info: Optional[PersistedCacheInfo] = self.backend.cacheman.get_existing_cache_info_for_local_path(self.device.uid, full_path)
        if not cache_info:
            logger.info(f'_read_single_node_for(): Could not find cache containing path: "{full_path}"')
            return None
        if cache_info.is_loaded:
            # If the cache is marked as loaded, then its contents should be represented in the in-memory master tree (queried above):
            if SUPER_DEBUG_ENABLED:
                logger.debug(f'_read_single_node_for(): Memstore ({cache_info.cache_location}) is loaded but node not found for: {node_uid}')
            # return None

        # 2. Disk cache
        if SUPER_DEBUG_ENABLED:
            logger.debug(f'_read_single_node_for(): Memstore miss; reading diskstore for {node_uid}')
        node = self._diskstore.get_file_or_dir_for_uid(cache_info, node_uid)
        if node:
            if TRACE_ENABLED:
                logger.debug(f'_read_single_node_for(): Found node in disk cache: {node}')
            return node

        if SUPER_DEBUG_ENABLED:
            logger.debug(f'_read_single_node_for(): Not found in memory or disk cache (will try disk scan): {node_uid}')

        # 3. Disk scan?
        if os.path.isdir(full_path):
            node = self.build_local_dir_node(full_path, is_live=True, all_children_fetched=False)
        else:
            node = self.build_local_file_node(full_path)
        if node:
            logger.debug(f'_read_single_node_for(): Scanned node from disk: {node}')
            self.upsert_single_node(node)
        else:
            logger.debug(f'_read_single_node_for(): Failed to scan node from disk: {node_uid}')
        return node

    def upsert_single_node(self, node: LocalNode) -> LocalNode:
        if not node or node.tree_type != TreeType.LOCAL_DISK or node.device_uid != self.device.uid:
            raise RuntimeError(f'Cannot upsert node: invalid node provided: {node}')

        assert self.uid_path_mapper.get_uid_for_path(node.get_single_path(), node.uid) == node.uid, \
            f'Internal error while trying to upsert node to cache: UID did not match expected ' \
            f'({self.uid_path_mapper.get_uid_for_path(node.get_single_path(), node.uid)}); node={node}'

        write_op = UpsertSingleNodeOp(node)
        self._execute_write_op(write_op)

        return write_op.node

    def update_single_node(self, node: LocalNode) -> LocalNode:
        if not node or node.tree_type != TreeType.LOCAL_DISK or node.device_uid != self.device.uid:
            raise RuntimeError(f'Cannot update node: invalid node provided: {node}')

        write_op = UpsertSingleNodeOp(node, update_only=True)
        self._execute_write_op(write_op)

        return write_op.node

    def remove_single_node(self, node: LocalNode, to_trash=False):
        if not node or node.tree_type != TreeType.LOCAL_DISK or node.device_uid != self.device.uid:
            raise RuntimeError(f'Cannot remove node: invalid node provided: {node}')

        logger.debug(f'Removing node from caches (to_trash={to_trash}): {node}')
        self._execute_write_op(DeleteSingleNodeOp(node, to_trash=to_trash))

    def _migrate_node(self, node: LocalNode, src_full_path: str, dst_full_path: str) -> LocalNode:
        new_node_full_path: str = file_util.change_path_to_new_root(node.get_single_path(), src_full_path, dst_full_path)
        new_node_uid: UID = self.get_uid_for_path(new_node_full_path)

        new_node = copy.deepcopy(node)
        new_node_identifier = LocalNodeIdentifier(uid=new_node_uid, device_uid=self.device.uid, full_path=new_node_full_path)
        new_node.set_node_identifier(new_node_identifier)

        new_parent_uid = self.get_uid_for_path(new_node_identifier.get_single_parent_path())
        new_node.set_parent_uids(new_parent_uid)
        if SUPER_DEBUG_ENABLED:
            logger.debug(f'MigrateNode: node_path="{node.get_single_path()}" src_root="{src_full_path}" dst_root="{dst_full_path}" '
                         f'-> new_path="{new_node_full_path}"')
        return new_node

    def move_local_subtree(self, this_task: Task, src_full_path: str, dst_full_path: str) -> Optional[Tuple[List[LocalNode], List[LocalNode]]]:
        if SUPER_DEBUG_ENABLED:
            logger.debug(f'Moving subtree src="{src_full_path}" dst="{dst_full_path}"')

        # canonicalize each path
        src_path = file_util.normalize_path(src_full_path)
        dst_path = file_util.normalize_path(dst_full_path)

        # TODO: signal to CacheMan that path changed, so that it can preserve expanded & selected state of nodes across moves

        if not src_path:
            raise RuntimeError(f'src_path is empty: "{src_path}"')
        if not dst_path:
            raise RuntimeError(f'dst_path is empty: "{dst_path}"')
        if src_path == dst_path:
            raise RuntimeError(f'src_path and dst_path are identical: "{dst_path}"')

        src_uid: UID = self.get_uid_for_path(src_path)
        src_subroot_node: LocalNode = self._memstore.master_tree.get_node_for_uid(src_uid)

        dst_uid: UID = self.get_uid_for_path(dst_path)
        dst_node_identifier: LocalNodeIdentifier = LocalNodeIdentifier(uid=dst_uid, device_uid=self.device.uid, full_path=dst_path)
        dst_subtree: LocalSubtree = LocalSubtree(subtree_root=dst_node_identifier, remove_node_list=[], upsert_node_list=[])

        existing_dst_subroot_node: LocalNode = self._memstore.master_tree.get_node_for_uid(dst_uid)
        if existing_dst_subroot_node:
            # This will be very rare and probably means there's a bug, but let's try to handle it:
            logger.warning(f'Subroot node already exists at MV dst; will remove all nodes in tree: {existing_dst_subroot_node.node_identifier}')
            existing_dst_node_list: List[LocalNode] = self._memstore.master_tree.get_subtree_bfs_node_list(dst_uid)
            dst_subtree.remove_node_list = existing_dst_node_list

        if src_subroot_node:
            existing_src_node_list: List[LocalNode] = self._memstore.master_tree.get_subtree_bfs_node_list(src_uid)
            if existing_src_node_list:
                # Use cached list of existing nodes in the old location to infer the nodes in the new location:
                src_subtree: LocalSubtree = LocalSubtree(src_subroot_node.node_identifier,
                                                         remove_node_list=existing_src_node_list, upsert_node_list=[])

                for src_node in existing_src_node_list:
                    dst_node = self._migrate_node(src_node, src_path, dst_path)
                    dst_subtree.upsert_node_list.append(dst_node)

                # here, src_subtree contains nodes to remove from cache, & dst_subtree contains the migrated nodes to upsert
                # (+ maybe cached nodes to remove)
                self._execute_write_op(BatchChangesOp(subtree_list=[src_subtree, dst_subtree]))

                return existing_src_node_list, dst_subtree.upsert_node_list
            else:
                logger.info(f'MV src node does not exist: UID={src_uid}, path={src_path}')
        else:
            logger.info(f'MV src node does not exist (will ignore src and sync dst only): {self.device_uid}:{src_uid} ("{src_path}")')

        # We don't have any src nodes. This can easily happen if temporary files are created and then renamed; the cache can't keep up.
        # This method will execute asynchronously as a set of child tasks, after we return from this method.
        # In this case, the child task will already handle all the updates to the dst subtree (and we have no src subtree),
        # so we won't have any work to do here.
        self._resync_with_file_system(this_task, dst_node_identifier, ID_GLOBAL_CACHE)
        return None

    def remove_subtree(self, subtree_root_node: LocalNode, to_trash: bool):
        """Recursively remove all nodes with the given subtree root from the cache. Param subtree_root_node can be either a file or dir"""
        logger.debug(f'Removing subtree_root from caches (to_trash={to_trash}): {subtree_root_node}')

        if to_trash:
            # TODO
            raise RuntimeError(f'Not supported: to_trash=true!')

        if not subtree_root_node.uid:
            raise RuntimeError(f'Cannot remove subtree_root from cache because it has no UID: {subtree_root_node}')

        if subtree_root_node.uid != self.get_uid_for_path(subtree_root_node.get_single_path()):
            raise RuntimeError(f'Internal error while trying to remove subtree_root ({subtree_root_node}): UID did not match expected '
                               f'({self.get_uid_for_path(subtree_root_node.get_single_path())})')

        assert isinstance(subtree_root_node.node_identifier, LocalNodeIdentifier)
        subtree_node_list: List[LocalNode] = self._get_subtree_bfs_from_cache(subtree_root_node.node_identifier)
        if not subtree_node_list:
            raise RuntimeError(f'Unexpected error: no nodes returned from BFS search for subroot: {subtree_root_node.node_identifier}')
        operation: DeleteSubtreeOp = DeleteSubtreeOp(subtree_root_node.node_identifier, node_list=subtree_node_list)
        self._execute_write_op(operation)

    # Various public getters
    # ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼

    def get_subtree_bfs_node_list(self, subtree_root: LocalNodeIdentifier) -> List[LocalNode]:
        return self._memstore.master_tree.get_subtree_bfs_node_list(subtree_root.node_uid)

    def get_subtree_bfs_sn_list(self, subtree_root_spid: LocalNodeIdentifier) -> List[SPIDNodePair]:
        return [self.to_sn(x) for x in self.get_subtree_bfs_node_list(subtree_root_spid)]

    def get_all_files_and_dirs_for_subtree(self, subtree_root: LocalNodeIdentifier) -> Tuple[List[LocalFileNode], List[LocalDirNode]]:
        if SUPER_DEBUG_ENABLED:
            logger.debug(f'Entered get_all_files_and_dirs_for_subtree(): subtree_root={subtree_root}')

        cache_info: Optional[PersistedCacheInfo] = \
            self.backend.cacheman.get_existing_cache_info_for_local_path(self.device.uid, subtree_root.get_single_path())
        if not cache_info:
            raise RuntimeError(f'get_all_files_and_dirs_for_subtree() could not find cache_info for: {subtree_root}')

        if not cache_info.is_loaded:
            raise RuntimeError(f'get_all_files_and_dirs_for_subtree() cache is not loaded: {cache_info.cache_location} '
                               f'(for requested subtree={subtree_root})')

        result = self._memstore.master_tree.get_all_files_and_dirs_for_subtree(subtree_root)
        return result

    def get_all_files_with_content(self, content_uid: UID, cache_info_list: List[PersistedCacheInfo]) -> List[LocalFileNode]:
        matching_file_list = []

        # Part 1 of 2: search memstore, which includes all loaded caches:
        def _add_if_content_matches(node):
            if node.is_file() and node.content_meta_uid == content_uid:
                matching_file_list.append(node)

        self._memstore.master_tree.for_each_node(_add_if_content_matches)

        # Part 2 of 2: search all non-loaded caches directly from disk:
        for cache_info in cache_info_list:
            if not cache_info.is_loaded:
                matching_file_list += self._diskstore.get_all_files_with_content(cache_info, content_uid)

        return matching_file_list

    def get_node_list_for_path_list(self, path_list: List[str]) -> List[LocalNode]:
        """
        Checks (1) memstore, (2) diskstore, (3) live disk
        """
        node_list: List[LocalNode] = []
        for full_path in path_list:
            node = self.read_node_for_path(full_path)
            if node:
                node_list.append(node)
        return node_list

    def get_uid_for_domain_id(self, domain_id: str, uid_suggestion: Optional[UID] = None) -> UID:
        return self.get_uid_for_path(domain_id, uid_suggestion)

    def get_uid_for_path(self, full_path: str, uid_suggestion: Optional[UID] = None) -> UID:
        assert isinstance(full_path, str), f'Expected a string but got: {type(full_path)}'
        return self.uid_path_mapper.get_uid_for_path(full_path, uid_suggestion)

    def get_path_for_uid(self, path_uid: UID) -> str:
        # Throws exception if no path found:
        return self.uid_path_mapper.get_path_for_uid(path_uid)

    @staticmethod
    def _cache_exists(cache_info: PersistedCacheInfo):
        """Check for the uncommon case of a cache being listed but failing to be created"""
        return os.path.exists(cache_info.cache_location) and os.stat(cache_info.cache_location).st_size > 0

    def get_node_for_domain_id(self, domain_id: str) -> LocalNode:
        """AKA get_node_for_full_path()"""
        uid: UID = self.get_uid_for_domain_id(domain_id)
        return self.read_node_for_uid(uid)

    def _get_subtree_bfs_from_cache(self, parent_spid: LocalNodeIdentifier) -> Optional[List[LocalNode]]:
        """Returns all nodes in the given subtree which can be found in the cache. If the cache is not loaded into memory, loads them from disk."""

        cache_info: Optional[PersistedCacheInfo] = \
            self.backend.cacheman.get_existing_cache_info_for_local_path(self.device.uid, parent_spid.get_single_path())
        if cache_info:
            if cache_info.is_loaded:
                return self._memstore.master_tree.get_subtree_bfs_node_list(parent_spid.node_uid)

            else:
                # Load subtree nodes directly from disk cache:
                return self._diskstore.get_subtree_bfs_from_cache(cache_info, parent_spid.node_uid)

        # both caches miss
        return None

    def _get_child_list_from_cache_for_spid(self, parent_spid: LocalNodeIdentifier, only_if_all_children_fetched: bool) \
            -> Optional[List[SPIDNodePair]]:
        """Searches in-memory cache, followed by disk cache, for children of the given SPID. Returns None if parent not found in either cache.
        If only_if_all_children_fetched=True, then we will only return a non-None value if we are certain that the list of children is complete;
        if False, then we are ok with returning a partial list."""

        # 1. Use in-memory cache if it exists. This will also allow pending op nodes to be handled
        if SUPER_DEBUG_ENABLED:
            logger.debug(f'_get_child_list_from_cache_for_spid(): Querying memstore for parent_spid={parent_spid} '
                         f'(only_if_all_children_fetched={only_if_all_children_fetched})')

        parent_node = self._memstore.master_tree.get_node_for_uid(parent_spid.node_uid)
        if parent_node:
            if TRACE_ENABLED:
                logger.debug(f'_get_child_list_from_cache_for_spid(): Found parent in memstore: {parent_node}')

            if not parent_node.is_dir():
                logger.debug(f'_get_child_list_from_cache_for_spid(): Found parent in memstore but it is not a dir: {parent_node}')
                return None
            elif not only_if_all_children_fetched or parent_node.all_children_fetched:
                try:
                    if SUPER_DEBUG_ENABLED:
                        logger.debug(f'_get_child_list_from_cache_for_spid(): Getting child list from memstore for parent: {parent_spid}')
                    return self._memstore.master_tree.get_child_list_for_spid(parent_spid)
                except NodeNotPresentError:
                    # In-memory cache miss. Try seeing if the relevant cache is loaded:
                    logger.debug(f'_get_child_list_from_cache_for_spid(): Could not find parent node in memstore: {parent_spid}')
                    pass
            else:
                logger.debug(f'_get_child_list_from_cache_for_spid(): Found parent in memstore but not all children are fetched: {parent_node}')
                # we can probably just return None at this point, but this should be such a rare case that there should b be little harm
                # in trying the disk store...
        elif SUPER_DEBUG_ENABLED:
            logger.debug(f'_get_child_list_from_cache_for_spid(): Failed to find parent node in memstore')

        # 2. Read from disk cache if it exists:
        cache_info: Optional[PersistedCacheInfo] = \
            self.backend.cacheman.get_existing_cache_info_for_local_path(self.device.uid, parent_spid.get_single_path())
        if cache_info:
            if cache_info.is_loaded:
                logger.debug(f'_get_child_list_from_cache_for_spid(): Disk cache ({cache_info.cache_location}) is loaded but does not '
                             f'contain parent: {parent_spid}')
                return None
            else:
                logger.debug(f'_get_child_list_from_cache_for_spid(): In-memory cache miss; trying disk cache for: {parent_spid}')
                return self._diskstore.get_child_list_for_node_uid(cache_info, parent_spid.node_uid, only_if_all_children_fetched)

        else:
            if SUPER_DEBUG_ENABLED:
                logger.debug(f'_get_child_list_from_cache_for_spid(): Could not find cache in registry for: {parent_spid}')

        logger.debug(f'_get_child_list_from_cache_for_spid(): both in-memory and disk caches missed: {parent_spid}')
        return None

    def get_child_list_for_spid(self, parent_spid: LocalNodeIdentifier, filter_state: FilterState) -> List[SPIDNodePair]:
        if SUPER_DEBUG_ENABLED:
            logger.debug(f'Entered get_child_list_for_spid(): spid={parent_spid} filter_state={filter_state}')

        if filter_state and filter_state.has_criteria():
            # This only works if cache already loaded:
            # FIXME: let's make this work even if the cache isn't loaded
            if not self.is_cache_loaded_for(parent_spid):
                raise RuntimeError(f'Cannot load filtered child list: cache not yet loaded for: {parent_spid}')

            child_list = filter_state.get_filtered_child_list(parent_spid, self._memstore.master_tree)
            logger.debug(f'get_child_list_for_spid(): Returning {len(child_list)} filtered children for parent {parent_spid.guid}')
        else:
            child_list = self._get_child_list_from_cache_for_spid(parent_spid, only_if_all_children_fetched=True)
            if child_list is None:
                # 3. No cache hits. Must do a live scan:
                logger.debug(f'get_child_list_for_spid(): Caches are missing or only partial for children of parent {parent_spid}; '
                             f'will attempt disk scan')
                child_list = self._scan_and_cache_dir(parent_spid)
                if SUPER_DEBUG_ENABLED:
                    logger.debug(f'get_child_list_for_spid(): Scanner returned {len(child_list)} children for parent: {parent_spid.guid}')
            elif SUPER_DEBUG_ENABLED:
                logger.debug(f'get_child_list_for_spid(): Returning {len(child_list)} children from cache for parent {parent_spid.guid}')

        return child_list

    def _scan_and_cache_dir(self, parent_spid: LocalNodeIdentifier) -> List[SPIDNodePair]:
        # Scan dir on disk (read-through)

        scanner = LocalDiskTreeScanner(backend=self.backend, master_local=self, root_node_identifer=parent_spid, tree_id=None)
        # This may call overwrite_dir_entries_list()
        child_list = scanner.scan_single_dir(parent_spid.get_single_path())
        return [self.to_sn(x) for x in child_list]

    def to_sn(self, node, single_path: Optional[str] = None) -> SPIDNodePair:
        # Trivial for LocalNodes
        return SPIDNodePair(node.node_identifier, node)

    def get_parent_for_sn(self, sn: SPIDNodePair) -> Optional[SPIDNodePair]:
        parent_node = self.get_single_parent_for_node(sn.node)
        if parent_node:
            return self.to_sn(parent_node)
        return None

    def get_node_for_uid(self, uid: UID) -> Optional[LocalNode]:
        if TRACE_ENABLED:
            logger.debug(f'Entered get_node_for_uid(): uid={uid}')
        # Just delegate to read_node_for_uid() and do a read-through:
        return self.read_node_for_uid(uid)

    def get_parent_list_for_node(self, node: LocalNode) -> List[LocalNode]:
        parent_node = self.get_single_parent_for_node(node)
        if parent_node:
            return [parent_node]
        return []

    def get_single_parent_for_node(self, node: LocalNode, required_subtree_path: str = None) -> Optional[LocalNode]:
        """LocalNodes are guaranteed to have at most 1 parent."""
        if TRACE_ENABLED:
            logger.debug(f'Entered get_single_parent_for_node(); node={node}, required_subtree_path={required_subtree_path}')

        if node.get_single_path() == ROOT_PATH:
            if TRACE_ENABLED:
                logger.debug(f'get_single_parent_for_node(): mode ({node}) is already root (required_path={required_subtree_path}))')
            return None

        try:
            # easiest and fastest if the tree is already in memory:
            parent: LocalNode = self._memstore.master_tree.get_parent(node.uid)
            if not parent:
                # parent not found in memory tree. Let's check everywhere now:
                assert isinstance(node.node_identifier, SinglePathNodeIdentifier)
                parent_path: str = node.node_identifier.get_single_parent_path()
                parent_uid: UID = self.get_uid_for_path(parent_path)
                parent = self.get_node_for_uid(parent_uid)
                if not parent:
                    logger.debug(f'get_single_parent_for_node(): Could not find parent for node {node.node_identifier.guid}')
                    return None

            if parent and required_subtree_path and not pathlib.PurePosixPath(parent.get_single_path()).is_relative_to(required_subtree_path):
                logger.debug(f'get_single_parent_for_node(): parent path ({parent.get_single_path()}) '
                             f'does not contain required subtree path ({required_subtree_path}) (orig node: {node}')
                return None

            return parent
        except NodeNotPresentError:
            return None
        except Exception:
            logger.error(f'Error getting parent for node: {node}, required_path: {required_subtree_path}')
            raise

    def _get_stats(self, full_path: str, staging_path: Optional[str] = None):
        if staging_path:
            path = staging_path
        else:
            path = full_path

        stat = os.stat(path)

        size_bytes = int(stat.st_size)

        modify_ts = int(stat.st_mtime * 1000)
        if IS_WINDOWS:
            # Windows deviates from the standard and stores creation time as ctime, and does not track metadata change time.
            create_ts = int(stat.st_ctime * 1000)
            change_ts = 0
        else:
            change_ts = int(stat.st_ctime * 1000)
            # This should work for both Mac and Linux.
            # To see create_ts via cmd line on Mac: /Developer/Tools/GetFileInfo {filename}
            create_ts = int(stat.st_birthtime * 1000)

        if IS_MACOS and staging_path:
            # MacOS has a bug where moving/copying a file will truncate its timestamps. We'll try to match its behavior.
            # See https://macperformanceguide.com/blog/2019/20190903_1600-macOS-truncates-file-dates.html
            modify_ts_mac = math.trunc(modify_ts / 1000) * 1000
            change_ts_mac = math.trunc(change_ts / 1000) * 1000
            if SUPER_DEBUG_ENABLED:
                logger.debug(f'MACOS: tweaked modify_ts ({modify_ts}->{modify_ts_mac}) & change_ts ({change_ts}->{change_ts_mac}) for '
                             f'device {self.device.uid}: "{full_path}"')
            modify_ts = modify_ts_mac
            change_ts = change_ts_mac

        assert create_ts > 100000000000, f'create_ts too small: {create_ts} for path: {path}'
        assert modify_ts > 100000000000, f'modify_ts too small: {modify_ts} for path: {path}'
        assert change_ts > 100000000000, f'change_ts too small: {change_ts} for path: {path}'

        # Get "now" in UNIX time:
        sync_ts = time_util.now_sec()

        return size_bytes, sync_ts, create_ts, modify_ts, change_ts

    def build_local_dir_node(self, full_path: str, is_live: bool, all_children_fetched: bool) -> Optional[LocalDirNode]:
        if SUPER_DEBUG_ENABLED:
            logger.debug(f'build_local_dir_node() called for path "{full_path}", is_live={is_live}, all_children_fetched={all_children_fetched}')
        uid = self.get_uid_for_path(full_path)

        if is_live:
            if not os.path.exists(full_path):
                logger.debug(f'build_local_dir_node(): path does not exist: "{full_path}"')
                return None
            elif not os.path.isdir(full_path):
                raise RuntimeError(f'build_local_dir_node(): path is not a dir: "{full_path}"')

            try:
                size_bytes, sync_ts, create_ts, modify_ts, change_ts = self._get_stats(full_path)
            except FileNotFoundError:
                # Caller didn't check whether file existed (may also be a missing dir). Let them know:
                logger.info(f'build_local_dir_node(): dir not found (returning None): "{full_path}" ')
                return None

        else:
            # if is_live==false, we don't even expect it to exist
            create_ts = None
            modify_ts = None
            change_ts = None
            sync_ts = time_util.now_sec()

        parent_path = str(pathlib.Path(full_path).parent)
        parent_uid: UID = self.get_uid_for_path(parent_path)
        return LocalDirNode(node_identifier=LocalNodeIdentifier(uid=uid, device_uid=self.device.uid, full_path=full_path), parent_uid=parent_uid,
                            trashed=TrashStatus.NOT_TRASHED, is_live=is_live, sync_ts=sync_ts, create_ts=create_ts,
                            modify_ts=modify_ts, change_ts=change_ts, all_children_fetched=all_children_fetched)

    def build_local_file_node(self, full_path: str, staging_path: str = None, must_scan_signature=False, is_live: bool = True) \
            -> Optional[LocalFileNode]:
        effective_path = full_path if staging_path is None else staging_path

        if is_live:
            if not os.path.exists(effective_path):
                logger.debug(f'build_local_file_node(): path does not exist: "{effective_path}"')
                return None

            elif os.path.isdir(effective_path):
                raise RuntimeError(f'build_local_file_node(): path is actually a dir: {effective_path}')

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

        try:
            size_bytes, sync_ts, create_ts, modify_ts, change_ts = self._get_stats(full_path, staging_path)
        except FileNotFoundError:
            # Caller didn't check whether file existed (may also be a missing dir). Let them know:
            logger.debug(f'build_local_file_node(): file not found when getting stats (returning None): "{effective_path}" ')
            return None

        if self.backend.cacheman.lazy_load_local_file_signatures and not must_scan_signature:
            # Skip signatures and set them NULL for now. TNode will be added to content scanning queue when it is upserted into cache (above)
            content_meta = None
        else:
            try:
                content_meta = self.backend.cacheman.calculate_signature_for_local_file(device_uid=self.device_uid, full_path=effective_path)
                if not content_meta:
                    raise RuntimeError(f'Failed to calculate signature for file "{effective_path}"')
            except FileNotFoundError:
                # bad link
                logger.debug(f'build_local_file_node(): file not found while calculating signature (returning None): "{effective_path}" ')
                return None

        uid = self.get_uid_for_path(full_path)
        parent_path = str(pathlib.Path(full_path).parent)
        parent_uid: UID = self.get_uid_for_path(parent_path)

        node_identifier = LocalNodeIdentifier(uid=uid, device_uid=self.device.uid, full_path=full_path)
        new_node = LocalFileNode(node_identifier, parent_uid, content_meta, size_bytes, sync_ts, create_ts, modify_ts, change_ts,
                                 TrashStatus.NOT_TRASHED, is_live)

        if TRACE_ENABLED:
            logger.debug(f'Built: {new_node} with sync_ts: {sync_ts}')

        assert new_node.modify_ts == modify_ts

        return new_node
