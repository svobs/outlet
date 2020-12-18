import copy
import logging
import os
import pathlib
import threading
from typing import List, Optional, Tuple

import store.local.content_hasher
from constants import SUPER_DEBUG, TrashStatus, TREE_TYPE_LOCAL_DISK
from model.local_disk_tree import LocalDiskTree
from model.node.local_disk_node import LocalDirNode, LocalFileNode, LocalNode
from model.node_identifier import LocalNodeIdentifier, SinglePathNodeIdentifier
from store.cache_manager import PersistedCacheInfo
from store.local.local_disk_scanner import LocalDiskScanner
from store.local.local_sig_calc_thread import SignatureCalcThread
from store.local.master_local_disk import LocalDiskDiskStore
from store.local.master_local_write_op import BatchChangesOp, DeleteSingleNodeOp, DeleteSubtreeOp, LocalDiskMemoryStore, LocalSubtree, \
    LocalWriteThroughOp, UpsertSingleNodeOp
from store.master import MasterStore
from store.sqlite.local_db import LocalDiskDatabase
from store.uid.uid_generator import UID
from store.uid.uid_mapper import UidPathMapper
from signal import ID_GLOBAL_CACHE
from model.display_tree.filter_criteria import FilterCriteria
from util import file_util, time_util
from util.simple_tree import NodeNotPresentError
from util.stopwatch_sec import Stopwatch

logger = logging.getLogger(__name__)


class LocalDiskMasterStore(MasterStore):
    """
    ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
    CLASS LocalDiskMasterStore

    Singleton in-memory cache for local filesystem

    # TODO: consider scanning only root dir at first, then enqueuing subdirectories
    ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼
    """
    def __init__(self, backend, uid_path_cache_path):
        MasterStore.__init__(self)
        self.backend = backend

        self.uid_mapper = UidPathMapper(backend, uid_path_cache_path)

        self._struct_lock = threading.Lock()
        self._memstore: LocalDiskMemoryStore = LocalDiskMemoryStore(backend)
        self._diskstore: LocalDiskDiskStore = LocalDiskDiskStore(backend)

        initial_sleep_sec: float = self.backend.config.get('cache.lazy_load_local_file_signatures_initial_delay_ms') / 1000.0
        self._signature_calc_thread = SignatureCalcThread(self.backend, initial_sleep_sec)

        self.lazy_load_signatures: bool = backend.config.get('cache.lazy_load_local_file_signatures')

    def start_signature_calc_thread(self):
        if not self._signature_calc_thread.is_alive():
            self._signature_calc_thread.start()

    def start(self):
        MasterStore.start(self)
        self.uid_mapper.start()
        self._diskstore.start()
        if self.lazy_load_signatures:
            self.start_signature_calc_thread()

    def shutdown(self):
        MasterStore.shutdown(self)
        try:
            self.backend = None
            self._memstore = None
            self._diskstore = None
            if self._signature_calc_thread:
                self._signature_calc_thread.shutdown()
                self._signature_calc_thread = None
        except NameError:
            pass

    def _execute_write_op(self, operation: LocalWriteThroughOp):
        if SUPER_DEBUG:
            logger.debug(f'Executing operation: {operation}')
        assert self._struct_lock.locked()

        # 1. Update memory
        operation.update_memstore(self._memstore)

        # 2. Update disk
        cacheman = self.backend.cacheman
        if cacheman.enable_save_to_disk:
            if SUPER_DEBUG:
                logger.debug(f'Updating diskstore for operation {operation}')
            self._diskstore.execute_op(operation)

        else:
            logger.debug(f'Save to disk is disabled: skipping save to disk for operation')

        # 3. Send signals
        if SUPER_DEBUG:
            logger.debug(f'Sending signals for operation {operation}')
        operation.send_signals()

    # Disk access
    # ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼

    def _save_subtree_to_disk(self, cache_info: PersistedCacheInfo, tree_id):
        assert isinstance(cache_info.subtree_root, LocalNodeIdentifier)

        file_list, dir_list = self.get_all_files_and_dirs_for_subtree(cache_info.subtree_root)

        self._diskstore.save_subtree(cache_info, file_list, dir_list, tree_id)

    def _scan_file_tree(self, subtree_root: LocalNodeIdentifier, tree_id: str) -> LocalDiskTree:
        """If subtree_root is a file, then a tree is returned with only 1 node"""
        logger.debug(f'[{tree_id}] Scanning filesystem subtree: {subtree_root}')
        scanner = LocalDiskScanner(backend=self.backend, root_node_identifer=subtree_root, tree_id=tree_id)
        return scanner.scan()

    def _resync_with_file_system(self, subtree_root: LocalNodeIdentifier, tree_id: str):
        """Scan directory tree and update master tree where needed."""
        fresh_tree: LocalDiskTree = self._scan_file_tree(subtree_root, tree_id)

        if SUPER_DEBUG:
            logger.debug(f'[{tree_id}] Scanned fresh tree: \n{fresh_tree.show()}')

        with self._struct_lock:
            # Just upsert all nodes in the updated tree and let God (or some logic) sort them out.
            # Need extra logic to find removed nodes and pending op nodes though:
            root_node: LocalNode = fresh_tree.get_root_node()
            if root_node.is_dir():
                # "Pending op" nodes are not stored in the regular cache (they should all have is_live()==False)
                pending_op_nodes: List[LocalNode] = []
                remove_node_list: List[LocalNode] = []

                for existing_node in self._memstore.master_tree.get_subtree_bfs(subtree_root.uid):
                    if not fresh_tree.get_node(existing_node.uid):
                        if existing_node.is_live():
                            remove_node_list.append(existing_node)
                        else:
                            pending_op_nodes.append(existing_node)

                if pending_op_nodes:
                    logger.debug(f'Attempting to transfer {len(pending_op_nodes)} pending op src/dst nodes to the newly synced tree')
                    for pending_op_node in pending_op_nodes:
                        if SUPER_DEBUG:
                            logger.debug(f'Inserting pending op node: {pending_op_node}')
                        assert not pending_op_node.is_live()
                        if fresh_tree.can_add_without_mkdir(pending_op_node):
                            fresh_tree.add_to_tree(pending_op_node)
                        else:
                            # TODO: notify the OpLedger and devise a recovery strategy
                            logger.error(f'Cannot add pending op node (its parent is gone): {pending_op_node}')

            subtree = LocalSubtree(subtree_root, remove_node_list, fresh_tree.get_subtree_bfs())
            batch_changes_op: BatchChangesOp = BatchChangesOp(subtree_list=[subtree])
            self._execute_write_op(batch_changes_op)

    # LocalSubtree-level methods
    # ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼

    def show_tree(self, subtree_root: LocalNodeIdentifier) -> str:
        with self._struct_lock:
            return self._memstore.master_tree.show(nid=subtree_root.uid)

    def load_subtree(self, subtree_root: LocalNodeIdentifier, tree_id: str):
        logger.debug(f'[{tree_id}] DisplayTree requested for root: {subtree_root}')
        self._get_display_tree(subtree_root, tree_id, is_live_refresh=False)

    def _get_display_tree(self, subtree_root: LocalNodeIdentifier, tree_id: str, is_live_refresh: bool = False) -> None:
        """
        Performs a read-through retrieval of all the LocalFileNodes in the given subtree
        on the local filesystem.
        """

        if not os.path.exists(subtree_root.get_single_path()):
            logger.error(f'Cannot load meta for subtree because it does not exist: "{subtree_root.get_single_path()}"')
            return

        self._ensure_uid_consistency(subtree_root)

        # If we have already loaded this subtree as part of a larger cache, use that:
        cache_man = self.backend.cacheman
        cache_info: Optional[PersistedCacheInfo] = cache_man.find_existing_cache_info_for_local_subtree(subtree_root.get_single_path())
        if cache_info:
            logger.debug(f'[{tree_id}] LocalSubtree ({subtree_root}) is part of existing cached supertree ({cache_info.subtree_root})')
        else:
            # No supertree found in cache. Create new cache entry:
            cache_info = cache_man.get_or_create_cache_info_entry(subtree_root)
        assert cache_info
        self._create_display_tree(cache_info, tree_id, subtree_root, is_live_refresh)

    def _create_display_tree(self, cache_info: PersistedCacheInfo, tree_id: str, requested_subtree_root: LocalNodeIdentifier = None,
                             is_live_refresh: bool = False) -> None:
        """requested_subtree_root, if present, is a subset of the cache_info's subtree and it will be used. Otherwise cache_info's will be used"""
        assert cache_info
        assert isinstance(cache_info.subtree_root, SinglePathNodeIdentifier), f'Found instead: {type(cache_info.subtree_root)}'
        stopwatch_total = Stopwatch()

        self._ensure_uid_consistency(cache_info.subtree_root)

        if not requested_subtree_root:
            requested_subtree_root = cache_info.subtree_root
        else:
            self._ensure_uid_consistency(requested_subtree_root)

        # LOAD into master tree. Only for first load!
        if not cache_info.is_loaded:
            if self.backend.cacheman.enable_load_from_disk:
                tree = self._diskstore.load_subtree(cache_info, tree_id)
                if tree:
                    if SUPER_DEBUG:
                        logger.debug(f'[{tree_id}] Loaded cached tree: \n{tree.show()}')
                    with self._struct_lock:
                        self._memstore.master_tree.replace_subtree(tree)
                        logger.debug(f'[{tree_id}] Updated in-memory cache: {self.get_summary()}')
            else:
                logger.debug(f'[{tree_id}] Skipping cache disk load because cache.enable_load_from_disk is false')

        # FS SYNC
        if is_live_refresh or not cache_info.is_loaded or \
                (cache_info.needs_refresh and self.backend.cacheman.sync_from_local_disk_on_cache_load):
            logger.debug(f'[{tree_id}] Will resync with file system (is_loaded={cache_info.is_loaded}, sync_on_cache_load='
                         f'{self.backend.cacheman.sync_from_local_disk_on_cache_load}, needs_refresh={cache_info.needs_refresh},'
                         f'is_live_refresh={is_live_refresh})')
            # Update from the file system, and optionally save any changes back to cache:
            self._resync_with_file_system(requested_subtree_root, tree_id)
            if SUPER_DEBUG:
                logger.debug(f'[{tree_id}] File system sync complete')
            # We can only mark this as 'done' (False) if the entire cache contents has been refreshed:
            if requested_subtree_root.uid == cache_info.subtree_root.uid:
                cache_info.needs_refresh = False
        elif not self.backend.cacheman.sync_from_local_disk_on_cache_load:
            logger.debug(f'[{tree_id}] Skipping filesystem sync because it is disabled for cache loads')
        elif not cache_info.needs_refresh:
            logger.debug(f'[{tree_id}] Skipping filesystem sync because the cache is still fresh for path: {cache_info.subtree_root}')

        # SAVE
        if cache_info.needs_save:
            if not cache_info.is_loaded:
                logger.warning(f'[{tree_id}] Skipping cache save: cache was never loaded!')
            elif self.backend.cacheman.enable_save_to_disk:
                # Save the updates back to local disk cache:
                self._save_subtree_to_disk(cache_info, tree_id)
            else:
                logger.debug(f'[{tree_id}] Skipping cache save because it is disabled')
        elif SUPER_DEBUG:
            logger.debug(f'[{tree_id}] Skipping cache save: not needed')

        logger.info(f'[{tree_id}] {stopwatch_total} Load complete for {requested_subtree_root}')

    def _ensure_uid_consistency(self, subtree_root: SinglePathNodeIdentifier):
        """Since the UID of the subtree root node is stored in 3 different locations (registry, cache file, and memory),
        checks that at least registry & memory match. If UID is not in memory, guarantees that it will be stored with the value from registry.
        This method should only be called for the subtree root of display trees being loaded"""
        existing_uid = subtree_root.uid
        new_uid = self.get_uid_for_path(subtree_root.get_single_path(), existing_uid, override_load_check=True)
        if existing_uid != new_uid:
            logger.warning(f'Requested UID "{existing_uid}" is invalid for given path; changing it to "{new_uid}"')
        subtree_root.uid = new_uid

    def consolidate_local_caches(self, local_caches: List[PersistedCacheInfo], tree_id) -> bool:
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
                with self._struct_lock:
                    self._memstore.master_tree.replace_subtree(super_tree)

                # this will resync with file system and/or save if configured
                supertree_cache.needs_save = True
                self._create_display_tree(supertree_cache, tree_id)
                # Now it is safe to delete the subtree cache:
                file_util.delete_file(subtree_cache.cache_location)

        registry_needs_update = len(supertree_sets) > 0
        return registry_needs_update

    def refresh_subtree(self, node_identifier: LocalNodeIdentifier, tree_id: str):
        assert isinstance(node_identifier, LocalNodeIdentifier)
        self._get_display_tree(node_identifier, tree_id, is_live_refresh=True)

    def refresh_subtree_stats(self, subtree_root_node: LocalNode, tree_id: str):
        with self._struct_lock:
            self._memstore.master_tree.refresh_stats(subtree_root_node, tree_id)

    # Cache CRUD operations
    # ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼

    def load_single_node_for_path(self, full_path: str) -> Optional[LocalNode]:
        """This actually reads directly from the disk cache"""
        logger.debug(f'Loading single node for path: "{full_path}"')
        cache_man = self.backend.cacheman
        cache_info: Optional[PersistedCacheInfo] = cache_man.find_existing_cache_info_for_local_subtree(full_path)
        if not cache_info:
            logger.debug(f'Could not find cache containing path: "{full_path}"')
            return None
        if cache_info.is_loaded:
            logger.debug(f'Cache is already loaded for subtree (will return node from memory): "{cache_info.subtree_root}"')
            return self.get_node_for_domain_id(full_path)
        with LocalDiskDatabase(cache_info.cache_location, self.backend) as cache:
            return cache.get_file_or_dir_for_path(full_path)

    def upsert_single_node(self, node: LocalNode):
        assert node and node.node_identifier.tree_type == TREE_TYPE_LOCAL_DISK, f'Cannot upsert node: invalid node provided: {node}'
        assert self.uid_mapper.get_uid_for_path(node.get_single_path(), node.uid) == node.uid, \
            f'Internal error while trying to upsert node to cache: UID did not match expected ' \
            f'({self.uid_mapper.get_uid_for_path(node.get_single_path(), node.uid)}); node={node}'

        with self._struct_lock:
            self._execute_write_op(UpsertSingleNodeOp(node))

    def update_single_node(self, node: LocalNode):
        assert node, 'Cannot update node: no node provided!'

        with self._struct_lock:
            self._execute_write_op(UpsertSingleNodeOp(node, update_only=True))

    def remove_single_node(self, node: LocalNode, to_trash=False):
        assert node, 'Cannot remove node: no node provided!'
        logger.debug(f'Removing node from caches (to_trash={to_trash}): {node}')

        with self._struct_lock:
            self._execute_write_op(DeleteSingleNodeOp(node, to_trash=to_trash))

    def _migrate_node(self, node: LocalNode, src_full_path: str, dst_full_path: str) -> LocalNode:
        new_node_full_path: str = file_util.change_path_to_new_root(node.get_single_path(), src_full_path, dst_full_path)
        new_node_uid: UID = self.get_uid_for_path(new_node_full_path)

        new_node = copy.deepcopy(node)
        new_node.set_node_identifier(LocalNodeIdentifier(uid=new_node_uid, path_list=new_node_full_path))
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
            src_node: LocalNode = self._memstore.master_tree.get_node(src_uid)
            if src_node:
                src_nodes: List[LocalNode] = self._memstore.master_tree.get_subtree_bfs(src_node.uid)
                src_subtree: LocalSubtree = LocalSubtree(src_node.node_identifier, remove_node_list=[], upsert_node_list=src_nodes)
            else:
                logger.error(f'MV src node does not exist: UID={src_uid}, path={src_full_path}')
                return

            # Create up to 3 tree operations which should be executed in a single transaction if possible
            dst_uid: UID = self.get_uid_for_path(dst_full_path)
            dst_node_identifier: LocalNodeIdentifier = LocalNodeIdentifier(uid=dst_uid, path_list=dst_full_path)
            dst_subtree: LocalSubtree = LocalSubtree(dst_node_identifier, [], [])

            existing_dst_node: LocalNode = self._memstore.master_tree.get_node(dst_uid)
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
                    fresh_tree: LocalDiskTree = self._scan_file_tree(dst_node_identifier, ID_GLOBAL_CACHE)
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

        with self._struct_lock:
            subtree_nodes: List[LocalNode] = self._memstore.master_tree.get_subtree_bfs(subtree_root_node.uid)
            assert isinstance(subtree_root_node.node_identifier, LocalNodeIdentifier)
            operation: DeleteSubtreeOp = DeleteSubtreeOp(subtree_root_node.node_identifier, node_list=subtree_nodes)
            self._execute_write_op(operation)

    # Various public getters
    # ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼

    def get_all_files_and_dirs_for_subtree(self, subtree_root: LocalNodeIdentifier) -> Tuple[List[LocalFileNode], List[LocalDirNode]]:
        if SUPER_DEBUG:
            logger.debug(f'Entered get_all_files_and_dirs_for_subtree(): locked={self._struct_lock.locked()}')
        with self._struct_lock:
            return self._memstore.master_tree.get_all_files_and_dirs_for_subtree(subtree_root)

    def get_node_list_for_path_list(self, path_list: List[str]) -> List[LocalNode]:
        node_list: List[LocalNode] = []
        for full_path in path_list:
            node = self.get_node_for_domain_id(full_path)
            if node:
                node_list.append(node)
        return node_list

    def get_uid_for_domain_id(self, domain_id: str, uid_suggestion: Optional[UID] = None) -> UID:
        return self.get_uid_for_path(domain_id, uid_suggestion)

    def get_uid_for_path(self, full_path: str, uid_suggestion: Optional[UID] = None, override_load_check: bool = False) -> UID:
        assert isinstance(full_path, str)

        if not override_load_check:
            # Very important to check this. If we ask the UID mapper to return a UID for a path it is not familiar with, it will generate
            # a new one, which will conflict with the one in the cache.
            cache_info: Optional[PersistedCacheInfo] = self.backend.cacheman.find_existing_cache_info_for_local_subtree(full_path)
            if cache_info and not cache_info.is_loaded:
                raise RuntimeError(f'Cache containing path: "{full_path}" is not loaded!')

        return self.uid_mapper.get_uid_for_path(full_path, uid_suggestion)

    def get_node_for_domain_id(self, domain_id: str) -> LocalNode:
        """AKA get_node_for_full_path()"""
        uid: UID = self.get_uid_for_domain_id(domain_id)
        return self.get_node_for_uid(uid)

    def get_children(self, node: LocalNode, filter_criteria: FilterCriteria = None) -> List[LocalNode]:
        if SUPER_DEBUG:
            logger.debug(f'Entered get_children(): node={node.node_identifier} filter_criteria={filter_criteria} locked={self._struct_lock.locked()}')
        if filter_criteria:
            return filter_criteria.get_filtered_child_list(node, self)
        else:
            with self._struct_lock:
                child_nodes = self._memstore.master_tree.get_child_list(node.uid)
        return child_nodes

    def get_node_for_uid(self, uid: UID) -> Optional[LocalNode]:
        if SUPER_DEBUG:
            logger.debug(f'Entered get_node_for_uid(): uid={uid} locked={self._struct_lock.locked()}')
        with self._struct_lock:
            return self._memstore.master_tree.get_node(uid)

    def get_parent_list_for_node(self, node: LocalNode) -> List[LocalNode]:
        parent_node = self.get_single_parent_for_node(node)
        if parent_node:
            return [parent_node]
        return []

    def get_single_parent_for_node(self, node: LocalNode, required_subtree_path: str = None) -> Optional[LocalNode]:
        """LocalNodes are guaranteed to have at most 1 parent."""
        if SUPER_DEBUG:
            logger.debug(f'Entered get_single_parent_for_node({node.node_identifier}): locked={self._struct_lock.locked()}')
        try:
            with self._struct_lock:
                try:
                    parent: LocalNode = self._memstore.master_tree.get_parent(node.identifier)
                except KeyError:
                    # parent not found in tree... maybe we can derive it however
                    parent_path: str = node.derive_parent_path()
                    parent_uid: UID = self.get_uid_for_path(parent_path)
                    parent = self._memstore.master_tree.get_node(parent_uid)
                    if not parent:
                        logger.debug(f'Parent not found for node ({node.uid})')
                        return None
                    logger.debug(f'Parent not found for node ({node.uid}) but found parent at path: {parent.get_single_path()}')
                if not required_subtree_path or parent.get_single_path().startswith(required_subtree_path):
                    return parent
                return None
        except NodeNotPresentError:
            return None
        except Exception:
            logger.error(f'Error getting parent for node: {node}, required_path: {required_subtree_path}')
            raise

    def get_summary(self):
        if self._memstore.use_md5:
            md5 = str(self._memstore.md5_dict.total_entries)
        else:
            md5 = 'disabled'
        return f'LocalDiskMasterStore tree_size={len(self._memstore.master_tree):n} md5={md5}'

    def build_local_dir_node(self, full_path: str, is_live: bool) -> LocalDirNode:
        uid = self.get_uid_for_path(full_path)

        parent_path = str(pathlib.Path(full_path).parent)
        parent_uid: UID = self.get_uid_for_path(parent_path)
        return LocalDirNode(node_identifier=LocalNodeIdentifier(uid=uid, path_list=full_path), parent_uid=parent_uid,
                            trashed=TrashStatus.NOT_TRASHED, is_live=is_live)

    def build_local_file_node(self, full_path: str, staging_path: str = None, must_scan_signature=False) -> Optional[LocalFileNode]:
        uid = self.get_uid_for_path(full_path)

        parent_path = str(pathlib.Path(full_path).parent)
        parent_uid: UID = self.get_uid_for_path(parent_path)

        if os.path.islink(full_path):
            target = os.readlink(full_path)
            if not os.path.exists(target):
                logger.error(f'Broken link, skipping: "{full_path}" -> "{target}"')
                return None

        if self.lazy_load_signatures and not must_scan_signature:
            # Skip MD5 and set it NULL for now. Node will be added to content scanning queue when it is upserted into cache (above)
            md5 = None
            sha256 = None
        else:
            try:
                md5, sha256 = store.local.content_hasher.calculate_signatures(full_path, staging_path)
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
        assert modify_ts > 100000000000, f'modify_ts too small: {modify_ts} for path: {path}'
        change_ts = int(stat.st_ctime * 1000)
        assert change_ts > 100000000000, f'change_ts too small: {change_ts} for path: {path}'

        node_identifier = LocalNodeIdentifier(uid=uid, path_list=full_path)
        return LocalFileNode(node_identifier, parent_uid, md5, sha256, size_bytes, sync_ts, modify_ts, change_ts, TrashStatus.NOT_TRASHED, True)
