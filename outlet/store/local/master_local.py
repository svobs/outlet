import copy
import logging
import os
import threading
import time
from typing import List, Optional, Tuple

from pydispatch import dispatcher
from treelib.exceptions import NodeIDAbsentError

import store.local.content_hasher
from constants import SUPER_DEBUG, TREE_TYPE_LOCAL_DISK
from model.display_tree.display_tree import DisplayTree
from model.display_tree.local_disk import LocalDiskDisplayTree
from model.display_tree.null import NullDisplayTree
from model.local_disk_tree import LocalDiskTree
from model.node.container_node import RootTypeNode
from model.node.display_node import DisplayNode
from model.node.local_disk_node import LocalDirNode, LocalFileNode, LocalNode
from model.node_identifier import LocalFsIdentifier, NodeIdentifier
from store.cache_manager import PersistedCacheInfo
from store.local.local_disk_scanner import LocalDiskScanner
from store.local.local_sig_calc_thread import SignatureCalcThread
from store.local.master_local_op import BatchChangesOp, DeleteSingleNodeOp, DeleteSubtreeOp, LocalDiskOpExecutor, LocalDiskMemoryStore, LocalSubtree, \
    UpsertSingleNodeOp
from store.master import MasterCache
from store.sqlite.local_db import LocalDiskDatabase
from store.uid.uid_generator import UID
from store.uid.uid_mapper import UidPathMapper
from ui import actions
from ui.actions import ID_GLOBAL_CACHE
from util import file_util
from util.stopwatch_sec import Stopwatch

logger = logging.getLogger(__name__)


# ⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛
# CLASS LocalDiskMasterCache
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼

# TODO: consider scanning only root dir at first, then enqueuing subdirectories

class LocalDiskMasterCache(MasterCache):
    """Singleton in-memory cache for local filesystem"""
    def __init__(self, app):
        MasterCache.__init__(self)
        self.app = app

        self.uid_mapper = UidPathMapper(app)

        self._struct_lock = threading.Lock()
        self._memstore: LocalDiskMemoryStore = LocalDiskMemoryStore(app)

        self._executor = LocalDiskOpExecutor(app, self._memstore)

        initial_sleep_sec: float = self.app.config.get('cache.lazy_load_local_file_signatures_initial_delay_ms') / 1000.0
        self._signature_calc_thread = SignatureCalcThread(self.app, initial_sleep_sec)

        self.lazy_load_signatures: bool = app.config.get('cache.lazy_load_local_file_signatures')

    def start_signature_calc_thread(self):
        if not self._signature_calc_thread.is_alive():
            self._signature_calc_thread.start()

    def start(self):
        super(LocalDiskMasterCache, self).start()
        if self.lazy_load_signatures:
            self.start_signature_calc_thread()

    def shutdown(self):
        super(LocalDiskMasterCache, self).shutdown()
        try:
            self.app = None
            self._memstore = None
            self._executor = None
            self._signature_calc_thread = None
        except NameError:
            pass

    # Disk access
    # ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼

    def _load_subtree_from_disk(self, cache_info: PersistedCacheInfo, tree_id) -> Optional[LocalDiskTree]:
        """Loads the given subtree disk cache from disk."""

        stopwatch_load = Stopwatch()

        # Load cache from file, and update with any local FS ops found:
        with LocalDiskDatabase(cache_info.cache_location, self.app) as disk_cache:
            if not disk_cache.has_local_files() and not disk_cache.has_local_dirs():
                logger.debug(f'No meta found in cache ({cache_info.cache_location}) - will skip loading it')
                return None

            status = f'[{tree_id}] Loading meta for "{cache_info.subtree_root}" from cache: "{cache_info.cache_location}"'
            logger.debug(status)
            dispatcher.send(actions.SET_PROGRESS_TEXT, sender=tree_id, msg=status)

            uid = self.get_uid_for_path(cache_info.subtree_root.full_path, cache_info.subtree_root.uid)
            if cache_info.subtree_root.uid != uid:
                logger.warning(f'Requested UID "{cache_info.subtree_root.uid}" is invalid for given path; changing it to "{uid}"')
            cache_info.subtree_root.uid = uid

            root_node_identifer = LocalFsIdentifier(full_path=cache_info.subtree_root.full_path, uid=uid)
            tree: LocalDiskTree = LocalDiskTree(self.app)
            root_node = LocalDirNode(node_identifier=root_node_identifer, exists=True)
            tree.add_node(node=root_node, parent=None)

            missing_nodes: List[DisplayNode] = []

            dir_list: List[LocalDirNode] = disk_cache.get_local_dirs()
            if len(dir_list) == 0:
                logger.debug('No dirs found in disk cache')

            # Dirs first
            for dir_node in dir_list:
                existing = tree.get_node(dir_node.identifier)
                # Overwrite older ops for the same path:
                if not existing:
                    tree.add_to_tree(dir_node)
                    if not dir_node.exists():
                        missing_nodes.append(dir_node)
                elif existing.full_path != dir_node.full_path:
                    raise RuntimeError(f'Existing={existing}, FromCache={dir_node}')

            file_list: List[LocalFileNode] = disk_cache.get_local_files()
            if len(file_list) == 0:
                logger.debug('No files found in disk cache')

            for change in file_list:
                existing = tree.get_node(change.identifier)
                # Overwrite older changes for the same path:
                if not existing:
                    tree.add_to_tree(change)
                    if not change.exists():
                        missing_nodes.append(change)
                elif existing.sync_ts < change.sync_ts:
                    tree.remove_single_node(change.identifier)
                    tree.add_to_tree(change)

            # logger.debug(f'Reduced {str(len(db_file_changes))} disk cache entries into {str(count_from_disk)} unique entries')
            logger.debug(f'{stopwatch_load} [{tree_id}] Finished loading {len(file_list)} files and {len(dir_list)} dirs from disk')

            if len(missing_nodes) > 0:
                logger.info(f'Found {len(missing_nodes)} cached nodes with exists=false: submitting to adjudicator...')
            # TODO: add code for adjudicator

            cache_info.is_loaded = True
            return tree

    def _save_subtree_to_disk(self, cache_info: PersistedCacheInfo, tree_id):
        assert isinstance(cache_info.subtree_root, LocalFsIdentifier)

        file_list, dir_list = self.get_all_files_and_dirs_for_subtree(cache_info.subtree_root)

        stopwatch_write_cache = Stopwatch()
        with self._struct_lock:
            with LocalDiskDatabase(cache_info.cache_location, self.app) as disk_cache:
                # Overwrite cache:
                disk_cache.truncate_local_files(commit=False)
                disk_cache.truncate_local_dirs(commit=False)
                disk_cache.insert_local_files(file_list, overwrite=False, commit=False)
                disk_cache.insert_local_dirs(dir_list, overwrite=False, commit=True)

        cache_info.needs_save = False
        logger.info(f'[{tree_id}] {stopwatch_write_cache} Wrote {len(file_list)} files and {len(dir_list)} dirs to "{cache_info.cache_location}"')

    def _scan_file_tree(self, subtree_root: LocalFsIdentifier, tree_id: str) -> LocalDiskTree:
        """If subtree_root is a file, then a tree is returned with only 1 node"""
        logger.debug(f'[{tree_id}] Scanning filesystem subtree: {subtree_root}')
        scanner = LocalDiskScanner(app=self.app, root_node_identifer=subtree_root, tree_id=tree_id)
        return scanner.scan()

    def _resync_with_file_system(self, subtree_root: LocalFsIdentifier, tree_id: str):
        """Scan directory tree and update master tree where needed."""
        fresh_tree: LocalDiskTree = self._scan_file_tree(subtree_root, tree_id)

        if SUPER_DEBUG:
            logger.debug(f'[{tree_id}] Scanned fresh tree: \n{fresh_tree.show(stdout=False)}')

        with self._struct_lock:
            # Just upsert all nodes in the updated tree and let God (or some logic) sort them out
            subtree = LocalSubtree(subtree_root, [], fresh_tree.get_subtree_bfs())
            batch_changes_op: BatchChangesOp = BatchChangesOp(subtree_list=[subtree])

            # Find removed nodes and append them to remove_single_nodes_op
            root_node: LocalNode = fresh_tree.get_node(fresh_tree.root)
            if root_node.is_dir():
                # "Pending op" nodes are not stored in the regular cache (they should all have exists()==False)
                pending_op_nodes: List[LocalNode] = []

                for existing_node in self._memstore.master_tree.get_subtree_bfs(subtree_root.uid):
                    if not fresh_tree.get_node(existing_node.uid):
                        if existing_node.exists():
                            subtree.remove_node_list.append(existing_node)
                        else:
                            pending_op_nodes.append(existing_node)

                if pending_op_nodes:
                    logger.debug(f'Attempting to transfer {len(pending_op_nodes)} pending op src/dst nodes to the newly synced tree')
                    for pending_op_node in pending_op_nodes:
                        assert not pending_op_node.exists()
                        if fresh_tree.can_add_without_mkdir(pending_op_node):
                            fresh_tree.add_node(pending_op_node)
                        else:
                            # TODO: notify the OpLedger and devise a recovery strategy
                            logger.error(f'Cannot add pending op node (its parent is gone): {pending_op_node}')

            self._executor.execute(batch_changes_op)

    # LocalSubtree-level methods
    # ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼

    def show_tree(self, subtree_root: LocalFsIdentifier) -> str:
        with self._struct_lock:
            return self._memstore.master_tree.show(stdout=False, nid=subtree_root.uid)

    def get_display_tree(self, subtree_root: LocalFsIdentifier, tree_id: str) -> DisplayTree:
        logger.debug(f'DisplayTree requested for root: {subtree_root}')
        return self._get_display_tree(subtree_root, tree_id, is_live_refresh=False)

    def _get_display_tree(self, subtree_root: NodeIdentifier, tree_id: str, is_live_refresh: bool = False) -> DisplayTree:
        """
        Performs a read-through retrieval of all the LocalFileNodes in the given subtree
        on the local filesystem.
        """

        if not os.path.exists(subtree_root.full_path):
            logger.info(f'Cannot load meta for subtree because it does not exist: "{subtree_root.full_path}"')
            root_node = RootTypeNode(subtree_root)
            return NullDisplayTree(root_node)

        existing_uid = subtree_root.uid
        new_uid = self.get_uid_for_path(subtree_root.full_path, existing_uid)
        if existing_uid != new_uid:
            logger.warning(f'Requested UID "{existing_uid}" is invalid for given path; changing it to "{new_uid}"')
        subtree_root.uid = new_uid

        # If we have already loaded this subtree as part of a larger cache, use that:
        cache_man = self.app.cacheman
        supertree_cache: Optional[PersistedCacheInfo] = cache_man.find_existing_cache_info_for_subtree(subtree_root.full_path, subtree_root.tree_type)
        if supertree_cache:
            logger.debug(f'LocalSubtree ({subtree_root.full_path}) is part of existing cached supertree ({supertree_cache.subtree_root.full_path})')
            assert isinstance(subtree_root, LocalFsIdentifier)
            return self._load_subtree(supertree_cache, tree_id, subtree_root, is_live_refresh)
        else:
            # no supertree found in cache. use exact match logic:
            cache_info = cache_man.get_or_create_cache_info_entry(subtree_root)
            assert cache_info is not None
            return self._load_subtree(cache_info, tree_id, cache_info.subtree_root, is_live_refresh)

    def _load_subtree(self, cache_info: PersistedCacheInfo, tree_id: str, requested_subtree_root: LocalFsIdentifier = None,
                      is_live_refresh: bool = False) -> LocalDiskDisplayTree:
        """requested_subtree_root, if present, is a subset of the cache_info's subtree and it will be used. Otherwise cache_info's will be used"""
        assert cache_info
        stopwatch_total = Stopwatch()

        # Update UID, assuming this is a new run and it has gone stale
        uid = self.get_uid_for_path(cache_info.subtree_root.full_path, cache_info.subtree_root.uid)
        if cache_info.subtree_root.uid != uid:
            logger.warning(f'Requested UID "{cache_info.subtree_root.uid}" is invalid for given path; changing it to "{uid}"')
        cache_info.subtree_root.uid = uid

        assert isinstance(cache_info.subtree_root, LocalFsIdentifier)
        if not requested_subtree_root:
            requested_subtree_root = cache_info.subtree_root
        else:
            # Update UID, assuming this is a new run and it has gone stale
            uid = self.get_uid_for_path(requested_subtree_root.full_path, requested_subtree_root.uid)
            if requested_subtree_root.uid != uid:
                logger.warning(f'Requested UID "{requested_subtree_root.uid}" is invalid for path "{requested_subtree_root.full_path}";'
                               f' changing it to "{uid}"')
            requested_subtree_root.uid = uid

        # LOAD into master tree. Only for first load!
        if not cache_info.is_loaded:
            if self.app.cacheman.enable_load_from_disk:
                tree = self._load_subtree_from_disk(cache_info, tree_id)
                if tree:
                    if SUPER_DEBUG:
                        logger.debug(f'[{tree_id}] Loaded cached tree: \n{tree.show(stdout=False)}')
                    with self._struct_lock:
                        self._memstore.master_tree.replace_subtree(tree)
                        logger.debug(f'[{tree_id}] Updated in-memory cache: {self.get_summary()}')
            else:
                logger.debug(f'[{tree_id}] Skipping cache disk load because cache.enable_load_from_disk is false')

        # FS SYNC
        if is_live_refresh or not cache_info.is_loaded or \
                (cache_info.needs_refresh and self.app.cacheman.sync_from_local_disk_on_cache_load):
            logger.debug(f'[{tree_id}] Will resync with file system (is_loaded={cache_info.is_loaded}, sync_on_cache_load='
                         f'{self.app.cacheman.sync_from_local_disk_on_cache_load}, needs_refresh={cache_info.needs_refresh},'
                         f'is_live_refresh={is_live_refresh})')
            # Update from the file system, and optionally save any changes back to cache:
            self._resync_with_file_system(requested_subtree_root, tree_id)
            # We can only mark this as 'done' (False) if the entire cache contents has been refreshed:
            if requested_subtree_root.uid == cache_info.subtree_root.uid:
                cache_info.needs_refresh = False
        elif not self.app.cacheman.sync_from_local_disk_on_cache_load:
            logger.debug(f'[{tree_id}] Skipping filesystem sync because it is disabled for cache loads')
        elif not cache_info.needs_refresh:
            logger.debug(f'[{tree_id}] Skipping filesystem sync because the cache is still fresh for path: {cache_info.subtree_root}')

        # SAVE
        if cache_info.needs_save:
            if not cache_info.is_loaded:
                logger.warning(f'[{tree_id}] Skipping cache save: cache was never loaded!')
            elif self.app.cacheman.enable_save_to_disk:
                # Save the updates back to local disk cache:
                self._save_subtree_to_disk(cache_info, tree_id)
            else:
                logger.debug(f'[{tree_id}] Skipping cache save because it is disabled')

        with self._struct_lock:
            root_node = self._memstore.master_tree.get_node(requested_subtree_root.uid)
        fmeta_tree = LocalDiskDisplayTree(root_node=root_node, app=self.app, tree_id=tree_id)
        logger.info(f'[{tree_id}] {stopwatch_total} Load complete. Returning subtree for {fmeta_tree.node_identifier.full_path}')
        return fmeta_tree

    def consolidate_local_caches(self, local_caches: List[PersistedCacheInfo], tree_id) -> bool:
        supertree_sets: List[Tuple[PersistedCacheInfo, PersistedCacheInfo]] = []

        if not self.app.cacheman.enable_save_to_disk:
            logger.debug(f'[{tree_id}] Will not consolidate caches; save to disk is disabled')
            return False

        for cache in local_caches:
            for other_cache in local_caches:
                if other_cache.subtree_root.full_path.startswith(cache.subtree_root.full_path) and \
                        not cache.subtree_root.full_path == other_cache.subtree_root.full_path:
                    # cache is a super-tree of other_cache
                    supertree_sets.append((cache, other_cache))

        for supertree_cache, subtree_cache in supertree_sets:
            local_caches.remove(subtree_cache)

            if supertree_cache.sync_ts > subtree_cache.sync_ts:
                logger.info(f'[{tree_id}] Cache for supertree (root={supertree_cache.subtree_root.full_path}, ts={supertree_cache.sync_ts}) is newer '
                            f'than for subtree (root={subtree_cache.subtree_root.full_path}, ts={subtree_cache.sync_ts}): it will be deleted')
                file_util.delete_file(subtree_cache.cache_location)
            else:
                logger.info(f'[{tree_id}] Cache for subtree (root={subtree_cache.subtree_root.full_path}, ts={subtree_cache.sync_ts}) is newer '
                            f'than for supertree (root={supertree_cache.subtree_root.full_path}, ts={supertree_cache.sync_ts}): it will be merged '
                            f'into supertree')

                # 1. Load super-tree into memory
                super_tree: LocalDiskTree = self._load_subtree_from_disk(supertree_cache, ID_GLOBAL_CACHE)
                # 2. Discard all paths from super-tree which fall under sub-tree:

                # 3. Load sub-tree into memory
                sub_tree: LocalDiskTree = self._load_subtree_from_disk(subtree_cache, ID_GLOBAL_CACHE)
                if sub_tree:
                    # 4. Add contents of sub-tree into super-tree:
                    super_tree.replace_subtree(sub_tree=sub_tree)

                # 5. We already loaded it into memory; add it to the in-memory cache:
                with self._struct_lock:
                    self._memstore.master_tree.replace_subtree(super_tree)

                # this will resync with file system and/or save if configured
                supertree_cache.needs_save = True
                self._load_subtree(supertree_cache, tree_id)
                # Now it is safe to delete the subtree cache:
                file_util.delete_file(subtree_cache.cache_location)

        registry_needs_update = len(supertree_sets) > 0
        return registry_needs_update
    
    def refresh_subtree(self, node: LocalNode, tree_id: str):
        self._get_display_tree(node.node_identifier, tree_id, is_live_refresh=True)

    def refresh_subtree_stats(self, subtree_root_node: LocalNode, tree_id: str):
        with self._struct_lock:
            self._memstore.master_tree.refresh_stats(subtree_root_node, tree_id)

    # Cache CRUD operations
    # ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼

    def load_node_for_path(self, full_path: str) -> Optional[LocalNode]:
        """This actually reads directly from the disk cache"""
        logger.debug(f'Loading single node for path: "{full_path}"')
        cache_man = self.app.cacheman
        cache_info: Optional[PersistedCacheInfo] = cache_man.find_existing_cache_info_for_subtree(full_path, TREE_TYPE_LOCAL_DISK)
        if not cache_info:
            logger.debug(f'Could not find cache containing path: "{full_path}"')
            return None
        with LocalDiskDatabase(cache_info.cache_location, self.app) as cache:
            return cache.get_file_or_dir_for_path(full_path)

    def upsert_single_node(self, node: LocalNode):
        assert node, 'Cannot upsert node: no node provided!'
        assert self.uid_mapper.get_uid_for_path(node.full_path, node.uid) == node.uid, \
            f'Internal error while trying to upsert node to cache: UID did not match expected ' \
            f'({self.uid_mapper.get_uid_for_path(node.full_path, node.uid)}); node={node}'

        with self._struct_lock:
            self._executor.execute(UpsertSingleNodeOp(node))

    def update_single_node(self, node: LocalNode):
        assert node, 'Cannot update node: no node provided!'

        with self._struct_lock:
            self._executor.execute(UpsertSingleNodeOp(node, update_only=True))

    def remove_single_node(self, node: LocalNode, to_trash=False):
        assert node, 'Cannot remove node: no node provided!'
        logger.debug(f'Removing node from caches (to_trash={to_trash}): {node}')

        with self._struct_lock:
            self._executor.execute(DeleteSingleNodeOp(node, to_trash=to_trash))

    def _migrate_node(self, node: LocalNode, src_full_path: str, dst_full_path: str) -> LocalNode:
        new_node_full_path: str = file_util.change_path_to_new_root(node.full_path, src_full_path, dst_full_path)
        new_node_uid: UID = self.get_uid_for_path(new_node_full_path)

        new_node = copy.deepcopy(node)
        # new_node.reset_pointers(self._master_tree.identifier)
        new_node._predecessor.clear()
        new_node._successors.clear()
        new_node.set_node_identifier(LocalFsIdentifier(full_path=new_node_full_path, uid=new_node_uid))
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
                self._memstore.expected_node_moves[src_node.full_path] = dst_node.full_path

    def move_local_subtree(self, src_full_path: str, dst_full_path: str, is_from_watchdog=False):
        with self._struct_lock:
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
            dst_node_identifier: LocalFsIdentifier = LocalFsIdentifier(dst_full_path, dst_uid)
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
            self._executor.execute(operation)

    def remove_subtree(self, subtree_root_node: LocalNode, to_trash: bool):
        """subtree_root can be either a file or dir"""
        logger.debug(f'Removing subtree_root from caches (to_trash={to_trash}): {subtree_root_node}')

        if to_trash:
            # TODO
            raise RuntimeError(f'Not supported: to_trash=true!')

        if not subtree_root_node.uid:
            raise RuntimeError(f'Cannot remove subtree_root from cache because it has no UID: {subtree_root_node}')

        if subtree_root_node.uid != self.get_uid_for_path(subtree_root_node.full_path):
            raise RuntimeError(f'Internal error while trying to remove subtree_root ({subtree_root_node}): UID did not match expected '
                               f'({self.get_uid_for_path(subtree_root_node.full_path)})')

        with self._struct_lock:
            subtree_nodes: List[LocalNode] = self._memstore.master_tree.get_subtree_bfs(subtree_root_node.uid)
            assert isinstance(subtree_root_node.node_identifier, LocalFsIdentifier)
            operation: DeleteSubtreeOp = DeleteSubtreeOp(subtree_root_node.node_identifier, node_list=subtree_nodes)
            self._executor.execute(operation)

    # Various public getters
    # ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼

    def get_all_files_and_dirs_for_subtree(self, subtree_root: LocalFsIdentifier) -> Tuple[List[LocalFileNode], List[LocalDirNode]]:
        with self._struct_lock:
            return self._memstore.master_tree.get_all_files_and_dirs_for_subtree(subtree_root)

    def get_uid_for_domain_id(self, domain_id: str, uid_suggestion: Optional[UID] = None) -> UID:
        return self.uid_mapper.get_uid_for_path(domain_id, uid_suggestion)

    def get_uid_for_path(self, path: str, uid_suggestion: Optional[UID] = None) -> UID:
        return self.uid_mapper.get_uid_for_path(path, uid_suggestion)

    def get_node_for_domain_id(self, domain_id: str) -> LocalNode:
        uid: UID = self.get_uid_for_domain_id(domain_id)
        return self.get_node_for_uid(uid)

    def get_children(self, node: LocalNode) -> List[LocalNode]:
        with self._struct_lock:
            return self._memstore.master_tree.children(node.uid)

    def get_node_for_uid(self, uid: UID) -> LocalNode:
        with self._struct_lock:
            return self._memstore.master_tree.get_node(uid)

    def get_parent_for_node(self, node: LocalNode, required_subtree_path: str = None):
        try:
            with self._struct_lock:
                try:
                    parent: LocalNode = self._memstore.master_tree.parent(nid=node.uid)
                except KeyError:
                    # parent not found in tree... maybe we can derive it however
                    parent_path: str = node.derive_parent_path()
                    parent_uid: UID = self.get_uid_for_path(parent_path)
                    parent = self._memstore.master_tree.get_node(parent_uid)
                    if not parent:
                        logger.debug(f'Parent not found for node ({node.uid})')
                        return None
                    logger.debug(f'Parent not found for node ({node.uid}) but found parent at path: {parent.full_path}')
                if not required_subtree_path or parent.full_path.startswith(required_subtree_path):
                    return parent
                return None
        except NodeIDAbsentError:
            return None
        except Exception:
            logger.error(f'Error getting parent for node: {node}, required_path: {required_subtree_path}')
            raise

    def get_summary(self):
        if self._memstore.use_md5:
            md5 = str(self._memstore.md5_dict.total_entries)
        else:
            md5 = 'disabled'
        return f'LocalDiskMasterCache tree_size={len(self._memstore.master_tree):n} md5={md5}'

    def build_local_dir_node(self, full_path: str) -> LocalDirNode:
        uid = self.get_uid_for_path(full_path)
        # logger.debug(f'Creating dir node: nid={uid}')
        return LocalDirNode(node_identifier=LocalFsIdentifier(full_path=full_path, uid=uid), exists=True)

    def build_local_file_node(self, full_path: str, staging_path: str = None, must_scan_signature=False) -> Optional[LocalFileNode]:
        uid = self.get_uid_for_path(full_path)

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
        sync_ts = int(time.time())

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

        node_identifier = LocalFsIdentifier(uid=uid, full_path=full_path)
        return LocalFileNode(node_identifier, md5, sha256, size_bytes, sync_ts, modify_ts, change_ts, True)

