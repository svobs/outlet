import copy
import logging
import os
import pathlib
import threading
import time
from abc import ABC, abstractmethod
from collections import deque
from typing import Deque, Dict, List, Optional, Tuple

from pydispatch import dispatcher
from treelib.exceptions import NodeIDAbsentError

import local.content_hasher
from constants import LOCAL_ROOT_UID, ROOT_PATH, TREE_TYPE_LOCAL_DISK
from index.cache_manager import PersistedCacheInfo
from index.sqlite.local_db import LocalDiskDatabase
from index.two_level_dict import Md5BeforePathDict, Sha256BeforePathDict
from index.uid.uid_generator import UID
from index.uid.uid_mapper import UidPathMapper
from local.local_disk_scanner import LocalDiskScanner
from model.display_tree.display_tree import DisplayTree
from model.display_tree.local_disk import LocalDiskDisplayTree
from model.display_tree.null import NullDisplayTree
from model.local_disk_tree import LocalDiskTree
from model.node.container_node import RootTypeNode
from model.node.display_node import DisplayNode
from model.node.local_disk_node import LocalDirNode, LocalFileNode, LocalNode
from model.node_identifier import LocalFsIdentifier, NodeIdentifier
from ui import actions
from ui.actions import ID_GLOBAL_CACHE
from util import file_util
from util.stopwatch_sec import Stopwatch

logger = logging.getLogger(__name__)

SUPER_DEBUG = False


def _calculate_signatures(full_path: str, staging_path: str = None) -> Tuple[Optional[str], Optional[str]]:
    try:
        # Open,close, read file and calculate hash of its contents
        if staging_path:
            md5: Optional[str] = local.content_hasher.md5(staging_path)
        else:
            md5: Optional[str] = local.content_hasher.md5(full_path)
        # sha256 = local.content_hasher.dropbox_hash(full_path)
        sha256: Optional[str] = None
        return md5, sha256
    except FileNotFoundError as err:
        if os.path.islink(full_path):
            target = os.readlink(full_path)
            logger.error(f'Broken link, skipping: "{full_path}" -> "{target}"')
        else:
            # Can happen often if temp files are rapidly created/destroyed. Assume it will be cleaned up elsewhere
            logger.debug(f'Could not calculate signature: file not found; skipping: {full_path}')
        # Return None. Will be assumed to be a deleted file
        return None, None


# ABSTRACT CLASS LocalDiskOperation
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
class LocalDiskOperation(ABC):
    @abstractmethod
    def update_memory_cache(self, master_cache):
        pass

    @abstractmethod
    def update_disk_cache(self, cache: LocalDiskDatabase):
        pass

    @abstractmethod
    def send_signals(self):
        pass


class DeleteSingleNodeOp(LocalDiskOperation):
    def __init__(self, node: LocalNode, to_trash: bool = False):
        self.node = node
        self.to_trash: bool = to_trash

    def update_memory_cache(self, master_tree: LocalDiskTree):
        existing_node = master_tree.get_node_for_uid(self.node.uid)
        if existing_node:
            master_tree.remove_node(existing_node)

    def update_disk_cache(self, cache: LocalDiskDatabase):
        cache.delete_single_node(self.node, commit=False)

    def send_signals(self):
        dispatcher.send(signal=actions.NODE_REMOVED, sender=ID_GLOBAL_CACHE, node=self.node)


# CLASS DeleteSubtreeOp
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
class DeleteSubtreeOp(LocalDiskOperation):
    def __init__(self, subtree_root_node: LocalNode, node_list: List[LocalNode]):
        self.subtree_root_node = subtree_root_node
        """If true, is a delete operation. If false, is upsert op."""
        self.node_list: List[LocalNode] = node_list

    def update_memory_cache(self, master_tree: LocalDiskTree):
        logger.debug(f'DeleteSubtreeOp: removing {len(self.node_list)} nodes from memory cache')
        for node in reversed(self.node_list):
            existing_node = master_tree.get_node_for_uid(node.uid)
            if existing_node:
                master_tree.remove_node(existing_node)
        logger.debug(f'DeleteSubtreeOp: done removing nodes from memory cache')

    def update_disk_cache(self, cache: LocalDiskDatabase):
        logger.debug(f'DeleteSubtreeOp: removing {len(self.node_list)} nodes from disk cache')
        for node in self.node_list:
            cache.delete_single_node(node, commit=False)
        logger.debug(f'DeleteSubtreeOp: done removing nodes from disk cache')

    def send_signals(self):
        logger.debug(f'DeleteSubtreeOp: sending "{actions.NODE_REMOVED}" signal for {len(self.node_list)} nodes')
        for node in self.node_list:
            dispatcher.send(signal=actions.NODE_REMOVED, sender=ID_GLOBAL_CACHE, node=node)

# CLASS SubtreeOperation
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
class SubtreeOperation:
    def __init__(self, subtree_root_path: str, is_delete: bool, node_list: List[LocalNode]):
        self.subtree_root_path = subtree_root_path
        self.is_delete: bool = is_delete
        """If true, is a delete operation. If false, is upsert op."""
        self.node_list: List[LocalNode] = node_list

    def get_summary(self) -> str:
        if self.is_delete:
            return f'RM "{self.subtree_root_path}" ({len(self.node_list)} nodes)'
        else:
            return f'UP "{self.subtree_root_path}" ({len(self.node_list)} nodes)'


# CLASS ContentScannerThread
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
class ContentScannerThread(threading.Thread):
    """Hasher thread which churns through hasher queue and sends updates to cacheman"""
    def __init__(self, parent):
        super().__init__(target=self._run_content_scanner_thread, name='ContentScannerThread', daemon=True)
        self.master_cache = parent
        self._shutdown: bool = False
        self._initial_sleep_sec: float = self.master_cache.app.config.get('cache.lazy_load_local_file_signatures_initial_delay_ms') / 1000.0
        self._node_queue: Deque[LocalFileNode] = deque()
        self._cv_can_get = threading.Condition()
        self._struct_lock = threading.Lock()

    def enqueue(self, node: LocalFileNode):
        logger.debug(f'[{self.name}] Enqueuing node: {node.node_identifier}')
        assert not node.md5 and not node.sha256
        with self._struct_lock:
            self._node_queue.append(node)

        with self._cv_can_get:
            self._cv_can_get.notifyAll()

    def request_shutdown(self):
        logger.debug(f'Requesting shutdown of thread {self.name}')
        self._shutdown = True
        with self._cv_can_get:
            # unblock thread:
            self._cv_can_get.notifyAll()

    def _process_single_node(self, node: LocalFileNode):
        md5, sha256 = _calculate_signatures(node.full_path)
        node.md5 = md5
        node.sha256 = sha256

        logger.debug(f'[{self.name}] Node {node.uid} has MD5: {node.md5}')

        # TODO: consider batching writes
        # Send back to ourselves to be re-stored in memory & disk caches:
        self.master_cache.upsert_local_node(node)

    def _run_content_scanner_thread(self):
        logger.info(f'Starting {self.name}...')

        # Wait for CacheMan to finish starting up so as not to deprive it of resources:
        self.master_cache.app.cacheman.wait_for_startup_done()

        logger.debug(f'[{self.name}] Doing inital sleep {self._initial_sleep_sec} sec to let things settle...')
        time.sleep(self._initial_sleep_sec)  # in seconds

        while not self._shutdown:
            node: Optional[LocalFileNode] = None

            with self._struct_lock:
                if len(self._node_queue) > 0:
                    node = self._node_queue.popleft()

            if node:
                try:
                    if node.md5 or node.sha256:
                        logger.warning(f'Node already has signature; skipping; {node}')
                        continue

                    logger.debug(f'[{self.name}] Calculating signature for node: {node.node_identifier}')
                    self._process_single_node(node)
                except Exception:
                    logger.exception(f'Unexpected error while processing node: {node}')
                continue
            else:
                logger.debug(f'[{self.name}] No pending ops; sleeping {self._initial_sleep_sec} sec then waiting till notified...')
                time.sleep(self._initial_sleep_sec)  # in seconds

            with self._cv_can_get:
                self._cv_can_get.wait()


# ⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛
# CLASS LocalDiskMasterCache
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼

# TODO: consider scanning only root dir at first, then enqueuing subdirectories

class LocalDiskMasterCache:
    def __init__(self, app):
        """Singleton in-memory cache for local filesystem"""
        self.app = app

        self._uid_mapper = UidPathMapper(app)

        self._struct_lock = threading.Lock()

        self._expected_node_moves: Dict[str, str] = {}
        """When the FileSystemEventHandler gives us MOVE notifications for a tree, it gives us a separate notification for each
        and every node. Since we want our tree move to be an atomic operation, we do it all at once, but then keep track of the
        nodes we've moved so that we know exactly which notifications to ignore after that.
        Dict is key-value pair of [old_file_path -> new_file_path]"""

        self.lazy_load_signatures: bool = app.config.get('cache.lazy_load_local_file_signatures')

        self._content_scanner_thread = ContentScannerThread(self)

        self.use_md5 = app.config.get('cache.enable_md5_lookup')
        if self.use_md5:
            self.md5_dict = Md5BeforePathDict()
        else:
            self.md5_dict = None

        self.use_sha256 = app.config.get('cache.enable_sha256_lookup')
        if self.use_sha256:
            self.sha256_dict = Sha256BeforePathDict()
        else:
            self.sha256_dict = None

        # Each node inserted here will have an entry created for its dir.
        # self.parent_path_dict = ParentPathBeforeFileNameDict()
        # But we still need a dir tree to look up child dirs:
        self._master_tree = LocalDiskTree(self.app)
        root_node = RootTypeNode(node_identifier=LocalFsIdentifier(full_path=ROOT_PATH, uid=LOCAL_ROOT_UID))
        self._master_tree.add_node(node=root_node, parent=None)

        if self.lazy_load_signatures:
            self.start_content_scanner_thread()

    def start_content_scanner_thread(self):
        if not self._content_scanner_thread.is_alive():
            self._content_scanner_thread.start()

    def shutdown(self):
        self._content_scanner_thread.request_shutdown()

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
                    tree.remove_node(change.identifier)
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
        with self._struct_lock:
            file_list, dir_list = self._master_tree.get_all_files_and_dirs_for_subtree(cache_info.subtree_root)

        stopwatch_write_cache = Stopwatch()
        with LocalDiskDatabase(cache_info.cache_location, self.app) as disk_cache:
            # Update cache:
            disk_cache.insert_local_files(file_list, overwrite=True, commit=False)
            disk_cache.insert_local_dirs(dir_list, overwrite=True, commit=True)

        cache_info.needs_save = False
        logger.info(f'[{tree_id}] {stopwatch_write_cache} Wrote {len(file_list)} files and {len(dir_list)} dirs to "{cache_info.cache_location}"')

    def _scan_file_tree(self, subtree_root: LocalFsIdentifier, tree_id: str) -> LocalDiskTree:
        """If subtree_root is a file, then a tree is returned with only 1 node"""
        logger.debug(f'[{tree_id}] Scanning filesystem subtree: {subtree_root}')
        scanner = LocalDiskScanner(app=self.app, root_node_identifer=subtree_root, tree_id=tree_id)
        return scanner.scan()

    def _resync_with_file_system(self, subtree_root: LocalFsIdentifier, tree_id: str, is_live_refresh: bool = False):
        """Scan directory tree and update master tree where needed."""
        fresh_tree: LocalDiskTree = self._scan_file_tree(subtree_root, tree_id)

        with self._struct_lock:
            remove_nodes_op: SubtreeOperation = SubtreeOperation(subtree_root.full_path, is_delete=True, node_list=[])
            # Just upsert all nodes in the updated tree and let God (or some logic) sort them out
            upsert_nodes_op: SubtreeOperation = SubtreeOperation(subtree_root.full_path, is_delete=False, node_list=fresh_tree.get_subtree_bfs())

            # Find removed nodes and append them to remove_nodes_op
            root_node: LocalNode = fresh_tree.get_node(fresh_tree.root)
            if root_node.is_dir():
                for existing_node in self._master_tree.get_subtree_bfs(subtree_root.uid):
                    if not fresh_tree.get_node(existing_node.uid):
                        if not existing_node.exists():
                            # If {not existing_node.exists()}, assume it's a "pending op" node.
                            ancestor = existing_node
                            # Iterate up the tree until we (a) encounter a "normal" ancestor which is also present in the fresh tree,
                            # or (b) pass the root of the master tree or encounter a "normal" ancestor in the master tree which doesn't exist in the
                            # fresh tree, which means its descendants are all removed.
                            while True:
                                ancestor = self._get_parent_node_from_master_tree(ancestor)
                                if ancestor and fresh_tree.contains(ancestor.uid):
                                    assert ancestor.exists()
                                    # no need to remove
                                    break
                                elif not ancestor or ancestor.exists():
                                    # FIXME: need a strategy for handling an error like this. This will likely muck up the op graph
                                    logger.error(f'Removing node belonging to a pending op because its ancestor was deleted: {existing_node}')
                                    remove_nodes_op.node_list.append(existing_node)
                                    break
                                # We can ignore any "pending op" ancestors we encounter:
                                assert not ancestor.exists()

            operation_list: List[SubtreeOperation] = [remove_nodes_op, upsert_nodes_op]

            self._execute(operation_list)

    # Subtree-level methods
    # ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼

    def load_local_subtree(self, subtree_root: NodeIdentifier, tree_id: str, is_live_refresh: bool = False) -> DisplayTree:
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
            logger.debug(f'Subtree ({subtree_root.full_path}) is part of existing cached supertree ({supertree_cache.subtree_root.full_path})')
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

        # LOAD into master tree
        if not cache_info.is_loaded:
            if self.app.cacheman.enable_load_from_disk:
                tree = self._load_subtree_from_disk(cache_info, tree_id)
                if tree:
                    with self._struct_lock:
                        self._master_tree.replace_subtree(tree)
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
            self._resync_with_file_system(requested_subtree_root, tree_id, is_live_refresh=is_live_refresh)
            # We can only mark this as 'done' (False) if the entire cache contents has been refreshed:
            if requested_subtree_root.uid == cache_info.subtree_root.uid:
                cache_info.needs_refresh = False
            if not is_live_refresh:
                cache_info.needs_save = True
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
            root_node = self._master_tree.get_node(requested_subtree_root.uid)
        fmeta_tree = LocalDiskDisplayTree(root_node=root_node, app=self.app)
        logger.info(f'[{tree_id}] {stopwatch_total} Load complete. Returning subtree for {fmeta_tree.node_identifier.full_path}')
        return fmeta_tree

    def consolidate_local_caches(self, local_caches: List[PersistedCacheInfo], tree_id) -> Tuple[List[PersistedCacheInfo], bool]:
        supertree_sets: List[Tuple[PersistedCacheInfo, PersistedCacheInfo]] = []

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
                    self._master_tree.replace_subtree(super_tree)

                # this will resync with file system and/or save if configured
                supertree_cache.needs_save = True
                self._load_subtree(supertree_cache, tree_id)
                # Now it is safe to delete the subtree cache:
                file_util.delete_file(subtree_cache.cache_location)

        registry_needs_update = len(supertree_sets) > 0
        return local_caches, registry_needs_update
    
    def refresh_subtree(self, node: LocalNode, tree_id: str):
        self.load_local_subtree(node.node_identifier, tree_id, is_live_refresh=True)

    def refresh_stats(self, tree_id: str, subtree_root_node: LocalNode):
        with self._struct_lock:
            self._master_tree.refresh_stats(tree_id, subtree_root_node)

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

    def upsert_local_node(self, node: LocalNode, fire_listeners=True):
        logger.debug(f'Upserting node to caches: {node}')

        # 1. Validate UID:
        if not node.uid:
            raise RuntimeError(f'Cannot upsert node to cache because it has no UID: {node}')

        uid = self._uid_mapper.get_uid_for_path(node.full_path, node.uid)
        if node.uid != uid:
            raise RuntimeError(f'Internal error while trying to upsert node to cache: UID did not match expected '
                               f'({self._uid_mapper.get_uid_for_path(node.full_path)}); node={node}')

        # 2. Update in-memory cache:
        with self._struct_lock:
            updated_node, needs_persistence = self._upsert_single_node_in_memory_cache(node)
            if updated_node:
                node = updated_node

            if needs_persistence:
                # 3. Update on-disk cache:
                self._upsert_single_node_in_disk_cache(node)

            if updated_node and fire_listeners:
                # Even if update was not needed in cache, certain transient properties (e.g. icon) may have changed
                dispatcher.send(signal=actions.NODE_UPSERTED, sender=ID_GLOBAL_CACHE, node=node)

    def _map_physical_cache(self, caches: List[LocalDiskDatabase], physical_cache_list: List[PersistedCacheInfo],
                            logical_cache_list: List[PersistedCacheInfo], logical_index: int) -> LocalDiskDatabase:
        for physical_index, physical_cache_info in enumerate(physical_cache_list):
            if logical_cache_list[logical_index].cache_location == physical_cache_info.cache_location:
                return caches[physical_index]
        raise RuntimeError(f'Could not map physical cache for index {logical_index} and location '
                           f'"{logical_cache_list[logical_index].cache_location}"')

    def _update_multiple_cache_files(self, caches: List[LocalDiskDatabase], physical_cache_list: List[PersistedCacheInfo],
                                     logical_cache_list: List[PersistedCacheInfo], subtree_op_list: List[SubtreeOperation]):
        for logical_index, op in enumerate(subtree_op_list):
            logger.debug(f'Writing subtree operation {logical_index} (subtree_root="{op.subtree_root_path}")')
            cache = self._map_physical_cache(caches, logical_cache_list, physical_cache_list, logical_index)

            if op.is_delete:
                dir_uid_list: List[UID] = []
                file_uid_list: List[UID] = []
                for node in op.node_list:
                    if node.is_dir():
                        dir_uid_list.append(node.uid)
                    else:
                        file_uid_list.append(node.uid)

                if dir_uid_list:
                    cache.delete_local_dirs_for_uid_list(dir_uid_list, commit=False)
                if file_uid_list:
                    cache.delete_local_files_for_uid_list(file_uid_list, commit=False)

            else:
                dir_list: List[LocalDirNode] = []
                file_list: List[LocalFileNode] = []
                for node in op.node_list:
                    if node.is_dir():
                        assert isinstance(node, LocalDirNode)
                        dir_list.append(node)
                    else:
                        assert isinstance(node, LocalFileNode)
                        file_list.append(node)
                if dir_list:
                    cache.upsert_local_dir_list(dir_list, commit=False)
                if file_list:
                    cache.upsert_local_file_list(file_list, commit=False)
        for logical_index, cache in enumerate(caches):
            logger.debug(f'Committing cache {logical_index}: "{cache.db_path}"')
            cache.commit()

    def _is_cache_info_in_list(self, cache_info, cache_list):
        for existing_cache in cache_list:
            if cache_info.cache_location == existing_cache.cache_location:
                return True
        return False

    def _update_disk_cache(self, subtree_op_list: List[SubtreeOperation]):
        """Attempt to come close to a transactional behavior by writing to all caches at once, and then committing all at the end"""
        cache_man = self.app.cacheman
        if not cache_man.enable_save_to_disk:
            logger.debug(f'Save to disk is disabled: skipping upsert of {len(subtree_op_list)} subtree operations')
            return

        if len(subtree_op_list) > 3:
            raise RuntimeError(f'Cannot update more than 3 disk caches simultaneously ({len(subtree_op_list)} specified)')

        physical_cache_list: List[PersistedCacheInfo] = []
        logical_cache_list: List[PersistedCacheInfo] = []

        for subtree_op in subtree_op_list:
            cache_info: Optional[PersistedCacheInfo] = cache_man.find_existing_cache_info_for_subtree(subtree_op.subtree_root_path,
                                                                                                     TREE_TYPE_LOCAL_DISK)
            if not cache_info:
                raise RuntimeError(f'Could not find a cache associated with file path: {subtree_op.subtree_root_path}')

            if not self._is_cache_info_in_list(cache_info, physical_cache_list):
                physical_cache_list.append(cache_info)

            logical_cache_list.append(cache_info)

        if len(physical_cache_list) == 1:
            with LocalDiskDatabase(physical_cache_list[0].cache_location, self.app) as cache0:
                caches = [cache0]
                self._update_multiple_cache_files(caches, logical_cache_list, physical_cache_list, subtree_op_list)

        elif len(physical_cache_list) == 2:
            with LocalDiskDatabase(physical_cache_list[0].cache_location, self.app) as cache0, \
                    LocalDiskDatabase(physical_cache_list[1].cache_location, self.app) as cache1:
                caches = [cache0, cache1]
                self._update_multiple_cache_files(caches, logical_cache_list, physical_cache_list, subtree_op_list)

        elif len(physical_cache_list) == 3:
            with LocalDiskDatabase(physical_cache_list[0].cache_location, self.app) as cache0, \
                    LocalDiskDatabase(physical_cache_list[1].cache_location, self.app) as cache1, \
                    LocalDiskDatabase(physical_cache_list[2].cache_location, self.app) as cache2:
                caches = [cache0, cache1, cache2]
                self._update_multiple_cache_files(caches, logical_cache_list, physical_cache_list, subtree_op_list)

    def remove_node(self, node: LocalNode, to_trash=False, fire_listeners=True):
        logger.debug(f'Removing node from caches (to_trash={to_trash}): {node}')

        with self._struct_lock:
            self._remove_node_nolock(node, to_trash, fire_listeners)

    def _remove_single_node_from_memory_cache(self, node: LocalNode):
        """Removes the given node from all in-memory structs (does nothing if it is not found in some or any of them).
        Will raise an exception if trying to remove a non-empty directory."""
        existing: DisplayNode = self._master_tree.get_node(node.uid)
        if existing:
            if existing.is_dir():
                children = self._master_tree.children(existing.identifier)
                if children:
                    # maybe allow deletion of dir with children in the future, but for now be careful
                    raise RuntimeError(f'Cannot remove dir from cache because it has {len(children)} children: {node}')

            count_removed = self._master_tree.remove_node(node.uid)
            assert count_removed <= 1, f'Deleted {count_removed} nodes at {node.full_path}'
        else:
            logger.warning(f'Cannot remove node because it has already been removed from cache: {node}')

        if self.use_md5 and node.md5:
            self.md5_dict.remove(node.md5, node.full_path)
        if self.use_sha256 and node.sha256:
            self.sha256_dict.remove(node.sha256, node.full_path)

    def _upsert_single_node_in_memory_cache(self, node: LocalNode) -> Tuple[Optional[LocalNode], bool]:
        """If a node already exists, the new node is merged into it and returned; otherwise the given node is returned.
        Second item in the tuple is True if update contained changes which should be saved to disk; False if otherwise"""
        existing_node: LocalNode = self._master_tree.get_node(node.uid)
        if existing_node:
            if existing_node.exists() and not node.exists():
                # In the future, let's close this hole with more elegant logic
                logger.warning(f'Cannot replace a node which exists with one which does not exist; skipping cache update')
                return None, False

            if existing_node.is_dir() and not node.is_dir():
                # need to replace all descendants...not ready to do this yet
                raise RuntimeError(f'Cannot replace a directory with a file: "{node.full_path}"')

            if existing_node == node:
                if SUPER_DEBUG:
                    logger.debug(f'Node being added (uid={node.uid}) is identical to node already in the cache; skipping cache update')
                return existing_node, False

            # just update the existing - much easier
            if SUPER_DEBUG:
                logger.debug(f'Merging node (PyID {id(node)}) into existing_node (PyID {id(existing_node)})')
            if node.is_file() and existing_node.is_file():
                assert isinstance(node, LocalFileNode) and isinstance(existing_node, LocalFileNode)
                _copy_signature_if_possible(existing_node, node)
                if SUPER_DEBUG:
                    _check_update_sanity(existing_node, node)
            existing_node.update_from(node)
        else:
            # new file or directory insert
            self._master_tree.add_to_tree(node)

        if node.is_file() and not node.md5 and not node.sha256:
            assert isinstance(node, LocalFileNode)
            self._content_scanner_thread.enqueue(node)
        else:
            # do this after the above, to avoid cache corruption in case of failure
            if self.use_md5 and node.md5:
                self.md5_dict.put(node, existing_node)
            if self.use_sha256 and node.sha256:
                self.sha256_dict.put(node, existing_node)

        if existing_node:
            return existing_node, True
        return node, True

    def _remove_single_node_from_disk_cache(self, node: LocalNode):
        cache_man = self.app.cacheman
        if not cache_man.enable_save_to_disk:
            logger.debug(f'Save to disk is disabled: skipping removal of node with UID={node.uid}')
            return

        assert node.get_tree_type() == TREE_TYPE_LOCAL_DISK
        cache_info: Optional[PersistedCacheInfo] = cache_man.find_existing_cache_info_for_subtree(node.full_path, node.get_tree_type())
        if not cache_info:
            logger.error(f'Could not find a cache associated with file path: {node.full_path}')
            return

        with LocalDiskDatabase(cache_info.cache_location, self.app) as cache:
            if node.is_dir():
                cache.delete_local_dir_with_uid(node.uid)
            else:
                cache.delete_local_file_with_uid(node.uid)

    def _upsert_single_node_in_disk_cache(self, node: LocalNode):
        if not node.exists():
            logger.debug(f'Node does not exist; skipping save to disk: {node}')
            return

        cache_man = self.app.cacheman
        if not cache_man.enable_save_to_disk:
            logger.debug(f'Save to disk is disabled: skipping add/update of node with UID={node.uid}')
            return

        assert node.get_tree_type() == TREE_TYPE_LOCAL_DISK
        cache_info: Optional[PersistedCacheInfo] = cache_man.find_existing_cache_info_for_subtree(node.full_path, node.get_tree_type())
        if not cache_info:
            raise RuntimeError(f'Could not find a cache associated with file path: {node.full_path}')

        with LocalDiskDatabase(cache_info.cache_location, self.app) as cache:
            if node.is_dir():
                cache.upsert_local_dir(node)
            else:
                cache.upsert_local_file(node)

    def _remove_node_nolock(self, node: LocalNode, to_trash=False, fire_listeners=True):
        # 1. Validate
        if not node.uid:
            raise RuntimeError(f'Cannot remove node from cache because it has no UID: {node}')

        if node.uid != self._uid_mapper.get_uid_for_path(node.full_path):
            raise RuntimeError(f'Internal error while trying to remove node ({node}): UID did not match expected '
                               f'({self._uid_mapper.get_uid_for_path(node.full_path)})')

        if to_trash:
            # TODO
            raise RuntimeError(f'Not supported: to_trash=true!')

        # 2. Update in-memory cache:
        self._remove_single_node_from_memory_cache(node)

        # 3. Update on-disk cache:
        self._remove_single_node_from_disk_cache(node)

        # 4. Notify UI:
        if fire_listeners:
            dispatcher.send(signal=actions.NODE_REMOVED, sender=ID_GLOBAL_CACHE, node=node)

    def _update_memory_cache(self, subtree_op_list: List[SubtreeOperation]):
        for subtree_operation in subtree_op_list:
            try:
                if subtree_operation.is_delete:
                    # Deletes must occur from bottom up:
                    for node in reversed(subtree_operation.node_list):
                        self._remove_single_node_from_memory_cache(node)
                    logger.debug(f'Removed {len(subtree_operation.node_list)} nodes from memcache path "{subtree_operation.subtree_root_path}"')
                else:
                    for index, node in enumerate(subtree_operation.node_list):
                        updated_node, needs_persistence = self._upsert_single_node_in_memory_cache(node)
                        if updated_node:
                            subtree_operation.node_list[index] = updated_node
                    logger.debug(f'Added {len(subtree_operation.node_list)} nodes to memcache path "{subtree_operation.subtree_root_path}"')
            except Exception:
                # TODO: clean up after exception
                logger.exception(f'Failed to update memory cache for operation: {subtree_operation.get_summary()}')

    def _migrate_node(self, node: LocalNode, src_full_path: str, dst_full_path: str) -> LocalNode:
        new_node_full_path: str = file_util.change_path_to_new_root(node.full_path, src_full_path, dst_full_path)
        new_node_uid: UID = self.get_uid_for_path(new_node_full_path)

        new_node = copy.deepcopy(node)
        # new_node.reset_pointers(self._master_tree.identifier)
        new_node._predecessor.clear()
        new_node._successors.clear()
        new_node.set_node_identifier(LocalFsIdentifier(full_path=new_node_full_path, uid=new_node_uid))
        return new_node

    def _add_to_expected_node_moves(self, rm_src_tree_op, add_dst_tree_op):
        first = True
        # Let's collate these two operations so that in case of failure, we have less inconsistent state
        for src_node, dst_node in zip(rm_src_tree_op.node_list, add_dst_tree_op.node_list):
            logger.debug(f'Migrating copy of node {src_node.node_identifier} to {dst_node.node_identifier}')
            if first:
                # ignore subroot
                first = False
            else:
                self._expected_node_moves[src_node.full_path] = dst_node.full_path

    def move_local_subtree(self, src_full_path: str, dst_full_path: str, is_from_watchdog=False):
        with self._struct_lock:
            if is_from_watchdog:
                # See if we safely ignore this:
                expected_move_dst = self._expected_node_moves.pop(src_full_path, None)
                if expected_move_dst:
                    if expected_move_dst == dst_full_path:
                        logger.debug(f'Ignoring MV ("{src_full_path}" -> "{dst_full_path}") because it was already done')
                        return
                    else:
                        logger.error(f'MV ("{src_full_path}" -> "{dst_full_path}"): was expecting dst = "{expected_move_dst}"!')

            src_uid: UID = self.get_uid_for_path(src_full_path)
            src_node: LocalNode = self._master_tree.get_node(src_uid)
            if src_node:
                rm_src_tree_op: Optional[SubtreeOperation] = self._build_subtree_removal_operation(src_node, to_trash=False)
            else:
                rm_src_tree_op = None
                logger.debug(f'MV src node does not exist: UID={src_uid}, path={src_full_path}')

            # Create up to 3 tree operations which should be executed in a single transaction if possible
            rm_existing_tree_op: Optional[SubtreeOperation] = None
            dst_uid: UID = self.get_uid_for_path(dst_full_path)
            dst_node: LocalNode = self._master_tree.get_node(dst_uid)
            if dst_node:
                logger.debug(f'Node already exists at MV dst; will remove: {dst_node.node_identifier}')
                rm_existing_tree_op = self._build_subtree_removal_operation(dst_node, to_trash=False)

            add_dst_tree_op: SubtreeOperation = SubtreeOperation(dst_full_path, is_delete=False, node_list=[])

            if rm_src_tree_op:
                for src_node in rm_src_tree_op.node_list:
                    dst_node = self._migrate_node(src_node, src_full_path, dst_full_path)
                    add_dst_tree_op.node_list.append(dst_node)
            else:
                # Rescan dir in dst_full_path for nodes
                if os.path.isdir(dst_full_path):
                    dst_node_identifier: LocalFsIdentifier = LocalFsIdentifier(dst_full_path, dst_uid)
                    fresh_tree: LocalDiskTree = self._scan_file_tree(dst_node_identifier, ID_GLOBAL_CACHE)
                    for dst_node in fresh_tree.get_subtree_bfs():
                        add_dst_tree_op.node_list.append(dst_node)
                    logger.debug(f'Added node list contains {len(add_dst_tree_op.node_list)} nodes')
                else:
                    local_node: LocalFileNode = self.build_local_file_node(dst_full_path)
                    add_dst_tree_op.node_list.append(local_node)

            subtree_op_list: List[SubtreeOperation] = [add_dst_tree_op]
            if rm_src_tree_op:
                subtree_op_list.append(rm_src_tree_op)
            if rm_existing_tree_op:
                subtree_op_list.append(rm_existing_tree_op)

            # 1. Update memory cache:
            self._update_memory_cache(subtree_op_list)

            # 1a. housekeeping
            if rm_src_tree_op and is_from_watchdog:
                self._add_to_expected_node_moves(rm_src_tree_op, add_dst_tree_op)

            # 2. Update on-disk cache:
            self._update_disk_cache(subtree_op_list)

            # 3. Send notifications:
            if rm_existing_tree_op:
                self._send_notifications([rm_existing_tree_op])

            if rm_src_tree_op:
                # MOVE from src to dst
                for src_node, dst_node in zip(rm_src_tree_op.node_list, add_dst_tree_op.node_list):
                    dispatcher.send(signal=actions.NODE_MOVED, sender=ID_GLOBAL_CACHE, src_node=src_node, dst_node=dst_node)
            else:
                # UPSERT at destination only
                self._send_notifications([add_dst_tree_op])

    def _send_notifications(self, operation_list: List[SubtreeOperation]):
        for operation in operation_list:
            if operation.is_delete:
                for node in operation.node_list:
                    dispatcher.send(signal=actions.NODE_REMOVED, sender=ID_GLOBAL_CACHE, node=node)
            else:
                for node in operation.node_list:
                    dispatcher.send(signal=actions.NODE_UPSERTED, sender=ID_GLOBAL_CACHE, node=node)

    def _execute(self, op_list: List[SubtreeOperation]):
        self._update_memory_cache(op_list)

        self._update_disk_cache(op_list)

        self._send_notifications(op_list)

    def remove_local_subtree(self, subtree_root: LocalNode, to_trash: bool):
        logger.debug(f'Removing subtree_root from caches (to_trash={to_trash}): {subtree_root}')

        # 1. Validate
        if not subtree_root.uid:
            raise RuntimeError(f'Cannot remove subtree_root from cache because it has no UID: {subtree_root}')

        if subtree_root.uid != self.get_uid_for_path(subtree_root.full_path):
            raise RuntimeError(f'Internal error while trying to remove subtree_root ({subtree_root}): UID did not match expected '
                               f'({self.get_uid_for_path(subtree_root.full_path)})')

        if not subtree_root.is_dir():
            raise RuntimeError(f'Not a folder: {subtree_root}')

        with self._struct_lock:
            operation: SubtreeOperation = self._build_subtree_removal_operation(subtree_root, to_trash)
            logger.info(f'Removing subtree with {len(operation.node_list)} nodes')
            op_list = [operation]

            self._execute(op_list)

    def _build_subtree_removal_operation(self, subtree_root: LocalNode, to_trash: bool) -> SubtreeOperation:
        """subtree_root can be either a file or dir"""
        if to_trash:
            # TODO
            raise RuntimeError(f'Not supported: to_trash=true!')

        subtree_nodes: List[LocalNode] = self._master_tree.get_subtree_bfs(subtree_root.uid)
        return SubtreeOperation(subtree_root.full_path, is_delete=True, node_list=subtree_nodes)

    def _get_parent_node_from_master_tree(self, node: LocalNode):
        parent_path = _derive_parent_path(node.full_path)
        parent_uid = self.get_uid_for_path(parent_path)
        return self._master_tree.get_node(parent_uid)

    # Various public getters
    # ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼

    def get_all_files_and_dirs_for_subtree(self, subtree_root: LocalFsIdentifier) -> Tuple[List[LocalFileNode], List[LocalDirNode]]:
        with self._struct_lock:
            return self._master_tree.get_all_files_and_dirs_for_subtree(subtree_root)

    def get_uid_for_path(self, path: str, uid_suggestion: Optional[UID] = None) -> UID:
        return self._uid_mapper.get_uid_for_path(path, uid_suggestion)

    def get_children(self, node: LocalNode) -> List[LocalNode]:
        with self._struct_lock:
            return self._master_tree.children(node.uid)

    def get_node(self, uid: UID) -> LocalNode:
        with self._struct_lock:
            return self._master_tree.get_node(uid)

    def get_parent_for_node(self, node: LocalNode, required_subtree_path: str = None):
        try:
            with self._struct_lock:
                try:
                    parent: LocalNode = self._master_tree.parent(nid=node.uid)
                except KeyError:
                    # parent not found in tree... maybe we can derive it however
                    parent = self._get_parent_node_from_master_tree(node)
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
        if self.use_md5:
            md5 = str(self.md5_dict.total_entries)
        else:
            md5 = 'disabled'
        return f'LocalDiskMasterCache tree_size={len(self._master_tree):n} md5={md5}'

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
                md5, sha256 = _calculate_signatures(full_path, staging_path)
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


def _derive_parent_path(full_path: str) -> str:
    return str(pathlib.Path(full_path).parent)


def _copy_signature_if_possible(src: LocalFileNode, dst: LocalFileNode):
    if src.modify_ts == dst.modify_ts and src.change_ts == dst.change_ts and src.get_size_bytes() == dst.get_size_bytes():
        # It is possible for the stored cache copy to be missing a signature. If so, the src may not have an MD5/SHA256.
        # In that case, do not overwrite possible real data with null values, but do check to make sure we don't overwrite one value with a different
        if dst.md5:
            if src.md5 and dst.md5 != src.md5:
                logger.error(f'Dst node already has MD5 but it is unexpected: {dst} (expected {src}')
        else:
            if SUPER_DEBUG:
                logger.debug(f'Copying MD5 for: {dst.node_identifier}')
            dst.md5 = src.md5
        if dst.sha256:
            if src.sha256 and dst.sha256 != src.sha256:
                logger.error(f'Dst node already has SHA256 but it is unexpected: {dst} (expected {src}')
        else:
            if SUPER_DEBUG:
                logger.debug(f'Copying SHA256 for: {dst.node_identifier}')
            dst.sha256 = src.sha256


def _check_update_sanity(old_node: LocalFileNode, new_node: LocalFileNode):
    try:
        if not isinstance(old_node, LocalFileNode):
            # Internal error; try to recover
            logger.error(f'Invalid node type for old_node: {type(old_node)}. Will overwrite cache entry')
            return

        if not isinstance(new_node, LocalFileNode):
            raise RuntimeError(f'Invalid node type for new_node: {type(new_node)}')

        if new_node.modify_ts < old_node.modify_ts:
            logger.warning(
                f'File "{new_node.full_path}": update has older modify_ts ({new_node.modify_ts}) than prev version ({old_node.modify_ts})')

        if new_node.change_ts < old_node.change_ts:
            logger.warning(
                f'File "{new_node.full_path}": update has older change_ts ({new_node.change_ts}) than prev version ({old_node.change_ts})')

        if new_node.get_size_bytes() != old_node.get_size_bytes() and new_node.md5 == old_node.md5 and old_node.md5:
            logger.warning(f'File "{new_node.full_path}": update has same MD5 ({new_node.md5}) ' +
                           f'but different size: (old={old_node.get_size_bytes()}, new={new_node.get_size_bytes()})')
    except Exception:
        logger.error(f'Error checking update sanity! Old={old_node} New={new_node}')
        raise
