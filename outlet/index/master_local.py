import copy
import logging
import os
import pathlib
import threading
import time
from abc import ABC, abstractmethod
from typing import Dict, List, Optional, Tuple

from pydispatch import dispatcher
from treelib.exceptions import NodeIDAbsentError

import local.content_hasher
from constants import LOCAL_ROOT_UID, ROOT_PATH, TREE_TYPE_LOCAL_DISK
from index.cache_manager import PersistedCacheInfo
from index.local_sig_calc_thread import SignatureCalcThread
from index.master import MasterCache
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

SUPER_DEBUG = True


# CLASS MasterCacheData
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
class MasterCacheData:
    def __init__(self, app):
        self.uid_mapper = UidPathMapper(app)

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
        self.master_tree = LocalDiskTree(app)
        root_node = RootTypeNode(node_identifier=LocalFsIdentifier(full_path=ROOT_PATH, uid=LOCAL_ROOT_UID))
        self.master_tree.add_node(node=root_node, parent=None)

        self.expected_node_moves: Dict[str, str] = {}
        """When the FileSystemEventHandler gives us MOVE notifications for a tree, it gives us a separate notification for each
        and every node. Since we want our tree move to be an atomic operation, we do it all at once, but then keep track of the
        nodes we've moved so that we know exactly which notifications to ignore after that.
        Dict is key-value pair of [old_file_path -> new_file_path]"""

    def remove_single_node(self, node: LocalNode):
        """Removes the given node from all in-memory structs (does nothing if it is not found in some or any of them).
        Will raise an exception if trying to remove a non-empty directory."""
        existing: DisplayNode = self.master_tree.get_node(node.uid)
        if existing:
            if existing.is_dir():
                children = self.master_tree.children(existing.identifier)
                if children:
                    # maybe allow deletion of dir with children in the future, but for now be careful
                    raise RuntimeError(f'Cannot remove dir from cache because it has {len(children)} children: {node}')

            count_removed = self.master_tree.remove_single_node(node.uid)
            assert count_removed <= 1, f'Deleted {count_removed} nodes at {node.full_path}'
        else:
            logger.warning(f'Cannot remove node because it has already been removed from cache: {node}')

        if self.use_md5 and node.md5:
            self.md5_dict.remove(node.md5, node.full_path)
        if self.use_sha256 and node.sha256:
            self.sha256_dict.remove(node.sha256, node.full_path)

    def upsert_single_node(self, node: LocalNode) -> Tuple[Optional[LocalNode], bool]:
        """If a node already exists, the new node is merged into it and returned; otherwise the given node is returned.
        Second item in the tuple is True if update contained changes which should be saved to disk; False if otherwise"""

        if SUPER_DEBUG:
            logger.debug(f'Upserting LocalNode to memory cache: {node}')

        # 1. Validate UID:
        if not node.uid:
            raise RuntimeError(f'Cannot upsert node to cache because it has no UID: {node}')

        uid = self.uid_mapper.get_uid_for_path(node.full_path, node.uid)
        if node.uid != uid:
            raise RuntimeError(f'Internal error while trying to upsert node to cache: UID did not match expected '
                               f'({uid}); node={node}')

        existing_node: LocalNode = self.master_tree.get_node(node.uid)
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
            self.master_tree.add_to_tree(node)

        # do this after the above, to avoid cache corruption in case of failure
        if node.md5 and self.use_md5:
            self.md5_dict.put(node, existing_node)
        if node.sha256 and self.use_sha256:
            self.sha256_dict.put(node, existing_node)

        if existing_node:
            return existing_node, True
        return node, True


# CLASS Subtree
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
class Subtree(ABC):
    def __init__(self, subtree_root: NodeIdentifier, upsert_node_list: List[LocalNode], remove_node_list: List[LocalNode]):
        self.subtree_root: NodeIdentifier = subtree_root
        self.upsert_node_list: List[LocalNode] = upsert_node_list
        self.remove_node_list: List[LocalNode] = remove_node_list


# ABSTRACT CLASS LocalDiskOp
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
class LocalDiskOp(ABC):
    @abstractmethod
    def update_memory_cache(self, data: MasterCacheData):
        pass

    @classmethod
    def is_subtree_op(cls) -> bool:
        return False

    @abstractmethod
    def send_signals(self):
        pass


# ABSTRACT CLASS LocalDiskOp
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
class LocalDiskSingleNodeOp(LocalDiskOp, ABC):
    def __init__(self, node: LocalNode):
        self.node: LocalNode = node

    @abstractmethod
    def update_disk_cache(self, cache: LocalDiskDatabase):
        pass


# ABSTRACT CLASS LocalDiskOp
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
class LocalDiskSubtreeOp(LocalDiskOp, ABC):
    @abstractmethod
    def get_subtree_list(self) -> List[Subtree]:
        pass

    @abstractmethod
    def update_disk_cache(self, cache: LocalDiskDatabase, subtree: Subtree):
        pass

    @classmethod
    def is_subtree_op(cls) -> bool:
        return True


# CLASS UpsertSingleNodeOp
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
class UpsertSingleNodeOp(LocalDiskSingleNodeOp):
    def __init__(self, node: LocalNode):
        super().__init__(node)
        self.was_updated: bool = True

    def update_memory_cache(self, data: MasterCacheData):
        self.was_updated = data.upsert_single_node(self.node)

    def update_disk_cache(self, cache: LocalDiskDatabase):
        if self.was_updated:
            if SUPER_DEBUG:
                logger.debug(f'Upserting LocalNode to disk cache: {self.node}')
            cache.upsert_single_node(self.node, commit=False)

    def send_signals(self):
        if self.was_updated:
            dispatcher.send(signal=actions.NODE_UPSERTED, sender=ID_GLOBAL_CACHE, node=self.node)


# CLASS DeleteSingleNodeOp
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
class DeleteSingleNodeOp(UpsertSingleNodeOp):
    def __init__(self, node: LocalNode, to_trash: bool = False):
        super().__init__(node)
        self.to_trash: bool = to_trash

        # 1. Validate
        if not node.uid:
            raise RuntimeError(f'Cannot remove node from cache because it has no UID: {node}')

        if to_trash:
            # TODO
            raise RuntimeError(f'Not supported: to_trash=true!')

    def update_memory_cache(self, data: MasterCacheData):
        if self.node.uid != data.uid_mapper.get_uid_for_path(self.node.full_path):
            raise RuntimeError(f'Internal error while trying to remove node ({self.node}): UID did not match expected '
                               f'({data.uid_mapper.get_uid_for_path(self.node.full_path)})')

        data.remove_single_node(self.node)

    def update_disk_cache(self, cache: LocalDiskDatabase):
        cache.delete_single_node(self.node, commit=False)

    def send_signals(self):
        dispatcher.send(signal=actions.NODE_REMOVED, sender=ID_GLOBAL_CACHE, node=self.node)


# CLASS BatchChangesOp
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
class BatchChangesOp(LocalDiskSubtreeOp):
    """ALWAYS REMOVE BEFORE ADDING!"""

    def __init__(self, subtree_list: List[Subtree] = None,
                 subtree_root: LocalFsIdentifier = None, upsert_node_list: List[LocalNode] = None, remove_node_list: List[LocalNode] = None):
        if subtree_list:
            self.subtree_list = subtree_list
        else:
            self.subtree_list = [Subtree(subtree_root, upsert_node_list, remove_node_list)]

    def get_subtree_list(self) -> List[Subtree]:
        return self.subtree_list

    def update_memory_cache(self, data: MasterCacheData):
        for subtree in self.subtree_list:
            # Deletes must occur from bottom up:
            if subtree.remove_node_list:
                for node in reversed(subtree.remove_node_list):
                    data.remove_single_node(node)
                logger.debug(f'Removed {len(subtree.remove_node_list)} nodes from memcache path "{subtree.subtree_root.full_path}"')
            if subtree.upsert_node_list:
                for node in subtree.upsert_node_list:
                    data.upsert_single_node(node)
                logger.debug(f'Upserted {len(subtree.upsert_node_list)} nodes to memcache path "{subtree.subtree_root.full_path}"')

    def update_disk_cache(self, cache: LocalDiskDatabase, subtree: Subtree):
        if subtree.remove_node_list:
            cache.delete_files_and_dirs(subtree.remove_node_list, commit=False)
        if subtree.upsert_node_list:
            cache.upsert_files_and_dirs(subtree.upsert_node_list, commit=False)

    def send_signals(self):
        for subtree in self.subtree_list:
            for node in reversed(subtree.remove_node_list):
                dispatcher.send(signal=actions.NODE_REMOVED, sender=ID_GLOBAL_CACHE, node=node)
            for node in subtree.upsert_node_list:
                dispatcher.send(signal=actions.NODE_UPSERTED, sender=ID_GLOBAL_CACHE, node=node)


# CLASS DeleteSubtreeOp
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
class DeleteSubtreeOp(BatchChangesOp):
    def __init__(self, subtree_root: LocalFsIdentifier, node_list: List[LocalNode]):
        super().__init__(subtree_root=subtree_root, upsert_node_list=[], remove_node_list=node_list)


# CLASS LocalDiskOpExecutor
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
class LocalDiskOpExecutor:
    def __init__(self, app, data: MasterCacheData):
        self.app = app
        self._data: MasterCacheData = data

    def execute(self, operation: LocalDiskOp):

        operation.update_memory_cache(self._data)

        cacheman = self.app.cacheman
        if cacheman.enable_save_to_disk:
            if operation.is_subtree_op():
                assert isinstance(operation, LocalDiskSubtreeOp)
                self._update_disk_cache_for_subtree(operation)
            else:
                assert isinstance(operation, LocalDiskSingleNodeOp)
                cache_info: Optional[PersistedCacheInfo] = cacheman.find_existing_cache_info_for_subtree(operation.node.full_path,
                                                                                                         operation.node.get_tree_type())
                if not cache_info:
                    raise RuntimeError(f'Could not find a cache associated with file path: {operation.node.full_path}')

                with LocalDiskDatabase(cache_info.cache_location, self.app) as cache:
                    operation.update_disk_cache(cache)

        else:
            logger.debug(f'Save to disk is disabled: skipping save to disk for operation')

        operation.send_signals()

    def _update_disk_cache_for_subtree(self, op: LocalDiskSubtreeOp):
        """Attempt to come close to a transactional behavior by writing to all caches at once, and then committing all at the end"""
        cache_man = self.app.cacheman
        if not cache_man.enable_save_to_disk:
            logger.debug(f'Save to disk is disabled: skipping save of {len(op.get_subtree_list())} subtrees')
            return

        physical_cache_list: List[PersistedCacheInfo] = []
        logical_cache_list: List[PersistedCacheInfo] = []

        for subtree in op.get_subtree_list():
            cache_info: Optional[PersistedCacheInfo] = cache_man.find_existing_cache_info_for_subtree(subtree.subtree_root.full_path,
                                                                                                      TREE_TYPE_LOCAL_DISK)
            if not cache_info:
                raise RuntimeError(f'Could not find a cache associated with file path: {subtree.subtree_root.full_path}')

            if not _is_cache_info_in_list(cache_info, physical_cache_list):
                physical_cache_list.append(cache_info)

            logical_cache_list.append(cache_info)

        if len(physical_cache_list) > 3:
            raise RuntimeError(f'Cannot update more than 3 disk caches simultaneously ({len(physical_cache_list)} needed)')

        if len(physical_cache_list) == 1:
            with LocalDiskDatabase(physical_cache_list[0].cache_location, self.app) as cache0:
                phys_cache_list = [cache0]
                log_cache_list = _map_physical_cache_list(phys_cache_list, logical_cache_list, physical_cache_list)
                _update_multiple_cache_files(log_cache_list, op)
                _commit(phys_cache_list)

        elif len(physical_cache_list) == 2:
            with LocalDiskDatabase(physical_cache_list[0].cache_location, self.app) as cache0, \
                    LocalDiskDatabase(physical_cache_list[1].cache_location, self.app) as cache1:
                phys_cache_list = [cache0, cache1]
                log_cache_list = _map_physical_cache_list(phys_cache_list, logical_cache_list, physical_cache_list)
                _update_multiple_cache_files(log_cache_list, op)
                _commit(phys_cache_list)

        elif len(physical_cache_list) == 3:
            with LocalDiskDatabase(physical_cache_list[0].cache_location, self.app) as cache0, \
                    LocalDiskDatabase(physical_cache_list[1].cache_location, self.app) as cache1, \
                    LocalDiskDatabase(physical_cache_list[2].cache_location, self.app) as cache2:
                phys_cache_list = [cache0, cache1, cache2]
                log_cache_list = _map_physical_cache_list(phys_cache_list, logical_cache_list, physical_cache_list)
                _update_multiple_cache_files(log_cache_list, op)
                _commit(phys_cache_list)


def _is_cache_info_in_list(cache_info, cache_list):
    for existing_cache in cache_list:
        if cache_info.cache_location == existing_cache.cache_location:
            return True
    return False


def _map_physical_cache(logical_cache: PersistedCacheInfo, cache_list: List[LocalDiskDatabase], physical_cache_list: List[PersistedCacheInfo]) \
        -> LocalDiskDatabase:
    for physical_index, physical_cache_info in enumerate(physical_cache_list):
        if logical_cache.cache_location == physical_cache_info.cache_location:
            return cache_list[physical_index]
    raise RuntimeError(f'Bad: {logical_cache}')


def _map_physical_cache_list(cache_list: List[LocalDiskDatabase], physical_cache_list: List[PersistedCacheInfo],
                             logical_cache_list: List[PersistedCacheInfo]) -> List[LocalDiskDatabase]:
    mapped_list: List[LocalDiskDatabase] = []
    for logical_cache in logical_cache_list:
        mapped_list.append(_map_physical_cache(logical_cache, cache_list, physical_cache_list))

    return mapped_list


def _commit(cache_list):
    for logical_index, cache in enumerate(cache_list):
        logger.debug(f'Committing cache {logical_index}: "{cache.db_path}"')
        cache.commit()


def _update_multiple_cache_files(logical_cache_list: List[LocalDiskDatabase], op: LocalDiskSubtreeOp):
    for cache, subtree in zip(logical_cache_list, op.get_subtree_list()):
        logger.debug(f'Writing subtree_root="{subtree.subtree_root.full_path}"')
        op.update_disk_cache(cache, subtree)


# ⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛
# CLASS LocalDiskMasterCache
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼

# TODO: consider scanning only root dir at first, then enqueuing subdirectories

class LocalDiskMasterCache(MasterCache):
    def __init__(self, app):
        """Singleton in-memory cache for local filesystem"""
        self.app = app

        self._struct_lock = threading.Lock()
        self._data: MasterCacheData = MasterCacheData(app)

        self._executor = LocalDiskOpExecutor(app, self._data)

        self._signature_calc_thread = SignatureCalcThread(self)

        self.lazy_load_signatures: bool = app.config.get('cache.lazy_load_local_file_signatures')
        if self.lazy_load_signatures:
            self.start_signature_calc_thread()

    def start_signature_calc_thread(self):
        if not self._signature_calc_thread.is_alive():
            self._signature_calc_thread.start()

    def shutdown(self):
        if self._signature_calc_thread:
            self._signature_calc_thread.request_shutdown()

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
            # Just upsert all nodes in the updated tree and let God (or some logic) sort them out
            subtree = Subtree(subtree_root, fresh_tree.get_subtree_bfs(), [])
            batch_changes_op: BatchChangesOp = BatchChangesOp(subtree_list=[subtree])

            # Find removed nodes and append them to remove_single_nodes_op
            root_node: LocalNode = fresh_tree.get_node(fresh_tree.root)
            if root_node.is_dir():
                for existing_node in self._data.master_tree.get_subtree_bfs(subtree_root.uid):
                    if not fresh_tree.get_node(existing_node.uid):
                        if not existing_node.exists():
                            # If {not existing_node.exists()}, assume it's a "pending op" node.
                            ancestor = existing_node
                            # Iterate up the tree until we (a) encounter a "normal" ancestor which is also present in the fresh tree,
                            # or (b) pass the root of the master tree or encounter a "normal" ancestor in the master tree which doesn't exist in the
                            # fresh tree, which means its descendants are all removed.
                            while True:
                                ancestor = self.get_parent_for_node(ancestor)
                                if ancestor and fresh_tree.contains(ancestor.uid):
                                    assert ancestor.exists()
                                    # no need to remove
                                    break
                                elif not ancestor or ancestor.exists():
                                    # FIXME: clean up the op graph when ancestors of src or dst are removed
                                    logger.error(f'Removing node belonging to a pending op because its ancestor was deleted: {existing_node}')
                                    subtree.remove_node_list.append(existing_node)
                                    break
                                # We can ignore any "pending op" ancestors we encounter:
                                assert not ancestor.exists()

            self._executor.execute(batch_changes_op)

    # Subtree-level methods
    # ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼

    def get_display_tree(self, subtree_root: LocalFsIdentifier, tree_id: str) -> DisplayTree:
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

        # LOAD into master tree. Only for first load!
        if not cache_info.is_loaded:
            if self.app.cacheman.enable_load_from_disk:
                tree = self._load_subtree_from_disk(cache_info, tree_id)
                if tree:
                    with self._struct_lock:
                        self._data.master_tree.replace_subtree(tree)
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
            root_node = self._data.master_tree.get_node(requested_subtree_root.uid)
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
                    self._data.master_tree.replace_subtree(super_tree)

                # this will resync with file system and/or save if configured
                supertree_cache.needs_save = True
                self._load_subtree(supertree_cache, tree_id)
                # Now it is safe to delete the subtree cache:
                file_util.delete_file(subtree_cache.cache_location)

        registry_needs_update = len(supertree_sets) > 0
        return local_caches, registry_needs_update
    
    def refresh_subtree(self, node: LocalNode, tree_id: str):
        self._get_display_tree(node.node_identifier, tree_id, is_live_refresh=True)

    def refresh_subtree_stats(self, subtree_root_node: LocalNode, tree_id: str):
        with self._struct_lock:
            self._data.master_tree.refresh_stats(subtree_root_node, tree_id)

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
        # 2. Update in-memory cache:
        with self._struct_lock:
            self._executor.execute(UpsertSingleNodeOp(node))

    def remove_single_node(self, node: LocalNode, to_trash=False):
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
                self._data.expected_node_moves[src_node.full_path] = dst_node.full_path

    def move_local_subtree(self, src_full_path: str, dst_full_path: str, is_from_watchdog=False):
        with self._struct_lock:
            if is_from_watchdog:
                # See if we safely ignore this:
                expected_move_dst = self._data.expected_node_moves.pop(src_full_path, None)
                if expected_move_dst:
                    if expected_move_dst == dst_full_path:
                        logger.debug(f'Ignoring MV ("{src_full_path}" -> "{dst_full_path}") because it was already done')
                        return
                    else:
                        logger.error(f'MV ("{src_full_path}" -> "{dst_full_path}"): was expecting dst = "{expected_move_dst}"!')

            src_uid: UID = self.get_uid_for_path(src_full_path)
            src_node: LocalNode = self._data.master_tree.get_node(src_uid)
            if src_node:
                src_nodes: List[LocalNode] = self._data.master_tree.get_subtree_bfs(src_node.uid)
                src_subtree: Subtree = Subtree(src_node.node_identifier, upsert_node_list=[], remove_node_list=src_nodes)
            else:
                logger.error(f'MV src node does not exist: UID={src_uid}, path={src_full_path}')
                return

            # Create up to 3 tree operations which should be executed in a single transaction if possible
            dst_uid: UID = self.get_uid_for_path(dst_full_path)
            dst_node_identifier: LocalFsIdentifier = LocalFsIdentifier(dst_full_path, dst_uid)
            dst_subtree: Subtree = Subtree(dst_node_identifier, [], [])

            existing_dst_node: LocalNode = self._data.master_tree.get_node(dst_uid)
            if existing_dst_node:
                logger.debug(f'Node already exists at MV dst; will remove: {existing_dst_node.node_identifier}')
                existing_dst_nodes: List[LocalNode] = self._data.master_tree.get_subtree_bfs(dst_uid)
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

            subtree_list: List[Subtree] = [src_subtree, dst_subtree]

            if is_from_watchdog:
                self._add_to_expected_node_moves(src_subtree.remove_node_list, dst_subtree.upsert_node_list)

            operation = BatchChangesOp(subtree_list=subtree_list)
            self._executor.execute(operation)

    def remove_subtree(self, subtree_root: LocalNode, to_trash: bool):
        logger.debug(f'Removing subtree_root from caches (to_trash={to_trash}): {subtree_root}')

        with self._struct_lock:
            operation: DeleteSubtreeOp = self._build_subtree_removal_operation(subtree_root, to_trash)
            self._executor.execute(operation)

    def _build_subtree_removal_operation(self, subtree_root_node: LocalNode, to_trash: bool) -> DeleteSubtreeOp:
        """subtree_root can be either a file or dir"""
        if to_trash:
            # TODO
            raise RuntimeError(f'Not supported: to_trash=true!')

        if not subtree_root_node.uid:
            raise RuntimeError(f'Cannot remove subtree_root from cache because it has no UID: {subtree_root_node}')

        if subtree_root_node.uid != self.get_uid_for_path(subtree_root_node.full_path):
            raise RuntimeError(f'Internal error while trying to remove subtree_root ({subtree_root_node}): UID did not match expected '
                               f'({self.get_uid_for_path(subtree_root_node.full_path)})')

        subtree_nodes: List[LocalNode] = self._data.master_tree.get_subtree_bfs(subtree_root_node.uid)
        assert isinstance(subtree_root_node.node_identifier, LocalFsIdentifier)
        return DeleteSubtreeOp(subtree_root_node.node_identifier, node_list=subtree_nodes)

    # Various public getters
    # ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼

    def get_all_files_and_dirs_for_subtree(self, subtree_root: LocalFsIdentifier) -> Tuple[List[LocalFileNode], List[LocalDirNode]]:
        with self._struct_lock:
            return self._data.master_tree.get_all_files_and_dirs_for_subtree(subtree_root)

    def get_uid_for_domain_id(self, domain_id: str, uid_suggestion: Optional[UID] = None) -> UID:
        return self._data.uid_mapper.get_uid_for_path(domain_id, uid_suggestion)

    def get_uid_for_path(self, path: str, uid_suggestion: Optional[UID] = None) -> UID:
        return self._data.uid_mapper.get_uid_for_path(path, uid_suggestion)

    def get_node_for_domain_id(self, domain_id: str) -> LocalNode:
        uid: UID = self.get_uid_for_domain_id(domain_id)
        return self.get_node_for_uid(uid)

    def get_children(self, node: LocalNode) -> List[LocalNode]:
        with self._struct_lock:
            return self._data.master_tree.children(node.uid)

    def get_node_for_uid(self, uid: UID) -> LocalNode:
        with self._struct_lock:
            return self._data.master_tree.get_node(uid)

    def get_parent_for_node(self, node: LocalNode, required_subtree_path: str = None):
        try:
            with self._struct_lock:
                try:
                    parent: LocalNode = self._data.master_tree.parent(nid=node.uid)
                except KeyError:
                    # parent not found in tree... maybe we can derive it however
                    parent_path = _derive_parent_path(node.full_path)
                    parent_uid = self.get_uid_for_path(parent_path)
                    parent = self._data.master_tree.get_node(parent_uid)
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
        if self._data.use_md5:
            md5 = str(self._data.md5_dict.total_entries)
        else:
            md5 = 'disabled'
        return f'LocalDiskMasterCache tree_size={len(self._data.master_tree):n} md5={md5}'

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
                md5, sha256 = local.content_hasher.calculate_signatures(full_path, staging_path)
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
