import logging
import os
import threading
import time
from typing import List, Optional, Tuple

import treelib
from pydispatch import dispatcher
from treelib.exceptions import NodeIDAbsentError

from util import file_util
import fmeta.content_hasher
from constants import LOCAL_ROOT_UID, ROOT_PATH, TREE_TYPE_LOCAL_DISK
from fmeta.local_disk_scanner import LocalDiskScanner
from index.cache_manager import PersistedCacheInfo
from index.sqlite.local_db import LocalDiskDatabase
from index.two_level_dict import Md5BeforePathDict, Sha256BeforePathDict
from index.uid.uid_generator import UID
from index.uid.uid_mapper import UidPathMapper
from model.node.container_node import ContainerNode, RootTypeNode
from model.node.display_node import DisplayNode
from model.node.local_disk_node import LocalDirNode, LocalFileNode
from model.display_tree.local_disk import LocalDiskSubtree
from model.local_disk_tree import LocalDiskTree
from model.node_identifier import LocalFsIdentifier, NodeIdentifier
from model.display_tree.null import NullDisplayTree
from model.display_tree.display_tree import DisplayTree
from util.stopwatch_sec import Stopwatch
from ui import actions
from ui.actions import ID_GLOBAL_CACHE

logger = logging.getLogger(__name__)


# ⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛⬛
# CLASS LocalDiskMasterCache
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼


class LocalDiskMasterCache:
    def __init__(self, application):
        """Singleton in-memory cache for local filesystem"""
        self.application = application

        self._struct_lock = threading.Lock()

        self._uid_mapper = UidPathMapper(application)

        self.use_md5 = application.config.get('cache.enable_md5_lookup')
        if self.use_md5:
            self.md5_dict = Md5BeforePathDict()
        else:
            self.md5_dict = None

        self.use_sha256 = application.config.get('cache.enable_sha256_lookup')
        if self.use_sha256:
            self.sha256_dict = Sha256BeforePathDict()
        else:
            self.sha256_dict = None

        # Each item inserted here will have an entry created for its dir.
        # self.parent_path_dict = ParentPathBeforeFileNameDict()
        # But we still need a dir tree to look up child dirs:
        with self._struct_lock:
            self.dir_tree = LocalDiskTree(self.application)
            root_node = RootTypeNode(node_identifier=LocalFsIdentifier(full_path=ROOT_PATH, uid=LOCAL_ROOT_UID))
            self.dir_tree.add_node(node=root_node, parent=None)

    # Disk access
    # ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼

    def _load_subtree_from_disk(self, cache_info: PersistedCacheInfo, tree_id) -> Optional[LocalDiskTree]:
        """Loads the given subtree disk cache from disk."""

        stopwatch_load = Stopwatch()

        # Load cache from file, and update with any local FS ops found:
        with LocalDiskDatabase(cache_info.cache_location, self.application) as fmeta_disk_cache:
            if not fmeta_disk_cache.has_local_files() and not fmeta_disk_cache.has_local_dirs():
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
            tree: LocalDiskTree = LocalDiskTree(self.application)
            root_node = LocalDirNode(node_identifier=root_node_identifer, exists=True)
            tree.add_node(node=root_node, parent=None)

            missing_items: List[DisplayNode] = []

            dir_list: List[LocalDirNode] = fmeta_disk_cache.get_local_dirs()
            if len(dir_list) == 0:
                logger.debug('No dirs found in disk cache')

            # Dirs first
            for dir_node in dir_list:
                existing = tree.get_node(dir_node.identifier)
                # Overwrite older ops for the same path:
                if not existing:
                    tree.add_to_tree(dir_node)
                    if not dir_node.exists():
                        missing_items.append(dir_node)
                elif existing.full_path != dir_node.full_path:
                    raise RuntimeError(f'Existing={existing}, FromCache={dir_node}')

            file_list: List[LocalFileNode] = fmeta_disk_cache.get_local_files()
            if len(file_list) == 0:
                logger.debug('No files found in disk cache')

            for change in file_list:
                existing = tree.get_node(change.identifier)
                # Overwrite older changes for the same path:
                if not existing:
                    tree.add_to_tree(change)
                    if not change.exists():
                        missing_items.append(change)
                elif existing.sync_ts < change.sync_ts:
                    tree.remove_node(change.identifier)
                    tree.add_to_tree(change)

            # logger.debug(f'Reduced {str(len(db_file_changes))} disk cache entries into {str(count_from_disk)} unique entries')
            logger.debug(f'{stopwatch_load} [{tree_id}] Finished loading {len(file_list)} files and {len(dir_list)} dirs from disk')

            if len(missing_items) > 0:
                logger.info(f'Found {len(missing_items)} cached items with exists=false: submitting to adjudicator...')
            # TODO: add code for adjudicator

            cache_info.is_loaded = True
            return tree

    def _cp_planning_nodes_into(self, tree: treelib.Tree):
        count = 0
        for node in self.dir_tree.bfs(tree.root):
            if not node.exists() and not tree.contains(node.uid):
                parent: DisplayNode = self.dir_tree.parent(node.identifier)
                if tree.contains(parent.identifier):
                    tree.add_node(node=node, parent=parent.identifier)
                    count += 1
                else:
                    # This will cause an error cascade. Need a strategy to handle it
                    logger.error(f'Dropping planning node because its parent does not exist: {node}')

        logger.debug(f'Transferred {count} planning nodes into new tree with root {tree.get_node(tree.root).full_path})')

    def _save_subtree_to_disk(self, cache_info: PersistedCacheInfo, tree_id):
        assert isinstance(cache_info.subtree_root, LocalFsIdentifier)
        with self._struct_lock:
            file_list, dir_list = self.dir_tree.get_all_files_and_dirs_for_subtree(cache_info.subtree_root)

        stopwatch_write_cache = Stopwatch()
        with LocalDiskDatabase(cache_info.cache_location, self.application) as fmeta_disk_cache:
            # Update cache:
            fmeta_disk_cache.insert_local_files(file_list, overwrite=True, commit=False)
            fmeta_disk_cache.insert_local_dirs(dir_list, overwrite=True, commit=True)

        cache_info.needs_save = False
        logger.info(f'[{tree_id}] {stopwatch_write_cache} Wrote {len(file_list)} files and {len(dir_list)} dirs to "{cache_info.cache_location}"')

    def _resync_with_file_system(self, subtree_root: LocalFsIdentifier, tree_id: str):
        # Scan directory tree and update where needed.
        logger.debug(f'[{tree_id}] Scanning filesystem subtree: {subtree_root}')
        scanner = LocalDiskScanner(application=self.application, root_node_identifer=subtree_root, tree_id=tree_id)
        fresh_tree: treelib.Tree = scanner.scan()

        with self._struct_lock:
            self._cp_planning_nodes_into(fresh_tree)
            self.dir_tree.replace_subtree(sub_tree=fresh_tree)

    # Subtree-level methods
    # ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼

    def load_local_subtree(self, subtree_root: NodeIdentifier, tree_id) -> DisplayTree:
        """
        Performs a read-through retreival of all the FMetas in the given subtree
        on the local filesystem.
        """

        if not os.path.exists(subtree_root.full_path):
            logger.info(f'Cannot load meta for subtree because it does not exist: "{subtree_root.full_path}"')
            root_node = ContainerNode(subtree_root)
            return NullDisplayTree(root_node)

        existing_uid = subtree_root.uid
        new_uid = self.get_uid_for_path(subtree_root.full_path, existing_uid)
        if existing_uid != new_uid:
            logger.warning(f'Requested UID "{existing_uid}" is invalid for given path; changing it to "{new_uid}"')
        subtree_root.uid = new_uid

        # If we have already loaded this subtree as part of a larger cache, use that:
        cache_man = self.application.cache_manager
        supertree_cache: Optional[PersistedCacheInfo] = cache_man.find_existing_supertree_for_subtree(subtree_root.full_path, subtree_root.tree_type)
        if supertree_cache:
            logger.debug(f'Subtree ({subtree_root.full_path}) is part of existing cached supertree ({supertree_cache.subtree_root.full_path})')
            assert isinstance(subtree_root, LocalFsIdentifier)
            return self._load_subtree(supertree_cache, tree_id, subtree_root)
        else:
            # no supertree found in cache. use exact match logic:
            cache_info = cache_man.get_or_create_cache_info_entry(subtree_root)
            assert cache_info is not None
            return self._load_subtree(cache_info, tree_id)

    def _load_subtree(self, cache_info: PersistedCacheInfo, tree_id, requested_subtree_root: LocalFsIdentifier = None) -> LocalDiskSubtree:
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

        # LOAD
        if not cache_info.is_loaded:
            if self.application.cache_manager.enable_load_from_disk:
                tree = self._load_subtree_from_disk(cache_info, tree_id)
                if tree:
                    with self._struct_lock:
                        self.dir_tree.replace_subtree(tree)
                        logger.debug(f'[{tree_id}] Updated in-memory cache: {self.get_summary()}')
            else:
                logger.debug(f'[{tree_id}] Skipping cache disk load because cache.enable_load_from_disk is false')

        # FS SYNC
        if not cache_info.is_loaded or (cache_info.needs_refresh and self.application.cache_manager.sync_from_local_disk_on_cache_load):
            logger.debug(f'[{tree_id}] Will resync with file system (is_loaded={cache_info.is_loaded}, sync_on_cache_load='
                         f'{self.application.cache_manager.sync_from_local_disk_on_cache_load}, needs_refresh={cache_info.needs_refresh})')
            # Update from the file system, and optionally save any changes back to cache:
            self._resync_with_file_system(requested_subtree_root, tree_id)
            cache_info.needs_refresh = False
            cache_info.needs_save = True
        elif not self.application.cache_manager.sync_from_local_disk_on_cache_load:
            logger.debug(f'[{tree_id}] Skipping filesystem sync because it is disabled for cache loads')
        elif not cache_info.needs_refresh:
            logger.debug(f'[{tree_id}] Skipping filesystem sync because the cache is still fresh for path: {cache_info.subtree_root}')

        # SAVE
        if cache_info.needs_save:
            # Save the updates back to local disk cache:
            if self.application.cache_manager.enable_save_to_disk:
                self._save_subtree_to_disk(cache_info, tree_id)
            else:
                logger.debug(f'[{tree_id}] Skipping cache save because it is disabled')

        with self._struct_lock:
            root_node = self.dir_tree.get_node(requested_subtree_root.uid)
        fmeta_tree = LocalDiskSubtree(root_node=root_node, application=self.application)
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
                    self.dir_tree.replace_subtree(super_tree)

                # this will resync with file system and/or save if configured
                supertree_cache.needs_save = True
                self._load_subtree(supertree_cache, tree_id)
                # Now it is safe to delete the subtree cache:
                file_util.delete_file(subtree_cache.cache_location)

        registry_needs_update = len(supertree_sets) > 0
        return local_caches, registry_needs_update

    # Individual node operations
    # ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼

    def get_node_for_path(self, full_path: str) -> DisplayNode:
        cache_man = self.application.cache_manager
        cache_info: Optional[PersistedCacheInfo] = cache_man.find_existing_supertree_for_subtree(full_path, TREE_TYPE_LOCAL_DISK)
        with LocalDiskDatabase(cache_info.cache_location, self.application) as cache:
            return cache.get_file_or_dir_for_path(full_path)

    def _update_dir_fields(self, update_to: LocalDirNode, update_from: LocalDirNode):
        # We only have one field to update for dirs so far
        if update_from.exists():
            # Only update to True. Do not overwrite with False
            update_to.set_exists(True)

    def upsert_local_node(self, node: DisplayNode, fire_listeners=True):
        # 1. Validate UID:
        if not node.uid:
            raise RuntimeError(f'Cannot upsert node to cache because it has no UID: {node}')

        uid = self._uid_mapper.get_uid_for_path(node.full_path, node.uid)
        if node.uid != uid:
            raise RuntimeError(f'Internal error while trying to upsert node to cache: UID did not match expected '
                               f'({self._uid_mapper.get_uid_for_path(node.full_path)}); node={node}')

        # 2. Update in-memory cache:
        with self._struct_lock:
            existing: DisplayNode = self.dir_tree.get_node(node.uid)
            if existing:
                if existing.exists() and not node.exists():
                    # In the future, let's close this hole with more elegant logic
                    logger.warning(f'Cannot replace a node which exists with one which does not exist; ignoring: {node}')
                    return

                if existing.is_dir():
                    if not node.is_dir():
                        # need to replace all descendants...not ready to do this yet
                        raise RuntimeError(f'Cannot replace a directory with a file: "{node.full_path}"')
                    else:
                        assert isinstance(existing, LocalDirNode)
                        assert isinstance(node, LocalDirNode)
                        # just update the existing - much easier
                        self._update_dir_fields(existing, node)
                        node = existing
                else:
                    # replace a file with either file or dir
                    assert isinstance(existing, LocalFileNode)
                    self.dir_tree.remove_node(existing.identifier)
                    self.dir_tree.add_to_tree(node)
            else:
                # new file or directory insert
                self.dir_tree.add_to_tree(node)

            # do this after the above, to avoid cache corruption in case of failure
            if self.use_md5 and node.md5:
                self.md5_dict.put(node, existing)
            if self.use_sha256 and node.sha256:
                self.sha256_dict.put(node, existing)

        # 3. Update on-disk cache:
        if node.exists():
            cache_man = self.application.cache_manager
            if cache_man.enable_save_to_disk:
                assert node.get_tree_type() == TREE_TYPE_LOCAL_DISK
                cache_info: Optional[PersistedCacheInfo] = cache_man.find_existing_supertree_for_subtree(node.full_path, node.get_tree_type())
                if cache_info:
                    with LocalDiskDatabase(cache_info.cache_location, self.application) as cache:
                        if node.is_dir():
                            cache.upsert_local_dir(node)
                        else:
                            cache.upsert_local_file(node)
                else:
                    logger.error(f'Could not find a cache associated with file path: {node.full_path}')
            else:
                logger.debug(f'Save to disk is disabled: skipping add/update of node with UID={node.uid}')
        else:
            logger.debug(f'Node does not exist; skipping save to disk: {node}')

        # 4. Notify UI:
        if fire_listeners:
            dispatcher.send(signal=actions.NODE_UPSERTED, sender=ID_GLOBAL_CACHE, node=node)

    def remove_node(self, node: LocalFileNode, to_trash=False, fire_listeners=True):
        # 1. Validate
        if not node.uid:
            raise RuntimeError(f'Cannot remove node from cache because it has no UID: {node}')

        if node.uid != self._uid_mapper.get_uid_for_path(node.full_path):
            raise RuntimeError(f'Internal error while trying to remove node ({node}): UID did not match expected '
                               f'({self._uid_mapper.get_uid_for_path(node.full_path)})')

        # 2. update in-memory cache
        with self._struct_lock:
            existing: DisplayNode = self.dir_tree.get_node(node.uid)
            if existing:
                if existing.is_dir():
                    children = self.dir_tree.children(existing.identifier)
                    if children:
                        # maybe allow deletion of dir with children in the future, but for now be careful
                        raise RuntimeError(f'Cannot remove dir from cache because it has {len(children)} children: {node}')

                count_removed = self.dir_tree.remove_node(node.uid)
                assert count_removed <= 1, f'Deleted {count_removed} nodes at {node.full_path}'
            else:
                logger.warning(f'Cannot remove node because it has already been removed from cache: {node}')

            if self.use_md5 and node.md5:
                self.md5_dict.remove(node.md5, node.full_path)
            if self.use_sha256 and node.sha256:
                self.sha256_dict.remove(node.sha256, node.full_path)

        # 3. Update on-disk cache:
        cache_man = self.application.cache_manager
        if cache_man.enable_save_to_disk:
            assert node.get_tree_type() == TREE_TYPE_LOCAL_DISK
            cache_info: Optional[PersistedCacheInfo] = cache_man.find_existing_supertree_for_subtree(node.full_path, node.get_tree_type())
            if cache_info:
                with LocalDiskDatabase(cache_info.cache_location, self.application) as cache:
                    if node.is_dir():
                        cache.delete_local_dir_with_uid(node.uid)
                    else:
                        cache.delete_local_file_with_uid(node.uid)
            else:
                logger.error(f'Could not find a cache associated with file path: {node.full_path}')
        else:
            logger.debug(f'Save to disk is disabled: skipping removal of node with UID={node.uid}')

        # 4. Notify UI:
        if fire_listeners:
            dispatcher.send(signal=actions.NODE_REMOVED, sender=ID_GLOBAL_CACHE, node=node)

    # Various public getters
    # ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
    def get_uid_for_path(self, path: str, uid_suggestion: Optional[UID] = None) -> UID:
        return self._uid_mapper.get_uid_for_path(path, uid_suggestion)

    def get_children(self, node: DisplayNode):
        with self._struct_lock:
            return self.dir_tree.children(node.uid)

    def get_item(self, uid: UID) -> DisplayNode:
        with self._struct_lock:
            return self.dir_tree.get_node(uid)

    def get_parent_for_item(self, item: DisplayNode, required_subtree_path: str = None):
        try:
            with self._struct_lock:
                parent: DisplayNode = self.dir_tree.parent(nid=item.uid)
                if not required_subtree_path or parent.full_path.startswith(required_subtree_path):
                    return parent
                return None
        except NodeIDAbsentError:
            return None
        except Exception:
            logger.error(f'Error getting parent for item: {item}, required_path: {required_subtree_path}')
            raise

    def get_summary(self):
        if self.use_md5:
            md5 = str(self.md5_dict.total_entries)
        else:
            md5 = 'disabled'
        return f'LocalDiskMasterCache tree_size={len(self.dir_tree):n} md5={md5}'

    def build_local_file_node(self, full_path: str, staging_path=None) -> Optional[LocalFileNode]:
        uid = self.get_uid_for_path(full_path)

        try:
            # Open,close, read file and calculate hash of its contents
            if staging_path:
                md5 = fmeta.content_hasher.md5(staging_path)
            else:
                md5 = fmeta.content_hasher.md5(full_path)
            # sha256 = fmeta.content_hasher.dropbox_hash(full_path)
            sha256 = None
        except FileNotFoundError:
            if os.path.islink(full_path):
                target = os.readlink(full_path)
                logger.error(f'Broken link, skipping: "{full_path}" -> "{target}"')
            else:
                logger.error(f'While building LocalFileNode: file not found; skipping: {full_path}')
            # Return None. Will be assumed to be a deleted file
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
