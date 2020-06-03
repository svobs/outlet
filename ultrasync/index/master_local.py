import logging
import os
import threading
import time
from typing import Dict, List, Optional, Tuple

import treelib
from pydispatch import dispatcher
from treelib.exceptions import NodeIDAbsentError

import file_util
import fmeta.content_hasher
from constants import ROOT_PATH
from fmeta.fmeta_tree_scanner import FMetaDiskScanner
from index.cache_manager import PersistedCacheInfo
from index.sqlite.fmeta_db import FMetaDatabase
from index.two_level_dict import Md5BeforePathDict, Sha256BeforePathDict
from index.uid_generator import ROOT_UID, UID
from model.category import Category
from model.display_node import DirNode, DisplayNode, RootTypeNode
from model.fmeta import FMeta
from model.fmeta_tree import FMetaTree
from model.local_disk_tree import LocalDiskTree
from model.node_identifier import LocalFsIdentifier, NodeIdentifier
from stopwatch_sec import Stopwatch
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

        self._lock = threading.Lock()

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

        # Every unique path must map to one unique UID
        self._full_path_uid_dict: Dict[str, UID] = {ROOT_PATH: ROOT_UID}

        # Each item inserted here will have an entry created for its dir.
        # self.parent_path_dict = ParentPathBeforeFileNameDict()
        # But we still need a dir tree to look up child dirs:
        self.dir_tree = LocalDiskTree(self.application)
        root_node = RootTypeNode(node_identifier=LocalFsIdentifier(full_path=ROOT_PATH, uid=ROOT_UID))
        self.dir_tree.add_node(node=root_node, parent=None)

    def get_uid_for_path(self, path: str, uid_suggestion: Optional[UID] = None) -> UID:
        with self._lock:
            return self._get_uid_for_path(path, uid_suggestion)

    def _get_uid_for_path(self, path: str, uid_suggestion: Optional[UID] = None) -> UID:
        assert path and path.startswith('/')
        if path is not ROOT_PATH and path.endswith('/'):
            # directories ending in '/' are logically equivalent and should be treated as such
            path = path[:-1]
        uid = self._full_path_uid_dict.get(path, None)
        if not uid:
            if uid_suggestion:
                self._full_path_uid_dict[path] = uid_suggestion
            else:
                uid = self.application.uid_generator.get_new_uid()
            self._full_path_uid_dict[path] = uid
        return uid

    def get_children(self, parent_identifier: NodeIdentifier):
        if isinstance(parent_identifier, NodeIdentifier):
            parent_identifier = parent_identifier.uid
        return self.dir_tree.children(parent_identifier)

    def get_item(self, uid: UID) -> DisplayNode:
        return self.dir_tree.get_node(uid)

    def get_parent_for_item(self, item: DisplayNode, required_subtree_path: str = None):
        try:
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

    def add_or_update_fmeta(self, item: FMeta, fire_listeners=True):
        existing = None
        with self._lock:
            uid = self._get_uid_for_path(item.full_path, item.uid)
            # logger.debug(f'ID: {uid}, path: {item.full_path}')
            assert (not item.uid) or (uid == item.uid)
            item.uid = uid
            if self.use_md5 and item.md5:
                self.md5_dict.put(item, existing)
            if self.use_sha256 and item.sha256:
                self.sha256_dict.put(item, existing)

            self.dir_tree.add_to_tree(item)

        if fire_listeners:
            if existing is not None:
                signal = actions.NODE_UPDATED
            else:
                signal = actions.NODE_ADDED
            dispatcher.send(signal=signal, sender=ID_GLOBAL_CACHE, node=item)

    def remove_fmeta(self, item: FMeta, to_trash=False, fire_listeners=True):
        with self._lock:
            assert item.uid == self._full_path_uid_dict.get(item.full_path, None), f'For item: {item}'

            if self.use_md5 and item.md5:
                self.md5_dict.remove(item.md5, item.full_path)
            if self.use_sha256 and item.sha256:
                self.sha256_dict.remove(item.sha256, item.full_path)

            if self.dir_tree.get_node(item.uid):
                count_removed = self.dir_tree.remove_node(item.uid)
                assert count_removed <= 1, f'Deleted {count_removed} nodes at {item.full_path}'

        if fire_listeners:
            dispatcher.send(signal=actions.NODE_REMOVED, sender=ID_GLOBAL_CACHE, node=item)

    # Loading stuff
    # ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼

    def get_subtree_from_memory_only(self, subtree_path: LocalFsIdentifier) -> FMetaTree:
        return FMetaTree(root_identifier=subtree_path, application=self.application)

    # Load/save on-disk cache:

    def _load_subtree_from_disk(self, cache_info: PersistedCacheInfo, tree_id) -> Optional[LocalDiskTree]:
        """Loads the given subtree disk cache from disk."""

        stopwatch_load = Stopwatch()

        # Load cache from file, and update with any local FS changes found:
        with FMetaDatabase(cache_info.cache_location, self.application) as fmeta_disk_cache:
            if not fmeta_disk_cache.has_local_files():
                logger.debug(f'No meta found in cache ({cache_info.cache_location}) - will skip loading it')
                return None

            status = f'[{tree_id}] Loading meta for "{cache_info.subtree_root}" from cache: "{cache_info.cache_location}"'
            logger.debug(status)
            dispatcher.send(actions.SET_PROGRESS_TEXT, sender=tree_id, msg=status)

            uid = self.get_uid_for_path(cache_info.subtree_root.full_path, cache_info.subtree_root.uid)

            root_node_identifer = LocalFsIdentifier(full_path=cache_info.subtree_root.full_path, uid=uid)
            tree: LocalDiskTree = LocalDiskTree(self.application)
            root_node = DirNode(node_identifier=root_node_identifer)
            tree.add_node(node=root_node, parent=None)

            db_file_changes: List[FMeta] = fmeta_disk_cache.get_local_files()
            if len(db_file_changes) == 0:
                logger.debug('No data found in disk cache')

            count_from_disk = 0
            for change in db_file_changes:
                existing = tree.get_node(change.identifier)
                # Overwrite older changes for the same path:
                if not existing:
                    tree.add_to_tree(change)
                    count_from_disk += 1
                elif existing[0].sync_ts < change.sync_ts:
                    tree.remove_node(change.identifier)
                    tree.add_to_tree(change)

            # logger.debug(f'Reduced {str(len(db_file_changes))} disk cache entries into {str(count_from_disk)} unique entries')
            logger.debug(f'{stopwatch_load} [{tree_id}] Finished loading {count_from_disk} items')

            cache_info.is_loaded = True
            return tree

    def _save_subtree_to_disk(self, cache_info: PersistedCacheInfo, tree_id):
        assert isinstance(cache_info.subtree_root, LocalFsIdentifier)
        to_insert: List[FMeta] = self.dir_tree.get_all_files_for_subtree(cache_info.subtree_root)

        stopwatch_write_cache = Stopwatch()
        with FMetaDatabase(cache_info.cache_location, self.application) as fmeta_disk_cache:
            # Update cache:
            fmeta_disk_cache.insert_local_files(to_insert, overwrite=True)

        cache_info.needs_save = False
        logger.info(f'[{tree_id}] {stopwatch_write_cache} Wrote {str(len(to_insert))} FMetas to "{cache_info.cache_location}"')

    def load_subtree(self, cache_info: PersistedCacheInfo, tree_id, requested_subtree_root: LocalFsIdentifier = None) -> FMetaTree:
        """requested_subtree_root, if present, is a subset of the cache_info's subtree and it will be used. Otherwise cache_info's will be used"""
        assert cache_info
        stopwatch_total = Stopwatch()

        # Update UID, assuming this is a new run and it has gone stale
        uid = self.get_uid_for_path(cache_info.subtree_root.full_path, cache_info.subtree_root.uid)
        cache_info.subtree_root.uid = uid

        assert isinstance(cache_info.subtree_root, LocalFsIdentifier)
        if not requested_subtree_root:
            requested_subtree_root = cache_info.subtree_root
        else:
            # Update UID, assuming this is a new run and it has gone stale
            uid = self.get_uid_for_path(requested_subtree_root.full_path, requested_subtree_root.uid)
            requested_subtree_root.uid = uid

        # LOAD
        if not cache_info.is_loaded:
            if self.application.cache_manager.enable_load_from_disk:
                tree = self._load_subtree_from_disk(cache_info, tree_id)
                if tree:
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

        fmeta_tree = FMetaTree(root_identifier=requested_subtree_root, application=self.application)
        logger.info(f'[{tree_id}] {stopwatch_total} Load complete. Returning subtree for {fmeta_tree.node_identifier.full_path}')
        return fmeta_tree

    def _resync_with_file_system(self, subtree_root: LocalFsIdentifier, tree_id: str):
        # Scan directory tree and update where needed.
        logger.debug(f'[{tree_id}] Scanning filesystem subtree: {subtree_root}')
        scanner = FMetaDiskScanner(application=self.application, root_node_identifer=subtree_root, tree_id=tree_id)
        fresh_tree: treelib.Tree = scanner.scan()

        self.dir_tree.replace_subtree(sub_tree=fresh_tree)

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
                self.dir_tree.replace_subtree(super_tree)

                # this will resync with file system and/or save if configured
                supertree_cache.needs_save = True
                self.load_subtree(supertree_cache, tree_id)
                # Now it is safe to delete the subtree cache:
                file_util.delete_file(subtree_cache.cache_location)

        registry_needs_update = len(supertree_sets) > 0
        return local_caches, registry_needs_update

    def build_fmeta(self, full_path: str, category=Category.NA, staging_path=None) -> Optional[FMeta]:
        uid = self.get_uid_for_path(full_path)

        if category == Category.Ignored:
            # Do not scan ignored files for content (optimization)
            md5 = None
            sha256 = None
        else:
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
                    # can this actually happen?
                    logger.error(f'File not found; skipping: {full_path}')
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

        return FMeta(uid, md5, sha256, size_bytes, sync_ts, modify_ts, change_ts, full_path, category)
