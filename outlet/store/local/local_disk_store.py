import logging
import threading
from typing import Dict, List, Optional

from pydispatch import dispatcher

from model.cache_info import PersistedCacheInfo
from model.local_disk_tree import LocalDiskTree
from model.node.local_disk_node import LocalDirNode, LocalFileNode, LocalNode
from model.node_identifier import LocalFsIdentifier
from model.uid import UID
from store.sqlite.local_db import LocalDiskDatabase
from ui import actions
from util.has_lifecycle import HasLifecycle
from util.stopwatch_sec import Stopwatch

logger = logging.getLogger(__name__)


# CLASS LocalDiskDiskStore
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼

class LocalDiskDiskStore(HasLifecycle):
    """Wrapper for OpDatabase; adds lifecycle and possibly complex logic"""
    def __init__(self, app):
        HasLifecycle.__init__(self)
        self.app = app
        self._struct_lock = threading.Lock()
        """Just use one big lock for now"""
        self._open_db_dict: Dict[str, LocalDiskDatabase] = {}
        """Dict of [cache_location -> LocalDiskDatabase] containing open connetions"""

    def start(self):
        HasLifecycle.start(self)

    def shutdown(self):
        HasLifecycle.shutdown(self)
        if self._open_db_dict:
            open_db_dict = self._open_db_dict
            self._open_db_dict = None

            for cache_location, db in open_db_dict.items():
                try:
                    db.close()
                except RuntimeError:
                    logger.exception(f'Failed to close database "{cache_location}"')

    def get_or_open_db(self, cache_info: PersistedCacheInfo) -> LocalDiskDatabase:
        with self._struct_lock:
            db = self._open_db_dict.get(cache_info.cache_location, None)
            if not db:
                db = LocalDiskDatabase(cache_info.cache_location, self.app)
                self._open_db_dict[cache_info.cache_location] = db
            return db

    def load_subtree(self, cache_info: PersistedCacheInfo, tree_id) -> Optional[LocalDiskTree]:
        """Loads the given subtree disk cache from disk."""
        disk_cache: LocalDiskDatabase = self.get_or_open_db(cache_info)

        with self._struct_lock:
            stopwatch_load = Stopwatch()

            if not disk_cache.has_local_files() and not disk_cache.has_local_dirs():
                logger.debug(f'No meta found in cache ({cache_info.cache_location}) - will skip loading it')
                return None

            status = f'[{tree_id}] Loading meta for "{cache_info.subtree_root}" from cache: "{cache_info.cache_location}"'
            logger.debug(status)
            dispatcher.send(actions.SET_PROGRESS_TEXT, sender=tree_id, msg=status)

            uid: UID = self.app.cacheman.get_uid_for_path(cache_info.subtree_root.full_path, cache_info.subtree_root.uid)
            if cache_info.subtree_root.uid != uid:
                logger.warning(f'Requested UID "{cache_info.subtree_root.uid}" is invalid for given path; changing it to "{uid}"')
            cache_info.subtree_root.uid = uid

            root_node_identifer = LocalFsIdentifier(full_path=cache_info.subtree_root.full_path, uid=uid)
            tree: LocalDiskTree = LocalDiskTree(self.app)
            root_node = LocalDirNode(node_identifier=root_node_identifer, exists=True)
            tree.add_node(node=root_node, parent=None)

            missing_nodes: List[LocalNode] = []

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

    def save_subtree(self, cache_info: PersistedCacheInfo, file_list, dir_list, tree_id):
        assert isinstance(cache_info.subtree_root, LocalFsIdentifier)
        disk_cache: LocalDiskDatabase = self.get_or_open_db(cache_info)

        with self._struct_lock:
            sw = Stopwatch()

            # Overwrite cache:
            disk_cache.truncate_local_files(commit=False)
            disk_cache.truncate_local_dirs(commit=False)
            disk_cache.insert_local_files(file_list, overwrite=False, commit=False)
            disk_cache.insert_local_dirs(dir_list, overwrite=False, commit=True)

            cache_info.needs_save = False

            logger.info(f'[{tree_id}] {sw} Wrote {len(file_list)} files and {len(dir_list)} dirs to "{cache_info.cache_location}"')
