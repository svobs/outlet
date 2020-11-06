import logging
import threading
from typing import Dict, List, Optional

from pydispatch import dispatcher

from constants import TREE_TYPE_LOCAL_DISK
from model.cache_info import PersistedCacheInfo
from model.local_disk_tree import LocalDiskTree
from model.node.local_disk_node import LocalDirNode, LocalFileNode, LocalNode
from model.node_identifier import LocalNodeIdentifier
from model.uid import UID
from store.local.master_local_write_op import LocalDiskSingleNodeOp, LocalDiskSubtreeOp
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

    def _get_or_open_db(self, cache_info: PersistedCacheInfo) -> LocalDiskDatabase:
        db = self._open_db_dict.get(cache_info.cache_location, None)
        if not db:
            db = LocalDiskDatabase(cache_info.cache_location, self.app)
            self._open_db_dict[cache_info.cache_location] = db
        return db

    def execute_op(self, operation):
        with self._struct_lock:
            if operation.is_subtree_op():
                assert isinstance(operation, LocalDiskSubtreeOp)
                self._update_diskstore_for_subtree(operation)
            else:
                assert isinstance(operation, LocalDiskSingleNodeOp)
                self._update_diskstore_for_single_op(operation)

    def _update_diskstore_for_subtree(self, op: LocalDiskSubtreeOp):
        """Attempt to come close to a transactional behavior by writing to all caches at once, and then committing all at the end"""
        cache_man = self.app.cacheman
        if not cache_man.enable_save_to_disk:
            logger.debug(f'Save to disk is disabled: skipping save of {len(op.get_subtree_list())} subtrees')
            return

        cache_dict: Dict[str, LocalDiskDatabase] = {}

        for subtree in op.get_subtree_list():
            assert subtree.subtree_root.tree_type == TREE_TYPE_LOCAL_DISK
            cache_info: Optional[PersistedCacheInfo] = cache_man.find_existing_cache_info_for_local_subtree(subtree.subtree_root.get_single_path())
            if not cache_info:
                raise RuntimeError(f'Could not find a cache associated with file path: {subtree.subtree_root.get_single_path()}')

            cache = self._get_or_open_db(cache_info)
            cache_dict[cache_info.cache_location] = cache
            op.update_diskstore(cache, subtree)

        for cache in cache_dict.values():
            cache.commit()

    def _update_diskstore_for_single_op(self, operation: LocalDiskSingleNodeOp):
        assert operation.node, f'No node for operation: {type(operation)}'
        cache_info: Optional[PersistedCacheInfo] = self.app.cacheman.find_existing_cache_info_for_local_subtree(operation.node.get_single_path())
        if not cache_info:
            raise RuntimeError(f'Could not find a cache associated with node: {operation.node.node_identifier}')

        cache = self._get_or_open_db(cache_info)
        operation.update_diskstore(cache)
        cache.commit()

    def load_subtree(self, cache_info: PersistedCacheInfo, tree_id) -> Optional[LocalDiskTree]:
        """Loads the given subtree disk cache from disk."""
        with self._struct_lock:
            db: LocalDiskDatabase = self._get_or_open_db(cache_info)

            stopwatch_load = Stopwatch()

            if not db.has_local_files() and not db.has_local_dirs():
                logger.debug(f'No meta found in cache ({cache_info.cache_location}) - will skip loading it')
                return None

            status = f'[{tree_id}] Loading meta for "{cache_info.subtree_root}" from cache: "{cache_info.cache_location}"'
            logger.debug(status)
            dispatcher.send(actions.SET_PROGRESS_TEXT, sender=tree_id, msg=status)

            uid: UID = self.app.cacheman.get_uid_for_path(cache_info.subtree_root.get_single_path(), cache_info.subtree_root.uid)
            if cache_info.subtree_root.uid != uid:
                logger.warning(f'Requested UID "{cache_info.subtree_root.uid}" is invalid for given path; changing it to "{uid}"')
            cache_info.subtree_root.uid = uid

            root_node_identifer = LocalNodeIdentifier(uid=uid, path_list=cache_info.subtree_root.get_path_list())
            tree: LocalDiskTree = LocalDiskTree(self.app)
            root_node = LocalDirNode(node_identifier=root_node_identifer, is_live=True)
            tree.add_node(node=root_node, parent=None)

            missing_nodes: List[LocalNode] = []

            # Dirs first
            dir_list: List[LocalDirNode] = db.get_local_dirs()
            if len(dir_list) == 0:
                logger.debug('No dirs found in disk cache')

            for dir_node in dir_list:
                if dir_node.uid != root_node_identifer.uid:
                    if dir_node.is_live():
                        tree.add_to_tree(dir_node)
                    else:
                        missing_nodes.append(dir_node)

            # Files next
            file_list: List[LocalFileNode] = db.get_local_files()
            if len(file_list) == 0:
                logger.debug('No files found in disk cache')

            for file_node in file_list:
                if file_node.is_live():
                    tree.add_to_tree(file_node)
                else:
                    missing_nodes.append(file_node)

            # logger.debug(f'Reduced {str(len(db_file_changes))} disk cache entries into {str(count_from_disk)} unique entries')
            logger.debug(f'{stopwatch_load} [{tree_id}] Finished loading {len(file_list)} files and {len(dir_list)} dirs from disk')

            if len(missing_nodes) > 0:
                # TODO: add code for adjudicator
                logger.warning(f'Found {len(missing_nodes)} cached nodes with is_live=false: submitting to adjudicator...TODO: build adjudicator')
                for node_index, node in enumerate(missing_nodes):
                    logger.info(f'Nonexistant node #{node_index}: {node}')

            cache_info.is_loaded = True
            return tree

    def save_subtree(self, cache_info: PersistedCacheInfo, file_list, dir_list, tree_id):
        assert isinstance(cache_info.subtree_root, LocalNodeIdentifier)
        with self._struct_lock:
            sw = Stopwatch()

            db: LocalDiskDatabase = self._get_or_open_db(cache_info)

            # Overwrite cache:
            db.truncate_local_files(commit=False)
            db.truncate_local_dirs(commit=False)
            db.insert_local_files(file_list, overwrite=False, commit=False)
            db.insert_local_dirs(dir_list, overwrite=False, commit=True)

            cache_info.needs_save = False

            logger.info(f'[{tree_id}] {sw} Wrote {len(file_list)} files and {len(dir_list)} dirs to "{cache_info.cache_location}"')
