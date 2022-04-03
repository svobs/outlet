import logging
import threading
from collections import deque
from typing import Deque, Dict, List, Optional

from pydispatch import dispatcher

from backend.sqlite.local_db import LocalDiskDatabase
from backend.tree_store.local.locald_tree import LocalDiskTree
from backend.tree_store.local.op_write import LocalDiskMultiNodeOp, LocalDiskSingleNodeOp
from constants import TreeType
from logging_constants import TRACE_ENABLED
from model.cache_info import PersistedCacheInfo
from model.node.local_disk_node import LocalDirNode, LocalFileNode, LocalNode
from model.node.node import SPIDNodePair
from model.node_identifier import LocalNodeIdentifier, SinglePathNodeIdentifier
from model.uid import UID
from signal_constants import Signal
from util.has_lifecycle import HasLifecycle
from util.stopwatch_sec import Stopwatch

logger = logging.getLogger(__name__)


class LocalDiskDiskStore(HasLifecycle):
    """
    ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
    CLASS LocalDiskDiskStore

    Wrapper for set of LocalDiskDatabases, each of which covers a different tree but all corresponding to the same device; adds lifecycle and
    possibly complex logic.

    An instance of this class should be encapsulated within a LocalDiskMasterStore for the same device.
    ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼
    """
    def __init__(self, backend, device_uid: UID):
        HasLifecycle.__init__(self)
        self.backend = backend
        self.device_uid: UID = device_uid
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

    # NOTE: This should be the ONLY place LocalDiskDatabase is instantiated!
    def _get_or_open_db(self, cache_info: PersistedCacheInfo) -> LocalDiskDatabase:
        db = self._open_db_dict.get(cache_info.cache_location, None)
        if not db:
            db = LocalDiskDatabase(cache_info.cache_location, self.backend, self.device_uid)
            self._open_db_dict[cache_info.cache_location] = db
        return db

    def execute_op(self, operation):
        with self._struct_lock:
            if operation.is_single_node_op():
                assert isinstance(operation, LocalDiskSingleNodeOp)
                self._update_diskstore_for_single_op(operation)
            else:
                assert isinstance(operation, LocalDiskMultiNodeOp)
                self._update_diskstore_for_subtree(operation)

    def _update_diskstore_for_subtree(self, op: LocalDiskMultiNodeOp):
        """Attempt to come close to a transactional behavior by writing to all caches at once, and then committing all at the end"""

        cache_dict: Dict[str, LocalDiskDatabase] = {}

        for subtree in op.get_subtree_list():
            assert subtree.subtree_root.tree_type == TreeType.LOCAL_DISK and subtree.subtree_root.device_uid == self.device_uid
            cache_info: Optional[PersistedCacheInfo] = self.backend.cacheman.get_cache_info_for_subtree(subtree.subtree_root)
            if not cache_info:
                raise RuntimeError(f'Could not find a cache associated with file path: {subtree.subtree_root.get_single_path()}')

            cache = self._get_or_open_db(cache_info)
            cache_dict[cache_info.cache_location] = cache
            op.update_diskstore(cache, subtree)

        for cache in cache_dict.values():
            cache.commit()

    def _update_diskstore_for_single_op(self, operation: LocalDiskSingleNodeOp):
        assert operation.node, f'No node for operation: {type(operation)}'
        cache_info: Optional[PersistedCacheInfo] = self.backend.cacheman.get_cache_info_for_subtree(operation.node.node_identifier)
        if not cache_info:
            raise RuntimeError(f'Could not find a cache associated with node: {operation.node.node_identifier}')

        db = self._get_or_open_db(cache_info)
        operation.update_diskstore(db)
        db.commit()

    def _ensure_uid_consistency(self, subtree_root: SinglePathNodeIdentifier):
        """Since the UID of the subtree root node is stored in 3 different locations (registry, cache file, and memory),
        checks that at least registry & memory match. If UID is not in memory, guarantees that it will be stored with the value from registry.
        This method should only be called for the subtree root of display trees being loaded"""
        existing_uid = subtree_root.node_uid
        new_uid = self.backend.cacheman.get_uid_for_local_path(subtree_root.get_single_path(), existing_uid)
        if existing_uid != new_uid:
            logger.warning(f'Requested UID "{existing_uid}" is invalid for given path; changing it to "{new_uid}"')
        subtree_root.uid = new_uid

    def load_subtree(self, cache_info: PersistedCacheInfo, tree_id) -> Optional[LocalDiskTree]:
        """Loads the given subtree disk cache from disk."""
        with self._struct_lock:
            db: LocalDiskDatabase = self._get_or_open_db(cache_info)

            stopwatch_load = Stopwatch()

            if not db.has_local_files() and not db.has_local_dirs():
                logger.debug(f'No meta found in cache ({cache_info.cache_location}) - will skip loading it')
                return None

            status = f'[{tree_id}] Loading meta for subtree {cache_info.subtree_root} from cache: "{cache_info.cache_location}"'
            logger.debug(status)
            dispatcher.send(Signal.SET_PROGRESS_TEXT, sender=tree_id, msg=status)

            self._ensure_uid_consistency(cache_info.subtree_root)

            tree: LocalDiskTree = LocalDiskTree(self.backend)
            root_node = self.backend.cacheman.build_local_dir_node(full_path=cache_info.subtree_root.get_single_path(),
                                                                   is_live=True, all_children_fetched=True)
            tree.add_node(node=root_node, parent=None)

            missing_nodes: List[LocalNode] = []

            # Dirs first
            dir_list: List[LocalDirNode] = db.get_local_dirs()
            if len(dir_list) == 0:
                logger.debug('No dirs found in disk cache')
            for dir_node in dir_list:
                if dir_node.is_live():
                    tree.add_to_tree(dir_node)
                else:
                    missing_nodes.append(dir_node)

            # Files next
            file_list: List[LocalFileNode] = db.get_local_files()
            if len(file_list) == 0:
                logger.debug('No files found in disk cache')

            for file_node in file_list:
                if not file_node.content_meta_uid:
                    if TRACE_ENABLED:
                        logger.debug(f'load_subtree(): loaded node is missing signature: {file_node}')
                if file_node.is_live():
                    tree.add_to_tree(file_node)
                else:
                    missing_nodes.append(file_node)

            # logger.debug(f'Reduced {str(len(db_file_changes))} disk cache entries into {str(count_from_disk)} unique entries')
            logger.debug(f'{stopwatch_load} [{tree_id}] Loaded {len(file_list)} files and {len(dir_list)} dirs from disk')

            if len(missing_nodes) > 0:
                # TODO: add code for adjudicator
                logger.warning(f'Found {len(missing_nodes)} cached nodes with is_live=false: submitting to adjudicator...TODO: build adjudicator')
                for node_index, node in enumerate(missing_nodes):
                    logger.info(f'Nonexistant node #{node_index}: {node}')

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

    def get_file_or_dir_for_uid(self, cache_info: PersistedCacheInfo, node_uid: UID) -> Optional[LocalNode]:
        with self._struct_lock:
            db: LocalDiskDatabase = self._get_or_open_db(cache_info)
            return db.get_file_or_dir_for_uid(node_uid)

    def get_all_files_with_content(self, cache_info: PersistedCacheInfo, content_uid: UID) -> List[LocalFileNode]:
        with self._struct_lock:
            db: LocalDiskDatabase = self._get_or_open_db(cache_info)
            return db.get_all_files_with_content(content_uid)

    def get_subtree_bfs_from_cache(self, cache_info, node_uid: UID):
        assert isinstance(cache_info.subtree_root, LocalNodeIdentifier)
        with self._struct_lock:
            db: LocalDiskDatabase = self._get_or_open_db(cache_info)
            subtree_node_list: List[LocalNode] = []
            dir_queue: Deque[LocalDirNode] = deque()

            # Compare logic with _get_child_list_from_cache_for_spid():
            parent_dir_node = db.get_file_or_dir_for_uid(node_uid)
            subtree_node_list.append(parent_dir_node)
            if parent_dir_node.is_dir():  # note: we don't care if all_children_fetched: we want to collect any children which were fetched
                assert isinstance(parent_dir_node, LocalDirNode)
                dir_queue.append(parent_dir_node)

            while len(dir_queue) > 0:
                parent_dir_node = dir_queue.popleft()
                child_node_list = db.get_child_list_for_node_uid(parent_dir_node.uid)
                subtree_node_list += child_node_list

                for child_node in child_node_list:
                    if child_node.is_dir():
                        assert isinstance(child_node, LocalDirNode)
                        dir_queue.append(child_node)

            return subtree_node_list

    def get_child_list_for_node_uid(self, cache_info, node_uid: UID, only_if_all_children_fetched: bool) -> Optional[List[SPIDNodePair]]:
        with self._struct_lock:
            db: LocalDiskDatabase = self._get_or_open_db(cache_info)
            parent_node = db.get_file_or_dir_for_uid(node_uid)
            if parent_node:
                if not parent_node.is_dir():
                    logger.debug(f'_get_child_list_from_cache_for_spid(): Found parent in diskstore but it is not a dir: {parent_node}')
                    return None
                elif not only_if_all_children_fetched or parent_node.all_children_fetched:
                    return [self._to_sn(x) for x in db.get_child_list_for_node_uid(node_uid)]
                else:
                    logger.debug(f'_get_child_list_from_cache_for_spid(): Disk cache ({cache_info.cache_location}) contains parent but '
                                 f'not all children fetched: {parent_node}')

    @staticmethod
    def _to_sn(node: LocalNode):
        return SPIDNodePair(node.node_identifier, node)
