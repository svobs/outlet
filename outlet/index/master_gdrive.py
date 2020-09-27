import logging
import threading
from abc import ABC, abstractmethod
from collections import defaultdict
from typing import DefaultDict, Dict, List, Optional, Tuple

from pydispatch import dispatcher

from constants import GDRIVE_FOLDER_MIME_TYPE_UID, GDRIVE_ME_USER_UID, NOT_TRASHED, ROOT_PATH, GDRIVE_ROOT_UID, TREE_TYPE_GDRIVE
from gdrive.change_observer import GDriveChange, GDriveNodeChange
from gdrive.gdrive_tree_loader import GDriveTreeLoader
from index.error import CacheNotLoadedError, GDriveItemNotFoundError
from index.sqlite.gdrive_db import GDriveDatabase
from index.two_level_dict import FullPathBeforeUidDict, Md5BeforeUidDict
from index.uid.uid import UID
from index.uid.uid_mapper import UidGoogIdMapper
from model.gdrive_meta import GDriveUser, MimeType
from model.node.display_node import DisplayNode
from model.display_tree.gdrive import GDriveDisplayTree
from model.gdrive_whole_tree import GDriveWholeTree
from model.node.gdrive_node import GDriveFile, GDriveFolder, GDriveNode
from model.node_identifier import GDriveIdentifier, NodeIdentifier
from model.node_identifier_factory import NodeIdentifierFactory
from util.stopwatch_sec import Stopwatch
from ui import actions
from ui.actions import ID_GLOBAL_CACHE

logger = logging.getLogger(__name__)

# TODO: lots of work to do to support drag & drop from GDrive to GDrive (e.g. "move" is really just changing parents)
# - support Move
# - support Delete Subtree


# ABSTRACT CLASS BatchOperation
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
class BatchOperation(ABC):
    @abstractmethod
    def update_memory_cache(self, my_gdrive: GDriveWholeTree):
        pass

    @abstractmethod
    def update_disk_cache(self, cache: GDriveDatabase):
        pass

    @abstractmethod
    def send_signals(self):
        pass


# CLASS DeleteSingleNodeOp
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
class DeleteSingleNodeOp(BatchOperation):
    def __init__(self, node: GDriveNode, to_trash: bool = False):
        self.node = node
        self.to_trash: bool = to_trash

    def update_memory_cache(self, my_gdrive: GDriveWholeTree):
        existing_node = my_gdrive.get_node_for_uid(self.node.uid)
        if existing_node:
            my_gdrive.remove_node(existing_node)

    def update_disk_cache(self, cache: GDriveDatabase):
        cache.delete_single_node(self.node, commit=False)

    def send_signals(self):
        dispatcher.send(signal=actions.NODE_REMOVED, sender=ID_GLOBAL_CACHE, node=self.node)


# CLASS DeleteSubtreeOp
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
class DeleteSubtreeOp(BatchOperation):
    def __init__(self, subtree_root_node: GDriveNode, node_list: List[GDriveNode]):
        self.subtree_root_node = subtree_root_node
        """If true, is a delete operation. If false, is upsert op."""
        self.node_list: List[GDriveNode] = node_list

    def update_memory_cache(self, my_gdrive: GDriveWholeTree):
        logger.debug(f'DeleteSubtreeOp: removing {len(self.node_list)} nodes from memory cache')
        for node in reversed(self.node_list):
            existing_node = my_gdrive.get_node_for_uid(node.uid)
            if existing_node:
                my_gdrive.remove_node(existing_node)
        logger.debug(f'DeleteSubtreeOp: done removing nodes from memory cache')

    def update_disk_cache(self, cache: GDriveDatabase):
        logger.debug(f'DeleteSubtreeOp: removing {len(self.node_list)} nodes from disk cache')
        for node in self.node_list:
            cache.delete_single_node(node, commit=False)
        logger.debug(f'DeleteSubtreeOp: done removing nodes from disk cache')

    def send_signals(self):
        logger.debug(f'DeleteSubtreeOp: sending "{actions.NODE_REMOVED}" signal for {len(self.node_list)} nodes')
        for node in self.node_list:
            dispatcher.send(signal=actions.NODE_REMOVED, sender=ID_GLOBAL_CACHE, node=node)


def _reduce_changes(change_list: List[GDriveChange]) -> List[GDriveChange]:
    change_list_by_goog_id: DefaultDict[str, List[GDriveChange]] = defaultdict(lambda: list())
    for change in change_list:
        assert change.goog_id, f'No goog_id for change: {change}'
        change_list_by_goog_id[change.goog_id].append(change)

    reduced_changes: List[GDriveChange] = []
    for single_goog_id_change_list in change_list_by_goog_id.values():
        last_change = single_goog_id_change_list[-1]
        if last_change.node:
            reduced_changes.append(last_change)
        else:
            # skip this node
            logger.debug(f'No node found in cache for removed goog_id: "{last_change.goog_id}"')

    logger.debug(f'Reduced {len(change_list)} changes into {len(reduced_changes)} changes')
    return reduced_changes


# CLASS BatchChangesOp
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
class BatchChangesOp(BatchOperation):
    def __init__(self, gdrive_master_cache, change_list: List[GDriveChange]):
        self.gdrive_master_cache = gdrive_master_cache
        self.change_list = _reduce_changes(change_list)

    def update_memory_cache(self, my_gdrive: GDriveWholeTree):
        for change in self.change_list:
            if change.is_removed():
                removed_node = my_gdrive.remove_node(change.node)
                if removed_node:
                    change.node = removed_node
            else:
                assert isinstance(change, GDriveNodeChange)
                # need to use existing object if available to fulfill our contract (node will be sent via signals below)
                change.node = my_gdrive.add_node(change.node)

                # ensure full_path is populated
                my_gdrive.get_full_path_for_node(change.node)

    def update_disk_cache(self, cache: GDriveDatabase):
        mappings_list_list: List[List[Tuple]] = []
        file_uid_to_delete_list: List[UID] = []
        folder_uid_to_delete_list: List[UID] = []
        files_to_upsert: List[GDriveFile] = []
        folders_to_upsert: List[GDriveFolder] = []

        for change in self.change_list:
            if change.is_removed():
                if change.node.is_dir():
                    folder_uid_to_delete_list.append(change.node.uid)
                else:
                    file_uid_to_delete_list.append(change.node.uid)
            else:
                parent_mapping_list = []
                parent_uids = change.node.get_parent_uids()
                if parent_uids:
                    parent_goog_ids = self.gdrive_master_cache.resolve_uids_to_goog_ids(parent_uids)
                    if len(change.node.get_parent_uids()) != len(parent_goog_ids):
                        raise RuntimeError(f'Internal error: could not map all parent goog_ids ({len(parent_goog_ids)}) to parent UIDs '
                                           f'({len(parent_uids)}) for node: {change.node}')
                    for parent_uid, parent_goog_id in zip(change.node.get_parent_uids(), parent_goog_ids):
                        parent_mapping_list.append((change.node.uid, parent_uid, parent_goog_id, change.node.sync_ts))
                mappings_list_list.append(parent_mapping_list)

                if change.node.is_dir():
                    assert isinstance(change.node, GDriveFolder)
                    folders_to_upsert.append(change.node)
                else:
                    assert isinstance(change.node, GDriveFile)
                    files_to_upsert.append(change.node)

        if mappings_list_list:
            logger.debug(f'Upserting id-parent mappings for {len(mappings_list_list)} nodes to the GDrive master cache')
            cache.upsert_parent_mappings(mappings_list_list, commit=False)

        if len(file_uid_to_delete_list) + len(folder_uid_to_delete_list) > 0:
            logger.debug(f'Removing {len(file_uid_to_delete_list)} files and {len(folder_uid_to_delete_list)} folders from the GDrive master cache')
            cache.delete_nodes(file_uid_to_delete_list, folder_uid_to_delete_list, commit=False)

        if len(folders_to_upsert) > 0:
            logger.debug(f'Upserting {len(folders_to_upsert)} folders to the GDrive master cache')
            cache.upsert_gdrive_folder_list(folders_to_upsert, commit=False)

        if len(files_to_upsert) > 0:
            logger.debug(f'Upserting {len(files_to_upsert)} files to the GDrive master cache')
            cache.upsert_gdrive_file_list(files_to_upsert, commit=False)

    def send_signals(self):
        for change in self.change_list:
            if change.is_removed():
                dispatcher.send(signal=actions.NODE_REMOVED, sender=ID_GLOBAL_CACHE, node=change.node)
            else:
                dispatcher.send(signal=actions.NODE_UPSERTED, sender=ID_GLOBAL_CACHE, node=change.node)


"""
# CLASS GDriveMasterCache
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼

Some notes:
- Need to create a store which can keep track of whether each parent has all children. If not we
will have to make a request to retrieve all nodes with 'X' as parent and update the store before
returning

- GoogRemote >= GoogDiskStores >= GoogInMemoryStore >= DisplayStore

- GoogDiskCache should try to download all dirs & files ASAP. But in the meantime, download level by level

- Every time you expand a node, you should call to sync it from the GoogStore.
- Every time you retrieve new data from G, you must perform sanity checks on it
"""


class GDriveMasterCache:
    """Singleton in-memory cache for Google Drive"""
    def __init__(self, application):
        self.application = application
        self.full_path_dict = FullPathBeforeUidDict()
        self.md5_dict = Md5BeforeUidDict()
        self._my_gdrive: Optional[GDriveWholeTree] = None

        self._struct_lock = threading.Lock()

        self._meta_lock = threading.Lock()
        self._mime_type_uid_nextval: int = GDRIVE_FOLDER_MIME_TYPE_UID + 1
        self._mime_type_for_str_dict: Dict[str, MimeType] = {}
        self._mime_type_for_uid_dict: Dict[UID, MimeType] = {}
        self._user_uid_nextval: int = GDRIVE_ME_USER_UID + 1
        self._user_for_permission_id_dict: Dict[str, GDriveUser] = {}
        self._user_for_uid_dict: Dict[UID, GDriveUser] = {}

        self._uid_mapper = UidGoogIdMapper(application)

    def _get_gdrive_cache_path(self) -> str:
        my_gdrive_root = NodeIdentifierFactory.get_gdrive_root_constant_identifier()
        cache_info = self.application.cache_manager.get_or_create_cache_info_entry(my_gdrive_root)
        return cache_info.cache_location

    # Subtree-level stuff
    # ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼

    def load_gdrive_master_cache(self, invalidate_cache: bool, sync_latest_changes: bool, tree_id: str):
        """Loads an EXISTING GDrive cache from disk and updates the in-memory cache from it"""
        if not self.application.cache_manager.enable_load_from_disk:
            logger.debug('Skipping cache load because cache.enable_cache_load is False')
            return None

        # Always load ROOT:
        my_gdrive_root = NodeIdentifierFactory.get_gdrive_root_constant_identifier()
        cache_info = self.application.cache_manager.get_or_create_cache_info_entry(my_gdrive_root)

        stopwatch_total = Stopwatch()

        tree_loader = GDriveTreeLoader(application=self.application, cache_path=cache_info.cache_location, tree_id=tree_id)

        if not cache_info.is_loaded or invalidate_cache:
            status = f'Loading meta for "{cache_info.subtree_root.full_path}" from cache: "{cache_info.cache_location}"'
            logger.debug(status)
            dispatcher.send(actions.SET_PROGRESS_TEXT, sender=tree_id, msg=status)

            with GDriveDatabase(cache_info.cache_location, self.application) as cache:
                with self._struct_lock:
                    # Load all users
                    for user in cache.get_all_users():
                        if user.uid > self._user_uid_nextval:
                            self._user_uid_nextval = user.uid + 1
                        self._user_for_permission_id_dict[user.permission_id] = user
                        self._user_for_uid_dict[user.uid] = user
                with self._struct_lock:
                    # Load all MIME types:
                    for mime_type in cache.get_all_mime_types():
                        if mime_type.uid > self._mime_type_uid_nextval:
                            self._mime_type_uid_nextval = mime_type.uid + 1
                        self._mime_type_for_str_dict[mime_type.type_string] = mime_type
                        self._mime_type_for_uid_dict[mime_type.uid] = mime_type

            self._my_gdrive = tree_loader.load_all(invalidate_cache=invalidate_cache)

        if sync_latest_changes:
            # This may add a noticeable delay:
            tree_loader.sync_latest_changes()

        logger.info(f'{stopwatch_total} GDrive cache for {cache_info.subtree_root.full_path} loaded')
        cache_info.is_loaded = True

    def load_gdrive_subtree(self, subtree_root: GDriveIdentifier, invalidate_cache: bool, sync_latest_changes: bool, tree_id: str)\
            -> GDriveDisplayTree:
        if not subtree_root:
            subtree_root = NodeIdentifierFactory.get_gdrive_root_constant_identifier()
        logger.debug(f'[{tree_id}] Getting meta for subtree: "{subtree_root}" (invalidate_cache={invalidate_cache})')

        self.load_gdrive_master_cache(invalidate_cache, sync_latest_changes, tree_id)

        gdrive_meta = self._make_gdrive_display_tree(subtree_root)
        if gdrive_meta:
            logger.debug(f'[{tree_id}] Made display tree: {gdrive_meta}')
        else:
            raise GDriveItemNotFoundError(node_identifier=subtree_root,
                                          msg=f'Cannot load subtree because it does not exist: "{subtree_root}"',
                                          offending_path=subtree_root.full_path)
        return gdrive_meta

    def refresh_stats(self, tree_id: str, subtree_root_node: GDriveFolder):
        with self._struct_lock:
            self._my_gdrive.refresh_stats(tree_id, subtree_root_node)

    def _make_gdrive_display_tree(self, subtree_root: GDriveIdentifier) -> Optional[GDriveDisplayTree]:
        if not subtree_root.uid:
            logger.debug(f'_make_gdrive_display_tree(): subtree_root.uid is empty!')
            return None

        root: GDriveNode = self._my_gdrive.get_node_for_uid(subtree_root.uid)
        if not root:
            logger.debug(f'_make_gdrive_display_tree(): could not find root node with UID {subtree_root.uid}')
            return None

        assert isinstance(root, GDriveFolder)
        return GDriveDisplayTree(cache_manager=self, whole_tree=self._my_gdrive, root_node=root)

    # Individual node cache updates
    # ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼

    def upsert_gdrive_node(self, node: GDriveNode):
        with self._struct_lock:
            self._upsert_gdrive_node_nolock(node)

    def _upsert_gdrive_node_nolock(self, node: GDriveNode):
        logger.debug(f'Upserting node to caches: {node}')

        # try to prevent cache corruption by doing some sanity checks
        if not node:
            raise RuntimeError(f'No node supplied!')
        if not node.uid:
            raise RuntimeError(f'Node is missing UID: {node}')
        if node.node_identifier.tree_type != TREE_TYPE_GDRIVE:
            raise RuntimeError(f'Unrecognized tree type: {node.node_identifier.tree_type}')
        if not self._my_gdrive:
            raise RuntimeError('GDriveWholeTree not loaded!')
        if not isinstance(node, GDriveNode):
            raise RuntimeError(f'Unrecognized node type: {node}')

        updated_node = self._upsert_single_node_in_memory_cache(node)

        if updated_node:
            node = updated_node
            self._upsert_single_node_in_disk_cache(node)

        # Generate full_path for node, if not already done (we assume this is a newly created node)
        self._my_gdrive.get_full_path_for_node(node)

        dispatcher.send(signal=actions.NODE_UPSERTED, sender=ID_GLOBAL_CACHE, node=node)

    def _upsert_single_node_in_memory_cache(self, node: GDriveNode) -> Optional[GDriveNode]:
        """Returns a GDriveNode if update was needed and successful; None if otherwise"""
        # Detect whether it's already in the cache
        existing_node = self._my_gdrive.get_node_for_uid(node.uid)
        if existing_node:
            # it is ok if we have an existing node which doesn't have a goog_id; that will be replaced
            if existing_node.goog_id and existing_node.goog_id != node.goog_id:
                raise RuntimeError(f'Serious error: cache already contains UID {node.uid} but Google ID does not match '
                                   f'(existing="{existing_node.goog_id}"; new="{node.goog_id}")')

            if existing_node.exists() and not node.exists():
                # In the future, let's close this hole with more elegant logic
                logger.warning(f'Cannot replace a node which exists with one which does not exist; ignoring: {node}')
                return None

            if existing_node.is_dir() and not node.is_dir():
                # need to replace all descendants...not ready to do this yet
                raise RuntimeError(f'Cannot replace a folder with a file: "{node.full_path}"')

            if existing_node == node:
                logger.info(f'Node being added (uid={node.uid}) is identical to node already in the cache; skipping cache update')
                dispatcher.send(signal=actions.NODE_UPSERTED, sender=ID_GLOBAL_CACHE, node=node)
                return None
            logger.debug(f'Found existing node in cache with UID={existing_node.uid}: doing an update')
        elif node.goog_id:
            previous_uid = self.get_uid_for_goog_id(goog_id=node.goog_id)
            if previous_uid and node.uid != previous_uid:
                logger.warning(f'Found node in cache with same GoogID ({node.goog_id}) but different UID ('
                               f'{previous_uid}). Changing UID of node (was: {node.uid}) to match and overwrite previous node')
                node.uid = previous_uid

        # Finally, update in-memory cache (tree). If an existing node is found with the same UID, it will update and return that instead:
        node = self._my_gdrive.add_node(node)

        return node

    def _upsert_single_node_in_disk_cache(self, node: GDriveNode):
        if not node.exists():
            logger.debug(f'Node does not exist; skipping save to disk: {node}')
            return

        cache_man = self.application.cache_manager
        if not cache_man.enable_save_to_disk:
            logger.debug(f'Save to disk is disabled: skipping add/update of node with UID={node.uid}')
            return

        parent_uids = node.get_parent_uids()
        if not parent_uids:
            logger.debug(f'Node has no parents; assuming it is a root node: {node}')

        parent_mappings = []
        if parent_uids:
            parent_goog_ids = self._my_gdrive.resolve_uids_to_goog_ids(parent_uids)
            if len(node.get_parent_uids()) != len(parent_goog_ids):
                raise RuntimeError(f'Internal error: could not map all parent goog_ids ({len(parent_goog_ids)}) to parent UIDs '
                                   f'({len(parent_uids)}) for node: {node}')
            for parent_uid, parent_goog_id in zip(node.get_parent_uids(), parent_goog_ids):
                parent_mappings.append((node.uid, parent_uid, parent_goog_id, node.sync_ts))

        cache_path: str = self._get_cache_path_for_master()

        # Write new values:
        with GDriveDatabase(cache_path, self.application) as cache:
            if parent_mappings:
                logger.debug(f'Writing id-parent mappings to the GDrive master cache: {parent_mappings}')
                cache.upsert_parent_mappings_for_id(parent_mappings, node.uid, commit=False)

            if node.is_dir():
                logger.debug(f'Writing folder node to the GDrive master cache: {node}')
                assert isinstance(node, GDriveFolder)
                cache.upsert_gdrive_folder_list([node])
            else:
                logger.debug(f'Writing file node to the GDrive master cache: {node}')
                assert isinstance(node, GDriveFile)
                cache.upsert_gdrive_file_list([node])

    def _update_memory_cache(self, subtree_operation: BatchOperation):
        subtree_operation.update_memory_cache(self._my_gdrive)

    def _update_disk_cache(self, subtree_operation: BatchOperation):
        if not self.application.cache_manager.enable_save_to_disk:
            logger.debug(f'Save to disk is disabled: skipping disk update')
            return

        cache_path: str = self._get_cache_path_for_master()
        with GDriveDatabase(cache_path, self.application) as cache:
            subtree_operation.update_disk_cache(cache)

            cache.commit()

    def _execute(self, operation: BatchOperation):
        """Executes a signal BatchOperation."""
        self._update_memory_cache(operation)

        self._update_disk_cache(operation)

        operation.send_signals()

    def remove_gdrive_subtree(self, subtree_root: GDriveNode, to_trash):
        assert isinstance(subtree_root, GDriveNode), f'For node: {subtree_root}'

        if to_trash:
            # TODO
            raise RuntimeError(f'Not supported: to_trash=true!')

        with self._struct_lock:
            if not subtree_root.is_dir():
                logger.debug(f'Requested subtree is not a folder; calling remove_gdrive_node()')
                self._remove_gdrive_node_nolock(subtree_root, to_trash=to_trash)
                return

            subtree_nodes: List[GDriveNode] = self._my_gdrive.get_subtree_bfs(subtree_root)
            operation: DeleteSubtreeOp = DeleteSubtreeOp(subtree_root, node_list=subtree_nodes)
            logger.info(f'Removing subtree with {len(operation.node_list)} nodes')
            self._execute(operation)

    def remove_gdrive_node(self, node: GDriveNode, to_trash):
        with self._struct_lock:
            self._remove_gdrive_node_nolock(node, to_trash)

    def _remove_gdrive_node_nolock(self, node: GDriveNode, to_trash):
        logger.debug(f'Removing node from caches: {node}')
        assert isinstance(node, GDriveNode), f'For node: {node}'

        # ensure full_path is populated
        self._my_gdrive.get_full_path_for_node(node)

        if node.is_dir():
            children: List[GDriveNode] = self._my_gdrive.get_children(node)
            if children:
                raise RuntimeError(f'Cannot remove GDrive folder from cache: it contains {len(children)} children!')

        if to_trash:
            if node.trashed == NOT_TRASHED:
                raise RuntimeError(f'Trying to trash Google node which is not marked as trashed: {node}')
            # this is actually an update
            self._upsert_gdrive_node_nolock(node)
            dispatcher.send(signal=actions.NODE_REMOVED, sender=ID_GLOBAL_CACHE, node=node)
        else:
            operation: DeleteSingleNodeOp = DeleteSingleNodeOp(node, to_trash)
            self._execute(operation)

    # Various public methods
    # ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼

    def apply_gdrive_changes(self, gdrive_change_list: List[GDriveChange]):
        operation: BatchChangesOp = BatchChangesOp(self, gdrive_change_list)

        with self._struct_lock:
            self._execute(operation)

    def get_goog_ids_for_uids(self, uids: List[UID]) -> List[str]:
        return self._my_gdrive.resolve_uids_to_goog_ids(uids)

    def get_uid_list_for_goog_id_list(self, goog_ids: List[str]) -> List[UID]:
        uid_list = []
        for goog_id in goog_ids:
            uid_list.append(self._uid_mapper.get_uid_for_goog_id(goog_id))

        return uid_list

    def get_uid_for_goog_id(self, goog_id: str, uid_suggestion: Optional[UID] = None) -> UID:
        return self._uid_mapper.get_uid_for_goog_id(goog_id, uid_suggestion)

    def get_node_for_goog_id(self, goog_id: str) -> Optional[GDriveNode]:
        uid = self._uid_mapper.get_uid_for_goog_id(goog_id)
        return self._my_gdrive.get_node_for_uid(uid)

    def get_node_for_uid(self, uid: UID) -> Optional[GDriveNode]:
        if not self._my_gdrive:
            raise RuntimeError(f'Cannot retrieve node (UID={uid}(: GDrive cache not loaded!')
        return self._my_gdrive.get_node_for_uid(uid)

    def get_node_for_name_and_parent_uid(self, name: str, parent_uid: UID) -> Optional[GDriveNode]:
        with self._struct_lock:
            return self._my_gdrive.get_node_for_name_and_parent_uid(name, parent_uid)

    def get_goog_id_for_uid(self, uid: UID) -> Optional[str]:
        node = self.get_node_for_uid(uid)
        if node:
            return node.goog_id
        return None

    def resolve_uids_to_goog_ids(self, uids: List[UID]) -> List[str]:
        return self._my_gdrive.resolve_uids_to_goog_ids(uids)

    def _get_cache_path_for_master(self) -> str:
        # Open master database...
        root = NodeIdentifierFactory.get_gdrive_root_constant_identifier()
        cache_info = self.application.cache_manager.get_or_create_cache_info_entry(root)
        cache_path = cache_info.cache_location
        return cache_path

    def get_children(self, node: DisplayNode) -> List[GDriveNode]:
        assert isinstance(node, GDriveNode)
        return self._my_gdrive.get_children(node)

    def get_parent_for_node(self, node: DisplayNode, required_subtree_path: str = None):
        assert isinstance(node, GDriveNode)
        return self._my_gdrive.get_parent_for_node(node, required_subtree_path)

    def get_all_for_path(self, path: str) -> List[NodeIdentifier]:
        if not self._my_gdrive:
            raise CacheNotLoadedError()
        return self._my_gdrive.get_all_identifiers_for_path(path)

    def get_all_gdrive_files_and_folders_for_subtree(self, subtree_root: GDriveIdentifier) -> Tuple[List[GDriveFile], List[GDriveFolder]]:
        return self._my_gdrive.get_all_files_and_folders_for_subtree(subtree_root)

    def get_gdrive_user_for_permission_id(self, permission_id: str) -> GDriveUser:
        with self._meta_lock:
            return self._user_for_permission_id_dict.get(permission_id, None)

    def get_gdrive_user_for_user_uid(self, uid: UID) -> GDriveUser:
        with self._meta_lock:
            return self._user_for_uid_dict.get(uid, None)

    def create_gdrive_user(self, user: GDriveUser):
        if user.uid:
            raise RuntimeError(f'create_gdrive_user(): user already has UID! (UID={user.uid})')
        if user.is_me:
            if not user.uid:
                user.uid = GDRIVE_ME_USER_UID
            elif user.uid != GDRIVE_ME_USER_UID:
                raise RuntimeError(f'create_gdrive_user(): cannot set is_me=true AND UID={user.uid}')

        with self._meta_lock:
            user_from_permission_id = self._user_for_permission_id_dict.get(user.permission_id, None)
            if user_from_permission_id:
                assert user_from_permission_id.permission_id == user.permission_id and user_from_permission_id.uid
                user.uid = user_from_permission_id.uid
                return
            if not user.is_me:
                user.uid = UID(self._user_uid_nextval)
            if self.application.cache_manager.enable_save_to_disk:
                with GDriveDatabase(self._get_gdrive_cache_path(), self.application) as cache:
                    cache.upsert_user(user)
            # wait until after DB write is successful:
            if not user.is_me:
                self._user_uid_nextval += 1
            self._user_for_permission_id_dict[user.permission_id] = user
            self._user_for_uid_dict[user.uid] = user

    def get_or_create_gdrive_mime_type(self, mime_type_string: str) -> MimeType:
        with self._meta_lock:
            mime_type: Optional[MimeType] = self._mime_type_for_str_dict.get(mime_type_string, None)
            if not mime_type:
                mime_type = MimeType(UID(self._mime_type_uid_nextval), mime_type_string)
                if self.application.cache_manager.enable_save_to_disk:
                    with GDriveDatabase(self._get_gdrive_cache_path(), self.application) as cache:
                        cache.upsert_mime_type(mime_type)
                self._mime_type_uid_nextval += 1
                self._mime_type_for_str_dict[mime_type_string] = mime_type
                self._mime_type_for_uid_dict[mime_type.uid] = mime_type
            return mime_type

    def get_mime_type_for_uid(self, uid: UID) -> Optional[MimeType]:
        with self._meta_lock:
            return self._mime_type_for_uid_dict.get(uid, None)

    def delete_all_gdrive_meta(self):
        with self._meta_lock:
            self._mime_type_uid_nextval = GDRIVE_FOLDER_MIME_TYPE_UID + 1
            self._mime_type_for_str_dict.clear()
            self._mime_type_for_uid_dict.clear()
            self._user_uid_nextval = GDRIVE_ME_USER_UID + 1
            self._user_for_permission_id_dict.clear()
            self._user_for_uid_dict.clear()
