import logging
import threading
from typing import Dict, List, Optional, Tuple

from pydispatch import dispatcher

from constants import GDRIVE_FOLDER_MIME_TYPE_UID, GDRIVE_ME_USER_UID
from error import CacheNotLoadedError, GDriveItemNotFoundError
from model.display_tree.gdrive import GDriveDisplayTree
from model.gdrive_meta import GDriveUser, MimeType
from model.gdrive_whole_tree import GDriveWholeTree
from model.node.display_node import DisplayNode
from model.node.gdrive_node import GDriveFile, GDriveFolder, GDriveNode
from model.node_identifier import GDriveIdentifier, NodeIdentifier
from model.node_identifier_factory import NodeIdentifierFactory
from model.uid import UID
from store.gdrive.change_observer import GDriveChange
from store.gdrive.gdrive_tree_loader import GDriveTreeLoader
from store.gdrive.master_gdrive_op import BatchChangesOp, DeleteSingleNodeOp, DeleteSubtreeOp, GDriveCacheOp, UpsertSingleNodeOp
from store.master import MasterCache
from store.sqlite.gdrive_db import GDriveDatabase
from store.uid.uid_mapper import UidGoogIdMapper
from ui import actions
from util.stopwatch_sec import Stopwatch

logger = logging.getLogger(__name__)

# TODO: lots of work to do to support drag & drop from GDrive to GDrive (e.g. "move" is really just changing parents)
# - support Move
# - support Delete Subtree


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


class GDriveMasterCache(MasterCache):
    """Singleton in-memory cache for Google Drive"""
    def __init__(self, app):
        self.app = app

        self._uid_mapper = UidGoogIdMapper(app)
        """Single source of UID<->GoogID mappings and UID assignments. Thread-safe."""

        self._struct_lock = threading.Lock()
        """Must be used to ensure structures below are thread-safe"""

        self._master_tree: Optional[GDriveWholeTree] = None

        self._mime_type_uid_nextval: int = GDRIVE_FOLDER_MIME_TYPE_UID + 1
        self._mime_type_for_str_dict: Dict[str, MimeType] = {}
        self._mime_type_for_uid_dict: Dict[UID, MimeType] = {}
        self._user_uid_nextval: int = GDRIVE_ME_USER_UID + 1
        self._user_for_permission_id_dict: Dict[str, GDriveUser] = {}
        self._user_for_uid_dict: Dict[UID, GDriveUser] = {}

    def shutdown(self):
        super(GDriveMasterCache, self).shutdown()
        try:
            self.app = None
        except NameError:
            pass

    def _get_gdrive_cache_path(self) -> str:
        master_tree_root = NodeIdentifierFactory.get_gdrive_root_constant_identifier()
        cache_info = self.app.cacheman.get_or_create_cache_info_entry(master_tree_root)
        return cache_info.cache_location

    def _execute(self, operation: GDriveCacheOp):
        """Executes a single GDriveCacheOp."""

        # 1. Update memory cache
        operation.update_memory_cache(self._master_tree)

        # 2. Update disk cache
        if self.app.cacheman.enable_save_to_disk:
            cache_path: str = self._get_gdrive_cache_path()
            with GDriveDatabase(cache_path, self.app) as cache:
                operation.update_disk_cache(cache)
                cache.commit()
        else:
            logger.debug(f'Save to disk is disabled: skipping disk update')

        # 3. Send signals
        operation.send_signals()

    # Subtree-level stuff
    # ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼

    def _load_master_cache_from_disk(self, invalidate_cache: bool, sync_latest_changes: bool, tree_id: str):
        """Loads an EXISTING GDrive cache from disk and updates the in-memory cache from it"""
        if not self.app.cacheman.enable_load_from_disk:
            logger.debug('Skipping cache load because cache.enable_cache_load is False')
            return None

        # Always load ROOT:
        master_tree_root = NodeIdentifierFactory.get_gdrive_root_constant_identifier()
        cache_info = self.app.cacheman.get_or_create_cache_info_entry(master_tree_root)

        stopwatch_total = Stopwatch()

        tree_loader = GDriveTreeLoader(app=self.app, cache_path=cache_info.cache_location, tree_id=tree_id)

        if not cache_info.is_loaded or invalidate_cache:
            status = f'Loading meta for "{cache_info.subtree_root.full_path}" from cache: "{cache_info.cache_location}"'
            logger.debug(status)
            dispatcher.send(actions.SET_PROGRESS_TEXT, sender=tree_id, msg=status)

            with GDriveDatabase(cache_info.cache_location, self.app) as cache:
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

            self._master_tree = tree_loader.load_all(invalidate_cache=invalidate_cache)
            cache_info.is_loaded = True

        if sync_latest_changes:
            # This may add a noticeable delay:
            tree_loader.sync_latest_changes()

        logger.info(f'{stopwatch_total} GDrive cache for {cache_info.subtree_root.full_path} loaded')

    def _make_gdrive_display_tree(self, subtree_root: GDriveIdentifier, tree_id: str) -> Optional[GDriveDisplayTree]:
        if not subtree_root.uid:
            logger.debug(f'_make_gdrive_display_tree(): subtree_root.uid is empty!')
            return None

        root: GDriveNode = self._master_tree.get_node_for_uid(subtree_root.uid)
        if not root:
            logger.debug(f'_make_gdrive_display_tree(): could not find root node with UID {subtree_root.uid}')
            return None

        assert isinstance(root, GDriveFolder)
        return GDriveDisplayTree(whole_tree=self._master_tree, root_node=root, tree_id=tree_id)

    def _load_gdrive_subtree(self, subtree_root: Optional[GDriveIdentifier], invalidate_cache: bool, sync_latest_changes: bool, tree_id: str)\
            -> GDriveDisplayTree:
        if not subtree_root:
            subtree_root = NodeIdentifierFactory.get_gdrive_root_constant_identifier()
        logger.debug(f'[{tree_id}] Getting meta for subtree: "{subtree_root}" (invalidate_cache={invalidate_cache})')

        self._load_master_cache_from_disk(invalidate_cache, sync_latest_changes, tree_id)

        gdrive_meta = self._make_gdrive_display_tree(subtree_root, tree_id)
        if gdrive_meta:
            logger.debug(f'[{tree_id}] Made display tree: {gdrive_meta}')
        else:
            raise GDriveItemNotFoundError(node_identifier=subtree_root,
                                          msg=f'Cannot load subtree because it does not exist: "{subtree_root}"',
                                          offending_path=subtree_root.full_path)
        return gdrive_meta

    def get_display_tree(self, subtree_root: GDriveIdentifier, tree_id: str) -> GDriveDisplayTree:
        return self._load_gdrive_subtree(subtree_root, sync_latest_changes=False, invalidate_cache=False, tree_id=tree_id)

    def get_synced_master_tree(self, invalidate_cache: bool = False, tree_id: str = None):
        """This will sync the latest changes before returning."""
        return self._load_gdrive_subtree(subtree_root=None, sync_latest_changes=True, invalidate_cache=invalidate_cache, tree_id=tree_id)

    def refresh_subtree_stats(self, subtree_root_node: GDriveFolder, tree_id: str):
        with self._struct_lock:
            self._master_tree.refresh_stats(subtree_root_node, tree_id)

    def refresh_subtree(self, subtree_root_node: GDriveFolder, tree_id: str):
        with self._struct_lock:
            pass
        # TODO call into client to get folder. Set has_all_children=False at first, then set to True when it's finished.

        # TODO then recursively call into client to download descendants. Only lock the struct when you are doing the modify

    # Individual node cache updates
    # ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼

    def upsert_single_node(self, node: GDriveNode):
        with self._struct_lock:
            self._upsert_single_node_nolock(node)

    def _upsert_single_node_nolock(self, node: GDriveNode):
        logger.debug(f'Upserting GDrive node to caches: {node}')
        self._execute(UpsertSingleNodeOp(node, self._uid_mapper))

    def update_single_node(self, node: GDriveNode):
        with self._struct_lock:
            logger.debug(f'Updating GDrive node in caches: {node}')
            self._execute(UpsertSingleNodeOp(node, self._uid_mapper, update_only=True))

    def remove_subtree(self, subtree_root: GDriveNode, to_trash):
        assert isinstance(subtree_root, GDriveNode), f'For node: {subtree_root}'

        with self._struct_lock:
            if not subtree_root.is_dir():
                logger.debug(f'Requested subtree is not a folder; calling remove_single_node()')
                self._remove_single_node_nolock(subtree_root, to_trash=to_trash)
                return

            if to_trash:
                # TODO
                raise RuntimeError(f'Not supported: to_trash=true!')

            subtree_nodes: List[GDriveNode] = self._master_tree.get_subtree_bfs(subtree_root)
            logger.info(f'Removing subtree with {len(subtree_nodes)} nodes')
            self._execute(DeleteSubtreeOp(subtree_root, node_list=subtree_nodes))

    def remove_single_node(self, node: GDriveNode, to_trash):
        with self._struct_lock:
            self._remove_single_node_nolock(node, to_trash)

    def _remove_single_node_nolock(self, node: GDriveNode, to_trash):
        logger.debug(f'Removing node from caches: {node}')

        if to_trash:
            if node.trashed.not_trashed():
                raise RuntimeError(f'Trying to trash Google node which is not marked as trashed: {node}')
            # this is actually an update
            self._upsert_single_node_nolock(node)
        else:
            self._execute(DeleteSingleNodeOp(node, to_trash))

    # Various public methods
    # ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼

    def apply_gdrive_changes(self, gdrive_change_list: List[GDriveChange]):
        operation: BatchChangesOp = BatchChangesOp(self.app, gdrive_change_list)

        with self._struct_lock:
            try:
                self._execute(operation)
            except RuntimeError:
                logger.error(f'While executing GDrive change list: {gdrive_change_list}')
                raise

    def get_goog_id_list_for_uid_list(self, uid_list: List[UID], fail_if_missing: bool = True) -> List[str]:
        try:
            return self._master_tree.resolve_uids_to_goog_ids(uid_list, fail_if_missing=fail_if_missing)
        except RuntimeError:
            # Unresolved UIDs. This can happen when one cache's node refers to a parent which no longer exists...
            # TODO: let's make this even more robust by keeping track of tombstones
            logger.debug(f'Failed to find UIDs in master tree; assuming they were deleted. Trying uid_mapper...')
            goog_id_list: List[str] = []
            for uid in uid_list:
                goog_id = self._uid_mapper.get_goog_id_for_uid(uid)
                if goog_id:
                    goog_id_list.append(goog_id)
                else:
                    raise RuntimeError(f'Could not find goog_id for UID: {uid}')
            return goog_id_list

    def get_uid_list_for_goog_id_list(self, goog_ids: List[str]) -> List[UID]:
        uid_list = []
        for goog_id in goog_ids:
            uid_list.append(self._uid_mapper.get_uid_for_goog_id(goog_id))

        return uid_list

    def get_uid_for_domain_id(self, domain_id: str, uid_suggestion: Optional[UID] = None) -> UID:
        return self.get_uid_for_goog_id(domain_id, uid_suggestion)

    def get_uid_for_goog_id(self, goog_id: str, uid_suggestion: Optional[UID] = None) -> UID:
        return self._uid_mapper.get_uid_for_goog_id(goog_id, uid_suggestion)

    def get_node_for_domain_id(self, goog_id: str) -> Optional[GDriveNode]:
        uid = self._uid_mapper.get_uid_for_goog_id(goog_id)
        return self._master_tree.get_node_for_uid(uid)

    def get_node_for_uid(self, uid: UID) -> Optional[GDriveNode]:
        if not self._master_tree:
            raise RuntimeError(f'Cannot retrieve node (UID={uid}(: GDrive cache not loaded!')
        return self._master_tree.get_node_for_uid(uid)

    def get_node_for_name_and_parent_uid(self, name: str, parent_uid: UID) -> Optional[GDriveNode]:
        with self._struct_lock:
            return self._master_tree.get_node_for_name_and_parent_uid(name, parent_uid)

    def get_goog_id_for_uid(self, uid: UID) -> Optional[str]:
        node = self.get_node_for_uid(uid)
        if node:
            return node.goog_id
        return None

    def get_children(self, node: DisplayNode) -> List[GDriveNode]:
        assert isinstance(node, GDriveNode)
        return self._master_tree.get_children(node)

    def get_parent_for_node(self, node: DisplayNode, required_subtree_path: str = None):
        assert isinstance(node, GDriveNode)
        return self._master_tree.get_parent_for_node(node, required_subtree_path)

    def get_all_for_path(self, path: str) -> List[NodeIdentifier]:
        if not self._master_tree:
            raise CacheNotLoadedError()
        return self._master_tree.get_all_identifiers_for_path(path)

    def get_all_gdrive_files_and_folders_for_subtree(self, subtree_root: GDriveIdentifier) -> Tuple[List[GDriveFile], List[GDriveFolder]]:
        return self._master_tree.get_all_files_and_folders_for_subtree(subtree_root)

    def get_gdrive_user_for_permission_id(self, permission_id: str) -> GDriveUser:
        with self._struct_lock:
            return self._user_for_permission_id_dict.get(permission_id, None)

    def get_gdrive_user_for_user_uid(self, uid: UID) -> GDriveUser:
        with self._struct_lock:
            return self._user_for_uid_dict.get(uid, None)

    def create_gdrive_user(self, user: GDriveUser):
        if user.uid:
            raise RuntimeError(f'create_gdrive_user(): user already has UID! (UID={user.uid})')
        if user.is_me:
            if not user.uid:
                user.uid = GDRIVE_ME_USER_UID
            elif user.uid != GDRIVE_ME_USER_UID:
                raise RuntimeError(f'create_gdrive_user(): cannot set is_me=true AND UID={user.uid}')

        with self._struct_lock:
            user_from_permission_id = self._user_for_permission_id_dict.get(user.permission_id, None)
            if user_from_permission_id:
                assert user_from_permission_id.permission_id == user.permission_id and user_from_permission_id.uid
                user.uid = user_from_permission_id.uid
                return
            if not user.is_me:
                user.uid = UID(self._user_uid_nextval)
            if self.app.cacheman.enable_save_to_disk:
                with GDriveDatabase(self._get_gdrive_cache_path(), self.app) as cache:
                    cache.upsert_user(user)
            # wait until after DB write is successful:
            if not user.is_me:
                self._user_uid_nextval += 1
            self._user_for_permission_id_dict[user.permission_id] = user
            self._user_for_uid_dict[user.uid] = user

    def get_or_create_gdrive_mime_type(self, mime_type_string: str) -> MimeType:
        with self._struct_lock:
            mime_type: Optional[MimeType] = self._mime_type_for_str_dict.get(mime_type_string, None)
            if not mime_type:
                mime_type = MimeType(UID(self._mime_type_uid_nextval), mime_type_string)
                if self.app.cacheman.enable_save_to_disk:
                    with GDriveDatabase(self._get_gdrive_cache_path(), self.app) as cache:
                        cache.upsert_mime_type(mime_type)
                self._mime_type_uid_nextval += 1
                self._mime_type_for_str_dict[mime_type_string] = mime_type
                self._mime_type_for_uid_dict[mime_type.uid] = mime_type
            return mime_type

    def get_mime_type_for_uid(self, uid: UID) -> Optional[MimeType]:
        with self._struct_lock:
            return self._mime_type_for_uid_dict.get(uid, None)

    def delete_all_gdrive_meta(self):
        with self._struct_lock:
            self._mime_type_uid_nextval = GDRIVE_FOLDER_MIME_TYPE_UID + 1
            self._mime_type_for_str_dict.clear()
            self._mime_type_for_uid_dict.clear()
            self._user_uid_nextval = GDRIVE_ME_USER_UID + 1
            self._user_for_permission_id_dict.clear()
            self._user_for_uid_dict.clear()
