import logging
import os
import threading
import time
from collections import deque
from typing import Deque, List, Optional, Tuple

from constants import GDRIVE_DOWNLOAD_TYPE_CHANGES, SUPER_DEBUG
from error import CacheNotLoadedError, GDriveItemNotFoundError
from model.display_tree.gdrive import GDriveDisplayTree
from model.gdrive_meta import GDriveUser, MimeType
from model.node.node import Node
from model.node.gdrive_node import GDriveFile, GDriveFolder, GDriveNode
from model.node_identifier import GDriveIdentifier, NodeIdentifier, SinglePathNodeIdentifier
from model.node_identifier_factory import NodeIdentifierFactory
from model.uid import UID
from store.gdrive.change_observer import GDriveChange, PagePersistingChangeObserver
from store.gdrive.client import GDriveClient
from store.gdrive.gdrive_tree_loader import GDriveTreeLoader
from store.gdrive.master_gdrive_disk import GDriveDiskStore
from store.gdrive.master_gdrive_memory import GDriveMemoryStore
from store.gdrive.master_gdrive_op_load import GDriveDiskLoadOp
from store.gdrive.master_gdrive_op_write import BatchChangesOp, CreateUserOp, DeleteAllDataOp, DeleteSingleNodeOp, DeleteSubtreeOp, \
    GDriveWriteThroughOp, RefreshFolderOp, UpsertMimeTypeOp, UpsertSingleNodeOp
from store.master import MasterStore
from store.sqlite.gdrive_db import CurrentDownload
from store.uid.uid_mapper import UidGoogIdMapper
from ui import actions
from ui.actions import ID_GLOBAL_CACHE
from util.stopwatch_sec import Stopwatch

logger = logging.getLogger(__name__)

"""
# CLASS GDriveMasterStore
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


# TODO: support drag & drop from GDrive to GDrive (e.g. "move" is really just changing parents)
class GDriveMasterStore(MasterStore):
    """Singleton in-memory cache for Google Drive"""
    def __init__(self, app):
        MasterStore.__init__(self)
        self.app = app

        self._uid_mapper = UidGoogIdMapper(app)
        """Single source of UID<->GoogID mappings and UID assignments. Thread-safe."""

        self._struct_lock = threading.Lock()
        """Used to protect structures inside memstore"""
        self._memstore: GDriveMemoryStore = GDriveMemoryStore(app, self._uid_mapper)
        self._diskstore: GDriveDiskStore = GDriveDiskStore(app, self._memstore)
        self.gdrive_client: GDriveClient = GDriveClient(self.app, ID_GLOBAL_CACHE)
        self.tree_loader = GDriveTreeLoader(app=self.app, diskstore=self._diskstore, tree_id=actions.ID_GLOBAL_CACHE)

    def start(self):
        MasterStore.start(self)
        self._diskstore.start()
        self.gdrive_client.start()

    def shutdown(self):
        super(GDriveMasterStore, self).shutdown()

        try:
            if self.gdrive_client:
                self.gdrive_client.shutdown()
                self.gdrive_client = None
        except NameError:
            pass

        try:
            self.app = None
        except NameError:
            pass

    def execute_load_op(self, operation: GDriveDiskLoadOp):
        """Executes a single GDriveDiskLoadOp ({start}->disk->memory"""
        if not self.app.cacheman.enable_load_from_disk:
            logger.debug(f'Load from disk is disable; ignoring load operation!')
            return

        # 1. Load from disk store
        self._diskstore.execute_load_op(operation)

        # 2. Update memory store
        operation.update_memstore(self._memstore)

    def _execute_write_op(self, operation: GDriveWriteThroughOp):
        """Executes a single GDriveWriteThroughOp ({start}->memory->disk->UI)"""
        assert self._struct_lock.locked()

        # 1. Update memory store
        operation.update_memstore(self._memstore)

        # 2. Update disk store
        if self.app.cacheman.enable_save_to_disk:
            self._diskstore.execute_write_op(operation)
        else:
            logger.debug(f'Save to disk is disabled: skipping disk update')

        # 3. Send signals
        operation.send_signals()

    # Tree-wide stuff
    # ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼

    def _load_master_cache(self, invalidate_cache: bool, sync_latest_changes: bool, tree_id: str):
        """Loads an EXISTING GDrive cache from disk and updates the in-memory cache from it"""
        logger.debug(f'Entered _load_master_cache(): locked={self._struct_lock.locked()}, invalidate_cache={invalidate_cache}')

        if not self.app.cacheman.enable_load_from_disk:
            logger.debug('Skipping cache load because cache.enable_cache_load is False')
            return None

        stopwatch_total = Stopwatch()

        if not self._memstore.master_tree or invalidate_cache:
            self._memstore.master_tree = self.tree_loader.load_all(invalidate_cache=invalidate_cache)
            logger.debug('Master tree set.')

        if sync_latest_changes:
            # This may add a noticeable delay:
            self.sync_latest_changes()

        logger.info(f'{stopwatch_total} GDrive master tree loaded')

    def sync_latest_changes(self):
        logger.debug(f'sync_latest_changes(): locked={self._struct_lock.locked()}')
        changes_download: CurrentDownload = self._diskstore.get_current_download(GDRIVE_DOWNLOAD_TYPE_CHANGES)
        if not changes_download:
            raise RuntimeError(f'Download state not found for GDrive change log!')

        self._sync_latest_changes(changes_download)

    def _sync_latest_changes(self, changes_download: CurrentDownload):
        sw = Stopwatch()

        if not changes_download.page_token:
            # covering all our bases here in case we are recovering from corruption
            changes_download.page_token = self.gdrive_client.get_changes_start_token()

        observer: PagePersistingChangeObserver = PagePersistingChangeObserver(self.app)
        sync_ts = int(time.time())
        self.gdrive_client.get_changes_list(changes_download.page_token, sync_ts, observer)

        # Now finally update download token
        if observer.new_start_token and observer.new_start_token != changes_download.page_token:
            changes_download.page_token = observer.new_start_token
            self._diskstore.create_or_update_download(changes_download)
            logger.debug(f'Updated changes download with token: {observer.new_start_token}')
        else:
            logger.debug(f'Changes download did not return a new start token. Will not update download.')

        logger.debug(f'{sw} Finished syncing GDrive changes from server')

    # Subtree-level stuff
    # ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼

    def _make_gdrive_display_tree(self, subtree_root: SinglePathNodeIdentifier, tree_id: str) -> Optional[GDriveDisplayTree]:
        if not subtree_root.uid:
            logger.debug(f'_make_gdrive_display_tree(): subtree_root.uid is empty!')
            return None

        root: GDriveNode = self._memstore.master_tree.get_node_for_uid(subtree_root.uid)
        if not root:
            logger.debug(f'_make_gdrive_display_tree(): could not find root node with UID {subtree_root.uid}')
            return None
        assert isinstance(root, GDriveFolder)

        return GDriveDisplayTree(app=self.app, tree_id=tree_id, root_identifier=subtree_root, whole_tree=self._memstore.master_tree)

    def _load_gdrive_subtree(self, subtree_root: Optional[SinglePathNodeIdentifier], invalidate_cache: bool,
                             sync_latest_changes: bool, tree_id: str) -> GDriveDisplayTree:
        if not subtree_root:
            subtree_root = NodeIdentifierFactory.get_gdrive_root_constant_single_path_identifier()
        logger.debug(f'[{tree_id}] Getting meta for subtree: "{subtree_root}" (invalidate_cache={invalidate_cache})')

        self._load_master_cache(invalidate_cache, sync_latest_changes, tree_id)

        gdrive_meta = self._make_gdrive_display_tree(subtree_root, tree_id)
        if gdrive_meta:
            logger.debug(f'[{tree_id}] Made display tree: {gdrive_meta}')
        else:
            raise GDriveItemNotFoundError(node_identifier=subtree_root,
                                          msg=f'Cannot load subtree because it does not exist: "{subtree_root}"',
                                          offending_path=subtree_root.get_single_path())
        return gdrive_meta

    def get_display_tree(self, subtree_root: SinglePathNodeIdentifier, tree_id: str) -> GDriveDisplayTree:
        assert isinstance(subtree_root, SinglePathNodeIdentifier)
        return self._load_gdrive_subtree(subtree_root, sync_latest_changes=False, invalidate_cache=False, tree_id=tree_id)

    def get_synced_master_tree(self, invalidate_cache: bool = False, tree_id: str = None):
        """This will sync the latest changes before returning."""
        return self._load_gdrive_subtree(subtree_root=None, sync_latest_changes=True, invalidate_cache=invalidate_cache, tree_id=tree_id)

    def refresh_subtree_stats(self, subtree_root_node: GDriveFolder, tree_id: str):
        logger.debug(f'refresh_subtree_stats(): locked={self._struct_lock.locked()}')
        with self._struct_lock:
            self._memstore.master_tree.refresh_stats(subtree_root_node, tree_id)

    def refresh_subtree(self, subtree_root_node: GDriveFolder, tree_id: str):
        # Call into client to get folder. Set has_all_children=False at first, then set to True when it's finished.
        logger.debug(f'[{tree_id}] Refresh requested. Querying GDrive for latest version of parent folder ({subtree_root_node})')
        stats_sw = Stopwatch()

        parent_node: Optional[GDriveNode] = self.gdrive_client.get_existing_node_by_id(subtree_root_node.goog_id)
        if not parent_node:
            # TODO: better handling
            raise RuntimeError(f'Node with goog_id "{subtree_root_node.goog_id}" was not found in Google Drive: {subtree_root_node}')
        if not parent_node.is_dir():
            self._execute_write_op(UpsertSingleNodeOp(parent_node))
            return

        assert isinstance(parent_node, GDriveFolder)
        parent_node.node_identifier.set_path_list(subtree_root_node.get_path_list())

        folders_to_process: Deque[GDriveFolder] = deque()
        folders_to_process.append(parent_node)

        count_folders: int = 0
        count_total: int = 0
        while len(folders_to_process) > 0:
            folder: GDriveFolder = folders_to_process.popleft()
            logger.debug(f'Querying GDrive for children of folder ({folder})')
            child_list: List[GDriveNode] = self.gdrive_client.get_all_children_for_parent(folder.goog_id)
            count_folders += 1
            count_total += len(child_list)
            # Derive paths with some cleverness:
            for child in child_list:
                child_path_list = []
                for path in folder.get_path_list():
                    child_path_list.append(os.path.join(path, child.name))
                child.node_identifier.set_path_list(child_path_list)

                if child.is_dir():
                    assert isinstance(child, GDriveFolder)
                    folders_to_process.append(child)
            folder.all_children_fetched = True

            if SUPER_DEBUG:
                logger.debug(f'refresh_subtree(): locked={self._struct_lock.locked()}')
            with self._struct_lock:
                self._execute_write_op(RefreshFolderOp(self.app, parent_node, child_list))

        logger.info(f'[{tree_id}] {stats_sw} Refresh subtree complete (SubtreeRoot={subtree_root_node.node_identifier} '
                    f'Folders={count_folders} Total={count_total})')

    # Individual node cache updates
    # ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼

    def upsert_single_node(self, node: GDriveNode):
        if SUPER_DEBUG:
            logger.debug(f'Entered upsert_single_node(): locked={self._struct_lock.locked()}')
        with self._struct_lock:
            self._upsert_single_node_nolock(node)

    def _upsert_single_node_nolock(self, node: GDriveNode):
        logger.debug(f'Upserting GDrive node to caches: {node}')
        self._execute_write_op(UpsertSingleNodeOp(node))

    def update_single_node(self, node: GDriveNode):
        if SUPER_DEBUG:
            logger.debug(f'Entered update_single_node(): locked={self._struct_lock.locked()}')
        with self._struct_lock:
            logger.debug(f'Updating GDrive node in caches: {node}')
            self._execute_write_op(UpsertSingleNodeOp(node, update_only=True))

    def remove_subtree(self, subtree_root: GDriveNode, to_trash):
        assert isinstance(subtree_root, GDriveNode), f'For node: {subtree_root}'

        if subtree_root.is_dir():
            if SUPER_DEBUG:
                logger.debug(f'remove_subtree(): locked={self._struct_lock.locked()}')
            with self._struct_lock:
                subtree_nodes: List[GDriveNode] = self._memstore.master_tree.get_subtree_bfs(subtree_root)
                logger.info(f'Removing subtree with {len(subtree_nodes)} nodes (to_trash={to_trash})')
                self._execute_write_op(DeleteSubtreeOp(subtree_root, node_list=subtree_nodes, to_trash=to_trash))
        else:
            logger.debug(f'Requested subtree is not a folder; calling remove_single_node()')
            self.remove_single_node(subtree_root, to_trash=to_trash)

    def remove_single_node(self, node: GDriveNode, to_trash):
        if SUPER_DEBUG:
            logger.debug(f'Entered remove_single_node(): locked={self._struct_lock.locked()}')
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
            self._execute_write_op(DeleteSingleNodeOp(node, to_trash))

    # Various public methods
    # ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼

    def apply_gdrive_changes(self, gdrive_change_list: List[GDriveChange]):
        if SUPER_DEBUG:
            logger.debug(f'Entered apply_gdrive_changes(): locked={self._struct_lock.locked()}')

        operation: BatchChangesOp = BatchChangesOp(self.app, gdrive_change_list)

        with self._struct_lock:
            try:
                self._execute_write_op(operation)
            except RuntimeError:
                logger.error(f'While executing GDrive change list: {gdrive_change_list}')
                raise

    def get_goog_id_list_for_uid_list(self, uid_list: List[UID], fail_if_missing: bool = True) -> List[str]:
        try:
            return self._memstore.master_tree.resolve_uids_to_goog_ids(uid_list, fail_if_missing=fail_if_missing)
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
        return self._memstore.master_tree.get_node_for_uid(uid)

    def get_node_for_uid(self, uid: UID) -> Optional[GDriveNode]:
        if not self._memstore.master_tree:
            raise RuntimeError(f'Cannot retrieve node (UID={uid}(: GDrive cache not loaded!')
        return self._memstore.master_tree.get_node_for_uid(uid)

    def get_node_for_name_and_parent_uid(self, name: str, parent_uid: UID) -> Optional[GDriveNode]:
        if SUPER_DEBUG:
            logger.debug(f'Entered get_node_for_name_and_parent_uid(): locked={self._struct_lock.locked()}')
        with self._struct_lock:
            return self._memstore.master_tree.get_node_for_name_and_parent_uid(name, parent_uid)

    def get_goog_id_for_uid(self, uid: UID) -> Optional[str]:
        node = self.get_node_for_uid(uid)
        if node:
            return node.goog_id
        return None

    def get_children(self, node: Node) -> List[GDriveNode]:
        assert isinstance(node, GDriveNode)
        return self._memstore.master_tree.get_children(node)

    def get_parent_list_for_node(self, node: GDriveNode) -> List[GDriveNode]:
        return self._memstore.master_tree.get_parent_list_for_node(node)

    def get_single_parent_for_node(self, node: Node, required_subtree_path: str = None):
        assert isinstance(node, GDriveNode)
        return self._memstore.master_tree.get_single_parent_for_node(node, required_subtree_path)

    def get_identifier_list_for_full_path_list(self, path_list: List[str]) -> List[NodeIdentifier]:
        if not self._memstore.master_tree:
            raise CacheNotLoadedError()
        return self._memstore.master_tree.get_identifier_list_for_path_list(path_list)

    def get_all_gdrive_files_and_folders_for_subtree(self, subtree_root: GDriveIdentifier) -> Tuple[List[GDriveFile], List[GDriveFolder]]:
        return self._memstore.master_tree.get_all_files_and_folders_for_subtree(subtree_root)

    def get_gdrive_user_for_permission_id(self, permission_id: str) -> GDriveUser:
        if SUPER_DEBUG:
            logger.debug(f'Entered get_gdrive_user_for_permission_id()')
        return self._memstore.get_gdrive_user_for_permission_id(permission_id)

    def get_gdrive_user_for_user_uid(self, uid: UID) -> GDriveUser:
        if SUPER_DEBUG:
            logger.debug(f'Entered get_gdrive_user_for_user_uid()')
        return self._memstore.get_gdrive_user_for_user_uid(uid)

    def create_gdrive_user(self, user: GDriveUser):
        if SUPER_DEBUG:
            logger.debug(f'Entered create_gdrive_user(): locked={self._struct_lock.locked()}')
        with self._struct_lock:
            self._execute_write_op(CreateUserOp(user))

    def get_or_create_gdrive_mime_type(self, mime_type_string: str) -> MimeType:
        # Note: this operation must be synchronous, so that it can return the MIME type
        if SUPER_DEBUG:
            logger.debug(f'Entered get_or_create_gdrive_mime_type(): locked={self._struct_lock.locked()}')
        op = UpsertMimeTypeOp(mime_type_string)
        with self._struct_lock:
            self._execute_write_op(op)
        return op.mime_type

    def get_mime_type_for_uid(self, uid: UID) -> Optional[MimeType]:
        if SUPER_DEBUG:
            logger.debug(f'Entered get_mime_type_for_uid()')
        return self._memstore.get_mime_type_for_uid(uid)

    def delete_all_gdrive_data(self):
        if SUPER_DEBUG:
            logger.debug(f'Entered delete_all_gdrive_meta(): locked={self._struct_lock.locked()}')
        with self._struct_lock:
            self._execute_write_op(DeleteAllDataOp())