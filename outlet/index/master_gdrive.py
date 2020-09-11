import logging
import threading
from typing import List, Optional, Tuple

from pydispatch import dispatcher

from constants import NOT_TRASHED, ROOT_PATH, GDRIVE_ROOT_UID, TREE_TYPE_GDRIVE
from gdrive.gdrive_tree_loader import GDriveTreeLoader
from index.cache_manager import PersistedCacheInfo
from index.error import CacheNotLoadedError, GDriveItemNotFoundError
from index.sqlite.gdrive_db import GDriveDatabase
from index.two_level_dict import FullPathBeforeUidDict, Md5BeforeUidDict
from index.uid.uid import UID
from index.uid.uid_mapper import UidGoogIdMapper
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

        self._uid_mapper = UidGoogIdMapper(application)

    # Subtree-level stuff
    # ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼

    def load_gdrive_cache(self, invalidate_cache: bool, tree_id: str):
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

            self._my_gdrive = tree_loader.load_all(invalidate_cache=invalidate_cache)

        # Always do this to bring tree up-to-date:
        # TODO: make this asynchronous. It's slowing down the UI
        tree_loader.sync_latest_changes()

        logger.info(f'{stopwatch_total} GDrive cache for {cache_info.subtree_root.full_path} loaded')
        cache_info.is_loaded = True

    def load_gdrive_subtree(self, subtree_root: GDriveIdentifier, invalidate_cache: bool, tree_id: str) -> GDriveDisplayTree:
        if not subtree_root:
            subtree_root = NodeIdentifierFactory.get_gdrive_root_constant_identifier()
        logger.debug(f'[{tree_id}] Getting meta for subtree: "{subtree_root}" (invalidate_cache={invalidate_cache})')

        self.load_gdrive_cache(invalidate_cache, tree_id)

        if subtree_root.uid == GDRIVE_ROOT_UID:
            # Special case. GDrive does not have a single root (it treats shared drives as roots, for example).
            # We'll use this special token to represent "everything"
            gdrive_meta = self._my_gdrive
        else:
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

        return GDriveDisplayTree(cache_manager=self, whole_tree=self._my_gdrive, root_node=root)

    # Individual node cache updates
    # ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼

    def _send_node_upserted_signal(self, node: GDriveNode):
        logger.debug(f'Sending signal: {actions.NODE_UPSERTED}')
        dispatcher.send(signal=actions.NODE_UPSERTED, sender=ID_GLOBAL_CACHE, node=node)

    def upsert_gdrive_node(self, node: GDriveNode):
        logger.debug(f'Upserting node to caches: {node}')
        with self._struct_lock:
            # try to prevent cache corruption by doing some sanity checks
            if not node:
                raise RuntimeError(f'No node supplied!')
            if not node.uid:
                raise RuntimeError(f'Node is missing UID: {node}')
            if node.node_identifier.tree_type != TREE_TYPE_GDRIVE:
                raise RuntimeError(f'Unrecognized tree type: {node.node_identifier.tree_type}')
            if not self._my_gdrive:
                # TODO: give more thought to lifecycle
                raise RuntimeError('GDriveWholeTree not loaded!')
            if not isinstance(node, GDriveNode):
                raise RuntimeError(f'Unrecognized node type: {node}')

            # Validate parent mappings
            parent_uids = node.get_parent_uids()
            if not parent_uids:
                # Adding a new root is currently not allowed (which is fine because there should be no way to do this via the UI)
                raise RuntimeError(f'Node is missing parent UIDs: {node}')

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
                    return

                if existing_node == node:
                    logger.info(f'Item being added (uid={node.uid}) is identical to node already in the cache; skipping cache update')
                    self._send_node_upserted_signal(node)
                    return
                logger.debug(f'Found existing node in cache with UID={existing_node.uid}: doing an update')
            elif node.goog_id:
                previous_uid = self.get_uid_for_goog_id(goog_id=node.goog_id)
                if previous_uid and node.uid != previous_uid:
                    logger.warning(f'Found node in cache with same GoogID ({node.goog_id}) but different UID ('
                                   f'{previous_uid}). Changing UID of node (was: {node.uid}) to match and overwrite previous node')
                    node.uid = previous_uid

            if node.exists():
                if self.application.cache_manager.enable_save_to_disk:

                    parent_goog_ids = self._my_gdrive.resolve_uids_to_goog_ids(parent_uids)
                    parent_mappings = []
                    if len(node.get_parent_uids()) != len(parent_goog_ids):
                        raise RuntimeError(f'Internal error: could not map all parent goog_ids ({len(parent_goog_ids)}) to parent UIDs '
                                           f'({len(parent_uids)}) for node: {node}')
                    for parent_uid, parent_goog_id in zip(node.get_parent_uids(), parent_goog_ids):
                        parent_mappings.append((node.uid, parent_uid, parent_goog_id, node.sync_ts))

                    cache_path: str = self._get_cache_path_for_master()

                    # Write new values:
                    with GDriveDatabase(cache_path, self.application) as cache:
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
                else:
                    logger.debug(f'Save to disk is disabled: skipping add/update of node with UID={node.uid}')
            else:
                logger.debug(f'Node does not exist; skipping save to disk: {node}')

            # Finally, update in-memory cache (tree). If an existing node is found with the same UID, it will update and return that instead:
            node = self._my_gdrive.add_node(node)

            # Generate full_path for node, if not already done (we assume this is a newly created node)
            self._my_gdrive.get_full_path_for_node(node)

            self._send_node_upserted_signal(node)

    def remove_gdrive_subtree(self, subtree_root: DisplayNode, to_trash):
        assert isinstance(subtree_root, GDriveNode), f'For node: {subtree_root}'

        with self._struct_lock:
            if not subtree_root.is_dir():
                logger.debug(f'Requested subtree is not a folder; calling remove_gdrive_node()')
                self._remove_gdrive_node_nolock(subtree_root, to_trash=to_trash)
                return

            subtree_nodes = self._my_gdrive.get_subtree_bfs(subtree_root)

            logger.info(f'Removing subtree with {len(subtree_nodes)} nodes')
            for node in reversed(subtree_nodes):
                self._remove_gdrive_node_nolock(node, to_trash=to_trash)

    def remove_gdrive_node(self, node: DisplayNode, to_trash):
        with self._struct_lock:
            self._remove_gdrive_node_nolock(node, to_trash)

    def _remove_gdrive_node_nolock(self, node: DisplayNode, to_trash):
        logger.debug(f'Removing node from caches: {node}')
        assert isinstance(node, GDriveNode), f'For node: {node}'

        if node.is_dir():
            children: List[GDriveNode] = self._my_gdrive.get_children(node)
            if children:
                raise RuntimeError(f'Cannot remove GDrive folder from cache: it contains {len(children)} children!')

        if to_trash:
            if node.trashed == NOT_TRASHED:
                raise RuntimeError(f'Trying to trash Google node which is not marked as trashed: {node}')
            # this is actually an update
            self.upsert_gdrive_node(node)
        else:
            # Remove from in-memory cache:
            existing_node = self._my_gdrive.get_node_for_uid(node.uid)
            if existing_node:
                self._my_gdrive.remove_node(existing_node)

            # Remove from disk cache:
            if self.application.cache_manager.enable_save_to_disk:
                cache_path: str = self._get_cache_path_for_master()
                with GDriveDatabase(cache_path, self.application) as cache:
                    cache.delete_parent_mappings_for_uid(node.uid, commit=False)
                    if node.is_dir():
                        cache.delete_gdrive_folder_with_uid(node.uid)
                    else:
                        cache.delete_gdrive_file_with_uid(node.uid)
            else:
                logger.debug(f'Save to disk is disabled: skipping removal of node with UID={node.uid}')

        dispatcher.send(signal=actions.NODE_REMOVED, sender=ID_GLOBAL_CACHE, node=node)

    # Various public methods
    # ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼

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
