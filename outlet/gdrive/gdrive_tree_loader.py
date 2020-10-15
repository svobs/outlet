import logging
import os
import threading
import time
from collections import deque
from typing import Dict, List, Optional, Tuple

from pydispatch import dispatcher

from constants import GDRIVE_DOWNLOAD_STATE_COMPLETE, GDRIVE_DOWNLOAD_STATE_GETTING_DIRS, GDRIVE_DOWNLOAD_STATE_GETTING_NON_DIRS, \
    GDRIVE_DOWNLOAD_STATE_NOT_STARTED, \
    GDRIVE_DOWNLOAD_STATE_READY_TO_COMPILE, GDRIVE_DOWNLOAD_TYPE_CHANGES, GDRIVE_DOWNLOAD_TYPE_INITIAL_LOAD, GDRIVE_ROOT_UID
from gdrive.change_observer import PagePersistingChangeObserver
from gdrive.client import GDriveClient
from gdrive.query_observer import FileMetaPersister, FolderMetaPersister
from index.sqlite.gdrive_db import CurrentDownload, GDriveDatabase
from index.uid.uid import UID
from model.gdrive_whole_tree import GDriveWholeTree
from model.node.gdrive_node import GDriveFile, GDriveFolder, GDriveNode
from util.stopwatch_sec import Stopwatch
from ui import actions

logger = logging.getLogger(__name__)


# CLASS GDriveTreeLoader
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼

class GDriveTreeLoader:
    __class_lock = threading.Lock()
    """Coarse-grained lock which ensures that (1) load operations and (2) change sync operations do not step on each other or
    on multiple instances of themselves."""

    def __init__(self, app, cache_path: str, tree_id: str = None):
        self.app = app
        self.node_identifier_factory = app.node_identifier_factory
        self.cacheman = app.cacheman
        self.tree_id: str = tree_id
        self.cache_path: str = cache_path
        self.cache: Optional[GDriveDatabase] = None
        self.gdrive_client: GDriveClient = self.app.cacheman.gdrive_client

    def __del__(self):
        pass

    def _get_previous_download_state(self, download_type: int):
        for download in self.cache.get_current_downloads():
            if download.download_type == download_type:
                return download
        return None

    def load_all(self, invalidate_cache=False) -> GDriveWholeTree:
        logger.debug(f'GDrive: load_all() called with invalidate_cache={invalidate_cache}')

        with GDriveTreeLoader.__class_lock:
            # This will create a new file if not found:
            with GDriveDatabase(self.cache_path, self.app) as self.cache:
                try:
                    # scroll down ⯆⯆⯆
                    return self._load_all(invalidate_cache)
                finally:
                    if self.tree_id:
                        logger.debug(f'Sending STOP_PROGRESS for tree_id: {self.tree_id}')
                        actions.get_dispatcher().send(actions.STOP_PROGRESS, sender=self.tree_id)

    def _load_all(self, invalidate_cache: bool) -> GDriveWholeTree:
        if self.tree_id:
            logger.debug(f'Sending START_PROGRESS_INDETERMINATE for tree_id: {self.tree_id}')
            actions.get_dispatcher().send(actions.START_PROGRESS_INDETERMINATE, sender=self.tree_id)

        sync_ts: int = int(time.time())

        changes_download: CurrentDownload = self._get_previous_download_state(GDRIVE_DOWNLOAD_TYPE_CHANGES)
        if not changes_download or invalidate_cache:
            logger.debug(f'Getting a new start token for changes (invalidate_cache={invalidate_cache})')
            token: str = self.gdrive_client.get_changes_start_token()
            changes_download = CurrentDownload(GDRIVE_DOWNLOAD_TYPE_CHANGES, GDRIVE_DOWNLOAD_STATE_NOT_STARTED, token, sync_ts)
            self.cache.create_or_update_download(changes_download)

        initial_download: CurrentDownload = self._get_previous_download_state(GDRIVE_DOWNLOAD_TYPE_INITIAL_LOAD)
        if not initial_download or invalidate_cache:
            logger.debug(f'Starting a fresh download for entire Google Drive tree meta (invalidate_cache={invalidate_cache})')
            initial_download = CurrentDownload(GDRIVE_DOWNLOAD_TYPE_INITIAL_LOAD, GDRIVE_DOWNLOAD_STATE_NOT_STARTED, None, sync_ts)
            self.cache.create_or_update_download(initial_download)

            # Notify UI trees that their old roots are invalid:
            dispatcher.send(signal=actions.GDRIVE_RELOADED, sender=self.tree_id)

        if initial_download.current_state == GDRIVE_DOWNLOAD_STATE_NOT_STARTED:
            # completely fresh tree
            tree = GDriveWholeTree(self.node_identifier_factory)
        else:
            # Start/resume: read cache
            msg = 'Reading cache...'
            logger.info(msg)

            if self.tree_id:
                actions.get_dispatcher().send(actions.SET_PROGRESS_TEXT, sender=self.tree_id, msg=msg)

            tree: GDriveWholeTree = self._load_tree_from_cache(initial_download.is_complete())

        # TODO
        tree.me = self.gdrive_client.get_about()

        if not initial_download.is_complete():
            state = 'Starting' if initial_download.current_state == GDRIVE_DOWNLOAD_STATE_NOT_STARTED else 'Resuming'
            logger.info(f'{state} download of all Google Drive tree (state={initial_download.current_state})')

        # BEGIN STATE MACHINE:

        if initial_download.current_state == GDRIVE_DOWNLOAD_STATE_NOT_STARTED:
            self.cache.delete_all_gdrive_data()
            # TODO: put this in the GDriveWholeTree instead
            self.cacheman.delete_all_gdrive_meta()

            # Need to make a special call to get the root node 'My Drive'. This node will not be included
            # in the "list files" call:
            initial_download.update_ts = sync_ts
            drive_root: GDriveFolder = self.gdrive_client.get_my_drive_root(initial_download.update_ts)
            tree.id_dict[drive_root.uid] = drive_root

            initial_download.current_state = GDRIVE_DOWNLOAD_STATE_GETTING_DIRS
            initial_download.page_token = None
            self.cache.insert_gdrive_folder_list(folder_list=[drive_root], commit=False)
            self.cache.create_or_update_download(download=initial_download)
            # fall through

        if initial_download.current_state <= GDRIVE_DOWNLOAD_STATE_GETTING_DIRS:
            observer = FolderMetaPersister(tree, initial_download, self.cache, self.cacheman)
            self.gdrive_client.get_all_folders(initial_download.page_token, initial_download.update_ts, observer)
            # fall through

        if initial_download.current_state <= GDRIVE_DOWNLOAD_STATE_GETTING_NON_DIRS:
            observer = FileMetaPersister(tree, initial_download, self.cache, self.cacheman)
            self.gdrive_client.get_all_non_folders(initial_download.page_token, initial_download.update_ts, observer)
            # fall through

        if initial_download.current_state <= GDRIVE_DOWNLOAD_STATE_READY_TO_COMPILE:
            initial_download.current_state = GDRIVE_DOWNLOAD_STATE_COMPLETE

            # Some post-processing needed...

            # (1) Update all_children_fetched state
            tuples: List[Tuple[int, bool]] = []
            for goog_node in tree.id_dict.values():
                if goog_node.is_dir():
                    goog_node.all_children_fetched = True
                    tuples.append((goog_node.uid, True))

            # Updating the entire table is much faster than doing a 'where' clause:
            self.cache.update_folder_fetched_status(commit=False)

            # (2) Set UIDs for parents, and assemble parent dict:
            parent_mappings: List[Tuple] = self.cache.get_id_parent_mappings()
            parent_mappings = self._translate_parent_ids(tree, parent_mappings)
            self.cache.insert_id_parent_mappings(parent_mappings, overwrite=True, commit=False)
            logger.debug(f'Updated {len(parent_mappings)} id-parent mappings')

            # (3) mark download finished
            self.cache.create_or_update_download(download=initial_download)
            logger.debug('GDrive data download complete.')

            # fall through

        # Still need to compute this in memory:
        self._determine_roots(tree)
        _compile_full_paths(tree)

        logger.debug('GDrive: load_all() done')
        return tree

    def sync_latest_changes(self):
        with GDriveTreeLoader.__class_lock:
            with GDriveDatabase(self.cache_path, self.app) as self.cache:
                changes_download: CurrentDownload = self._get_previous_download_state(GDRIVE_DOWNLOAD_TYPE_CHANGES)
                if not changes_download:
                    raise RuntimeError(f'Download state not found for GDrive change log!')
                self._sync_latest_changes(changes_download)

    def _sync_latest_changes(self, changes_download: CurrentDownload):
        sw = Stopwatch()

        if not changes_download.page_token:
            # covering all our bases here in case we are recovering from corruption
            changes_download.page_token = self.gdrive_client.get_changes_start_token()

        # TODO: is this needed?
        # shared_with_me: List[GDriveNode] = self.gdrive_client.get_all_shared_with_me()
        # logger.debug(f'Found {len(shared_with_me)} shared with me')
        # for node in shared_with_me:
        #     logger.debug(f'Shared with me: {node}')
        #     self.cacheman.add_or_update_node(node)

        observer: PagePersistingChangeObserver = PagePersistingChangeObserver(self.app)
        sync_ts = int(time.time())
        self.gdrive_client.get_changes_list(changes_download.page_token, sync_ts, observer)

        # Now finally update download token
        if observer.new_start_token and observer.new_start_token != changes_download.page_token:
            changes_download.page_token = observer.new_start_token
            self.cache.create_or_update_download(changes_download)
            logger.debug(f'Updated changes download with token: {observer.new_start_token}')
        else:
            logger.debug(f'Changes download did not return a new start token. Will not update download.')

        logger.debug(f'{sw} Finished syncing GDrive changes from server')

    def _load_tree_from_cache(self, is_complete: bool) -> GDriveWholeTree:
        """
        Retrieves and reassembles (to the extent it was during the download) a partially or completely downloaded
        GDrive tree.
        """
        sw_total = Stopwatch()
        max_uid = GDRIVE_ROOT_UID + 1
        tree = GDriveWholeTree(self.node_identifier_factory)
        invalidate_uids: Dict[UID, str] = {}

        items_without_goog_ids: List[GDriveNode] = []

        # DIRs:
        sw = Stopwatch()
        folder_list: List[GDriveFolder] = self.cache.get_gdrive_folder_object_list()

        actions.get_dispatcher().send(actions.SET_PROGRESS_TEXT, sender=self.tree_id, msg=f'Retrieved {len(folder_list):n} Google Drive folders')

        count_folders_loaded = 0
        for folder in folder_list:
            if folder.goog_id:
                uid = self.cacheman.get_uid_for_goog_id(folder.goog_id, folder.uid)
                if folder.uid != uid:
                    # Duplicate entry with same goog_id. Here's a useful SQLite query:
                    # "SELECT goog_id, COUNT(*) c FROM gdrive_file GROUP BY goog_id HAVING c > 1;"
                    logger.warning(f'Skipping what appears to be a duplicate entry: goog_id="{folder.goog_id}", uid={folder.uid}')
                    invalidate_uids[folder.uid] = folder.goog_id
                    continue
            else:
                raise RuntimeError(f'GDriveFolder is missing goog_id: {folder}')

            if tree.id_dict.get(folder.uid, None):
                raise RuntimeError(f'GDrive folder cache conflict for UID: {folder.uid} (1st: {tree.id_dict[folder.uid]}; 2nd: {folder}')
            tree.id_dict[folder.uid] = folder
            count_folders_loaded += 1

            if folder.uid >= max_uid:
                max_uid = folder.uid

        logger.debug(f'{sw} Loaded {count_folders_loaded} Google Drive folders')

        # FILES:
        sw = Stopwatch()
        file_list: List[GDriveFile] = self.cache.get_gdrive_file_object_list()

        actions.get_dispatcher().send(actions.SET_PROGRESS_TEXT, sender=self.tree_id, msg=f'Retreived {len(file_list):n} Google Drive files')

        count_files_loaded = 0
        for file in file_list:
            if file.goog_id:
                uid = self.cacheman.get_uid_for_goog_id(file.goog_id, file.uid)
                if file.uid != uid:
                    # Duplicate entry with same goog_id. Here's a useful SQLite query:
                    # "SELECT goog_id, COUNT(*) c FROM gdrive_file GROUP BY goog_id HAVING c > 1;"
                    logger.warning(f'Skipping what appears to be a duplicate entry: goog_id="{file.goog_id}", uid={file.uid}')
                    invalidate_uids[file.uid] = file.goog_id
                    continue
            else:
                raise RuntimeError(f'GDriveFile is missing goog_id: {file}')

            if tree.id_dict.get(file.uid, None):
                raise RuntimeError(f'GDrive cache conflict for UID: {file.uid} (1st: {tree.id_dict[file.uid]}; 2nd: {file}')
            tree.id_dict[file.uid] = file
            count_files_loaded += 1

            if file.uid >= max_uid:
                max_uid = file.uid

        logger.debug(f'{sw} Loaded {count_files_loaded} Google Drive files')

        if is_complete:
            # CHILD-PARENT MAPPINGS:
            sw = Stopwatch()
            id_parent_mappings = self.cache.get_id_parent_mappings()
            mapping_count = len(id_parent_mappings)

            for mapping in id_parent_mappings:
                item_uid = mapping[0]
                if invalidate_uids.get(item_uid, None):
                    logger.warning(f'Skipping parent mappings for uid={item_uid}')
                else:
                    parent_uid = mapping[1]
                    if parent_uid:
                        tree.add_parent_mapping(UID(item_uid), UID(parent_uid))

                        if parent_uid >= max_uid:
                            max_uid = parent_uid

            logger.debug(f'{sw} Loaded {mapping_count} Google Drive file-folder mappings')

        logger.debug(f'{sw_total} Loaded {len(tree.id_dict):n} items from {count_files_loaded:n} files and {count_folders_loaded:n} folders')

        self.app.uid_generator.ensure_next_uid_greater_than(max_uid)
        return tree

    def _determine_roots(self, tree: GDriveWholeTree):
        max_uid = GDRIVE_ROOT_UID + 1
        for item in tree.id_dict.values():
            if not item.get_parent_uids():
                tree.roots.append(item)

            if item.uid >= max_uid:
                max_uid = item.uid

        self.app.uid_generator.ensure_next_uid_greater_than(max_uid + 1)

    def _translate_parent_ids(self, tree: GDriveWholeTree, id_parent_mappings: List[Tuple[UID, None, str, int]]) -> List[Tuple]:
        sw = Stopwatch()
        logger.debug(f'Translating parent IDs for {len(tree.id_dict)} items...')

        new_mappings: List[Tuple] = []
        for mapping in id_parent_mappings:
            # [0]=item_uid, [1]=parent_uid, [2]=parent_goog_id, [3]=sync_ts
            assert not mapping[1]
            parent_goog_id: str = mapping[2]

            # Add parent UID to tuple for later DB update:
            parent_uid = self.cacheman.get_uid_for_goog_id(parent_goog_id)
            mapping = mapping[0], parent_uid, mapping[2], mapping[3]
            tree.add_parent_mapping(mapping[0], parent_uid)
            new_mappings.append(mapping)

        logger.debug(f'{sw} Filled in parent IDs')
        return new_mappings


# ▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲
# CLASS GDriveTreeLoader end


def _compile_full_paths(tree: GDriveWholeTree):
    full_path_stopwatch = Stopwatch()

    item_count: int = 0
    path_count: int = 0

    queue = deque()
    for root in tree.roots:
        root.node_identifier.full_path = '/' + root.name
        queue.append(root)
        item_count += 1
        path_count += 1

    while len(queue) > 0:
        item: GDriveNode = queue.popleft()
        children = tree.first_parent_dict.get(item.uid, None)
        if children:
            parent_paths = item.full_path
            if type(parent_paths) == str:
                parent_paths = [parent_paths]
            for child in children:
                # ensure list
                existing_child_paths = child.full_path
                if existing_child_paths:
                    if type(existing_child_paths) == str:
                        existing_child_paths = [existing_child_paths]
                else:
                    existing_child_paths = []

                # add parents:
                for parent_path in parent_paths:
                    existing_child_paths.append(os.path.join(parent_path, child.name))
                    path_count += 1

                if len(existing_child_paths) == 1:
                    child.node_identifier.full_path = existing_child_paths[0]
                else:
                    child.node_identifier.full_path = existing_child_paths

                item_count += 1

                if child.is_dir():
                    queue.append(child)

    logger.debug(f'{full_path_stopwatch} Constructed {path_count:n} full paths for {item_count:n} items')
