import logging
import os
from collections import deque
from typing import List, Tuple

from pydispatch import dispatcher

from backend.store.gdrive.query_observer import FileMetaPersister, FolderMetaPersister
from constants import GDRIVE_DOWNLOAD_STATE_COMPLETE, GDRIVE_DOWNLOAD_STATE_GETTING_DIRS, GDRIVE_DOWNLOAD_STATE_GETTING_NON_DIRS, \
    GDRIVE_DOWNLOAD_STATE_NOT_STARTED, \
    GDRIVE_DOWNLOAD_STATE_READY_TO_COMPILE, GDRIVE_DOWNLOAD_TYPE_CHANGES, GDRIVE_DOWNLOAD_TYPE_INITIAL_LOAD, GDRIVE_ROOT_UID, SUPER_DEBUG
from backend.store.gdrive.gdrive_whole_tree import GDriveWholeTree
from model.node.gdrive_node import GDriveFolder, GDriveNode
from model.node_identifier_factory import NodeIdentifierFactory
from model.uid import UID
from backend.store.gdrive.gdrive_client import GDriveClient
from backend.store.gdrive.master_gdrive_disk import GDriveDiskStore
from backend.store.gdrive.master_gdrive_op_load import GDriveLoadAllMetaOp
from backend.store.sqlite.gdrive_db import CurrentDownload
from signal_constants import Signal
from util import time_util
from util.stopwatch_sec import Stopwatch

logger = logging.getLogger(__name__)


class GDriveTreeLoader:
    """
    ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
    CLASS GDriveTreeLoader
    ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼
    """
    def __init__(self, backend, diskstore: GDriveDiskStore, tree_id: str = None):
        self.backend = backend
        self._diskstore: GDriveDiskStore = diskstore
        self.tree_id: str = tree_id

    @property
    def gdrive_client(self) -> GDriveClient:
        return self.backend.cacheman.get_gdrive_client()

    def load_all(self, invalidate_cache=False) -> GDriveWholeTree:
        logger.debug(f'GDriveTreeLoader.load_all() called with invalidate_cache={invalidate_cache}')

        try:
            # scroll down ⯆⯆⯆
            return self._load_all(invalidate_cache)
        finally:
            if self.tree_id:
                logger.debug(f'Sending STOP_PROGRESS for tree_id: {self.tree_id}')
                dispatcher.send(Signal.STOP_PROGRESS, sender=self.tree_id)

    def _load_all(self, invalidate_cache: bool) -> GDriveWholeTree:
        if self.tree_id:
            logger.debug(f'Sending START_PROGRESS_INDETERMINATE for tree_id: {self.tree_id}')
            dispatcher.send(Signal.START_PROGRESS_INDETERMINATE, sender=self.tree_id)

        sync_ts: int = time_util.now_sec()

        changes_download: CurrentDownload = self._diskstore.get_current_download(GDRIVE_DOWNLOAD_TYPE_CHANGES)
        if not changes_download or invalidate_cache:
            logger.debug(f'Getting a new start token for changes (invalidate_cache={invalidate_cache})')
            token: str = self.gdrive_client.get_changes_start_token()
            changes_download = CurrentDownload(GDRIVE_DOWNLOAD_TYPE_CHANGES, GDRIVE_DOWNLOAD_STATE_NOT_STARTED, token, sync_ts)
            self._diskstore.create_or_update_download(changes_download)

        initial_download: CurrentDownload = self._diskstore.get_current_download(GDRIVE_DOWNLOAD_TYPE_INITIAL_LOAD)
        if not initial_download or invalidate_cache:
            logger.debug(f'Starting a fresh download for entire Google Drive tree meta (invalidate_cache={invalidate_cache})')
            initial_download = CurrentDownload(GDRIVE_DOWNLOAD_TYPE_INITIAL_LOAD, GDRIVE_DOWNLOAD_STATE_NOT_STARTED, None, sync_ts)
            self._diskstore.create_or_update_download(initial_download)

            # Notify UI trees that their old roots are invalid:
            dispatcher.send(signal=Signal.GDRIVE_RELOADED, sender=self.tree_id)

        if initial_download.current_state == GDRIVE_DOWNLOAD_STATE_NOT_STARTED:
            # completely fresh tree
            tree = GDriveWholeTree(self.backend.node_identifier_factory)
        else:
            # Start/resume: read cache
            msg = 'Reading disk cache...'
            logger.info(msg)

            if self.tree_id:
                dispatcher.send(Signal.SET_PROGRESS_TEXT, sender=self.tree_id, msg=msg)

            # Load all users and MIME types
            self.backend.cacheman.execute_gdrive_load_op(GDriveLoadAllMetaOp())

            tree: GDriveWholeTree = self._diskstore.load_tree_from_cache(initial_download.is_complete(), self.tree_id)

        # TODO
        tree.me = self.gdrive_client.get_about()

        if not initial_download.is_complete():
            state = 'Starting' if initial_download.current_state == GDRIVE_DOWNLOAD_STATE_NOT_STARTED else 'Resuming'
            logger.info(f'{state} download of all Google Drive tree (state={initial_download.current_state})')

        # BEGIN STATE MACHINE:

        if initial_download.current_state == GDRIVE_DOWNLOAD_STATE_NOT_STARTED:
            self.backend.cacheman.delete_all_gdrive_data()

            # Need to make a special call to get the root node 'My Drive'. This node will not be included
            # in the "list files" call:
            initial_download.update_ts = sync_ts
            drive_root: GDriveFolder = self.gdrive_client.get_my_drive_root(initial_download.update_ts)
            tree.uid_dict[drive_root.uid] = drive_root

            initial_download.current_state = GDRIVE_DOWNLOAD_STATE_GETTING_DIRS
            initial_download.page_token = None
            self._diskstore.insert_gdrive_folder_list(folder_list=[drive_root], commit=False)
            self._diskstore.create_or_update_download(download=initial_download)
            # fall through

        if initial_download.current_state <= GDRIVE_DOWNLOAD_STATE_GETTING_DIRS:
            observer = FolderMetaPersister(tree, initial_download, self._diskstore, self.backend.cacheman)
            self.gdrive_client.get_all_folders(initial_download.page_token, initial_download.update_ts, observer)
            # fall through

        if initial_download.current_state <= GDRIVE_DOWNLOAD_STATE_GETTING_NON_DIRS:
            observer = FileMetaPersister(tree, initial_download, self._diskstore, self.backend.cacheman)
            self.gdrive_client.get_all_non_folders(initial_download.page_token, initial_download.update_ts, observer)
            # fall through

        if initial_download.current_state <= GDRIVE_DOWNLOAD_STATE_READY_TO_COMPILE:
            initial_download.current_state = GDRIVE_DOWNLOAD_STATE_COMPLETE

            # Some post-processing needed...

            # (1) Update all_children_fetched state
            tuples: List[Tuple[int, bool]] = []
            for goog_node in tree.uid_dict.values():
                if goog_node.is_dir():
                    goog_node.all_children_fetched = True
                    tuples.append((goog_node.uid, True))

            # Updating the entire table is much faster than doing a 'where' clause:
            self._diskstore.update_folder_fetched_status(commit=False)

            # (2) Set UIDs for parents, and assemble parent dict:
            parent_mappings: List[Tuple] = self._diskstore.get_id_parent_mappings()
            parent_mappings = self._translate_parent_ids(tree, parent_mappings)
            self._diskstore.insert_id_parent_mappings(parent_mappings, overwrite=True, commit=False)
            logger.debug(f'Updated {len(parent_mappings)} id-parent mappings')

            # (3) mark download finished
            self._diskstore.create_or_update_download(download=initial_download)
            logger.debug('GDrive data download complete.')

            # fall through

        # Still need to compute this in memory:
        self._determine_roots(tree)
        self._compile_full_paths(tree)

        # set cache_info.is_loaded=True:
        master_tree_root = NodeIdentifierFactory.get_gdrive_root_constant_single_path_identifier()
        cache_info = self.backend.cacheman.get_or_create_cache_info_entry(master_tree_root)
        cache_info.is_loaded = True

        logger.debug('GDrive: load_all() done')
        return tree

    def _determine_roots(self, tree: GDriveWholeTree):
        # TODO: can we roll this into the node loading?
        if SUPER_DEBUG:
            logger.debug(f'Determining roots for {len(tree.uid_dict)} GDrive nodes')
        max_uid = GDRIVE_ROOT_UID + 1
        for item in tree.uid_dict.values():
            if item.uid == GDRIVE_ROOT_UID:
                continue

            if not item.get_parent_uids():
                tree.get_children_for_root().append(item)

            if item.uid >= max_uid:
                max_uid = item.uid

        self.backend.uid_generator.ensure_next_uid_greater_than(max_uid + 1)

    def _translate_parent_ids(self, tree: GDriveWholeTree, id_parent_mappings: List[Tuple[UID, None, str, int]]) -> List[Tuple]:
        sw = Stopwatch()
        logger.debug(f'Translating parent IDs for {len(tree.uid_dict)} items...')

        new_mappings: List[Tuple] = []
        for mapping in id_parent_mappings:
            # [0]=item_uid, [1]=parent_uid, [2]=parent_goog_id, [3]=sync_ts
            assert not mapping[1]
            parent_goog_id: str = mapping[2]

            # Add parent UID to tuple for later DB update:
            parent_uid = self.backend.cacheman.get_uid_for_goog_id(parent_goog_id)
            mapping = mapping[0], parent_uid, mapping[2], mapping[3]
            tree.add_parent_mapping(mapping[0], parent_uid)
            new_mappings.append(mapping)

        logger.debug(f'{sw} Filled in parent IDs')
        return new_mappings

    @staticmethod
    def _compile_full_paths(tree: GDriveWholeTree):
        if SUPER_DEBUG:
            logger.debug(f'Compiling paths for {len(tree.uid_dict)} GDrive nodes')

        full_path_stopwatch = Stopwatch()

        item_count: int = 0
        path_count: int = 0

        queue = deque()
        for root in tree.get_children_for_root():
            root.node_identifier.set_path_list(f'/{root.name}')
            queue.append(root)
            item_count += 1
            path_count += 1

        while len(queue) > 0:
            parent: GDriveNode = queue.popleft()
            children = tree.parent_child_dict.get(parent.uid, None)
            if children:
                for child in children:
                    child_path_list: List[str] = child.get_path_list()

                    # add paths for this parent:
                    parent_path_list = parent.get_path_list()
                    # if len(parent_path_list) > 1:
                    #     logger.info(f'MULTIPLE PARENT PATHS ({len(parent_path_list)}) for node {parent.uid} ("{parent.name}")')
                    # if len(parent_path_list) > 10:
                    #     logger.warning(f'Large number of parents for node {child.uid} (possible cycle?): {len(parent_path_list)}')
                    for parent_path in parent_path_list:
                        new_child_path = os.path.join(parent_path, child.name)
                        # if len(new_child_path) > 1000:
                        #     logger.warning(f'Very long path found (possible cycle?): {new_child_path}')
                        # logger.info(f'[{path_count}] ({child.uid}) Adding path "{new_child_path}" to  paths ({child_path_list})')
                        if new_child_path not in child_path_list:
                            child_path_list.append(new_child_path)
                            path_count += 1

                    child.node_identifier.set_path_list(child_path_list)

                    item_count += 1

                    if child.is_dir():
                        queue.append(child)

        logger.debug(f'{full_path_stopwatch} Constructed {path_count:n} full paths for {item_count:n} items')


# ▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲
# CLASS GDriveTreeLoader end
