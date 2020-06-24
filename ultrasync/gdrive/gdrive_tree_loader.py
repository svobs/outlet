import logging
import os
import time
from collections import deque
from typing import Dict, List, Optional, Tuple

from constants import GDRIVE_DOWNLOAD_STATE_COMPLETE, GDRIVE_DOWNLOAD_STATE_GETTING_DIRS, GDRIVE_DOWNLOAD_STATE_GETTING_NON_DIRS, \
    GDRIVE_DOWNLOAD_STATE_NOT_STARTED, \
    GDRIVE_DOWNLOAD_STATE_READY_TO_COMPILE, GDRIVE_DOWNLOAD_TYPE_LOAD_ALL, ROOT_UID, TREE_TYPE_GDRIVE
from gdrive.client import GDriveClient, MetaObserver
from index.sqlite.gdrive_db import CurrentDownload, GDriveDatabase
from index.uid import UID
from model.gdrive_whole_tree import GDriveWholeTree
from model.goog_node import GoogFile, GoogFolder, GoogNode
from model.node_identifier import GDriveIdentifier
from stopwatch_sec import Stopwatch
from ui import actions

logger = logging.getLogger(__name__)


# CLASS FolderMetaPersister
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼

class FolderMetaPersister(MetaObserver):
    """Collect GDrive folder metas for mass insertion into database"""
    def __init__(self, tree: GDriveWholeTree, download: CurrentDownload, cache: GDriveDatabase):
        super().__init__()
        self.tree = tree
        assert download.current_state == GDRIVE_DOWNLOAD_STATE_GETTING_DIRS
        self.download: CurrentDownload = download
        self.cache: GDriveDatabase = cache
        self.dir_tuples: List[Tuple] = []
        self.id_parent_mappings: List[Tuple] = []

    def meta_received(self, goog_node, item):
        parent_google_ids = item.get('parents', [])
        self.tree.id_dict[goog_node.uid] = goog_node
        self.dir_tuples.append(goog_node.to_tuple())

        self.id_parent_mappings += parent_mappings_tuples(goog_node.uid, parent_google_ids, sync_ts=self.download.update_ts)

    def end_of_page(self, next_page_token):
        self.download.page_token = next_page_token
        if not next_page_token:
            # done
            assert self.download.current_state == GDRIVE_DOWNLOAD_STATE_GETTING_DIRS
            self.download.current_state = GDRIVE_DOWNLOAD_STATE_GETTING_NON_DIRS
            # fall through

        self.cache.insert_gdrive_dirs_and_parents(dir_list=self.dir_tuples, parent_mappings=self.id_parent_mappings, current_download=self.download)

        if next_page_token:
            # Clear the buffers for reuse:
            self.dir_tuples = []
            self.id_parent_mappings = []


# CLASS FileMetaPersister
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼

class FileMetaPersister(MetaObserver):
    """Collect GDrive file metas for mass insertion into database"""
    def __init__(self, tree: GDriveWholeTree, download: CurrentDownload, cache: GDriveDatabase):
        super().__init__()
        self.tree = tree
        assert download.current_state == GDRIVE_DOWNLOAD_STATE_GETTING_NON_DIRS
        self.download: CurrentDownload = download
        self.cache: GDriveDatabase = cache
        self.file_tuples: List[Tuple] = []
        self.id_parent_mappings: List[Tuple] = []

    def meta_received(self, goog_node, item):
        parent_google_ids = item.get('parents', [])
        self.tree.id_dict[goog_node.uid] = goog_node
        self.file_tuples.append(goog_node.to_tuple())

        self.id_parent_mappings += parent_mappings_tuples(goog_node.uid, parent_google_ids, sync_ts=self.download.update_ts)

    def end_of_page(self, next_page_token):
        self.download.page_token = next_page_token
        if not next_page_token:
            # done
            assert self.download.current_state == GDRIVE_DOWNLOAD_STATE_GETTING_NON_DIRS
            self.download.current_state = GDRIVE_DOWNLOAD_STATE_READY_TO_COMPILE
            # fall through

        self.cache.insert_gdrive_files_and_parents(file_list=self.file_tuples, parent_mappings=self.id_parent_mappings,
                                                   current_download=self.download)

        if next_page_token:
            # Clear the buffers for reuse:
            self.file_tuples = []
            self.id_parent_mappings = []


def parent_mappings_tuples(item_uid: UID, parent_goog_ids: List[str], sync_ts: int) -> List[Tuple[UID, Optional[UID], str, int]]:
    tuples = []
    for parent_goog_id in parent_goog_ids:
        tuples.append((item_uid, None, parent_goog_id, sync_ts))
    return tuples


# CLASS GDriveTreeLoader
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼

class GDriveTreeLoader:
    def __init__(self, application, cache_path, tree_id=None):
        self.node_identifier_factory = application.node_identifier_factory
        self.uid_generator = application.uid_generator
        self.cache_manager = application.cache_manager
        self.config = application.config
        self.tree_id = tree_id
        self.cache_path = cache_path
        self.cache = None
        self.gdrive_client = GDriveClient(application, tree_id)

    def _get_previous_download_state(self, download_type: int):
        for download in self.cache.get_current_downloads():
            if download.download_type == download_type:
                return download
        return None

    def load_all(self, invalidate_cache=False) -> GDriveWholeTree:
        # This will create a new file if not found:
        with GDriveDatabase(self.cache_path) as self.cache:
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

        download: CurrentDownload = self._get_previous_download_state(GDRIVE_DOWNLOAD_TYPE_LOAD_ALL)
        if not download or invalidate_cache:
            logger.debug(f'Starting a fresh download for entire Google Drive tree meta (invalidate_cache={invalidate_cache}')
            download = CurrentDownload(GDRIVE_DOWNLOAD_TYPE_LOAD_ALL, GDRIVE_DOWNLOAD_STATE_NOT_STARTED, None, sync_ts)
            self.cache.create_or_update_download(download)

        if download.current_state == GDRIVE_DOWNLOAD_STATE_NOT_STARTED:
            # completely fresh tree
            tree = GDriveWholeTree(self.node_identifier_factory)
        else:
            # Start/resume: read cache
            msg = 'Reading cache...'
            logger.info(msg)

            if self.tree_id:
                actions.get_dispatcher().send(actions.SET_PROGRESS_TEXT, sender=self.tree_id, msg=msg)

            tree: GDriveWholeTree = self._load_tree_from_cache(download.is_complete())

        # TODO
        tree.me = self.gdrive_client.get_about()

        if not download.is_complete():
            state = 'Starting' if download.current_state == GDRIVE_DOWNLOAD_STATE_NOT_STARTED else 'Resuming'
            logger.info(f'{state} download of all Google Drive tree (state={download.current_state})')

        # BEGIN STATE MACHINE:

        if download.current_state == GDRIVE_DOWNLOAD_STATE_NOT_STARTED:
            self.cache.delete_all_gdrive_data()

            # Need to make a special call to get the root node 'My Drive'. This node will not be included
            # in the "list files" call:
            download.update_ts = sync_ts
            drive_root: GoogFolder = self.gdrive_client.get_meta_my_drive_root(download.update_ts)
            tree.id_dict[drive_root.uid] = drive_root

            download.current_state = GDRIVE_DOWNLOAD_STATE_GETTING_DIRS
            download.page_token = None
            self.cache.insert_gdrive_dirs(dir_list=[drive_root.to_tuple()], commit=False)
            self.cache.create_or_update_download(download=download)
            # fall through

        if download.current_state <= GDRIVE_DOWNLOAD_STATE_GETTING_DIRS:
            observer = FolderMetaPersister(tree, download, self.cache)
            self.gdrive_client.get_meta_all_directories(download.page_token, download.update_ts, observer)
            # fall through

        if download.current_state <= GDRIVE_DOWNLOAD_STATE_GETTING_NON_DIRS:
            observer = FileMetaPersister(tree, download, self.cache)
            self.gdrive_client.get_meta_all_files(download.page_token, download.update_ts, observer)
            # fall through

        if download.current_state <= GDRIVE_DOWNLOAD_STATE_READY_TO_COMPILE:
            download.current_state = GDRIVE_DOWNLOAD_STATE_COMPLETE

            # Some post-processing needed...

            # (1) Update all_children_fetched state
            tuples: List[Tuple[int, bool]] = []
            for goog_node in tree.id_dict.values():
                if goog_node.is_dir():
                    goog_node.all_children_fetched = True
                    tuples.append((goog_node.uid, True))

            # Updating the entire table is much faster than doing a 'where' clause:
            self.cache.update_dir_fetched_status(commit=False)

            # (2) Set UIDs for parents, and assemble parent dict:
            parent_mappings: List[Tuple] = self.cache.get_id_parent_mappings()
            parent_mappings = self._translate_parent_ids(tree, parent_mappings)
            self.cache.insert_id_parent_mappings(parent_mappings, overwrite=True, commit=False)
            logger.debug(f'Updated {len(parent_mappings)} id-parent mappings')

            # (3) mark download finished
            self.cache.create_or_update_download(download=download)
            logger.debug('GDrive data download complete.')

            # fall through

        # Still need to compute this in memory:
        self._determine_roots(tree)
        _compile_full_paths(tree)

        return tree

    def _load_tree_from_cache(self, is_complete: bool) -> GDriveWholeTree:
        """
        Retrieves and reassembles (to the extent it was during the download) a partially or completely downloaded
        GDrive tree.
        """
        sw_total = Stopwatch()
        max_uid = ROOT_UID + 1
        tree = GDriveWholeTree(self.node_identifier_factory)
        invalidate_uids: Dict[UID, str] = {}

        items_without_goog_ids: List[GoogNode] = []

        # DIRs:
        sw = Stopwatch()
        dir_rows = self.cache.get_gdrive_dirs()
        dir_count = len(dir_rows)

        actions.get_dispatcher().send(actions.SET_PROGRESS_TEXT, sender=self.tree_id, msg=f'Retrieved {len(dir_rows):n} Google Drive dirs')

        for uid_int, goog_id, item_name, item_trashed, drive_id, my_share, sync_ts, all_children_fetched in dir_rows:
            uid_from_cache = UID(uid_int)
            item = GoogFolder(GDriveIdentifier(uid=uid_from_cache, full_path=None), goog_id=goog_id, item_name=item_name,
                              trashed=item_trashed, drive_id=drive_id, my_share=my_share,
                              sync_ts=sync_ts, all_children_fetched=all_children_fetched)

            if goog_id:
                uid = self.cache_manager.get_uid_for_goog_id(goog_id, uid_from_cache)
                if uid_from_cache != uid:
                    # Duplicate entry with same goog_id. Here's a useful SQLite query:
                    # "SELECT goog_id, COUNT(*) c FROM goog_file GROUP BY goog_id HAVING c > 1;"
                    logger.warning(f'Skipping what appears to be a duplicate entry: goog_id="{goog_id}", uid={uid_from_cache}')
                    invalidate_uids[uid_from_cache] = goog_id
                    continue
            else:
                # no goog_id: indicates a node being copied
                self.uid_generator.ensure_next_uid_greater_than(max_uid)
                items_without_goog_ids.append(item)

            tree.id_dict[uid_from_cache] = item

            if item.uid >= max_uid:
                max_uid = item.uid

        logger.debug(f'{sw} Loaded {dir_count} Google Drive folders')

        # FILES:
        sw = Stopwatch()
        file_rows = self.cache.get_gdrive_files()
        file_count = len(file_rows)

        actions.get_dispatcher().send(actions.SET_PROGRESS_TEXT, sender=self.tree_id, msg=f'Retreived {len(file_rows):n} Google Drive files')

        for uid_int, goog_id, item_name, item_trashed, size_bytes, md5, create_ts, modify_ts, owner_id, drive_id, \
                my_share, version, head_revision_id, sync_ts in file_rows:
            uid_from_cache = UID(uid_int)

            item = GoogFile(GDriveIdentifier(uid=uid_from_cache, full_path=None), goog_id=goog_id, item_name=item_name,
                            trashed=item_trashed, drive_id=drive_id, my_share=my_share, version=version,
                            head_revision_id=head_revision_id, md5=md5,
                            create_ts=create_ts, modify_ts=modify_ts, size_bytes=size_bytes,
                            owner_id=owner_id, sync_ts=sync_ts)

            if goog_id:
                uid = self.cache_manager.get_uid_for_goog_id(goog_id, uid_from_cache)
                if uid_from_cache != uid:
                    # Duplicate entry with same goog_id. Here's a useful SQLite query:
                    # "SELECT goog_id, COUNT(*) c FROM goog_file GROUP BY goog_id HAVING c > 1;"
                    logger.warning(f'Skipping what appears to be a duplicate entry: goog_id="{goog_id}", uid={uid_from_cache}')
                    invalidate_uids[uid_from_cache] = goog_id
                    continue
            else:
                # no goog_id: indicates a node being copied
                self.uid_generator.ensure_next_uid_greater_than(max_uid)
                items_without_goog_ids.append(item)

            tree.id_dict[uid_from_cache] = item

            if item.uid >= max_uid:
                max_uid = item.uid

        logger.debug(f'{sw} Loaded {file_count} Google Drive files')

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

        logger.debug(f'{sw_total} Loaded {len(tree.id_dict):n} items from {file_count:n} file rows and {dir_count:n} dir rows')
        logger.info(f'Found {len(items_without_goog_ids)} cached items which do not have goog_ids')
        # TODO: do stuff with the above items ^^^

        self.uid_generator.ensure_next_uid_greater_than(max_uid)
        return tree

    def _determine_roots(self, tree: GDriveWholeTree):
        max_uid = ROOT_UID + 1
        for item in tree.id_dict.values():
            if not item.get_parent_uids():
                tree.roots.append(item)

            if item.uid >= max_uid:
                max_uid = item.uid

        self.uid_generator.ensure_next_uid_greater_than(max_uid + 1)

    def _translate_parent_ids(self, tree: GDriveWholeTree, id_parent_mappings: List[Tuple[UID, None, str, int]]) -> List[Tuple]:
        sw = Stopwatch()
        logger.debug(f'Translating parent IDs for {len(tree.id_dict)} items...')

        new_mappings: List[Tuple] = []
        for mapping in id_parent_mappings:
            # [0]=item_uid, [1]=parent_uid, [2]=parent_goog_id, [3]=sync_ts
            assert not mapping[1]
            parent_goog_id: str = mapping[2]

            # Add parent UID to tuple for later DB update:
            parent_uid = self.cache_manager.get_uid_for_goog_id(parent_goog_id)
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
        item: GoogNode = queue.popleft()
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
