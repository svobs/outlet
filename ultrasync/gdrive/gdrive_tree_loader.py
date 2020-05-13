import logging
import os
import time
import uuid
from collections import deque
from typing import Dict, List, Tuple

from constants import GDRIVE_DOWNLOAD_STATE_COMPLETE, GDRIVE_DOWNLOAD_STATE_GETTING_DIRS, GDRIVE_DOWNLOAD_STATE_GETTING_NON_DIRS, \
    GDRIVE_DOWNLOAD_STATE_NOT_STARTED, \
    GDRIVE_DOWNLOAD_STATE_READY_TO_COMPILE, GDRIVE_DOWNLOAD_TYPE_LOAD_ALL, ROOT_UID
from gdrive.client import GDriveClient
from index.sqlite.gdrive_db import CurrentDownload, GDriveDatabase
from model.gdrive_whole_tree import GDriveWholeTree
from model.goog_node import GoogFile, GoogFolder, GoogNode
from stopwatch_sec import Stopwatch
from ui import actions

logger = logging.getLogger(__name__)

"""
▛▝▝▝▝▝▝▝▝▝▝▝▝▝▝▝▝▝▝▝▝▝▝▝▝▝▝▝▝▝▝▝▝▝ ▜
          Class GDriveTreeLoader
▙ ▖▖▖▖▖▖▖▖▖▖▖▖▖▖▖▖▖▖▖▖▖▖▖▖▖▖▖▖▖▖▖▖▖▟
"""


class GDriveTreeLoader:
    def __init__(self, config, cache_path, tree_id=None):
        self.config = config
        self.tree_id = tree_id
        self.tx_id = uuid.uuid1()
        self.cache_path = cache_path
        self.cache = None
        self.gdrive_client = GDriveClient(self.config, tree_id)

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
                    actions.get_dispatcher().send(actions.STOP_PROGRESS, sender=self.tree_id, tx_id=self.tx_id)

    def _load_all(self, invalidate_cache: bool) -> GDriveWholeTree:
        if self.tree_id:
            logger.debug(f'Sending START_PROGRESS_INDETERMINATE for tree_id: {self.tree_id}')
            actions.get_dispatcher().send(actions.START_PROGRESS_INDETERMINATE, sender=self.tree_id, tx_id=self.tx_id)

        sync_ts: int = int(time.time())

        download: CurrentDownload = self._get_previous_download_state(GDRIVE_DOWNLOAD_TYPE_LOAD_ALL)
        if not download or invalidate_cache:
            logger.debug(f'Starting a fresh download for all Google Drive meta (invalidate_cache={invalidate_cache}')
            download = CurrentDownload(GDRIVE_DOWNLOAD_TYPE_LOAD_ALL, GDRIVE_DOWNLOAD_STATE_NOT_STARTED, None, sync_ts)
            self.cache.create_or_update_download(download)

        if download.current_state == GDRIVE_DOWNLOAD_STATE_NOT_STARTED:
            # completely fresh tree
            meta = GDriveWholeTree()
        else:
            # Start/resume: read cache
            msg = 'Reading cache...'
            logger.info(msg)

            if self.tree_id:
                actions.get_dispatcher().send(actions.SET_PROGRESS_TEXT, sender=self.tree_id, tx_id=self.tx_id, msg=msg)

            meta: GDriveWholeTree = self._load_tree_from_cache(download.is_complete())

        # TODO
        meta.me = self.gdrive_client.get_about()

        if not download.is_complete():
            state = 'Starting' if download.current_state == GDRIVE_DOWNLOAD_STATE_NOT_STARTED else 'Resuming'
            logger.info(f'{state} download of all Google Drive meta (state={download.current_state})')

        # BEGIN STATE MACHINE:

        if download.current_state == GDRIVE_DOWNLOAD_STATE_NOT_STARTED:
            self.cache.delete_all_gdrive_data()

            # Need to make a special call to get the root node 'My Drive'. This node will not be included
            # in the "list files" call:
            download.update_ts = sync_ts
            drive_root: GoogFolder = self.gdrive_client.get_my_drive_root(meta.get_new_uid(), download.update_ts)
            meta.id_dict[drive_root.uid] = drive_root

            download.current_state = GDRIVE_DOWNLOAD_STATE_GETTING_DIRS
            download.page_token = None
            self.cache.insert_gdrive_dirs(dir_list=[drive_root.to_tuple()], commit=False)
            self.cache.create_or_update_download(download=download)
            # fall through

        if download.current_state <= GDRIVE_DOWNLOAD_STATE_GETTING_DIRS:
            self.gdrive_client.download_directory_structure(meta, self.cache, download)
            # fall through

        if download.current_state <= GDRIVE_DOWNLOAD_STATE_GETTING_NON_DIRS:
            self.gdrive_client.download_all_file_meta(meta, self.cache, download)
            # fall through

        if download.current_state <= GDRIVE_DOWNLOAD_STATE_READY_TO_COMPILE:
            download.current_state = GDRIVE_DOWNLOAD_STATE_COMPLETE

            # Some post-processing needed...

            # (1) Update all_children_fetched state
            tuples: List[Tuple[int, bool]] = []
            for goog_node in meta.id_dict.values():
                if goog_node.is_dir():
                    goog_node.all_children_fetched = True
                    tuples.append((goog_node.uid, True))

            # Updating the entire table is much faster than doing a 'where' clause:
            self.cache.update_dir_fetched_status(commit=False)

            # (2) Set UIDs for parents, and assemble parent dict:
            parent_mappings: List[Tuple] = self.cache.get_id_parent_mappings()
            parent_mappings = _translate_parent_ids(meta, parent_mappings)
            self.cache.insert_id_parent_mappings(parent_mappings, overwrite=True, commit=False)
            logger.debug(f'Updated {len(parent_mappings)} id-parent mappings')

            # (3) mark download finished
            self.cache.create_or_update_download(download=download)
            logger.debug('GDrive data download complete.')

            # fall through

        # Still need to compute this in memory:
        _determine_roots(meta)
        _compile_full_paths(meta)

        return meta

    def _load_tree_from_cache(self, is_complete: bool) -> GDriveWholeTree:
        """
        Retrieves and reassembles (to the extent it was during the download) a partially or completely downloaded
        GDrive tree.
        """

        if not self.cache.has_gdrive_dirs() or not self.cache.has_gdrive_files():
            raise RuntimeError(f'Cache is corrupted: {self.cache_path}')

        sw_total = Stopwatch()
        tree = GDriveWholeTree()

        # DIRs:
        sw = Stopwatch()
        dir_rows = self.cache.get_gdrive_dirs()
        dir_count = len(dir_rows)

        actions.get_dispatcher().send(actions.SET_PROGRESS_TEXT, sender=self.tree_id, tx_id=self.tx_id, msg=f'Retreived {len(dir_rows):n} dirs')

        for uid, goog_id, item_name, item_trashed, drive_id, my_share, sync_ts, all_children_fetched in dir_rows:
            item = GoogFolder(uid=uid, goog_id=goog_id, item_name=item_name,
                              trashed=item_trashed, drive_id=drive_id, my_share=my_share,
                              sync_ts=sync_ts, all_children_fetched=all_children_fetched)
            tree.id_dict[uid] = item

        logger.debug(f'{sw} Loaded {dir_count} folders')

        # FILES:
        sw = Stopwatch()
        file_rows = self.cache.get_gdrive_files()
        file_count = len(file_rows)

        actions.get_dispatcher().send(actions.SET_PROGRESS_TEXT, sender=self.tree_id, tx_id=self.tx_id, msg=f'Retreived {len(file_rows):n} files')

        for uid, goog_id, item_name, item_trashed, size_bytes_str, md5, create_ts, modify_ts, owner_id, drive_id, \
                my_share, version, head_revision_id, sync_ts in file_rows:
            size_bytes = None if size_bytes_str is None else int(size_bytes_str)
            item = GoogFile(uid=uid, goog_id=goog_id, item_name=item_name,
                            trashed=item_trashed, drive_id=drive_id, my_share=my_share, version=int(version),
                            head_revision_id=head_revision_id, md5=md5,
                            create_ts=int(create_ts), modify_ts=int(modify_ts), size_bytes=size_bytes,
                            owner_id=owner_id, sync_ts=sync_ts)
            tree.id_dict[uid] = item

        logger.debug(f'{sw} Loaded {file_count} files')

        if is_complete:

            # CHILD-PARENT MAPPINGS:
            sw = Stopwatch()
            id_parent_mappings = self.cache.get_id_parent_mappings()
            mapping_count = len(id_parent_mappings)

            for mapping in id_parent_mappings:
                item_uid = mapping[0]
                parent_uid = mapping[1]

                tree.add_parent_mapping(item_uid, parent_uid)

            logger.debug(f'{sw} Loaded {mapping_count} mappings')

        logger.debug(f'{sw_total} Loaded {len(tree.id_dict):n} items from {file_count:n} file rows and {dir_count:n} dir rows')
        return tree


def _translate_parent_ids(tree: GDriveWholeTree, id_parent_mappings: List[Tuple]) -> List[Tuple]:
    sw = Stopwatch()
    logger.debug(f'Translating parent IDs for {len(tree.id_dict)} items...')

    # Create temporary dict for Google IDs
    goog_id_dict: dict[str, GoogNode] = {}
    for item in tree.id_dict.values():
        goog_id_dict[item.goog_id] = item

    unresolved_parents = []
    new_mappings: List[Tuple] = []
    for mapping in id_parent_mappings:
        # [0]=item_uid, [1]=parent_uid, [2]=parent_goog_id, [3]=sync_ts
        assert not mapping[1]
        parent_goog_id = mapping[2]

        # Add parent UID to tuple for later DB update:
        parent = goog_id_dict.get(parent_goog_id)
        if not parent:
            unresolved_parents.append(parent_goog_id)
        else:
            mapping = mapping[0], parent.uid, mapping[2], mapping[3]
            tree.add_parent_mapping(mapping[0], parent.uid)
        new_mappings.append(mapping)

    logger.debug(f'{sw} Filled in parent IDs')
    if unresolved_parents:
        logger.warning(f'{len(unresolved_parents)} parent IDs could not be resolved: {unresolved_parents}')
    return new_mappings


def _determine_roots(tree: GDriveWholeTree):
    max_uid = ROOT_UID + 1
    for item in tree.id_dict.values():
        if not item.parent_ids:
            tree.roots.append(item)

        if item.uid >= max_uid:
            max_uid = item.uid

    tree.set_next_uid(max_uid + 1)


def _compile_full_paths(tree: GDriveWholeTree):
    full_path_stopwatch = Stopwatch()

    item_count: int = 0
    path_count: int = 0

    queue = deque()
    for root in tree.roots:
        root.identifier.full_path = '/' + root.name
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
                    child.identifier.full_path = existing_child_paths[0]
                else:
                    child.identifier.full_path = existing_child_paths

                item_count += 1

                if child.is_dir():
                    queue.append(child)

    logger.debug(f'{full_path_stopwatch} Constructed {path_count:n} full paths for {item_count:n} items')
