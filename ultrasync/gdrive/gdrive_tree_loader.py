import logging
import os
import time
import uuid
from typing import List, Tuple

from constants import GDRIVE_DOWNLOAD_STATE_COMPLETE, GDRIVE_DOWNLOAD_STATE_GETTING_DIRS, GDRIVE_DOWNLOAD_STATE_GETTING_NON_DIRS, \
    GDRIVE_DOWNLOAD_STATE_NOT_STARTED, \
    GDRIVE_DOWNLOAD_STATE_READY_TO_COMPILE, GDRIVE_DOWNLOAD_TYPE_LOAD_ALL
from gdrive.client import GDriveClient
from index.sqlite.gdrive_db import CurrentDownload, GDriveDatabase
from model.gdrive_whole_tree import GDriveWholeTree
from model.goog_node import GoogFile, GoogFolder
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

    def _compile_full_paths(self, meta: GDriveWholeTree, sync_ts: int) -> List[Tuple]:
        full_path_stopwatch = Stopwatch()
        id_parent_path_tuples: List[Tuple] = []

        for item in meta.id_dict.values():
            full_paths: List[str] = meta.get_full_paths_for_item(item)
            parent_ids: List[str] = item.parent_ids
            if parent_ids:
                if len(parent_ids) == 1 and len(full_paths) == 1:
                    id_parent_path_tuples.append((item.uid, item.parent_ids[0], full_paths[0], sync_ts))
                else:
                    tuples: List[Tuple] = []
                    for full_path in full_paths:
                        matching_parent_id = _find_matching_parent(meta, item, full_path)
                        if matching_parent_id:
                            tuples.append((item.uid, matching_parent_id, full_path, sync_ts))
                        else:
                            # Node is serving as a root
                            tuples.append((item.uid, None, full_path, sync_ts))
                    assert len(tuples) == len(full_paths)
                    # this expression is beautiful
                    assert len(parent_ids) == len(set(map(lambda x: x[1], [x for x in tuples if x[1]]))), \
                        f'Failed to match each full_path to a unique parent! item={item} full_paths={full_paths} tuples={tuples}'
                    id_parent_path_tuples += tuples

            else:
                if len(full_paths) > 1:
                    logger.warning(f'It appears a root node has multiple paths somehow! Paths: {full_paths}, item: {item}')
                for full_path in full_paths:
                    id_parent_path_tuples.append((item.uid, None, full_path, sync_ts))

        logger.debug(f'{full_path_stopwatch} Full paths calculated for {len(meta.id_dict)} items')
        return id_parent_path_tuples

    def load_all(self, invalidate_cache=False) -> GDriveWholeTree:
        if not invalidate_cache and not os.path.exists(self.cache_path):
            raise FileNotFoundError(self.cache_path)
        # This will create a new file if not found:
        with GDriveDatabase(self.cache_path) as self.cache:
            try:
                # scroll down ⯆⯆⯆
                return self._load_all(invalidate_cache)
            finally:
                if self.tree_id:
                    logger.debug(f'Sending STOP_PROGRESS for tree_id: {self.tree_id}')
                    actions.get_dispatcher().send(actions.STOP_PROGRESS, sender=self.tree_id, tx_id=self.tx_id)

    def _load_all(self, invalidate_cache=False) -> GDriveWholeTree:
        if self.tree_id:
            logger.debug(f'Sending START_PROGRESS_INDETERMINATE for tree_id: {self.tree_id}')
            actions.get_dispatcher().send(actions.START_PROGRESS_INDETERMINATE, sender=self.tree_id, tx_id=self.tx_id)

        meta: GDriveWholeTree = GDriveWholeTree()

        meta.me = self.gdrive_client.get_about()

        sync_ts: int = int(time.time())

        download: CurrentDownload = self._get_previous_download_state(GDRIVE_DOWNLOAD_TYPE_LOAD_ALL)
        if not download or invalidate_cache:
            logger.info('Starting a new download of all Google Drive meta')
            download = CurrentDownload(GDRIVE_DOWNLOAD_TYPE_LOAD_ALL, GDRIVE_DOWNLOAD_STATE_NOT_STARTED, None, sync_ts)
            self.cache.create_or_update_download(download)

        if download.current_state == GDRIVE_DOWNLOAD_STATE_COMPLETE:
            logger.info('All meta already downloaded; loading from cache')
            # Load all data
            if self.tree_id:
                msg = 'Reading cache...'
                actions.get_dispatcher().send(actions.SET_PROGRESS_TEXT, sender=self.tree_id, tx_id=self.tx_id, msg=msg)

            self._load_all_from_cache(meta)
            return meta

        state = 'Starting' if download.current_state == GDRIVE_DOWNLOAD_STATE_NOT_STARTED else 'Resuming'
        logger.info(f'{state} download of all Google Drive meta (state={download.current_state})')

        if download.current_state == GDRIVE_DOWNLOAD_STATE_NOT_STARTED:
            self.cache.delete_all_gdrive_data()

            # Need to make a special call to get the root node 'My Drive'. This node will not be included
            # in the "list files" call:
            download.update_ts = sync_ts
            drive_root: GoogFolder = self.gdrive_client.get_my_drive_root(download.update_ts)
            meta.add_item(drive_root)

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

        if download.current_state <= GDRIVE_DOWNLOAD_STATE_READY_TO_COMPILE:
            id_parent_path_mappings = self._compile_full_paths(meta, sync_ts)
            download.current_state = GDRIVE_DOWNLOAD_STATE_COMPLETE
            # FIXME: after we confirm it works, update instead of insert
            self.cache.insert_id_parent_mappings(id_parent_mappings=id_parent_path_mappings, commit=False)
            self.cache.create_or_update_download(download=download)

        # Save a copy of the entire cache:
        for goog_dir in meta.first_parent_dict.values():
            goog_dir.all_children_fetched = True

        # Save to cache:
        msg = 'Saving to cache...'
        if self.tree_id:
            actions.get_dispatcher().send(actions.SET_PROGRESS_TEXT, sender=self.tree_id, tx_id=self.tx_id, msg=msg)
        logger.debug(msg)
        self.save_all_to_cache(meta=meta, overwrite=True)

        return meta

    def save_all_to_cache(self, meta, overwrite):
        logger.info('Saving to cache again')
        self.cache.close()
        self.cache = GDriveDatabase(self.cache_path + '.second.db')

        # Convert to tuples for insert into DB:
        dir_tuples: List[Tuple] = []
        file_tuples: List[Tuple] = []
        for item in meta.id_dict.values():
            if item.is_dir():
                dir_tuples.append(item.to_tuple())
            else:
                file_tuples.append(item.to_tuple())

        sync_ts: int = int(time.time())
        id_parent_path_mappings = self._compile_full_paths(meta, sync_ts)

        self.cache.insert_gdrive_dirs(dir_list=dir_tuples, overwrite=overwrite, commit=False)
        self.cache.insert_gdrive_files(file_list=file_tuples, overwrite=overwrite, commit=False)
        self.cache.insert_id_parent_mappings(id_parent_mappings=id_parent_path_mappings, overwrite=overwrite)

    def _load_all_from_cache(self, meta: GDriveWholeTree):

        if not self.cache.has_gdrive_dirs() or not self.cache.has_gdrive_files():
            raise RuntimeError(f'Cache is corrupted: {self.cache_path}')

        # DIRs:
        dir_rows = self.cache.get_gdrive_dirs()
        actions.get_dispatcher().send(actions.SET_PROGRESS_TEXT, sender=self.tree_id, tx_id=self.tx_id, msg=f'Retreived {len(dir_rows):n} dirs')

        for item_id, item_name, item_trashed, drive_id, my_share, sync_ts, all_children_fetched in dir_rows:
            item = GoogFolder(item_id=item_id, item_name=item_name,
                              trashed=item_trashed, drive_id=drive_id, my_share=my_share,
                              sync_ts=sync_ts, all_children_fetched=all_children_fetched)
            meta.add_item(item)

        # FILES:
        file_rows = self.cache.get_gdrive_files()
        actions.get_dispatcher().send(actions.SET_PROGRESS_TEXT, sender=self.tree_id, tx_id=self.tx_id, msg=f'Retreived {len(file_rows):n} files')
        for item_id, item_name, item_trashed, size_bytes_str, md5, create_ts, modify_ts, owner_id, drive_id, \
                my_share, version, head_revision_id, sync_ts in file_rows:
            size_bytes = None if size_bytes_str is None else int(size_bytes_str)
            file_node = GoogFile(item_id=item_id, item_name=item_name,
                                 trashed=item_trashed, drive_id=drive_id, my_share=my_share, version=int(version),
                                 head_revision_id=head_revision_id, md5=md5,
                                 create_ts=int(create_ts), modify_ts=int(modify_ts), size_bytes=size_bytes,
                                 owner_id=owner_id, sync_ts=sync_ts)
            meta.add_item(file_node)

        # CHILD-PARENT MAPPINGS:
        id_parent_mappings = self.cache.get_id_parent_mappings()
        for item_id, parent_id, full_path in id_parent_mappings:
            # FIXME
            pass


def _find_matching_parent(meta, item, full_path):
    # The matching parent will have at least one path which matches the given full path (going up one path segment)
    potential_parent_path = os.path.split(full_path)[0]
    assert potential_parent_path and potential_parent_path != '/', f'For {potential_parent_path}'
    for parent_id in item.parent_ids:
        for parent_full_path in meta.get_full_paths_for_item(meta.get_item_for_id(parent_id)):
            if potential_parent_path == parent_full_path:
                return parent_id
    return None
