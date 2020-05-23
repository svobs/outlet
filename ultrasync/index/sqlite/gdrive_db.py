import logging
from typing import List, Optional, Tuple

from constants import GDRIVE_DOWNLOAD_STATE_COMPLETE
from index.sqlite.base_db import MetaDatabase

logger = logging.getLogger(__name__)


class CurrentDownload:
    def __init__(self, download_type: int, current_state: int, page_token: Optional[str], update_ts: int):
        self.download_type = download_type
        self.current_state = current_state
        self.page_token = page_token
        self.update_ts = update_ts

    def to_tuple(self):
        return self.download_type, self.current_state, self.page_token, self.update_ts

    def is_complete(self):
        return self.current_state == GDRIVE_DOWNLOAD_STATE_COMPLETE


class GDriveDatabase(MetaDatabase):

    TABLE_GRDIVE_CURRENT_DOWNLOADS = {
        'name': 'current_downloads',
        'cols': (('download_type', 'INTEGER'),
                 ('current_state', 'INTEGER'),
                 ('page_token', 'TEXT'),
                 ('update_ts', 'INTEGER'))
    }

    TABLE_GRDIVE_DIRS = {
        'name': 'goog_folder',
        'cols': (('uid', 'INTEGER PRIMARY KEY'),
                 ('goog_id', 'TEXT'),
                 ('name', 'TEXT'),
                 ('trashed', 'INTEGER'),
                 ('drive_id', 'TEXT'),
                 ('my_share', 'INTEGER'),
                 ('sync_ts', 'INTEGER'),
                 ('all_children_fetched', 'INTEGER'))
    }

    TABLE_GRDIVE_ID_PARENT_MAPPINGS = {
        'name': 'goog_id_parent_mappings',
        'cols': (('item_uid', 'INTEGER'),
                 ('parent_uid', 'INTEGER'),
                 ('parent_goog_id', 'TEXT'),
                 ('sync_ts', 'INTEGER'))
    }

    TABLE_GRDIVE_FILES = {
        'name': 'goog_file',
        'cols': (('uid', 'INTEGER PRIMARY KEY'),
                 ('goog_id', 'TEXT'),
                 ('name', 'TEXT'),
                 ('trashed', 'INTEGER'),
                 ('size_bytes', 'INTEGER'),
                 ('md5', 'TEXT'),
                 ('create_ts', 'INTEGER'),
                 ('modify_ts', 'INTEGER'),
                 ('owner_id', 'TEXT'),
                 ('drive_id', 'TEXT'),
                 ('my_share', 'INTEGER'),
                 ('version', 'INTEGER'),
                 ('head_revision_id', 'TEXT'),
                 ('sync_ts', 'INTEGER'))

    }

    def __init__(self, db_path):
        super().__init__(db_path)

    # GDRIVE_DIRS operations ⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆

    def has_gdrive_dirs(self):
        return self.has_rows(self.TABLE_GRDIVE_DIRS)

    def insert_gdrive_dirs(self, dir_list: List[Tuple], overwrite=False, commit=True):
        if self.has_gdrive_dirs():
            if overwrite:
                self.drop_table_if_exists(self.TABLE_GRDIVE_DIRS)

        self.create_table_if_not_exist(self.TABLE_GRDIVE_DIRS, commit=False)
        self.insert_many(self.TABLE_GRDIVE_DIRS, dir_list, commit=commit)

    def get_gdrive_dirs(self):
        return self.get_all_rows(self.TABLE_GRDIVE_DIRS)

    def update_dir_fetched_status(self, commit=True):
        self.update(self.TABLE_GRDIVE_DIRS, stmt_vars=(True,), cols=['all_children_fetched'], commit=commit)

    # GDRIVE_FILES operations ⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆

    def has_gdrive_files(self):
        return self.has_rows(self.TABLE_GRDIVE_FILES)

    def insert_gdrive_files(self, file_list: List[Tuple], overwrite=False, commit=True):
        if self.has_gdrive_files():
            if overwrite:
                self.drop_table_if_exists(self.TABLE_GRDIVE_FILES, commit=False)

        self.create_table_if_not_exist(self.TABLE_GRDIVE_FILES, commit=False)
        self.insert_many(self.TABLE_GRDIVE_FILES, file_list, commit=commit)

    def get_gdrive_files(self):
        return self.get_all_rows(self.TABLE_GRDIVE_FILES)

    # TABLE goog_id_parent_mappings
    # ⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟

    def insert_id_parent_mappings(self, id_parent_mappings: List[Tuple], overwrite=False, commit=True):
        if self.is_table(self.TABLE_GRDIVE_ID_PARENT_MAPPINGS):
            if overwrite:
                self.drop_table_if_exists(self.TABLE_GRDIVE_ID_PARENT_MAPPINGS, commit=False)

        self.create_table_if_not_exist(self.TABLE_GRDIVE_ID_PARENT_MAPPINGS, commit=False)
        self.insert_many(self.TABLE_GRDIVE_ID_PARENT_MAPPINGS, id_parent_mappings, commit=commit)

    def get_id_parent_mappings(self):
        parent_uids = self.get_all_rows(self.TABLE_GRDIVE_ID_PARENT_MAPPINGS)
        logger.debug(f'Retrieved {len(parent_uids)} id-parent mappings')
        return parent_uids

    def insert_gdrive_files_and_parents(self, file_list: List[Tuple], parent_mappings: List[Tuple], current_download: CurrentDownload):
        self.insert_gdrive_files(file_list=file_list, commit=False)
        self.insert_id_parent_mappings(parent_mappings, commit=False)
        self.create_or_update_download(current_download)

    def insert_gdrive_dirs_and_parents(self, dir_list: List[Tuple], parent_mappings: List[Tuple], current_download: CurrentDownload):
        self.insert_gdrive_dirs(dir_list=dir_list, commit=False)
        self.insert_id_parent_mappings(parent_mappings, commit=False)
        self.create_or_update_download(current_download)

    # TABLE current_downloads
    # ⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟

    def create_or_update_download(self, download: CurrentDownload):
        self.create_table_if_not_exist(self.TABLE_GRDIVE_CURRENT_DOWNLOADS)

        download_type = download.download_type
        if not type(download_type) == int:
            download_type = int(download_type)
        rows = self.select(self.TABLE_GRDIVE_CURRENT_DOWNLOADS, f' WHERE download_type = {download_type}')
        row_to_save = download.to_tuple()
        if rows:
            self.update(self.TABLE_GRDIVE_CURRENT_DOWNLOADS, stmt_vars=row_to_save, where_clause=f' WHERE download_type = {download_type}')
        else:
            self.insert_one(self.TABLE_GRDIVE_CURRENT_DOWNLOADS, row_to_save)

    def get_current_downloads(self) -> List[CurrentDownload]:
        downloads = []
        if self.is_table(self.TABLE_GRDIVE_CURRENT_DOWNLOADS):
            rows = self.get_all_rows(self.TABLE_GRDIVE_CURRENT_DOWNLOADS)

            for row in rows:
                downloads.append(CurrentDownload(*row))

        logger.debug(f'Retrieved {len(downloads)} current downloads')
        return downloads

    def delete_all_gdrive_data(self):
        # Not the downloads table though
        self.drop_table_if_exists(self.TABLE_GRDIVE_FILES)
        self.drop_table_if_exists(self.TABLE_GRDIVE_DIRS)
        self.drop_table_if_exists(self.TABLE_GRDIVE_ID_PARENT_MAPPINGS)
