import logging
from collections import OrderedDict
from typing import Callable, List, Optional, Tuple

from constants import GDRIVE_DOWNLOAD_STATE_COMPLETE
from index.sqlite.base_db import MetaDatabase, Table
from index.uid.uid import UID
from model.node.gdrive_node import GDriveFile, GDriveFolder
from model.node_identifier import GDriveIdentifier

logger = logging.getLogger(__name__)


def _tuple_to_gdrive_folder(row: Tuple) -> GDriveFolder:
    uid_int, goog_id, item_name, item_trashed, drive_id, my_share, sync_ts, all_children_fetched = row
    uid_from_cache = UID(uid_int)

    return GDriveFolder(GDriveIdentifier(uid=uid_from_cache, full_path=None), goog_id=goog_id, item_name=item_name,
                        trashed=item_trashed, drive_id=drive_id, my_share=my_share, sync_ts=sync_ts, all_children_fetched=all_children_fetched)


def _tuple_to_gdrive_file(row: Tuple) -> GDriveFile:
    uid_int, goog_id, item_name, item_trashed, size_bytes, md5, create_ts, modify_ts, owner_id, drive_id, my_share, version, head_revision_id, \
        sync_ts = row

    uid_from_cache = UID(uid_int)

    return GDriveFile(GDriveIdentifier(uid=uid_from_cache, full_path=None), goog_id=goog_id, item_name=item_name,
                      trashed=item_trashed, drive_id=drive_id, my_share=my_share, version=version,
                      head_revision_id=head_revision_id, md5=md5,
                      create_ts=create_ts, modify_ts=modify_ts, size_bytes=size_bytes, owner_id=owner_id, sync_ts=sync_ts)


# CLASS CurrentDownload
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼

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

    def __repr__(self):
        return f'CurrentDownload(type={self.download_type}, state={self.current_state}, page_token={self.page_token}, update_ts={self.update_ts})'


# CLASS GDriveDatabase
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼

class GDriveDatabase(MetaDatabase):
    TABLE_GRDIVE_CURRENT_DOWNLOAD = Table(name='current_download',
                                          cols=OrderedDict([
                                              ('download_type', 'INTEGER'),
                                              ('current_state', 'INTEGER'),
                                              ('page_token', 'TEXT'),
                                              ('update_ts', 'INTEGER')
                                          ]))

    TABLE_GRDIVE_DIR = Table(name='goog_folder',
                             cols=OrderedDict([
                                 ('uid', 'INTEGER PRIMARY KEY'),
                                 ('goog_id', 'TEXT'),
                                 ('name', 'TEXT'),
                                 ('trashed', 'INTEGER'),
                                 ('drive_id', 'TEXT'),
                                 ('my_share', 'INTEGER'),
                                 ('sync_ts', 'INTEGER'),
                                 ('all_children_fetched', 'INTEGER')
                             ]))

    TABLE_GRDIVE_FILE = Table(name='goog_file',
                              cols=OrderedDict([
                                  ('uid', 'INTEGER PRIMARY KEY'),
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
                                  ('sync_ts', 'INTEGER')
                              ]))

    TABLE_GRDIVE_ID_PARENT_MAPPING = Table(name='goog_id_parent_mappings',
                                           cols=OrderedDict([
                                               ('item_uid', 'INTEGER'),
                                               ('parent_uid', 'INTEGER'),
                                               ('parent_goog_id', 'TEXT'),
                                               ('sync_ts', 'INTEGER')
                                           ]))

    def __init__(self, db_path, application):
        super().__init__(db_path)
        self.cache_manager = application.cache_manager
        self.uid_generator = application.uid_generator

    # GDRIVE_DIR operations
    # ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼

    def insert_gdrive_folders(self, dir_list: List[Tuple], overwrite=False, commit=True):
        self.TABLE_GRDIVE_DIR.insert_object_list(self.conn, dir_list, None, overwrite, commit)

    def upsert_gdrive_folders(self, dir_list: List[Tuple], commit=True):
        self.TABLE_GRDIVE_DIR.create_table_if_not_exist(self.conn, commit=False)
        self.TABLE_GRDIVE_DIR.upsert_many(self.conn, dir_list, commit=commit)

    def get_gdrive_folder_object_list(self) -> List[GDriveFolder]:
        return self.TABLE_GRDIVE_DIR.select_object_list(self.conn, tuple_to_obj_func=_tuple_to_gdrive_folder)

    def update_folder_fetched_status(self, commit=True):
        self.TABLE_GRDIVE_DIR.update(self.conn, stmt_vars=(True,), col_names=['all_children_fetched'], commit=commit)

    def delete_gdrive_folder_with_uid(self, uid: UID, commit=True):
        self.TABLE_GRDIVE_DIR.delete_for_uid(self.conn, uid, commit)

    # TABLE goog_file operations
    # ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼

    def has_gdrive_files(self):
        return self.TABLE_GRDIVE_FILE.has_rows(self.conn)

    def insert_gdrive_files(self, file_list: List[Tuple], overwrite=False, commit=True):
        self.TABLE_GRDIVE_FILE.insert_object_list(self.conn, file_list, None, overwrite, commit)

    def upsert_gdrive_files(self, file_list: List[Tuple], commit=True):
        self.TABLE_GRDIVE_FILE.create_table_if_not_exist(self.conn, commit=False)
        self.TABLE_GRDIVE_FILE.upsert_many(self.conn, file_list, commit=commit)

    def get_gdrive_file_object_list(self):
        return self.TABLE_GRDIVE_FILE.select_object_list(self.conn, tuple_to_obj_func=_tuple_to_gdrive_file)

    def delete_gdrive_file_with_uid(self, uid: UID, commit=True):
        self.TABLE_GRDIVE_FILE.delete_for_uid(self.conn, uid, commit)

    # TABLE goog_id_parent_mapping operations
    # ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼

    def insert_id_parent_mappings(self, id_parent_mappings: List[Tuple], overwrite=False, commit=True):
        if self.TABLE_GRDIVE_ID_PARENT_MAPPING.is_table(self.conn):
            if overwrite:
                self.TABLE_GRDIVE_ID_PARENT_MAPPING.drop_table_if_exists(self.conn, commit=False)

        self.TABLE_GRDIVE_ID_PARENT_MAPPING.create_table_if_not_exist(self.conn, commit=False)
        logger.debug(f'Inserting {len(id_parent_mappings)} id-par mappings into DB, commit={commit}')
        self.TABLE_GRDIVE_ID_PARENT_MAPPING.insert_many(self.conn, id_parent_mappings, commit=commit)

    def upsert_parent_mappings_for_id(self, id_parent_mappings: List[Tuple], uid: UID, commit=True):
        # just do this the easy way for now. Need to replace all mappings for this UID
        self.delete_parent_mappings_for_uid(uid=uid, commit=False)

        self.insert_id_parent_mappings(id_parent_mappings, commit)

    def get_id_parent_mappings(self) -> List[Tuple]:
        parent_uids = self.TABLE_GRDIVE_ID_PARENT_MAPPING.get_all_rows(self.conn)
        logger.debug(f'Retrieved {len(parent_uids)} id-parent mappings')
        return parent_uids

    def insert_gdrive_files_and_parents(self, file_list: List[Tuple], parent_mappings: List[Tuple], current_download: CurrentDownload):
        self.insert_gdrive_files(file_list=file_list, commit=False)
        self.insert_id_parent_mappings(parent_mappings, commit=False)
        self.create_or_update_download(current_download)

    def insert_gdrive_folders_and_parents(self, dir_list: List[Tuple], parent_mappings: List[Tuple], current_download: CurrentDownload):
        self.insert_gdrive_folders(dir_list=dir_list, commit=False)
        self.insert_id_parent_mappings(parent_mappings, commit=False)
        self.create_or_update_download(current_download)

    def delete_parent_mappings_for_uid(self, uid: UID, commit=True):
        logger.debug(f'Deleting id-parent mappings for {uid}')
        sql = self.TABLE_GRDIVE_ID_PARENT_MAPPING.build_delete() + f' WHERE item_uid = ?'
        self.conn.execute(sql, (uid,))
        if commit:
            logger.debug('Committing!')
            self.conn.commit()

    # TABLE current_download operations
    # ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼

    def create_or_update_download(self, download: CurrentDownload):
        self.TABLE_GRDIVE_CURRENT_DOWNLOAD.create_table_if_not_exist(self.conn)

        download_type = download.download_type
        if not type(download_type) == int:
            download_type = int(download_type)
        rows = self.TABLE_GRDIVE_CURRENT_DOWNLOAD.select(self.conn, f' WHERE download_type = {download_type}')
        row_to_save = download.to_tuple()
        if rows:
            logger.debug(f'Updating download in DB: {download}')
            self.TABLE_GRDIVE_CURRENT_DOWNLOAD.update(self.conn, stmt_vars=row_to_save, where_clause=f' WHERE download_type = {download_type}')
        else:
            logger.debug(f'Inserting download in DB: {download}')
            self.TABLE_GRDIVE_CURRENT_DOWNLOAD.insert_one(self.conn, row_to_save)

    def get_current_downloads(self) -> List[CurrentDownload]:
        downloads = []
        if self.TABLE_GRDIVE_CURRENT_DOWNLOAD.is_table(self.conn):
            rows = self.TABLE_GRDIVE_CURRENT_DOWNLOAD.get_all_rows(self.conn)

            for row in rows:
                downloads.append(CurrentDownload(*row))

        logger.debug(f'Retrieved {len(downloads)} current downloads')
        return downloads

    # COMPOSITE operations
    # ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼

    def delete_all_gdrive_data(self):
        # Not the downloads table though
        self.TABLE_GRDIVE_FILE.drop_table_if_exists(self.conn)
        self.TABLE_GRDIVE_DIR.drop_table_if_exists(self.conn)
        self.TABLE_GRDIVE_ID_PARENT_MAPPING.drop_table_if_exists(self.conn)
        self.TABLE_GRDIVE_CURRENT_DOWNLOAD.drop_table_if_exists(self.conn)
