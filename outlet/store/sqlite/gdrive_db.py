import logging
from collections import OrderedDict
from typing import List, Optional, Tuple

from constants import GDRIVE_DOWNLOAD_STATE_COMPLETE, GDRIVE_ME_USER_UID
from store.sqlite.base_db import LiveTable, MetaDatabase, Table
from model.uid import UID
from model.gdrive_meta import GDriveUser, MimeType
from model.node.gdrive_node import GDriveFile, GDriveFolder, GDriveNode
from model.node_identifier import ensure_int, GDriveIdentifier

logger = logging.getLogger(__name__)


def _tuple_to_gdrive_folder(row: Tuple) -> GDriveFolder:
    uid_int, goog_id, node_name, item_trashed, create_ts, modify_ts, owner_uid, drive_id, is_shared, shared_by_user_uid, sync_ts, \
        all_children_fetched = row
    uid_from_cache = UID(uid_int)

    return GDriveFolder(GDriveIdentifier(uid=uid_from_cache, path_list=None), goog_id=goog_id, node_name=node_name,
                        trashed=item_trashed, create_ts=create_ts, modify_ts=modify_ts, owner_uid=owner_uid, drive_id=drive_id, is_shared=is_shared,
                        shared_by_user_uid=shared_by_user_uid, sync_ts=sync_ts, all_children_fetched=all_children_fetched)


def _gdrive_folder_to_tuple(folder: GDriveFolder) -> Tuple:
    return folder.to_tuple()


def _tuple_to_gdrive_file(row: Tuple) -> GDriveFile:
    uid_int, goog_id, node_name, mime_type_uid, item_trashed, size_bytes, md5, create_ts, modify_ts, owner_uid, drive_id, is_shared, \
        shared_by_user_uid, version, sync_ts = row

    uid_from_cache = UID(uid_int)

    return GDriveFile(GDriveIdentifier(uid=uid_from_cache, path_list=None), goog_id=goog_id, node_name=node_name, mime_type_uid=mime_type_uid,
                      trashed=item_trashed, drive_id=drive_id, is_shared=is_shared, version=version,
                      md5=md5, create_ts=create_ts, modify_ts=modify_ts, size_bytes=size_bytes,
                      owner_uid=owner_uid, shared_by_user_uid=shared_by_user_uid, sync_ts=sync_ts)


def _gdrive_file_to_tuple(file: GDriveFile) -> Tuple:
    return file.to_tuple()


def _tuple_to_gdrive_user(row: Tuple) -> GDriveUser:
    uid_int, display_name, permission_id, email_address, photo_link = row
    return GDriveUser(display_name=display_name, permission_id=permission_id, email_address=email_address, photo_link=photo_link,
                      is_me=(uid_int == GDRIVE_ME_USER_UID), user_uid=UID(uid_int))


def _gdrive_user_to_tuple(user: GDriveUser) -> Tuple:
    return user.uid, user.permission_id, user.display_name, user.email_address, user.photo_link


def _tuple_to_mime_type(row: Tuple) -> MimeType:
    uid_int, type_string = row
    return MimeType(uid=UID(uid_int), type_string=type_string)


def _mime_type_to_tuple(mime_type: MimeType) -> Tuple:
    return mime_type.uid, mime_type.type_string


# CLASS CurrentDownload
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼

class CurrentDownload:
    def __init__(self, download_type: int, current_state: int, page_token: Optional[str], update_ts: int):
        self.download_type: int = ensure_int(download_type)
        self.current_state: int = ensure_int(current_state)
        self.page_token: Optional[str] = page_token
        self.update_ts: int = ensure_int(update_ts)

    def to_tuple(self):
        return self.download_type, self.current_state, self.page_token, self.update_ts

    def is_complete(self):
        return self.current_state == GDRIVE_DOWNLOAD_STATE_COMPLETE

    def __repr__(self):
        return f'CurrentDownload(type={self.download_type}, state={self.current_state}, page_token={self.page_token}, update_ts={self.update_ts})'


def _download_to_tuple(d: CurrentDownload) -> Tuple:
    assert isinstance(d, CurrentDownload), f'Expected CurrentDownload; got instead: {d}'
    return d.to_tuple()


def _tuple_to_download(row: Tuple) -> CurrentDownload:
    assert isinstance(row, Tuple), f'Expected Tuple; got instead: {row}'
    return CurrentDownload(*row)


# CLASS GDriveDatabase
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼

class GDriveDatabase(MetaDatabase):
    TABLE_GRDIVE_CURRENT_DOWNLOAD = Table(name='current_download',
                                          cols=OrderedDict([
                                              ('download_type', 'INTEGER PRIMARY KEY'),
                                              ('current_state', 'INTEGER'),
                                              ('page_token', 'TEXT'),
                                              ('update_ts', 'INTEGER')
                                          ]))

    TABLE_GDRIVE_USER = Table(name='gdrive_user',
                              cols=OrderedDict([
                                  ('uid', 'INTEGER PRIMARY KEY'),
                                  ('permission_id', 'TEXT'),
                                  ('display_name', 'TEXT'),
                                  ('email_address', 'TEXT'),
                                  ('photo_link', 'TEXT'),
                              ]))

    TABLE_MIME_TYPE = Table(name='mime_type',
                            cols=OrderedDict([
                                ('uid', 'INTEGER PRIMARY KEY'),
                                ('mime_type', 'TEXT'),
                            ]))

    TABLE_GRDIVE_FOLDER = Table(name='gdrive_folder',
                                cols=OrderedDict([
                                    ('uid', 'INTEGER PRIMARY KEY'),
                                    ('goog_id', 'TEXT'),
                                    ('name', 'TEXT'),
                                    ('trashed', 'INTEGER'),
                                    ('create_ts', 'INTEGER'),
                                    ('modify_ts', 'INTEGER'),
                                    ('owner_uid', 'INTEGER'),
                                    ('drive_id', 'TEXT'),
                                    ('is_shared', 'INTEGER'),
                                    ('shared_by_user_uid', 'INTEGER'),
                                    ('sync_ts', 'INTEGER'),
                                    ('all_children_fetched', 'INTEGER')
                                ]))

    TABLE_GRDIVE_FILE = Table(name='gdrive_file',
                              cols=OrderedDict([
                                  ('uid', 'INTEGER PRIMARY KEY'),
                                  ('goog_id', 'TEXT'),
                                  ('name', 'TEXT'),
                                  ('mime_type_uid', 'INTEGER'),
                                  ('trashed', 'INTEGER'),
                                  ('size_bytes', 'INTEGER'),
                                  ('md5', 'TEXT'),
                                  ('create_ts', 'INTEGER'),
                                  ('modify_ts', 'INTEGER'),
                                  ('owner_uid', 'TEXT'),
                                  ('drive_id', 'TEXT'),
                                  ('is_shared', 'INTEGER'),
                                  ('shared_by_user_uid', 'INTEGER'),
                                  ('version', 'INTEGER'),
                                  ('sync_ts', 'INTEGER')
                              ]))

    TABLE_GRDIVE_ID_PARENT_MAPPING = Table(name='gdrive_id_parent_mapping',
                                           cols=OrderedDict([
                                               ('item_uid', 'INTEGER'),
                                               ('parent_uid', 'INTEGER'),
                                               ('parent_goog_id', 'TEXT'),
                                               ('sync_ts', 'INTEGER')
                                           ]))

    def __init__(self, db_path, backend):
        super().__init__(db_path)
        self.cacheman = backend.cacheman
        self.uid_generator = backend.uid_generator

        self.table_current_download = LiveTable(GDriveDatabase.TABLE_GRDIVE_CURRENT_DOWNLOAD, self.conn, _download_to_tuple, _tuple_to_download)
        self.table_gdrive_folder = LiveTable(GDriveDatabase.TABLE_GRDIVE_FOLDER, self.conn, _gdrive_folder_to_tuple, _tuple_to_gdrive_folder)
        self.table_gdrive_file = LiveTable(GDriveDatabase.TABLE_GRDIVE_FILE, self.conn, _gdrive_file_to_tuple, _tuple_to_gdrive_file)
        self.id_parent_mapping = LiveTable(GDriveDatabase.TABLE_GRDIVE_ID_PARENT_MAPPING, self.conn, None, None)
        self.table_gdrive_user = LiveTable(GDriveDatabase.TABLE_GDRIVE_USER, self.conn, _gdrive_user_to_tuple, _tuple_to_gdrive_user)
        self.table_mime_type = LiveTable(GDriveDatabase.TABLE_MIME_TYPE, self.conn, _mime_type_to_tuple, _tuple_to_mime_type)

    # FOLDER operations
    # ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼

    def insert_gdrive_folder_list(self, folder_list: List[GDriveFolder], overwrite=False, commit=True):
        self.table_gdrive_folder.insert_object_list(folder_list, overwrite, commit)

    def upsert_gdrive_folder_list(self, folder_list: List[GDriveFolder], commit=True):
        self.table_gdrive_folder.create_table_if_not_exist(commit=False)
        self.table_gdrive_folder.upsert_object_list(folder_list, commit=commit)

    def get_gdrive_folder_object_list(self) -> List[GDriveFolder]:
        return self.table_gdrive_folder.select_object_list()

    def update_folder_fetched_status(self, commit=True):
        self.table_gdrive_folder.update(stmt_vars=(True,), col_names=['all_children_fetched'], commit=commit)

    def delete_gdrive_folder_with_uid(self, uid: UID, commit=True):
        self.table_gdrive_folder.delete_for_uid(uid, commit=commit)

    # FILE operations
    # ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼

    def has_gdrive_files(self):
        return self.table_gdrive_file.has_rows()

    def insert_gdrive_files(self, file_list: List[GDriveFile], overwrite=False, commit=True):
        self.table_gdrive_file.insert_object_list(file_list, overwrite, commit)

    def upsert_gdrive_file_list(self, file_list: List[GDriveFile], commit=True):
        self.table_gdrive_file.create_table_if_not_exist(commit=False)
        self.table_gdrive_file.upsert_object_list(file_list, commit=commit)

    def get_gdrive_file_object_list(self):
        return self.table_gdrive_file.select_object_list()

    def delete_gdrive_file_with_uid(self, uid: UID, commit=True):
        self.table_gdrive_file.delete_for_uid(uid, commit=commit)

    # gdrive_id_parent_mappings operations
    # ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼

    def insert_id_parent_mappings(self, id_parent_mappings: List[Tuple], overwrite=False, commit=True):
        if self.id_parent_mapping.is_table():
            if overwrite:
                self.id_parent_mapping.drop_table_if_exists(commit=False)

        self.id_parent_mapping.create_table_if_not_exist(commit=False)
        logger.debug(f'Inserting {len(id_parent_mappings)} id-par mappings into DB, commit={commit}')
        self.id_parent_mapping.insert_many(id_parent_mappings, commit=commit)

    def _delete_parent_mappings(self, uid_list: List[UID], commit=True):
        uid_str_list: List[str] = list(map(lambda uid: str(uid), uid_list))
        sql = self.id_parent_mapping.build_delete() + f' WHERE item_uid IN ({",".join(uid_str_list)})'
        logger.debug(f'Executing SQL: {sql}')
        self.conn.execute(sql)

        if commit:
            logger.debug('Committing!')
            self.conn.commit()

    def upsert_parent_mappings(self, mapping_list_list: List[List[Tuple]], commit=True):
        uid_list: List[UID] = []
        flattened_list: List[Tuple] = []
        for mapping_list in mapping_list_list:
            try:
                uid_list.append(mapping_list[0][0])
                for mapping in mapping_list:
                    flattened_list.append(mapping)
            except IndexError:
                logger.error(f'For mapping_list: {mapping_list}, in list_list: {mapping_list_list}')
                raise
        self._delete_parent_mappings(uid_list, commit=False)

        logger.debug(f'Inserting {len(flattened_list)} parent mappings into DB, commit={commit}')
        self.id_parent_mapping.insert_many(flattened_list, commit=commit)

    def upsert_parent_mappings_for_id(self, id_parent_mappings: List[Tuple], uid: UID, commit=True):
        # just do this the easy way for now. Need to replace all mappings for this UID
        self.delete_parent_mappings_for_uid(uid=uid, commit=False)

        self.insert_id_parent_mappings(id_parent_mappings, commit)

    def get_id_parent_mappings(self) -> List[Tuple]:
        parent_uids = self.id_parent_mapping.get_all_rows()
        logger.debug(f'Retrieved {len(parent_uids)} id-parent mappings')
        return parent_uids

    def delete_parent_mappings_for_uid(self, uid: UID, commit: bool = True):
        logger.debug(f'Deleting id-parent mappings for {uid}')
        sql = self.id_parent_mapping.build_delete() + f' WHERE item_uid = ?'
        self.conn.execute(sql, (uid,))
        if commit:
            logger.debug('Committing!')
            self.conn.commit()

    # TABLE current_download operations
    # ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼

    def upsert_download(self, download: CurrentDownload, commit: bool = True):
        self.table_current_download.create_table_if_not_exist(commit=False)
        self.table_current_download.upsert_object(download, commit=commit)

    def get_current_download_list(self) -> List[CurrentDownload]:
        return self.table_current_download.select_object_list()

    # GDriveUser operations
    # ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼

    def get_all_users(self) -> List[GDriveUser]:
        self.table_gdrive_user.create_table_if_not_exist()
        return self.table_gdrive_user.select_object_list()

    def upsert_user(self, user: GDriveUser, commit=True):
        self.table_gdrive_user.create_table_if_not_exist()
        self.table_gdrive_user.upsert_object(user, commit=commit)

    # MIME Type operations
    # ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼

    def get_all_mime_types(self) -> List[MimeType]:
        self.table_mime_type.create_table_if_not_exist()
        return self.table_mime_type.select_object_list()

    def upsert_mime_type(self, mime_type: MimeType, commit=True):
        self.table_mime_type.create_table_if_not_exist()
        self.table_mime_type.upsert_object(mime_type, commit=commit)

    # COMPOSITE operations
    # ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼

    def get_node_with_uid(self, uid: UID) -> Optional[GDriveNode]:
        node: Optional[GDriveNode] = self.table_gdrive_file.select_object_for_uid(uid)
        if not node:
            node = self.table_gdrive_folder.select_object_for_uid(uid)

        if node:
            id_parent_mappings = self.id_parent_mapping.select_row_for_uid(uid, uid_col_name='item_uid')
            if id_parent_mappings:
                for mapping in id_parent_mappings:
                    node.add_parent(mapping[1])

        logger.debug(f'get_node_with_uid(): returning {node}')
        return node

    def delete_all_gdrive_data(self):
        # Not the current_download table though!
        self.table_gdrive_file.drop_table_if_exists(self.conn)
        self.table_gdrive_folder.drop_table_if_exists(self.conn)
        self.id_parent_mapping.drop_table_if_exists(self.conn)
        self.table_gdrive_user.drop_table_if_exists(self.conn)
        self.table_mime_type.drop_table_if_exists(self.conn)

    def delete_nodes(self, file_uid_list: List[UID], folder_uid_list: List[UID], commit=True):
        folder_uid_tuple_list = list(map(lambda uid: (uid,), folder_uid_list))
        self.table_gdrive_folder.delete_for_uid_list(folder_uid_tuple_list, commit=False)

        file_uid_tuple_list = list(map(lambda uid: (uid,), file_uid_list))
        self.table_gdrive_file.delete_for_uid_list(file_uid_tuple_list, commit=False)

        self._delete_parent_mappings(file_uid_list + folder_uid_list, commit=commit)

    def delete_single_node(self, node: GDriveNode, commit=True):
        self.delete_parent_mappings_for_uid(node.uid, commit=False)
        if node.is_dir():
            self.delete_gdrive_folder_with_uid(node.uid, commit=False)
        else:
            self.delete_gdrive_file_with_uid(node.uid, commit=commit)
