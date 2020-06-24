import logging
from typing import List

from index.sqlite.base_db import MetaDatabase, Table
from index.uid import UID
from model.fmeta import LocalDirNode, LocalFileNode
from model.node_identifier import LocalFsIdentifier

logger = logging.getLogger(__name__)


# CLASS LocalDiskDatabase
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼

class LocalDiskDatabase(MetaDatabase):
    TABLE_LOCAL_FILE = Table(name='local_file', cols={'uid': 'INTEGER PRIMARY KEY',
                                                      'md5': 'TEXT',
                                                      'sha256': 'TEXT',
                                                      'size_bytes': 'INTEGER',
                                                      'sync_ts': 'INTEGER',
                                                      'modify_ts': 'INTEGER',
                                                      'change_ts': 'INTEGER',
                                                      'full_path': 'TEXT',
                                                      'exist': 'INTEGER'})

    # 2020-06: So far this is really just a mapping of UIDs to paths, to keep things consistent across runs.
    TABLE_LOCAL_DIR = Table(name='local_dir', cols={'uid': 'INTEGER PRIMARY KEY',
                                                    'full_path': 'TEXT',
                                                    'exist': 'INTEGER'})

    def __init__(self, db_path, application):
        super().__init__(db_path)
        self.cache_manager = application.cache_manager

    # LOCAL_FILE operations ⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆

    def has_local_files(self):
        return self.has_rows(self.TABLE_LOCAL_FILE)

    def get_local_files(self) -> List[LocalFileNode]:
        entries: List[LocalFileNode] = []

        """ Gets all changes in the table """
        if not self.is_table(self.TABLE_LOCAL_FILE):
            return entries

        rows = self.get_all_rows(self.TABLE_LOCAL_FILE)
        for row in rows:
            full_path = row[7]
            uid = self.cache_manager.get_uid_for_path(full_path, row[0])
            assert uid == row[0], f'UID conflict! Got {uid} but read {row[0]} in row: {row}'

            node_identifier = LocalFsIdentifier(uid=uid, full_path=full_path)
            entries.append(LocalFileNode(node_identifier, row[1], row[2], row[3], row[4], row[5], row[6], row[8]))
        return entries

    def insert_local_files(self, entries: List[LocalFileNode], overwrite, commit=True):
        """ Takes a list of LocalFileNode objects: """
        to_insert = []
        for e in entries:
            assert isinstance(e, LocalFileNode), f'Expected LocalFileNode; got instead: {e}'
            e_tuple = _make_file_tuple(e)
            to_insert.append(e_tuple)

        if overwrite:
            self.drop_table_if_exists(self.TABLE_LOCAL_FILE)

        self.create_table_if_not_exist(self.TABLE_LOCAL_FILE)

        self.insert_many(self.TABLE_LOCAL_FILE, to_insert, commit)

    def truncate_local_files(self):
        self.truncate_table(self.TABLE_LOCAL_FILE)

    def upsert_local_file(self, item, commit=True):
        self.upsert_one(self.TABLE_LOCAL_FILE, _make_file_tuple(item), commit=commit)

    def delete_local_file_with_uid(self, uid: UID, commit=True):
        sql = self.build_delete(self.TABLE_LOCAL_FILE) + f' WHERE uid = ?'
        self.conn.execute(sql, (uid,))
        if commit:
            logger.debug('Committing!')
            self.conn.commit()

    # LOCAL_DIR operations ⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆

    def has_local_dirs(self):
        return self.has_rows(self.TABLE_LOCAL_DIR)

    def get_local_dirs(self) -> List[LocalDirNode]:
        """ Gets all changes in the table """
        entries: List[LocalDirNode] = []

        if not self.is_table(self.TABLE_LOCAL_DIR):
            return entries

        rows = self.get_all_rows(self.TABLE_LOCAL_DIR)
        for row in rows:
            full_path = row[1]
            uid = self.cache_manager.get_uid_for_path(full_path, row[0])
            assert uid == row[0], f'UID conflict! Got {uid} but read {row}'
            entries.append(LocalDirNode(LocalFsIdentifier(uid=uid, full_path=full_path), bool(row[2])))
        return entries

    def insert_local_dirs(self, entries: List[LocalDirNode], overwrite, commit=True):
        """ Takes a list of LocalDirNode objects: """
        to_insert = []
        for d in entries:
            assert isinstance(d, LocalDirNode), f'Expected LocalDirNode; got instead: {d}'
            d_tuple = _make_dir_tuple(d)
            to_insert.append(d_tuple)

        if overwrite:
            self.drop_table_if_exists(self.TABLE_LOCAL_DIR)

        self.create_table_if_not_exist(self.TABLE_LOCAL_DIR)

        self.insert_many(self.TABLE_LOCAL_DIR, to_insert, commit)

    def upsert_local_dir(self, item, commit=True):
        self.upsert_one(self.TABLE_LOCAL_DIR, _make_dir_tuple(item), commit=commit)

    def delete_local_dir_with_uid(self, uid: UID, commit=True):
        sql = self.build_delete(self.TABLE_LOCAL_DIR) + f' WHERE uid = ?'
        self.conn.execute(sql, (uid,))
        if commit:
            logger.debug('Committing!')
            self.conn.commit()


def _make_file_tuple(f: LocalFileNode):
    return f.uid, f.md5, f.sha256, f.get_size_bytes(), f.sync_ts, f.modify_ts, f.change_ts, f.full_path, f.exists()


def _make_dir_tuple(d):
    return d.uid, d.full_path, d.exists()
