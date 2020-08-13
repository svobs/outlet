import logging
from collections import OrderedDict
from typing import List, Tuple

from index.sqlite.base_db import MetaDatabase, Table
from index.uid.uid import UID
from model.node.local_disk_node import LocalDirNode, LocalFileNode
from model.node_identifier import LocalFsIdentifier

logger = logging.getLogger(__name__)


def _file_to_tuple(f: LocalFileNode):
    assert isinstance(f, LocalFileNode), f'Expected LocalFileNode; got instead: {f}'
    return f.uid, f.md5, f.sha256, f.get_size_bytes(), f.sync_ts, f.modify_ts, f.change_ts, f.full_path, f.exists()


def _dir_to_tuple(d):
    assert isinstance(d, LocalDirNode), f'Expected LocalDirNode; got instead: {d}'
    return d.uid, d.full_path, d.exists()


# CLASS LocalDiskDatabase
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼

class LocalDiskDatabase(MetaDatabase):
    TABLE_LOCAL_FILE = Table(name='local_file', cols=OrderedDict([
        ('uid', 'INTEGER PRIMARY KEY'),
        ('md5', 'TEXT'),
        ('sha256', 'TEXT'),
        ('size_bytes', 'INTEGER'),
        ('sync_ts', 'INTEGER'),
        ('modify_ts', 'INTEGER'),
        ('change_ts', 'INTEGER'),
        ('full_path', 'TEXT'),
        ('exist', 'INTEGER')
    ]))

    # 2020-06, So far this is really just a mapping of UIDs to paths, to keep things consistent across runs.
    TABLE_LOCAL_DIR = Table(name='local_dir', cols=OrderedDict([
        ('uid', 'INTEGER PRIMARY KEY'),
        ('full_path', 'TEXT'),
        ('exist', 'INTEGER')
    ]))

    def __init__(self, db_path, application):
        super().__init__(db_path)
        self.cache_manager = application.cache_manager

    # LOCAL_FILE operations ⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆

    def _tuple_to_file(self, row: Tuple) -> LocalFileNode:
        full_path = row[7]
        uid = self.cache_manager.get_uid_for_path(full_path, row[0])
        assert uid == row[0], f'UID conflict! Got {uid} but read {row[0]} in row: {row}'
        node_identifier = LocalFsIdentifier(uid=uid, full_path=full_path)
        return LocalFileNode(node_identifier, row[1], row[2], row[3], row[4], row[5], row[6], row[8])

    def has_local_files(self):
        return self.TABLE_LOCAL_FILE.has_rows(self.conn)

    def get_local_files(self) -> List[LocalFileNode]:
        return self.TABLE_LOCAL_FILE.select_object_list(self.conn, tuple_to_obj_func=self._tuple_to_file)

    def insert_local_files(self, entries: List[LocalFileNode], overwrite, commit=True):
        self.TABLE_LOCAL_FILE.insert_object_list(self.conn, entries, obj_to_tuple_func=_file_to_tuple, overwrite=overwrite, commit=commit)

    def truncate_local_files(self):
        self.TABLE_LOCAL_FILE.truncate_table(self.conn)

    def upsert_local_file(self, item, commit=True):
        self.TABLE_LOCAL_FILE.upsert_object(self.conn, _file_to_tuple, item, commit=commit)

    def delete_local_file_with_uid(self, uid: UID, commit=True):
        self.TABLE_LOCAL_FILE.delete_for_uid(self.conn, uid, commit)

    # LOCAL_DIR operations ⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆

    def _tuple_to_dir(self, row: Tuple) -> LocalDirNode:
        full_path = row[1]
        uid = self.cache_manager.get_uid_for_path(full_path, row[0])
        assert uid == row[0], f'UID conflict! Got {uid} but read {row}'
        return LocalDirNode(LocalFsIdentifier(uid=uid, full_path=full_path), bool(row[2]))

    def has_local_dirs(self):
        return self.TABLE_LOCAL_DIR.has_rows(self.conn)

    def get_local_dirs(self) -> List[LocalDirNode]:
        return self.TABLE_LOCAL_DIR.select_object_list(self.conn, tuple_to_obj_func=self._tuple_to_dir)

    def insert_local_dirs(self, entries: List[LocalDirNode], overwrite, commit=True):
        self.TABLE_LOCAL_DIR.insert_object_list(self.conn, entries, obj_to_tuple_func=_dir_to_tuple, overwrite=overwrite, commit=commit)

    def upsert_local_dir(self, item, commit=True):
        self.TABLE_LOCAL_DIR.upsert_object(self.conn, _dir_to_tuple, item, commit=commit)

    def delete_local_dir_with_uid(self, uid: UID, commit=True):
        sql = self.TABLE_LOCAL_DIR.build_delete() + f' WHERE uid = ?'
        self.conn.execute(sql, (uid,))
        if commit:
            logger.debug('Committing!')
            self.conn.commit()

