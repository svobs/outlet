import logging
from collections import OrderedDict
from typing import List, Optional, Tuple, Union

from index.sqlite.base_db import LiveTable, MetaDatabase, Table
from index.uid.uid import UID
from model.node.local_disk_node import LocalDirNode, LocalFileNode
from model.node_identifier import LocalFsIdentifier

logger = logging.getLogger(__name__)


def _file_to_tuple(f: LocalFileNode):
    assert isinstance(f, LocalFileNode), f'Expected LocalFileNode; got instead: {f}'
    return f.to_tuple()


def _dir_to_tuple(d):
    assert isinstance(d, LocalDirNode), f'Expected LocalDirNode; got instead: {d}'
    return d.to_tuple()


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
        self.table_local_file = LiveTable(LocalDiskDatabase.TABLE_LOCAL_FILE, self.conn, _file_to_tuple, self._tuple_to_file)
        self.table_local_dir = LiveTable(LocalDiskDatabase.TABLE_LOCAL_DIR, self.conn, _dir_to_tuple, self._tuple_to_dir)

    # LOCAL_FILE operations ⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆

    def _tuple_to_file(self, row: Tuple) -> LocalFileNode:
        uid_int, md5, sha256, size_bytes, sync_ts, modify_ts, change_ts, full_path, exists = row

        uid = self.cache_manager.get_uid_for_path(full_path, uid_int)
        assert uid == row[0], f'UID conflict! Got {uid} but read {uid_int} in row: {row}'
        node_identifier = LocalFsIdentifier(uid=uid, full_path=full_path)
        return LocalFileNode(node_identifier, md5, sha256, size_bytes, sync_ts, modify_ts, change_ts, exists)

    def has_local_files(self):
        return self.table_local_file.has_rows()

    def get_local_files(self) -> List[LocalFileNode]:
        return self.table_local_file.select_object_list()

    def insert_local_files(self, entries: List[LocalFileNode], overwrite, commit=True):
        self.table_local_file.insert_object_list(entries, overwrite=overwrite, commit=commit)

    def truncate_local_files(self):
        self.table_local_file.truncate_table(self.conn)

    def upsert_local_file(self, item, commit=True):
        self.table_local_file.upsert_object(item, commit=commit)

    def delete_local_file_with_uid(self, uid: UID, commit=True):
        self.table_local_file.delete_for_uid(uid, commit=commit)

    # LOCAL_DIR operations ⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆

    def _tuple_to_dir(self, row: Tuple) -> LocalDirNode:
        full_path = row[1]
        uid = self.cache_manager.get_uid_for_path(full_path, row[0])
        assert uid == row[0], f'UID conflict! Got {uid} from memcache but read from disk: {row}'
        return LocalDirNode(LocalFsIdentifier(uid=uid, full_path=full_path), bool(row[2]))

    def has_local_dirs(self):
        return self.table_local_dir.has_rows()

    def get_local_dirs(self) -> List[LocalDirNode]:
        return self.table_local_dir.select_object_list()

    def insert_local_dirs(self, entries: List[LocalDirNode], overwrite, commit=True):
        self.table_local_dir.insert_object_list(entries, overwrite=overwrite, commit=commit)

    def upsert_local_dir(self, item, commit=True):
        self.table_local_dir.upsert_object(item, commit=commit)

    def delete_local_dir_with_uid(self, uid: UID, commit=True):
        sql = self.table_local_dir.build_delete() + f' WHERE uid = ?'
        self.conn.execute(sql, (uid,))
        if commit:
            logger.debug('Committing!')
            self.conn.commit()

    # Other ⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆

    def get_file_or_dir_for_path(self, full_path: str) -> Optional[Union[LocalDirNode, LocalFileNode]]:
        dir_list = self.table_local_dir.select_object_list(where_clause='WHERE full_path = ?', where_tuple=(full_path,))
        if dir_list:
            return dir_list[0]

        file_list = self.table_local_file.select_object_list(where_clause='WHERE full_path = ?', where_tuple=(full_path,))
        if file_list:
            return file_list[0]

        return None
