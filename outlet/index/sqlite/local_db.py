import logging
from collections import OrderedDict
from typing import List, Optional, Tuple

from index.sqlite.base_db import LiveTable, MetaDatabase, Table
from index.uid.uid import UID
from model.node.local_disk_node import LocalDirNode, LocalFileNode, LocalNode
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

    def __init__(self, db_path, app):
        super().__init__(db_path)
        self.cacheman = app.cacheman
        self.table_local_file = LiveTable(LocalDiskDatabase.TABLE_LOCAL_FILE, self.conn, _file_to_tuple, self._tuple_to_file)
        self.table_local_dir = LiveTable(LocalDiskDatabase.TABLE_LOCAL_DIR, self.conn, _dir_to_tuple, self._tuple_to_dir)

    # LOCAL_FILE operations ⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆

    def _tuple_to_file(self, row: Tuple) -> LocalFileNode:
        uid_int, md5, sha256, size_bytes, sync_ts, modify_ts, change_ts, full_path, exists = row

        uid = self.cacheman.get_uid_for_path(full_path, uid_int)
        assert uid == row[0], f'UID conflict! Got {uid} but read {uid_int} in row: {row}'
        node_identifier = LocalFsIdentifier(uid=uid, full_path=full_path)
        return LocalFileNode(node_identifier, md5, sha256, size_bytes, sync_ts, modify_ts, change_ts, exists)

    def has_local_files(self):
        return self.table_local_file.has_rows()

    def get_local_files(self) -> List[LocalFileNode]:
        return self.table_local_file.select_object_list()

    def insert_local_files(self, entries: List[LocalFileNode], overwrite, commit=True):
        self.table_local_file.insert_object_list(entries, overwrite=overwrite, commit=commit)

    def upsert_local_file(self, item, commit=True):
        self.table_local_file.upsert_object(item, commit=commit)

    def upsert_local_file_list(self, file_list: List[LocalFileNode], commit=True):
        self.table_local_file.upsert_object_list(file_list, commit=commit)

    def delete_local_file_with_uid(self, uid: UID, commit=True):
        self.table_local_file.delete_for_uid(uid, commit=commit)

    def delete_local_files_for_uid_list(self, uid_list: List[UID], commit=True):
        uid_tuple_list = list(map(lambda uid: (uid,), uid_list))
        self.table_local_file.delete_for_uid_list(uid_tuple_list, commit=commit)

    def truncate_local_files(self, commit=True):
        self.table_local_file.truncate_table(commit=commit)

    # LOCAL_DIR operations ⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆

    def _tuple_to_dir(self, row: Tuple) -> LocalDirNode:
        full_path = row[1]
        uid = self.cacheman.get_uid_for_path(full_path, row[0])
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

    def upsert_local_dir_list(self, dir_list: List[LocalDirNode], commit=True):
        self.table_local_dir.upsert_object_list(dir_list, commit=commit)

    def delete_local_dir_with_uid(self, uid: UID, commit=True):
        self.table_local_dir.delete_for_uid(uid, commit=commit)

    def delete_local_dirs_for_uid_list(self, uid_list: List[UID], commit=True):
        uid_tuple_list = list(map(lambda uid: (uid,), uid_list))
        self.table_local_dir.delete_for_uid_list(uid_tuple_list, commit=commit)

    def truncate_local_dirs(self, commit=True):
        self.table_local_dir.truncate_table(commit=commit)

    # Mixed type operations ⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆

    def upsert_single_node(self, node: LocalNode, commit=True):
        if node.is_dir():
            self.upsert_local_dir(node, commit)
        else:
            self.upsert_local_file(node, commit)

    def delete_single_node(self, node: LocalNode, commit=True):
        if node.is_dir():
            self.delete_local_dir_with_uid(node.uid, commit)
        else:
            self.delete_local_file_with_uid(node.uid, commit)

    def upsert_files_and_dirs(self, node_list: List[LocalNode], commit=True):
        """Sorts files from dirs in the given list and batches them. Much faster than single-node operations."""
        dir_list: List[LocalDirNode] = []
        file_list: List[LocalFileNode] = []
        for node in node_list:
            if node.is_dir():
                assert isinstance(node, LocalDirNode)
                dir_list.append(node)
            else:
                assert isinstance(node, LocalFileNode)
                file_list.append(node)
        if dir_list:
            self.upsert_local_dir_list(dir_list, commit=False)
        if file_list:
            self.upsert_local_file_list(file_list, commit=commit)

    def delete_files_and_dirs(self, node_list: List[LocalNode], commit=True):
        """Sorts files from dirs in the given list and batches them. Much faster than single-node operations."""
        dir_uid_list: List[UID] = []
        file_uid_list: List[UID] = []
        for node in node_list:
            if node.is_dir():
                dir_uid_list.append(node.uid)
            else:
                file_uid_list.append(node.uid)

        if dir_uid_list:
            self.delete_local_dirs_for_uid_list(dir_uid_list, commit=False)
        if file_uid_list:
            self.delete_local_files_for_uid_list(file_uid_list, commit=commit)

    def get_file_or_dir_for_path(self, full_path: str) -> Optional[LocalNode]:
        dir_list = self.table_local_dir.select_object_list(where_clause='WHERE full_path = ?', where_tuple=(full_path,))
        if dir_list:
            return dir_list[0]

        file_list = self.table_local_file.select_object_list(where_clause='WHERE full_path = ?', where_tuple=(full_path,))
        if file_list:
            return file_list[0]

        return None
