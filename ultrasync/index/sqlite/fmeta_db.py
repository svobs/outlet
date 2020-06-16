import logging
from typing import List

from index.sqlite.base_db import MetaDatabase
from model.display_node import DirNode
from model.fmeta import FMeta
from model.node_identifier import LocalFsIdentifier

logger = logging.getLogger(__name__)


class FMetaDatabase(MetaDatabase):
    TABLE_LOCAL_FILE = {
        'name': 'local_file',
        'cols': (('uid', 'INTEGER PRIMARY KEY'),
                 ('md5', 'TEXT'),
                 ('sha256', 'TEXT'),
                 ('size_bytes', 'INTEGER'),
                 ('sync_ts', 'INTEGER'),
                 ('modify_ts', 'INTEGER'),
                 ('change_ts', 'INTEGER'),
                 ('full_path', 'TEXT'))
    }

    # 2020-06: So far this is really just a mapping of UIDs to paths, to keep things consistent across runs.
    TABLE_LOCAL_DIR = {
        'name': 'local_dir',
        'cols': (('uid', 'INTEGER PRIMARY KEY'),
                 ('full_path', 'TEXT'))
    }

    def __init__(self, db_path, application):
        super().__init__(db_path)
        self.cache_manager = application.cache_manager

    # LOCAL_FILE operations ⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆

    def has_local_files(self):
        return self.has_rows(self.TABLE_LOCAL_FILE)

    def get_local_files(self) -> List[FMeta]:
        entries: List[FMeta] = []

        """ Gets all changes in the table """
        if not self.is_table(self.TABLE_LOCAL_FILE):
            return entries

        rows = self.get_all_rows(self.TABLE_LOCAL_FILE)
        for row in rows:
            full_path = row[7]
            uid = self.cache_manager.get_uid_for_path(full_path, row[0])
            assert uid == row[0], f'UID conflict! Got {uid} but read {row[0]} in row: {row}'
            entries.append(FMeta(uid, *row[1:]))
        return entries

    def insert_local_files(self, entries: List[FMeta], overwrite, commit=True):
        """ Takes a list of FMeta objects: """
        to_insert = []
        for e in entries:
            if not e.is_planning_node():
                assert isinstance(e, FMeta), f'Expected FMeta; got instead: {e}'
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

    # LOCAL_DIR operations ⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆

    def has_local_dirs(self):
        return self.has_rows(self.TABLE_LOCAL_DIR)

    def get_local_dirs(self) -> List[DirNode]:
        """ Gets all changes in the table """
        entries: List[DirNode] = []

        if not self.is_table(self.TABLE_LOCAL_DIR):
            return entries

        rows = self.get_all_rows(self.TABLE_LOCAL_DIR)
        for row in rows:
            full_path = row[1]
            uid = self.cache_manager.get_uid_for_path(full_path, row[0])
            assert uid == row[0], f'UID conflict! Got {uid} but read {row}'
            entries.append(DirNode(LocalFsIdentifier(uid=uid, full_path=full_path)))
        return entries

    def insert_local_dirs(self, entries: List[DirNode], overwrite, commit=True):
        """ Takes a list of DirNode objects: """
        to_insert = []
        for d in entries:
            if not d.is_planning_node():
                assert isinstance(d, DirNode), f'Expected DirNode; got instead: {d}'
                d_tuple = _make_dir_tuple(d)
                to_insert.append(d_tuple)

        if overwrite:
            self.drop_table_if_exists(self.TABLE_LOCAL_DIR)

        self.create_table_if_not_exist(self.TABLE_LOCAL_DIR)

        self.insert_many(self.TABLE_LOCAL_DIR, to_insert, commit)

    def upsert_local_dir(self, item, commit=True):
        self.upsert_one(self.TABLE_LOCAL_DIR, _make_dir_tuple(item), commit=commit)


def _make_file_tuple(f):
    return f.uid, f.md5, f.sha256, f.size_bytes, f.sync_ts, f.modify_ts, f.change_ts, f.full_path


def _make_dir_tuple(d):
    return d.uid, d.full_path
