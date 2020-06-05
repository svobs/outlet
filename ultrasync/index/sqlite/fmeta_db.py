import logging

from index.sqlite.base_db import MetaDatabase
from index.uid_generator import UidGenerator
from model.fmeta import FMeta
from model.planning_node import PlanningNode

logger = logging.getLogger(__name__)


class FMetaDatabase(MetaDatabase):
    TABLE_LOCAL_FILE = {
        'name': 'local_file',
        'cols': (('md5', 'TEXT'),
                 ('sha256', 'TEXT'),
                 ('size_bytes', 'INTEGER'),
                 ('sync_ts', 'INTEGER'),
                 ('modify_ts', 'INTEGER'),
                 ('change_ts', 'INTEGER'),
                 ('full_path', 'TEXT'),
                 ('category', 'TEXT'))
    }

    def __init__(self, db_path, application):
        super().__init__(db_path)
        self.cache_manager = application.cache_manager

    # FILE_LOG operations ---------------------

    def has_local_files(self):
        return self.has_rows(self.TABLE_LOCAL_FILE)

    def get_local_files(self):
        """ Gets all changes in the table """
        rows = self.get_all_rows(self.TABLE_LOCAL_FILE)
        entries = []
        for row in rows:
            path = row[6]
            uid = self.cache_manager.get_uid_for_path(path)
            entries.append(FMeta(uid, *row))
        return entries

    def insert_local_files(self, entries, overwrite):
        """ Takes a list of FMeta objects: """
        to_insert = []
        for e in entries:
            if not isinstance(e, PlanningNode):
                e_tuple = _make_tuple(e)
                to_insert.append(e_tuple)

        if overwrite:
            self.drop_table_if_exists(self.TABLE_LOCAL_FILE)

        self.create_table_if_not_exist(self.TABLE_LOCAL_FILE)

        self.insert_many(self.TABLE_LOCAL_FILE, to_insert)

    def truncate_local_files(self):
        self.truncate_table(self.TABLE_LOCAL_FILE)

    def insert_local_file(self, item, commit=True):
        logger.debug(f'Inserting DB entry for: {item.full_path}')
        self.insert_one(self.TABLE_LOCAL_FILE, _make_tuple(item), commit=commit)

    def update_local_file(self, item, commit=True):
        # We just add another row with the same full_path, which will implicitly overwrite the previous version
        logger.debug(f'Inserting updated DB entry for: {item.full_path}')
        self.insert_one(self.TABLE_LOCAL_FILE, _make_tuple(item), commit=commit)


def _make_tuple(e):
    return e.md5, e.sha256, e.size_bytes, e.sync_ts, e.modify_ts, e.change_ts, e.full_path, e.category
