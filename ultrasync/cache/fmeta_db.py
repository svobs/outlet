import logging

from cache.base_db import MetaDatabase
from model.fmeta import FMeta

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
                 ('category', 'TEXT'),
                 ('prev_rel_path', 'TEXT'))
    }

    def __init__(self, db_path):
        super().__init__(db_path)

    # FILE_LOG operations ---------------------

    def has_local_files(self):
        return self.has_rows(self.TABLE_LOCAL_FILE)

    def get_local_files(self):
        """ Gets all changes in the table """
        cursor = self.conn.cursor()
        sql = self.build_select(self.TABLE_LOCAL_FILE)
        cursor.execute(sql)
        rows = cursor.fetchall()
        entries = []
        for row in rows:
            entries.append(FMeta(*row))
        return entries

    def insert_local_files(self, entries, overwrite):
        """ Takes a list of FMeta objects: """
        to_insert = []
        for e in entries:
            e_tuple = (e.md5, e.sha256, e.size_bytes, e.sync_ts, e.modify_ts, e.change_ts, e.full_path, e.category.value, e.prev_path)
            to_insert.append(e_tuple)

        if overwrite:
            self.drop_table_if_exists(self.TABLE_LOCAL_FILE)
            self.create_table_if_not_exist(self.TABLE_LOCAL_FILE)

        self.insert_many(self.TABLE_LOCAL_FILE, to_insert)

    def truncate_local_files(self):
        self.truncate_table(self.TABLE_LOCAL_FILE)
