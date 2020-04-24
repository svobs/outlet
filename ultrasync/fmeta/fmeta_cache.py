import logging

from database import MetaDatabase
from fmeta.fmeta import FMeta

logger = logging.getLogger(__name__)


class FMetaCache(MetaDatabase):
    TABLE_LOCAL_FILE = {
        'name': 'local_file',
        'cols': (('sig', 'TEXT'),
                 # TODO: sig -> md5 and sha256
                 ('size_bytes', 'INTEGER'),
                 ('sync_ts', 'INTEGER'),
                 ('modify_ts', 'INTEGER'),
                 ('change_ts', 'INTEGER'),
                 ('rel_path', 'TEXT'),
                 ('category', 'TEXT'),
                 ('prev_rel_path', 'TEXT'))
    }

    def __init__(self, db_path):
        super().__init__(db_path)

    # FILE_LOG operations ---------------------

    def has_local_files(self):
        return self.has_rows(self.TABLE_LOCAL_FILE)

    # Gets all changes in the table
    def get_local_files(self):
        cursor = self.conn.cursor()
        sql = self.build_select(self.TABLE_LOCAL_FILE)
        cursor.execute(sql)
        changes = cursor.fetchall()
        entries = []
        for c in changes:
            entries.append(FMeta(c[0], int(c[1]), int(c[2]), int(c[3]), int(c[4]), c[5], int(c[6]), c[7]))
        return entries

    # Takes a list of FMeta objects:
    def insert_local_files(self, entries):
        to_insert = []
        for e in entries:
            e_tuple = (e.signature, e.size_bytes, e.sync_ts, e.modify_ts, e.change_ts, e.file_path, e.category.value, e.prev_path)
            to_insert.append(e_tuple)
        self.insert_many(self.TABLE_LOCAL_FILE, to_insert)

    def truncate_local_files(self):
        self.truncate_table(self.TABLE_LOCAL_FILE)
