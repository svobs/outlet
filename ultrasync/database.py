import sqlite3
import logging
from fmeta.fmeta import FMeta

logger = logging.getLogger(__name__)


class MetaDatabase:
    TABLE_CACHE_REGISTRY = {
        'name': 'cache_registry',
        'cols': (('cache_location', 'TEXT'),
                 ('cache_type', 'INTEGER'),
                 ('subtree_root', 'TEXT'),
                 ('sync_ts', 'INTEGER'),
                 ('complete', 'INTEGER'))
    }

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

    TABLE_GRDIVE_DIRS = {
        'name': 'gdrive_directory',
        'cols': (('id', 'TEXT'),
                 ('name', 'TEXT'),
                 ('parent_id', 'TEXT'),
                 ('trashed', 'INTEGER'),
                 ('drive_id', 'TEXT'),
                 ('my_share', 'INTEGER'),
                 ('sync_ts', 'INTEGER'))
        # TODO: children_fetched
    }

    TABLE_GRDIVE_MULTIPLE_PARENTS = {
        'name': 'gdrive_multiple_parents',
        'cols': (('id', 'TEXT'),)
    }

    TABLE_GRDIVE_FILES = {
        'name': 'gdrive_files',
        'cols': (('id', 'TEXT'),
                 ('name', 'TEXT'),
                 ('parent_id', 'TEXT'),
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
                 ('sync_ts', 'INTEGER'))

    }

    def __init__(self, db_path):
        logger.info(f'Connecting to database: {db_path}')
        self.conn = sqlite3.connect(db_path)

    # Utility Functions ---------------------

    @staticmethod
    def build_insert(table):
        return 'INSERT INTO ' + table['name'] + '(' + ','.join(col[0] for col in table['cols']) +\
               ') VALUES (' + ','.join('?' for col in table['cols']) + ')'

    @staticmethod
    def build_select(table):
        return 'SELECT ' + ','.join(col[0] for col in table['cols']) + ' FROM ' + table['name']

    @staticmethod
    def build_create_table(table):
        return 'CREATE TABLE ' + table['name'] + '(' + ', '.join(col[0] + ' ' + col[1] for col in table['cols']) + ')'

    def is_table(self, table):
        query = "SELECT name FROM sqlite_master WHERE type='table' AND name='" + table['name'] + "';"
        cursor = self.conn.execute(query)
        result = cursor.fetchone()
        return result is not None

    def create_table_if_not_exist(self, table):
        if not self.is_table(table):
            self.create_table(table)

    def insert_many(self, table, tuples):
        sql = self.build_insert(table)
        logger.debug(f"Inserting {len(tuples)} tuples into table {table['name']}")
        self.conn.executemany(sql, tuples)
        self.conn.commit()

    def close(self):
        # We can also close the connection if we are done with it.
        # Just be sure any changes have been committed or they will be lost.
        self.conn.close()

    def create_table(self, table):
        sql = self.build_create_table(table)
        logger.debug('Executing SQL: ' + sql)
        self.conn.execute(sql)
        self.conn.commit()

    def truncate_table(self, table):
        sql = f"DELETE FROM {table['name']}"
        logger.debug('Executing SQL: ' + sql)
        self.conn.execute(sql)
        self.conn.commit()

    def drop_table_if_exists(self, table):
        sql = f"DROP TABLE IF EXISTS {table['name']}"
        logger.debug('Executing SQL: ' + sql)
        self.conn.execute(sql)
        self.conn.commit()

    def has_rows(self, table):
        if not self.is_table(table):
            return False

        cursor = self.conn.cursor()
        sql = f"SELECT * FROM {table['name']} LIMIT 1"
        cursor.execute(sql)
        rows = cursor.fetchall()
        has_rows = len(rows) > 0
        logger.debug(f'Table {table["name"]} has rows = {has_rows}')
        return has_rows

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

    # GDRIVE_DIRS operations ---------------------

    def truncate_gdrive_dirs(self):
        self.truncate_table(self.TABLE_GRDIVE_MULTIPLE_PARENTS)
        self.truncate_table(self.TABLE_GRDIVE_DIRS)

    def has_gdrive_dirs(self):
        return self.has_rows(self.TABLE_GRDIVE_DIRS) or self.has_rows(self.TABLE_GRDIVE_MULTIPLE_PARENTS)

    def insert_gdrive_dirs(self, dir_list, overwrite=False):
        if not overwrite and self.has_gdrive_dirs():
            raise RuntimeError('Will not insert GDrive meta into a non-empty table!')

        self.drop_table_if_exists(self.TABLE_GRDIVE_DIRS)
        self.create_table_if_not_exist(self.TABLE_GRDIVE_DIRS)

        self.insert_many(self.TABLE_GRDIVE_DIRS, dir_list)

    def get_gdrive_dirs(self):
        cursor = self.conn.cursor()
        sql = self.build_select(self.TABLE_GRDIVE_DIRS)
        cursor.execute(sql)
        dir_rows = cursor.fetchall()

        logger.debug(f'Retrieved {len(dir_rows)} dirs')
        return dir_rows

    # GDRIVE_FILES operations ---------------------

    def has_gdrive_files(self):
        return self.has_rows(self.TABLE_GRDIVE_FILES)

    def insert_gdrive_files(self, file_list, overwrite=False):
        if not overwrite and self.has_gdrive_files():
            raise RuntimeError('Will not insert GDrive meta into a non-empty table!')

        self.drop_table_if_exists(self.TABLE_GRDIVE_FILES)
        self.create_table_if_not_exist(self.TABLE_GRDIVE_FILES)

        self.insert_many(self.TABLE_GRDIVE_FILES, file_list)

    def get_gdrive_files(self):
        cursor = self.conn.cursor()
        sql = self.build_select(self.TABLE_GRDIVE_FILES)
        cursor.execute(sql)
        file_rows = cursor.fetchall()

        logger.debug(f'Retrieved {len(file_rows)} file metas')
        return file_rows

    def insert_multiple_parent_mappings(self, ids_with_multiple_parents, overwrite=False):
        if not overwrite and self.is_table(self.TABLE_GRDIVE_MULTIPLE_PARENTS):
            raise RuntimeError('Will not insert GDrive meta into a non-empty table!')

        self.drop_table_if_exists(self.TABLE_GRDIVE_MULTIPLE_PARENTS)
        self.create_table_if_not_exist(self.TABLE_GRDIVE_MULTIPLE_PARENTS)
        self.insert_many(self.TABLE_GRDIVE_MULTIPLE_PARENTS, ids_with_multiple_parents)

    def get_multiple_parent_ids(self):
        # this will include extraneous stuff from parent meta
        cursor = self.conn.cursor()
        sql = self.build_select(self.TABLE_GRDIVE_MULTIPLE_PARENTS)
        cursor.execute(sql)
        parent_ids = cursor.fetchall()
        logger.debug(f'Retrieved {len(parent_ids)} items with multiple parents')
        return parent_ids
