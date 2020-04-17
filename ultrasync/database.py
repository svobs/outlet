import sqlite3
import logging
from fmeta.fmeta import FMeta

logger = logging.getLogger(__name__)


class MetaDatabase:
    TABLE_FILE_LOG = {
        'name': 'file_log',
        'cols': (('sig', 'TEXT'),
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
        'cols': (('gd_id', 'TEXT'),
                 ('name', 'TEXT'),
                 ('par_id', 'TEXT'))
    }

    TABLE_GRDIVE_ROOTS = {
        'name': 'gdrive_root',
        'cols': (('gd_id', 'TEXT'),
                 ('name', 'TEXT'))
    }

    TABLE_GRDIVE_MORE_PARENTS = {
        'name': 'gdrive_extra_parent',
        'cols': (('par_id', 'TEXT'),
                 ('gd_id', 'TEXT'))
    }

    def __init__(self, db_path):
        logger.info(f'Connecting to database: {db_path}')
        self.conn = sqlite3.connect(db_path)
        self.create_table_if_not_exist(self.TABLE_FILE_LOG)

    # Utility Functions ---------------------

    @staticmethod
    def build_insert(table):
        return 'INSERT INTO ' + table['name'] + '(' + ','.join(col[0] for col in table['cols']) + ') VALUES (' + ','.join('?' for col in table['cols']) + ')'

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

    def has_rows(self, table):
        cursor = self.conn.cursor()
        sql = self.build_select(table) + ' LIMIT 1'
        cursor.execute(sql)
        rows = cursor.fetchall()
        has_rows = len(rows) > 0
        logger.debug(f'Table {table["name"]} has rows = {has_rows}')
        return has_rows

    # FILE_LOG operations ---------------------

    def has_file_changes(self):
        return self.has_rows(self.TABLE_FILE_LOG)

    # Gets all changes in the table
    def get_file_changes(self):
        cursor = self.conn.cursor()
        sql = self.build_select(self.TABLE_FILE_LOG)
        cursor.execute(sql)
        changes = cursor.fetchall()
        entries = []
        for c in changes:
            entries.append(FMeta(c[0], int(c[1]), int(c[2]), int(c[3]), int(c[4]), c[5], int(c[6]), c[7]))
        return entries

    # Takes a list of FMeta objects:
    def insert_file_changes(self, entries):
        to_insert = []
        for e in entries:
            e_tuple = (e.signature, e.size_bytes, e.sync_ts, e.modify_ts, e.change_ts, e.file_path, e.category.value, e.prev_path)
            to_insert.append(e_tuple)
        self.insert_many(self.TABLE_FILE_LOG, to_insert)

    def truncate_file_changes(self):
        self.truncate_table(self.TABLE_FILE_LOG)

    # GDRIVE_DIRS operations ---------------------

    def truncate_gdrive_dirs(self):
        self.truncate_table(self.TABLE_GRDIVE_MORE_PARENTS)
        self.truncate_table(self.TABLE_GRDIVE_DIRS)
        self.truncate_table(self.TABLE_GRDIVE_ROOTS)

    def has_gdrive_dirs(self):
        return self.has_rows(self.TABLE_GRDIVE_DIRS) or self.has_rows(self.TABLE_GRDIVE_MORE_PARENTS) or self.has_rows(self.TABLE_GRDIVE_ROOTS)

    def insert_gdrive_dirs(self, root_list, dir_list, more_parent_mappings):
        # if self.has_gdrive_dirs():
        #     raise RuntimeError('Will not insert GDrive meta into a non-empty table!')

        self.create_table_if_not_exist(self.TABLE_GRDIVE_ROOTS)
        self.create_table_if_not_exist(self.TABLE_GRDIVE_DIRS)
        self.create_table_if_not_exist(self.TABLE_GRDIVE_MORE_PARENTS)
        self.truncate_gdrive_dirs()

        self.insert_many(self.TABLE_GRDIVE_ROOTS, root_list)
        self.insert_many(self.TABLE_GRDIVE_DIRS, dir_list)
        self.insert_many(self.TABLE_GRDIVE_MORE_PARENTS, more_parent_mappings)

    def get_gdrive_dirs(self):
        cursor = self.conn.cursor()
        sql = self.build_select(self.TABLE_GRDIVE_ROOTS)
        cursor.execute(sql)
        root_rows = cursor.fetchall()

        cursor = self.conn.cursor()
        sql = self.build_select(self.TABLE_GRDIVE_DIRS)
        cursor.execute(sql)
        dir_rows = cursor.fetchall()

        cursor = self.conn.cursor()
        sql = self.build_select(self.TABLE_GRDIVE_MORE_PARENTS)
        cursor.execute(sql)
        parent_mappings = cursor.fetchall()
        logger.debug(f'Retrieved {len(root_rows)} roots, {len(dir_rows)} dirs, and {len(parent_mappings)} additional mappings')
        return root_rows, dir_rows, parent_mappings
