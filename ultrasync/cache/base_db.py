import sqlite3
import logging

logger = logging.getLogger(__name__)


class MetaDatabase:
    """Example:
    TABLE_CACHE_REGISTRY = {
        'name': 'cache_registry',
        'cols': (('cache_location', 'TEXT'),
                 ('cache_type', 'INTEGER'),
                 ('subtree_root', 'TEXT'),
                 ('sync_ts', 'INTEGER'),
                 ('complete', 'INTEGER'))
    }
    """
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

    def get_all_rows(self, table):
        cursor = self.conn.cursor()
        sql = self.build_select(table)
        cursor.execute(sql)
        tuples = cursor.fetchall()
        return tuples

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
