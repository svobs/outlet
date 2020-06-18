import sqlite3
import logging
from typing import Dict, Iterable, List, Optional, Tuple, Union

logger = logging.getLogger(__name__)


class Table:
    def __init__(self, name: str, cols: Dict[str, str]):
        self.name: str = name
        self.cols = cols


class MetaDatabase:
    """Example:
    TABLE_LOCAL_FILE = Table(name='local_file', cols={'uid': 'INTEGER PRIMARY KEY',
                                                      'md5': 'TEXT',
                                                      'sha256': 'TEXT',
                                                      'size_bytes': 'INTEGER',
                                                      'sync_ts': 'INTEGER',
                                                      'modify_ts': 'INTEGER',
                                                      'change_ts': 'INTEGER',
                                                      'full_path': 'TEXT'})
    """
    def __init__(self, db_path):
        logger.debug(f'Opening database: {db_path}')
        self.conn = sqlite3.connect(db_path)
        if logger.isEnabledFor(logging.DEBUG):
            self.db_path = db_path

    def __enter__(self):
        assert self.conn is not None
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    # Utility Functions ⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆

    @staticmethod
    def build_insert(table: Table):
        return 'INSERT INTO ' + table.name + '(' + ','.join(col_name for col_name in table.cols.keys()) +\
               ') VALUES (' + ','.join('?' for i in range(len(table.cols))) + ')'

    @staticmethod
    def build_upsert(table: Table):
        return 'INSERT OR REPLACE INTO ' + table.name + '(' + ','.join(col[0] for col in table.cols) + \
               ') VALUES (' + ','.join('?' for i in range(len(table.cols))) + ')'

    @staticmethod
    def build_select(table: Table):
        return 'SELECT ' + ','.join(col_name for col_name in table.cols.keys()) + ' FROM ' + table.name

    @staticmethod
    def build_delete(table: Table):
        return f"DELETE FROM {table.name} "

    @staticmethod
    def build_update(table, col_names: Iterable[str] = None):
        if not col_names:
            col_names = table.cols.keys()
            col_setters = ','.join(col_name + '=?' for col_name in col_names)
        else:
            col_setters = ','.join(col_name + '=?' for col_name in col_names)
        return f"UPDATE {table.name} SET {col_setters} "

    @staticmethod
    def build_create_table(table: Table):
        return 'CREATE TABLE ' + table.name + '(' + ', '.join(col_name + ' ' + col_type for col_name, col_type in table.cols.items()) + ')'

    def close(self):
        # We can also close the connection if we are done with it.
        # Just be sure any changes have been committed or they will be lost.
        self.conn.close()
        if logger.isEnabledFor(logging.DEBUG):
            logger.debug(f'Closed database: {self.db_path}')

    def select(self, table: Table, where_clause: str) -> List[Tuple]:
        cursor = self.conn.cursor()
        sql = self.build_select(table) + where_clause
        cursor.execute(sql)
        return cursor.fetchall()

    def get_all_rows(self, table) -> List[Tuple]:
        return self.select(table, '')

    def update(self, table: Table, stmt_vars: Union[Tuple, List[Tuple]], col_names: Optional[List[str]] = None,
               where_clause: Optional[str] = '', commit: Optional[bool] = True):
        sql = self.build_update(table=table, col_names=col_names) + where_clause
        cursor = self.conn.cursor()
        if type(stmt_vars) == list:
            logger.debug('Executing batch SQL: ' + sql)
            cursor.executemany(sql, stmt_vars)
        else:
            logger.debug('Executing SQL: ' + sql)
            cursor.execute(sql, stmt_vars)
        if commit:
            logger.debug('Committing!')
            self.conn.commit()

    def is_table(self, table):
        query = "SELECT name FROM sqlite_master WHERE type='table' AND name='" + table.name + "';"
        cursor = self.conn.execute(query)
        result = cursor.fetchone()
        return result is not None

    def create_table_if_not_exist(self, table: Table, commit=True):
        if not self.is_table(table):
            self.create_table(table, commit)

    def upsert_one(self, table: Table, row: Tuple, commit=True):
        sql = self.build_upsert(table)
        logger.debug(f"Upserting one tuple into table {table.name}")
        self.conn.execute(sql, row)
        if commit:
            logger.debug('Committing!')
            self.conn.commit()

    def insert_one(self, table: Table, row: Tuple, commit=True):
        sql = self.build_insert(table)
        logger.debug(f"Inserting one tuple into table {table.name}")
        self.conn.execute(sql, row)
        if commit:
            logger.debug('Committing!')
            self.conn.commit()

    def insert_many(self, table: Table, tuples, commit=True):
        sql = self.build_insert(table)
        logger.debug(f"Inserting {len(tuples)} tuples into table {table.name}")
        self.conn.executemany(sql, tuples)
        if commit:
            logger.debug('Committing!')
            self.conn.commit()

    def upsert_many(self, table: Table, tuples, commit=True):
        sql = self.build_upsert(table)
        logger.debug(f"Upserting {len(tuples)} tuples into table {table.name}")
        self.conn.executemany(sql, tuples)
        if commit:
            logger.debug('Committing!')
            self.conn.commit()

    def create_table(self, table: Table, commit=True):
        sql = self.build_create_table(table)
        logger.debug('Executing SQL: ' + sql)
        self.conn.execute(sql)
        if commit:
            logger.debug('Committing!')
            self.conn.commit()

    def truncate_table(self, table: Table, commit=True):
        sql = f"DELETE FROM {table.name}"
        logger.debug('Executing SQL: ' + sql)
        self.conn.execute(sql)
        if commit:
            logger.debug('Committing!')
            self.conn.commit()

    def drop_table_if_exists(self, table: Table, commit=True):
        sql = f"DROP TABLE IF EXISTS {table.name}"
        logger.debug('Executing SQL: ' + sql)
        self.conn.execute(sql)
        if commit:
            logger.debug('Committing!')
            self.conn.commit()

    def has_rows(self, table: Table):
        if not self.is_table(table):
            return False

        cursor = self.conn.cursor()
        sql = f"SELECT * FROM {table.name} LIMIT 1"
        cursor.execute(sql)
        rows = cursor.fetchall()
        has_rows = len(rows) > 0
        logger.debug(f'Table {table.name} has rows = {has_rows}')
        return has_rows
