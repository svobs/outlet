import sqlite3
import logging
from typing import Dict, Iterable, List, Optional, OrderedDict, Tuple, Union

logger = logging.getLogger(__name__)


# CLASS Table
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼

class Table:
    def __init__(self, name: str, cols: OrderedDict[str, str]):
        self.name: str = name
        self.cols: OrderedDict[str, str] = cols

    # Factory methods:

    def build_insert(self):
        return 'INSERT INTO ' + self.name + '(' + ','.join(col_name for col_name in self.cols.keys()) + \
               ') VALUES (' + ','.join('?' for i in range(len(self.cols))) + ')'

    def build_upsert(self):
        return 'INSERT OR REPLACE INTO ' + self.name + '(' + ','.join(col for col in self.cols) + \
               ') VALUES (' + ','.join('?' for i in range(len(self.cols))) + ')'

    def build_update(self, col_names: Iterable[str] = None):
        if not col_names:
            col_names = self.cols.keys()
            col_setters = ','.join(col_name + '=?' for col_name in col_names)
        else:
            col_setters = ','.join(col_name + '=?' for col_name in col_names)
        return f"UPDATE {self.name} SET {col_setters} "

    def build_select(self):
        return 'SELECT ' + ','.join(col_name for col_name in self.cols.keys()) + ' FROM ' + self.name

    def build_delete(self):
        return f"DELETE FROM {self.name} "

    def build_create_table(self):
        return 'CREATE TABLE ' + self.name + '(' + ', '.join(col_name + ' ' + col_type for col_name, col_type in self.cols.items()) + ')'

    # General-purpose CRUD:

    def create_table(self, conn, commit=True):
        sql = self.build_create_table()
        logger.debug('Executing SQL: ' + sql)
        conn.execute(sql)
        if commit:
            logger.debug('Committing!')
            conn.commit()

    def create_table_if_not_exist(self, conn, commit=True):
        if not self.is_table(conn):
            self.create_table(conn, commit)

    def is_table(self, conn):
        query = "SELECT name FROM sqlite_master WHERE type='table' AND name='" + self.name + "';"
        cursor = conn.execute(query)
        result = cursor.fetchone()
        return result is not None

    def has_rows(self, conn):
        if not self.is_table(conn):
            return False

        cursor = conn.cursor()
        sql = f"SELECT * FROM {self.name} LIMIT 1"
        cursor.execute(sql)
        rows = cursor.fetchall()
        has_rows = len(rows) > 0
        logger.debug(f'Table {self.name} has rows = {has_rows}')
        return has_rows

    def select(self, conn, where_clause: str) -> List[Tuple]:
        cursor = conn.cursor()
        sql = self.build_select() + where_clause
        cursor.execute(sql)
        return cursor.fetchall()

    def get_all_rows(self, conn) -> List[Tuple]:
        return self.select(conn, '')

    def update(self, conn, stmt_vars: Union[Tuple, List[Tuple]], col_names: Optional[List[str]] = None,
               where_clause: Optional[str] = '', commit: Optional[bool] = True):
        sql = self.build_update(col_names=col_names) + where_clause
        cursor = conn.cursor()
        if type(stmt_vars) == list:
            logger.debug('Executing batch SQL: ' + sql)
            cursor.executemany(sql, stmt_vars)
        else:
            logger.debug('Executing SQL: ' + sql)
            cursor.execute(sql, stmt_vars)
        if commit:
            logger.debug('Committing!')
            conn.commit()

    def upsert_one(self, conn, row: Tuple, commit=True):
        sql = self.build_upsert()
        logger.debug(f"Upserting one tuple into table {self.name}")
        conn.execute(sql, row)
        if commit:
            logger.debug('Committing!')
            conn.commit()

    def insert_one(self, conn, row: Tuple, commit=True):
        sql = self.build_insert()
        logger.debug(f"Inserting one tuple into table {self.name}")
        conn.execute(sql, row)
        if commit:
            logger.debug('Committing!')
            conn.commit()

    def insert_many(self, conn, tuples, commit=True):
        sql = self.build_insert()
        logger.debug(f"Inserting {len(tuples)} tuples into table {self.name}")
        conn.executemany(sql, tuples)
        if commit:
            logger.debug('Committing!')
            conn.commit()

    def upsert_many(self, conn, tuples, commit=True):
        sql = self.build_upsert()
        logger.debug(f"Upserting {len(tuples)} tuples into table {self.name}")
        conn.executemany(sql, tuples)
        if commit:
            logger.debug('Committing!')
            conn.commit()

    def truncate_table(self, conn, commit=True):
        sql = f"DELETE FROM {self.name}"
        logger.debug('Executing SQL: ' + sql)
        conn.execute(sql)
        if commit:
            logger.debug('Committing!')
            conn.commit()

    def drop_table_if_exists(self, conn, commit=True):
        sql = f"DROP TABLE IF EXISTS {self.name}"
        logger.debug('Executing SQL: ' + sql)
        conn.execute(sql)
        if commit:
            logger.debug('Committing!')
            conn.commit()


# CLASS MetaDatabase
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼

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

    def close(self):
        # We can also close the connection if we are done with it.
        # Just be sure any changes have been committed or they will be lost.
        self.conn.close()
        if logger.isEnabledFor(logging.DEBUG):
            logger.debug(f'Closed database: {self.db_path}')
