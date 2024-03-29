import itertools
import sqlite3
import logging
from typing import Any, Callable, Iterable, List, Optional, OrderedDict, Tuple, Union

from logging_constants import TRACE_ENABLED
from model.uid import UID

logger = logging.getLogger(__name__)


class Table:
    """
    ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
    CLASS Table
    Might want to migrate to SQLAlchemy in the future ?
    ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼
    """
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
        col_names = ','.join(col_name for col_name in self.cols.keys())
        return f'SELECT {col_names} FROM {self.name} '

    def build_delete(self):
        return f'DELETE FROM {self.name} '

    def build_create_table(self):
        return 'CREATE TABLE ' + self.name + '(' + ', '.join(col_name + ' ' + col_type for col_name, col_type in self.cols.items()) + ')'


class LiveTable(Table):
    """
    ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
    CLASS LiveTable

    Decorates functionality of Table by adding operations which require a connection
    ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼
    """
    def __init__(self, table: Table, conn,
                 obj_to_tuple_func: Optional[Callable[[Any], Tuple]] = None,
                 tuple_to_obj_func: Optional[Callable[[Tuple], Any]] = None):
        super().__init__(table.name, table.cols)
        self.conn = conn
        self.obj_to_tuple_func: Optional[Callable[[Any], Tuple]] = obj_to_tuple_func
        self.tuple_to_obj_func: Optional[Callable[[Tuple], Any]] = tuple_to_obj_func

    def __repr__(self):
        return f'LiveTable(name="{self.name}" cols={self.cols} obj_to_tuple_func={self.obj_to_tuple_func} ' \
               f'tuple_to_obj_func={self.tuple_to_obj_func}")'

    def commit(self):
        if TRACE_ENABLED:
            logger.debug('Committing!')
        self.conn.commit()

    def create_table(self, commit=True):
        sql = self.build_create_table()
        logger.debug('Executing SQL: ' + sql)
        self.conn.execute(sql)
        if commit:
            self.commit()

    def create_table_if_not_exist(self, commit=True):
        if not self.is_table():
            self.create_table(commit)

    def is_table(self):
        query = f"SELECT name FROM sqlite_master WHERE type='table' AND name='{self.name}';"
        cursor = self.conn.execute(query)
        result = cursor.fetchone()
        return result is not None

    def has_rows(self):
        if not self.is_table():
            return False

        cursor = self.conn.cursor()
        sql = f"SELECT * FROM {self.name} LIMIT 1"
        cursor.execute(sql)
        rows = cursor.fetchall()
        has_rows = len(rows) > 0
        logger.debug(f'Table {self.name} has rows = {has_rows}')
        return has_rows

    def select(self, where_clause: str = '', where_tuple: Tuple = None) -> List[Tuple]:
        assert not where_clause or (where_clause and where_tuple)
        cursor = self.conn.cursor()
        sql = self.build_select() + where_clause
        if where_tuple:
            cursor.execute(sql, where_tuple)
        else:
            cursor.execute(sql)
        return cursor.fetchall()

    def get_all_rows(self) -> List[Tuple]:
        return self.select()

    def update(self, stmt_vars: Union[Tuple, List[Tuple]], col_names: Optional[List[str]] = None,
               where_clause: Optional[str] = '', commit: Optional[bool] = True):
        sql = self.build_update(col_names=col_names) + where_clause
        cursor = self.conn.cursor()
        if type(stmt_vars) == list:
            logger.debug('Executing batch SQL: ' + sql)
            cursor.executemany(sql, stmt_vars)
        else:
            logger.debug('Executing SQL: ' + sql)
            cursor.execute(sql, stmt_vars)
        if commit:
            self.commit()

    def upsert_one(self, row: Tuple, commit=True):
        sql = self.build_upsert()
        logger.debug(f"Upserting one tuple into table {self.name}")
        self.conn.execute(sql, row)
        if commit:
            self.commit()

    def insert_one(self, row: Tuple, commit=True):
        sql = self.build_insert()
        logger.debug(f"Inserting one tuple into table {self.name}")
        self.conn.execute(sql, row)
        if commit:
            self.commit()

    def insert_many(self, tuple_list: List[Tuple], commit=True):
        sql = self.build_insert()
        logger.debug(f"Inserting {len(tuple_list)} tuples into table {self.name}")
        self.conn.executemany(sql, tuple_list)
        if commit:
            self.commit()

    def upsert_many(self, tuple_list: List[Tuple], commit=True):
        sql = self.build_upsert()
        logger.debug(f"Upserting {len(tuple_list)} tuples into table {self.name}")
        if len(tuple_list) == 1:
            logger.debug(f"Upserting tuple: {tuple_list[0]}")
        self.conn.executemany(sql, tuple_list)
        if commit:
            self.commit()

    def truncate_table(self, commit=True):
        sql = f"DELETE FROM {self.name}"
        logger.debug('Executing SQL: ' + sql)
        self.conn.execute(sql)
        if commit:
            self.commit()

    def drop_table_if_exists(self, commit=True):
        sql = f"DROP TABLE IF EXISTS {self.name}"
        logger.debug('Executing SQL: ' + sql)
        self.conn.execute(sql)
        if commit:
            self.commit()

    def _to_tuple_list(self, entries: List, obj_to_tuple_func_override: Optional[Callable[[Any], Tuple]] = None) -> List[Tuple]:
        if obj_to_tuple_func_override:
            to_insert = []
            for e in entries:
                e_tuple: Tuple = obj_to_tuple_func_override(e)
                to_insert.append(e_tuple)
        elif self.obj_to_tuple_func:
            to_insert = []
            for e in entries:
                e_tuple: Tuple = self.obj_to_tuple_func(e)
                to_insert.append(e_tuple)
        else:
            to_insert = entries

        return to_insert

    def next_id_in_seq(self) -> str:
        """Gets the next value in the table's ID sequence. (NOTE: the identifier column must have AUTOINCREMENT specified in its definition!)"""
        sql = f"SELECT seq FROM sqlite_sequence WHERE name = ? LIMIT 1"
        logger.debug('Executing SQL: ' + sql)
        cursor = self.conn.execute(sql, (self.name,))
        row = cursor.fetchone()
        return row

    def get_last_insert_rowid(self) -> int:
        sql = ' SELECT last_insert_rowid()'
        logger.debug('Executing SQL: ' + sql)
        cursor = self.conn.execute(sql)
        return cursor.fetchone()[0]

    def insert_object(self, item: Any, commit=True, obj_to_tuple_func_override: Optional[Callable[[Any], Tuple]] = None):
        if obj_to_tuple_func_override:
            item: Tuple = obj_to_tuple_func_override(item)
        elif self.obj_to_tuple_func:
            item: Tuple = self.obj_to_tuple_func(item)
        else:
            assert isinstance(item, Tuple)
        self.insert_one(item, commit=commit)

    def insert_object_list(self, entries: List, overwrite: bool, commit: bool = True,
                           obj_to_tuple_func_override: Optional[Callable[[Any], Tuple]] = None):
        """ Takes a list of objects and inserts them all """
        to_insert: List[Tuple] = self._to_tuple_list(entries, obj_to_tuple_func_override)

        if overwrite:
            # Hmm this doesn't look smart
            logger.info(f'insert_object_list(): dropping table "{self.name}" because overwrite==True')
            self.drop_table_if_exists(commit=False)

        self.create_table_if_not_exist(commit=False)

        self.insert_many(to_insert, commit)

    def upsert_object_list(self, entries: List, overwrite: bool = False, commit: bool = True,
                           obj_to_tuple_func_override: Optional[Callable[[Any], Tuple]] = None):
        """ Takes a list of objects and inserts them all """
        to_insert: List[Tuple] = self._to_tuple_list(entries, obj_to_tuple_func_override)

        if overwrite:
            self.drop_table_if_exists(commit=False)

        self.create_table_if_not_exist(commit=False)

        self.upsert_many(to_insert, commit)

    def upsert_object(self, item: Any, commit=True, obj_to_tuple_func_override: Optional[Callable[[Any], Tuple]] = None):
        if obj_to_tuple_func_override:
            item: Tuple = obj_to_tuple_func_override(item)
        elif self.obj_to_tuple_func:
            item: Tuple = self.obj_to_tuple_func(item)
        else:
            assert isinstance(item, Tuple)
        self.upsert_one(item, commit=commit)

    def select_object_list(self, where_clause: str = '', where_tuple: Tuple = None,
                           tuple_to_obj_func_override: Optional[Callable[[Tuple], Any]] = None) -> List[Any]:
        """ Gets all changes in the table. If 'where_clause' is used, 'where_tuple' supplies the arguments to it """

        entries: List[Any] = []

        if not self.is_table():
            return entries

        rows = self.select(where_clause, where_tuple)
        if tuple_to_obj_func_override:
            for row in rows:
                entries.append(tuple_to_obj_func_override(row))
        elif self.tuple_to_obj_func:
            for row in rows:
                entries.append(self.tuple_to_obj_func(row))
        else:
            entries = rows
        logger.debug(f'Retrieved {len(entries)} objects from table {self.name}')
        return entries

    def select_row_for_uid(self, uid: UID, uid_col_name: str = 'uid'):
        rows = self.select(where_clause=f' WHERE {uid_col_name} = ?', where_tuple=(uid,))
        if len(rows) > 1:
            raise RuntimeError(f'Expected at most 1 row but got {len(rows)} for uid={uid}')

        if rows:
            return rows[0]
        return None

    def select_object_for_uid(self, uid: UID, uid_col_name: str = 'uid', tuple_to_obj_func_override: Optional[Callable[[Tuple], Any]] = None):
        row = self.select_row_for_uid(uid, uid_col_name)
        if row:
            if tuple_to_obj_func_override:
                return tuple_to_obj_func_override(row)
            elif self.tuple_to_obj_func:
                return self.tuple_to_obj_func(row)
            else:
                return row

        return None

    def delete_for_uid(self, uid: UID, uid_col_name: str = 'uid', commit=True):
        sql = self.build_delete() + f' WHERE {uid_col_name} = ?'
        count_deleted = self.conn.execute(sql, (uid,)).rowcount
        logger.debug(f'Removed {count_deleted} rows (UID={uid}) from table "{self.name}"')

        if commit:
            self.commit()

    def delete_for_uid_list(self, uid_tuple_list: List[Tuple[UID]], uid_col_name: str = 'uid', commit=True):
        """NOTE: uid_tuple_list must be a list of TUPLES, each containing one UID (this is needed for SQLite)"""
        sql = self.build_delete() + f' WHERE {uid_col_name} = ?'
        count_deleted = self.conn.executemany(sql, uid_tuple_list).rowcount
        logger.debug(f'Removed {count_deleted} rows (expected {len(uid_tuple_list)}) from table "{self.name}"')

        if commit:
            self.commit()

    def select_max(self, row_name: str) -> int:
        cursor = self.conn.cursor()
        # more efficient than using max(), but achieves same result
        sql = f"SELECT {row_name} FROM {self.name} ORDER BY {row_name} DESC LIMIT 1"
        cursor.execute(sql)
        result = cursor.fetchone()
        if result:
            return result[0]
        return -1

    @staticmethod
    def build_question_list(size: int) -> str:
        return ",".join(list(itertools.repeat('?', size)))


class MetaDatabase:
    """
    ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
    CLASS MetaDatabase

    Example:
    TABLE_LOCAL_FILE = Table(name='local_file', cols={'uid': 'INTEGER PRIMARY KEY',
                                                      'md5': 'TEXT',
                                                      'sha256': 'TEXT',
                                                      'size_bytes': 'INTEGER',
                                                      'sync_ts': 'INTEGER',
                                                      'modify_ts': 'INTEGER',
                                                      'change_ts': 'INTEGER',
                                                      'full_path': 'TEXT'})
    ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼
    """
    def __init__(self, db_path: str):
        logger.debug(f'Opening database: {db_path}')
        # Use check_same_thread=False to tell SQLite that we are grownups and can handle multi-threading
        # 'DEFERRED' means to disable auto-commit mode
        self.conn = sqlite3.connect(db_path, check_same_thread=False, isolation_level='DEFERRED')
        self.db_path = db_path

    def __enter__(self):
        assert self.conn is not None
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    # Utility Functions
    # ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼

    def commit(self):
        if TRACE_ENABLED:
            logger.debug('Committing!')
        self.conn.commit()

    def close(self):
        # We can also close the connection if we are done with it.
        # Just be sure any changes have been committed or they will be lost.
        self.conn.close()
        if logger.isEnabledFor(logging.DEBUG):
            logger.debug(f'Closed database: {self.db_path}')
