import copy
import logging
import time
from collections import OrderedDict
from typing import Dict, Iterable, List, Union

from index.sqlite.gdrive_db import GDriveDatabase
from index.sqlite.local_db import LocalDiskDatabase
from model.change_action import ChangeAction, ChangeActionRef, ChangeType
from index.sqlite.base_db import MetaDatabase, Table
from index.uid.uid import UID
from model.node.display_node import DisplayNode

logger = logging.getLogger(__name__)

PENDING = 'pending'
ARCHIVE = 'archive'
SRC = 'src'
DST = 'dst'


def _ensure_uid(val):
    """Converts val to UID but allows for null"""
    if val:
        return UID(val)
    return None


def _copy_and_augment_table(src_table: Table, prefix: str, suffix: str) -> Table:
    table: Table = copy.deepcopy(src_table)
    table.name = f'{prefix}_{table.name}_{suffix}'
    # uid is no longer primary key
    table.cols.update({'uid': 'INTEGER'})
    # only one row per change action
    table.cols.update({'action_uid': 'INTEGER PRIMARY KEY'})
    # move to front:
    table.cols.move_to_end('action_uid', last=False)

    return table


# CLASS PendingChangeDatabase
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼

class PendingChangeDatabase(MetaDatabase):
    TABLE_PENDING_CHANGE = Table(name='pending_change', cols=OrderedDict([
        ('uid', 'INTEGER PRIMARY KEY'),
        ('batch_uid', 'INTEGER'),
        ('change_type', 'INTEGER'),
        ('src_node_uid', 'INTEGER'),
        ('dst_node_uid', 'INTEGER'),
        ('create_ts', 'INTEGER')
    ]))

    TABLE_COMPLETED_CHANGE = Table(name='completed_change',
                                   cols=OrderedDict([
                                       ('uid', 'INTEGER PRIMARY KEY'),
                                       ('batch_uid', 'INTEGER'),
                                       ('change_type', 'INTEGER'),
                                       ('src_node_uid', 'INTEGER'),
                                       ('dst_node_uid', 'INTEGER'),
                                       ('create_ts', 'INTEGER'),
                                       ('complete_ts', 'INTEGER')
                                   ]))

    TABLE_FAILED_CHANGE = Table(name='failed_change',
                                cols=OrderedDict([
                                    ('uid', 'INTEGER PRIMARY KEY'),
                                    ('batch_uid', 'INTEGER'),
                                    ('change_type', 'INTEGER'),
                                    ('src_node_uid', 'INTEGER'),
                                    ('dst_node_uid', 'INTEGER'),
                                    ('create_ts', 'INTEGER'),
                                    ('complete_ts', 'INTEGER'),
                                    ('error_msg', 'TEXT')
                                ]))

    # pending ...
    TABLE_LOCAL_FILE_PENDING_SRC = _copy_and_augment_table(LocalDiskDatabase.TABLE_LOCAL_FILE, PENDING, SRC)
    TABLE_LOCAL_FILE_PENDING_DST = _copy_and_augment_table(LocalDiskDatabase.TABLE_LOCAL_FILE, PENDING, DST)
    TABLE_LOCAL_DIR_PENDING_SRC = _copy_and_augment_table(LocalDiskDatabase.TABLE_LOCAL_DIR, PENDING, SRC)
    TABLE_LOCAL_DIR_PENDING_DST = _copy_and_augment_table(LocalDiskDatabase.TABLE_LOCAL_DIR, PENDING, DST)
    TABLE_GRDIVE_DIR_PENDING_SRC = _copy_and_augment_table(GDriveDatabase.TABLE_GRDIVE_DIR, PENDING, SRC)
    TABLE_GRDIVE_DIR_PENDING_DST = _copy_and_augment_table(GDriveDatabase.TABLE_GRDIVE_DIR, PENDING, DST)
    TABLE_GRDIVE_FILE_PENDING_SRC = _copy_and_augment_table(GDriveDatabase.TABLE_GRDIVE_FILE, PENDING, SRC)
    TABLE_GRDIVE_FILE_PENDING_DST = _copy_and_augment_table(GDriveDatabase.TABLE_GRDIVE_FILE, PENDING, DST)

    # archive ...
    TABLE_LOCAL_FILE_ARCHIVE_SRC = _copy_and_augment_table(LocalDiskDatabase.TABLE_LOCAL_FILE, ARCHIVE, SRC)
    TABLE_LOCAL_FILE_ARCHIVE_DST = _copy_and_augment_table(LocalDiskDatabase.TABLE_LOCAL_FILE, ARCHIVE, DST)
    TABLE_LOCAL_DIR_ARCHIVE_SRC = _copy_and_augment_table(LocalDiskDatabase.TABLE_LOCAL_DIR, ARCHIVE, SRC)
    TABLE_LOCAL_DIR_ARCHIVE_DST = _copy_and_augment_table(LocalDiskDatabase.TABLE_LOCAL_DIR, ARCHIVE, DST)
    TABLE_GRDIVE_DIR_ARCHIVE_SRC = _copy_and_augment_table(GDriveDatabase.TABLE_GRDIVE_DIR, ARCHIVE, SRC)
    TABLE_GRDIVE_DIR_ARCHIVE_DST = _copy_and_augment_table(GDriveDatabase.TABLE_GRDIVE_DIR, ARCHIVE, DST)
    TABLE_GRDIVE_FILE_ARCHIVE_SRC = _copy_and_augment_table(GDriveDatabase.TABLE_GRDIVE_FILE, ARCHIVE, SRC)
    TABLE_GRDIVE_FILE_ARCHIVE_DST = _copy_and_augment_table(GDriveDatabase.TABLE_GRDIVE_FILE, ARCHIVE, DST)

    SRC_PENDING_TABLE_LIST = [TABLE_LOCAL_FILE_PENDING_SRC, TABLE_LOCAL_DIR_PENDING_SRC, TABLE_GRDIVE_DIR_PENDING_SRC, TABLE_GRDIVE_FILE_PENDING_SRC]
    DST_PENDING_TABLE_LIST = [TABLE_LOCAL_FILE_PENDING_DST, TABLE_LOCAL_DIR_PENDING_DST, TABLE_GRDIVE_DIR_PENDING_DST, TABLE_GRDIVE_FILE_PENDING_DST]

    def __init__(self, db_path, application):
        super().__init__(db_path)
        self.cache_manager = application.cache_manager

    # PENDING_CHANGE operations ⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆

    def _get_nodes_by_action_uid(self, table: Table, nodes_by_action_uid: Dict[UID, DisplayNode]):
        pass

    def get_all_pending_changes(self) -> List[ChangeActionRef]:
        entries: List[ChangeActionRef] = []

        src_node_by_action_uid: Dict[UID, DisplayNode] = {}
        for table in PendingChangeDatabase.SRC_PENDING_TABLE_LIST:
            self._get_nodes_by_action_uid(table, src_node_by_action_uid)

        dst_node_by_action_uid: Dict[UID, DisplayNode] = {}
        for table in PendingChangeDatabase.DST_PENDING_TABLE_LIST:
            self._get_nodes_by_action_uid(table, dst_node_by_action_uid)

        """ Gets all changes in the table """
        if not self.TABLE_PENDING_CHANGE.is_table(self.conn):
            return entries

        rows = self.TABLE_PENDING_CHANGE.get_all_rows(self.conn)
        for row in rows:
            entries.append(ChangeActionRef(UID(row[0]), UID(row[1]), ChangeType(row[2]), UID(row[3]), _ensure_uid(row[4]), int(row[5])))
        return entries

    def upsert_pending_changes(self, entries: Iterable[ChangeAction], overwrite, commit=True):
        """Inserts or updates a list of ChangeActions (remember that each action's UID is its primary key).
        If overwrite=true, then removes all existing changes first."""
        to_insert = []
        for e in entries:
            assert isinstance(e, ChangeAction), f'Expected ChangeAction; got instead: {e}'
            e_tuple = _make_change_tuple(e)
            to_insert.append(e_tuple)

        if overwrite:
            self.TABLE_PENDING_CHANGE.drop_table_if_exists(self.conn, commit=False)

        self.TABLE_PENDING_CHANGE.create_table_if_not_exist(self.conn, commit=False)

        self.TABLE_PENDING_CHANGE.upsert_many(self.conn, to_insert, commit)

    def _delete_pending_changes(self, changes: Iterable[Union[ChangeAction, ChangeActionRef]], commit=True):
        sql = self.TABLE_PENDING_CHANGE.build_delete() + f' WHERE uid = ?'
        tuples = list(map(lambda x: (x.action_uid,), changes))
        count_deleted = self.conn.executemany(sql, tuples).rowcount
        if count_deleted != len(tuples):
            logger.error(f'Expected to remove {len(tuples)} from DB but instead removed {count_deleted}!)')

        if commit:
            logger.debug('Committing!')
            self.conn.commit()

    # COMPLETED_CHANGE operations ⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆

    def upsert_completed_changes(self, entries: Iterable[ChangeAction], commit=True):
        """Inserts or updates a list of ChangeActions (remember that each action's UID is its primary key)."""
        current_time = int(time.time())
        to_insert = []
        for e in entries:
            assert isinstance(e, ChangeAction), f'Expected ChangeAction; got instead: {e}'
            e_tuple = _make_completed_change_tuple(e, current_time)
            to_insert.append(e_tuple)

        self.TABLE_COMPLETED_CHANGE.create_table_if_not_exist(self.conn, commit=False)

        self.TABLE_COMPLETED_CHANGE.upsert_many(self.conn, to_insert, commit)

    # FAILED_CHANGE operations ⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆

    def upsert_failed_changes(self, entries: Iterable[ChangeActionRef], error_msg: str, commit=True):
        """Inserts or updates a list of ChangeActionRefs (remember that each action's UID is its primary key)."""
        current_time = int(time.time())
        to_insert = []
        for e in entries:
            assert isinstance(e, ChangeActionRef), f'Expected ChangeActionRef; got instead: {e}'
            e_tuple = e.action_uid, e.batch_uid, e.change_type, e.src_uid, e.dst_uid, e.create_ts, current_time, error_msg
            to_insert.append(e_tuple)

        self.TABLE_FAILED_CHANGE.create_table_if_not_exist(self.conn, commit=False)

        self.TABLE_FAILED_CHANGE.upsert_many(self.conn, to_insert, commit)

    # Compound operations ⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆

    def archive_changes(self, entries: Iterable[ChangeAction]):
        self._delete_pending_changes(changes=entries, commit=False)
        self.upsert_completed_changes(entries)

    def archive_failed_changes(self, entries: Iterable[ChangeActionRef], error_msg: str):
        self._delete_pending_changes(changes=entries, commit=False)
        self.upsert_failed_changes(entries, error_msg)


def _make_change_tuple(e: ChangeAction):
    src_uid = None
    dst_uid = None
    if e.src_node:
        src_uid = e.src_node.uid

    if e.dst_node:
        dst_uid = e.dst_node.uid

    return e.action_uid, e.batch_uid, e.change_type, src_uid, dst_uid, e.create_ts


def _make_completed_change_tuple(e: ChangeAction, current_time):
    src_uid = None
    dst_uid = None
    if e.src_node:
        src_uid = e.src_node.uid

    if e.dst_node:
        dst_uid = e.dst_node.uid

    return e.action_uid, e.batch_uid, e.change_type, src_uid, dst_uid, e.create_ts, current_time
