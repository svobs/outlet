import logging
import time
from typing import Iterable, List

from model.change_action import ChangeAction, ChangeActionRef, ChangeType
from index.sqlite.base_db import MetaDatabase, Table
from index.uid.uid import UID

logger = logging.getLogger(__name__)


# CLASS PendingChangeDatabase
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼

class PendingChangeDatabase(MetaDatabase):
    TABLE_PENDING_CHANGE = Table(name='pending_change', cols={'uid': 'INTEGER PRIMARY KEY',
                                                              'batch_uid': 'INTEGER',
                                                              'change_type': 'INTEGER',
                                                              'src_node_uid': 'INTEGER',
                                                              'dst_node_uid': 'INTEGER',
                                                              'create_ts': 'INTEGER'})

    TABLE_COMPLETED_CHANGE = Table(name='completed_change', cols={'uid': 'INTEGER PRIMARY KEY',
                                                              'batch_uid': 'INTEGER',
                                                              'change_type': 'INTEGER',
                                                              'src_node_uid': 'INTEGER',
                                                              'dst_node_uid': 'INTEGER',
                                                              'create_ts': 'INTEGER'})

    def __init__(self, db_path, application):
        super().__init__(db_path)
        self.cache_manager = application.cache_manager

    # PENDING_CHANGE operations ⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆

    def get_all_pending_changes(self) -> List[ChangeActionRef]:
        entries: List[ChangeActionRef] = []

        """ Gets all changes in the table """
        if not self.is_table(self.TABLE_PENDING_CHANGE):
            return entries

        rows = self.get_all_rows(self.TABLE_PENDING_CHANGE)
        for row in rows:
            entries.append(ChangeActionRef(UID(row[0]), UID(row[1]), ChangeType(row[2]), UID(row[3]), UID(row[4]), int(row[5])))
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
            self.drop_table_if_exists(self.TABLE_PENDING_CHANGE, commit=False)

        self.create_table_if_not_exist(self.TABLE_PENDING_CHANGE, commit=False)

        self.upsert_many(self.TABLE_PENDING_CHANGE, to_insert, commit)

    def _delete_pending_changes(self, changes: Iterable[ChangeAction], commit=True):
        sql = self.build_delete(self.TABLE_PENDING_CHANGE) + f' WHERE uid = ?'
        tuples = list(map(lambda x: tuple(x.action_uid), changes))
        count_deleted = self.conn.executemany(sql, tuples).rowcount
        if count_deleted != len(tuples):
            logger.error(f'Expected to remove {len(tuples)} from DB but instead removed {count_deleted}!)')

        if commit:
            logger.debug('Committing!')
            self.conn.commit()

    # COMPLETED_CHANGE operations ⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆

    def upsert_completed_changes(self, entries: Iterable[ChangeAction], commit=True):
        """Inserts or updates a list of ChangeActions (remember that each action's UID is its primary key).
        If overwrite=true, then removes all existing changes first."""
        current_time = int(time.time())
        to_insert = []
        for e in entries:
            assert isinstance(e, ChangeAction), f'Expected ChangeAction; got instead: {e}'
            e_tuple = _make_completed_change_tuple(e, current_time)
            to_insert.append(e_tuple)

        self.create_table_if_not_exist(self.TABLE_COMPLETED_CHANGE, commit=False)

        self.upsert_many(self.TABLE_COMPLETED_CHANGE, to_insert, commit)

    # Compound operations ⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆

    def archive_changes(self, entries: Iterable[ChangeAction]):
        self._delete_pending_changes(changes=entries, commit=False)
        self.upsert_completed_changes(entries)


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
