import logging
from typing import Iterable, List

from model.change_action import ChangeAction, ChangeActionRef, ChangeType
from index.sqlite.base_db import MetaDatabase, Table
from index.uid.uid import UID

logger = logging.getLogger(__name__)


# CLASS PendingChangeDatabase
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼

class PendingChangeDatabase(MetaDatabase):
    TABLE_PENDING_CHANGE = Table(name='pending_change', cols={'uid': 'INTEGER PRIMARY KEY',
                                                              'change_type': 'INTEGER',
                                                              'src_node_uid': 'INTEGER',
                                                              'dst_node_uid': 'INTEGER',
                                                              'create_ts': 'INTEGER'})

    def __init__(self, db_path, application):
        super().__init__(db_path)
        self.cache_manager = application.cache_manager

    # PENDING_CHANGE operations ⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆

    def get_all_changes(self) -> List[ChangeActionRef]:
        entries: List[ChangeActionRef] = []

        """ Gets all changes in the table """
        if not self.is_table(self.TABLE_PENDING_CHANGE):
            return entries

        rows = self.get_all_rows(self.TABLE_PENDING_CHANGE)
        for row in rows:
            entries.append(ChangeActionRef(UID(row[0]), ChangeType(row[1]), UID(row[2]), UID(row[3])))
        return entries

    def upsert_changes(self, entries: Iterable[ChangeAction], overwrite, commit=True):
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


def _make_change_tuple(e: ChangeAction):
    src_uid = None
    dst_uid = None
    if e.src_node:
        src_uid = e.src_node.uid

    if e.dst_node:
        dst_uid = e.dst_node.uid

    return e.action_uid, e.change_type, src_uid, dst_uid
