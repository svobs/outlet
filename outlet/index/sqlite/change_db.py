import copy
import itertools
import logging
import time
from collections import OrderedDict
from functools import partial
from typing import Callable, Dict, Iterable, List, Tuple, Union

from index.sqlite.gdrive_db import GDriveDatabase
from index.sqlite.local_db import LocalDiskDatabase
from model.change_action import ChangeAction, ChangeActionRef, ChangeType
from index.sqlite.base_db import MetaDatabase, Table
from index.uid.uid import UID
from model.node.display_node import DisplayNode
from model.node.gdrive_node import GDriveFile, GDriveFolder
from model.node.local_disk_node import LocalDirNode, LocalFileNode
from model.node_identifier import GDriveIdentifier, LocalFsIdentifier

logger = logging.getLogger(__name__)

PENDING = 'pending'
ARCHIVE = 'archive'
SRC = 'src'
DST = 'dst'


# CLASS TableListCollection
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
class TableListCollection:
    def __init__(self):
        self.local_file = []
        self.local_dir = []
        self.gdrive_file = []
        self.gdrive_dir = []
        self.src_pending = []
        self.dst_pending = []
        self.src_archive = []
        self.dst_archive = []
        self.orm_map: Dict[str, Callable] = {}


def _ensure_uid(val):
    """Converts val to UID but allows for null"""
    if val:
        return UID(val)
    return None


def _add_gdrive_parent_cols(table: Table):
    table.cols.update({('parent_uid', 'INTEGER'),
                       ('parent_goog_id', 'TEXT')})


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

    def __init__(self, db_path, application):
        super().__init__(db_path)
        self.cache_manager = application.cache_manager

        self.table_lists: TableListCollection = TableListCollection()

        # pending ...
        TABLE_LOCAL_FILE_PENDING_SRC = self._copy_and_augment_table(LocalDiskDatabase.TABLE_LOCAL_FILE, PENDING, SRC, self.table_lists)
        TABLE_LOCAL_FILE_PENDING_DST = self._copy_and_augment_table(LocalDiskDatabase.TABLE_LOCAL_FILE, PENDING, DST, self.table_lists)
        TABLE_LOCAL_DIR_PENDING_SRC = self._copy_and_augment_table(LocalDiskDatabase.TABLE_LOCAL_DIR, PENDING, SRC, self.table_lists)
        TABLE_LOCAL_DIR_PENDING_DST = self._copy_and_augment_table(LocalDiskDatabase.TABLE_LOCAL_DIR, PENDING, DST, self.table_lists)
        TABLE_GRDIVE_DIR_PENDING_SRC = self._copy_and_augment_table(GDriveDatabase.TABLE_GRDIVE_DIR, PENDING, SRC, self.table_lists)
        TABLE_GRDIVE_DIR_PENDING_DST = self._copy_and_augment_table(GDriveDatabase.TABLE_GRDIVE_DIR, PENDING, DST, self.table_lists)
        TABLE_GRDIVE_FILE_PENDING_SRC = self._copy_and_augment_table(GDriveDatabase.TABLE_GRDIVE_FILE, PENDING, SRC, self.table_lists)
        TABLE_GRDIVE_FILE_PENDING_DST = self._copy_and_augment_table(GDriveDatabase.TABLE_GRDIVE_FILE, PENDING, DST, self.table_lists)

        # archive ...
        TABLE_LOCAL_FILE_ARCHIVE_SRC = self._copy_and_augment_table(LocalDiskDatabase.TABLE_LOCAL_FILE, ARCHIVE, SRC, self.table_lists)
        TABLE_LOCAL_FILE_ARCHIVE_DST = self._copy_and_augment_table(LocalDiskDatabase.TABLE_LOCAL_FILE, ARCHIVE, DST, self.table_lists)
        TABLE_LOCAL_DIR_ARCHIVE_SRC = self._copy_and_augment_table(LocalDiskDatabase.TABLE_LOCAL_DIR, ARCHIVE, SRC, self.table_lists)
        TABLE_LOCAL_DIR_ARCHIVE_DST = self._copy_and_augment_table(LocalDiskDatabase.TABLE_LOCAL_DIR, ARCHIVE, DST, self.table_lists)
        TABLE_GRDIVE_DIR_ARCHIVE_SRC = self._copy_and_augment_table(GDriveDatabase.TABLE_GRDIVE_DIR, ARCHIVE, SRC, self.table_lists)
        TABLE_GRDIVE_DIR_ARCHIVE_DST = self._copy_and_augment_table(GDriveDatabase.TABLE_GRDIVE_DIR, ARCHIVE, DST, self.table_lists)
        TABLE_GRDIVE_FILE_ARCHIVE_SRC = self._copy_and_augment_table(GDriveDatabase.TABLE_GRDIVE_FILE, ARCHIVE, SRC, self.table_lists)
        TABLE_GRDIVE_FILE_ARCHIVE_DST = self._copy_and_augment_table(GDriveDatabase.TABLE_GRDIVE_FILE, ARCHIVE, DST, self.table_lists)

        for table in itertools.chain(self.table_lists.gdrive_dir, self.table_lists.gdrive_file):
            _add_gdrive_parent_cols(table)

    def _tuple_to_gdrive_folder(self, nodes_by_action_uid: Dict[UID, DisplayNode], row: Tuple) -> GDriveFolder:
        action_uid_int, uid_int, goog_id, item_name, item_trashed, drive_id, my_share, sync_ts, all_children_fetched, parent_uid_int, \
            parent_goog_id = row

        obj = GDriveFolder(GDriveIdentifier(uid=UID(uid_int), full_path=None), goog_id=goog_id, item_name=item_name, trashed=item_trashed,
                           drive_id=drive_id, my_share=my_share, sync_ts=sync_ts, all_children_fetched=all_children_fetched)

        uid_from_cacheman = self.cache_manager.get_uid_for_goog_id(goog_id, obj.uid)
        if uid_from_cacheman != obj.uid:
            raise RuntimeError(f'UID from cacheman ({uid_from_cacheman}) does not match UID from change cache ({obj.uid}) '
                               f'for goog_id "{goog_id}"')

        obj.set_parent_uids(parent_uid_int)
        action_uid = UID(action_uid_int)
        if nodes_by_action_uid.get(action_uid, None):
            raise RuntimeError(f'Duplicate node for action_uid: {action_uid}')
        nodes_by_action_uid[action_uid] = obj
        return obj

    def _tuple_to_gdrive_file(self, nodes_by_action_uid: Dict[UID, DisplayNode], row: Tuple) -> GDriveFile:
        action_uid_int, uid_int, goog_id, item_name, item_trashed, size_bytes, md5, create_ts, modify_ts, owner_id, drive_id, my_share, \
            version, head_revision_id, sync_ts, parent_uid_int, parent_goog_id = row

        obj = GDriveFile(GDriveIdentifier(uid=UID(uid_int), full_path=None), goog_id=goog_id, item_name=item_name,
                         trashed=item_trashed, drive_id=drive_id, my_share=my_share, version=version,
                         head_revision_id=head_revision_id, md5=md5,
                         create_ts=create_ts, modify_ts=modify_ts, size_bytes=size_bytes, owner_id=owner_id, sync_ts=sync_ts)

        uid_from_cacheman = self.cache_manager.get_uid_for_goog_id(goog_id, obj.uid)
        if uid_from_cacheman != obj.uid:
            raise RuntimeError(f'UID from cacheman ({uid_from_cacheman}) does not match UID from change cache ({obj.uid}) '
                               f'for goog_id "{goog_id}"')

        obj.set_parent_uids(parent_uid_int)
        action_uid = UID(action_uid_int)
        if nodes_by_action_uid.get(action_uid, None):
            raise RuntimeError(f'Duplicate node for action_uid: {action_uid}')
        nodes_by_action_uid[action_uid] = obj
        return obj

    def _tuple_to_local_dir(self, nodes_by_action_uid: Dict[UID, DisplayNode], row: Tuple) -> LocalDirNode:
        action_uid_int, uid_int, full_path, exists = row

        uid = self.cache_manager.get_uid_for_path(full_path, uid_int)
        assert uid == uid_int, f'UID conflict! Got {uid} but read {row}'
        obj = LocalDirNode(LocalFsIdentifier(uid=uid, full_path=full_path), bool(exists))
        action_uid = UID(action_uid_int)
        if nodes_by_action_uid.get(action_uid, None):
            raise RuntimeError(f'Duplicate node for action_uid: {action_uid}')
        nodes_by_action_uid[action_uid] = obj
        return obj

    def _tuple_to_local_file(self, nodes_by_action_uid: Dict[UID, DisplayNode], row: Tuple) -> LocalFileNode:
        action_uid_int, uid_int, md5, sha256, size_bytes, sync_ts, modify_ts, change_ts, full_path, exists = row

        uid = self.cache_manager.get_uid_for_path(full_path, uid_int)
        assert uid == uid_int, f'UID conflict! Got {uid} but read {uid_int} in row: {row}'
        node_identifier = LocalFsIdentifier(uid=uid, full_path=full_path)
        obj = LocalFileNode(node_identifier, md5, sha256, size_bytes, sync_ts, modify_ts, change_ts, exists)
        action_uid = UID(action_uid_int)
        if nodes_by_action_uid.get(action_uid, None):
            raise RuntimeError(f'Duplicate node for action_uid: {action_uid}')
        nodes_by_action_uid[action_uid] = obj
        return obj

    def _gdrive_folder_to_tuple(self):
        pass

    def _gdrive_file_to_tuple(self):
        pass

    def _local_file_to_tuple(self):
        pass

    def _local_dir_to_tuple(self):
        pass


    def _copy_and_augment_table(self, src_table: Table, prefix: str, suffix: str, table_list_collection: TableListCollection) -> Table:
        table: Table = copy.deepcopy(src_table)
        table.name = f'{prefix}_{table.name}_{suffix}'
        # uid is no longer primary key
        table.cols.update({'uid': 'INTEGER'})
        # primary key is also foreign key (not enforced) to ChangeAction (ergo, only one row per ChangeAction):
        table.cols.update({'action_uid': 'INTEGER PRIMARY KEY'})
        # move to front:
        table.cols.move_to_end('action_uid', last=False)

        if src_table.name == LocalDiskDatabase.TABLE_LOCAL_FILE:
            table_list_collection.local_file.append(table)
            table_list_collection.orm_map[table.name] = self._tuple_to_local_file
        elif src_table.name == LocalDiskDatabase.TABLE_LOCAL_DIR:
            table_list_collection.local_dir.append(table)
            table_list_collection.orm_map[table.name] = self._tuple_to_local_dir
        elif src_table.name == GDriveDatabase.TABLE_GRDIVE_FILE:
            table_list_collection.gdrive_file.append(table)
            table_list_collection.orm_map[table.name] = self._tuple_to_gdrive_file
        elif src_table.name == GDriveDatabase.TABLE_GRDIVE_DIR:
            table_list_collection.gdrive_dir.append(table)
            table_list_collection.orm_map[table.name] = self._tuple_to_gdrive_folder

        if prefix == PENDING:
            if suffix == SRC:
                table_list_collection.src_pending.append(table)
            elif suffix == DST:
                table_list_collection.dst_pending.append(table)
        elif prefix == ARCHIVE:
            if suffix == SRC:
                table_list_collection.src_archive.append(table)
            elif suffix == DST:
                table_list_collection.dst_archive.append(table)

        return table

    # PENDING_CHANGE operations ⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆

    def _get_all_in_table(self, table: Table, nodes_by_action_uid: Dict[UID, DisplayNode]):
        # Look up appropriate ORM function and bind its first param to nodes_by_action_uid
        table_func: Callable = partial(self.table_lists.orm_map[table.name], nodes_by_action_uid)

        table.select_object_list(self.conn, tuple_to_obj_func=table_func)

    def get_all_pending_changes(self) -> List[ChangeAction]:
        entries: List[ChangeAction] = []

        src_node_by_action_uid: Dict[UID, DisplayNode] = {}
        for table in self.table_lists.src_pending:
            self._get_all_in_table(table, src_node_by_action_uid)

        dst_node_by_action_uid: Dict[UID, DisplayNode] = {}
        for table in self.table_lists.dst_pending:
            self._get_all_in_table(table, dst_node_by_action_uid)

        """ Gets all changes in the table """
        if not self.TABLE_PENDING_CHANGE.is_table(self.conn):
            return entries

        rows = self.TABLE_PENDING_CHANGE.get_all_rows(self.conn)
        for row in rows:
            ref = ChangeActionRef(UID(row[0]), UID(row[1]), ChangeType(row[2]), UID(row[3]), _ensure_uid(row[4]), int(row[5]))
            src_node = src_node_by_action_uid.get(ref.action_uid, None)
            dst_node = dst_node_by_action_uid.get(ref.action_uid, None)

            if not src_node:
                raise RuntimeError(f'No src node found for: {ref}')
            if src_node.uid != ref.src_uid:
                raise RuntimeError(f'Src node UID ({src_node.uid}) does not match ref in: {ref}')
            if ref.dst_uid:
                if not dst_node:
                    raise RuntimeError(f'No dst node found for: {ref}')

                if dst_node.uid != ref.dst_uid:
                    raise RuntimeError(f'Dst node UID ({dst_node.uid}) does not match ref in: {ref}')

            entries.append(ChangeAction(ref.action_uid, ref.batch_uid, ref.change_type, src_node, dst_node))
        return entries

    def upsert_pending_changes(self, entries: Iterable[ChangeAction], overwrite, commit=True):
        """Inserts or updates a list of ChangeActions (remember that each action's UID is its primary key).
        If overwrite=true, then removes all existing changes first."""
        to_insert = []
        for e in entries:
            assert isinstance(e, ChangeAction), f'Expected ChangeAction; got instead: {e}'
            e_tuple = _make_change_tuple(e)
            to_insert.append(e_tuple)

        # TODO: upsert into child tables

        if overwrite:
            self.TABLE_PENDING_CHANGE.drop_table_if_exists(self.conn, commit=False)

        self.TABLE_PENDING_CHANGE.create_table_if_not_exist(self.conn, commit=False)

        self.TABLE_PENDING_CHANGE.upsert_many(self.conn, to_insert, commit)

    def _delete_pending_changes(self, changes: Iterable[ChangeAction], commit=True):
        uid_tuple_list = list(map(lambda x: (x.action_uid,), changes))
        self.TABLE_PENDING_CHANGE.delete_for_uid_list(self.conn, uid_tuple_list, commit)
        # TODO delete from child tables

    # COMPLETED_CHANGE operations ⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆

    def _upsert_completed_changes(self, entries: Iterable[ChangeAction], commit=True):
        """Inserts or updates a list of ChangeActions (remember that each action's UID is its primary key)."""
        current_time = int(time.time())
        to_insert = []
        for e in entries:
            assert isinstance(e, ChangeAction), f'Expected ChangeAction; got instead: {e}'
            e_tuple = _make_completed_change_tuple(e, current_time)
            to_insert.append(e_tuple)

        # TODO: insert into child tables

        self.TABLE_COMPLETED_CHANGE.create_table_if_not_exist(self.conn, commit=False)

        self.TABLE_COMPLETED_CHANGE.upsert_many(self.conn, to_insert, commit)

    # FAILED_CHANGE operations ⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆

    def _upsert_failed_changes(self, entries: Iterable[ChangeAction], error_msg: str, commit=True):
        """Inserts or updates a list of ChangeAction (remember that each action's UID is its primary key)."""
        current_time = int(time.time())
        to_insert = []
        for e in entries:
            assert isinstance(e, ChangeAction), f'Expected ChangeAction; got instead: {e}'
            e_tuple = e.action_uid, e.batch_uid, e.change_type, e.src_node.uid, e.dst_node.uid, e.create_ts, current_time, error_msg
            to_insert.append(e_tuple)

        # TODO: insert into child tables

        self.TABLE_FAILED_CHANGE.create_table_if_not_exist(self.conn, commit=False)

        self.TABLE_FAILED_CHANGE.upsert_many(self.conn, to_insert, commit)

    # Compound operations ⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆

    def archive_completed_changes(self, entries: Iterable[ChangeAction]):
        self._delete_pending_changes(changes=entries, commit=False)
        self._upsert_completed_changes(entries)

    def archive_failed_changes(self, entries: Iterable[ChangeAction], error_msg: str):
        self._delete_pending_changes(changes=entries, commit=False)
        self._upsert_failed_changes(entries, error_msg)


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
