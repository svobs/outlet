import copy
import itertools
import logging
import time
from collections import OrderedDict
from functools import partial
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple

from constants import OBJ_TYPE_DIR, OBJ_TYPE_FILE, TREE_TYPE_GDRIVE, TREE_TYPE_LOCAL_DISK
from index.sqlite.base_db import LiveTable, MetaDatabase, Table
from index.sqlite.gdrive_db import GDriveDatabase
from index.sqlite.local_db import LocalDiskDatabase
from index.uid.uid import UID
from model.op import Op, OpRef, OpType
from model.node.display_node import DisplayNode
from model.node.gdrive_node import GDriveFile, GDriveFolder, GDriveNode
from model.node.local_disk_node import LocalDirNode, LocalFileNode
from model.node_identifier import GDriveIdentifier, LocalFsIdentifier

logger = logging.getLogger(__name__)

PENDING = 'pending'
ARCHIVE = 'archive'
SRC = 'src'
DST = 'dst'

ACTION_UID_COL_NAME = 'action_uid'


def _pending_op_to_tuple(e: Op):
    assert isinstance(e, Op), f'Expected Op; got instead: {e}'
    src_uid = None
    dst_uid = None
    if e.src_node:
        src_uid = e.src_node.uid

    if e.dst_node:
        dst_uid = e.dst_node.uid

    return e.action_uid, e.batch_uid, e.op_type, src_uid, dst_uid, e.create_ts


def _completed_op_to_tuple(e: Op, current_time):
    assert isinstance(e, Op), f'Expected Op; got instead: {e}'
    src_uid = None
    dst_uid = None
    if e.src_node:
        src_uid = e.src_node.uid

    if e.dst_node:
        dst_uid = e.dst_node.uid

    return e.action_uid, e.batch_uid, e.op_type, src_uid, dst_uid, e.create_ts, current_time


def _failed_op_to_tuple(e: Op, current_time, error_msg):
    partial_tuple = _completed_op_to_tuple(e, current_time)
    return *partial_tuple, error_msg


def _tuple_to_op_ref(row: Tuple) -> OpRef:
    assert isinstance(row, Tuple), f'Expected Tuple; got instead: {row}'
    return OpRef(UID(row[0]), UID(row[1]), OpType(row[2]), UID(row[3]), _ensure_uid(row[4]), int(row[5]))


# CLASS TableMultiMap
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼

class TableMultiMap:
    def __init__(self):
        self.all_dict: Dict[str, Dict[str, Dict[int, Dict[str, Any]]]] = {}

    def put(self, lifecycle_state: str, src_or_dst: str, tree_type: int, obj_type: str, obj):
        dict2 = self.all_dict.get(lifecycle_state, None)
        if not dict2:
            dict2 = {}
            self.all_dict[lifecycle_state] = dict2

        dict3 = dict2.get(src_or_dst, None)
        if not dict3:
            dict3 = {}
            dict2[src_or_dst] = dict3

        dict4 = dict3.get(tree_type, None)
        if not dict4:
            dict4 = {}
            dict3[tree_type] = dict4

        dict4[obj_type] = obj

    def append(self, lifecycle_state: str, src_or_dst: str, tree_type: int, obj_type: str, obj):
        dict2 = self.all_dict.get(lifecycle_state, None)
        if not dict2:
            dict2 = {}
            self.all_dict[lifecycle_state] = dict2

        dict3 = dict2.get(src_or_dst, None)
        if not dict3:
            dict3 = {}
            dict2[src_or_dst] = dict3

        dict4 = dict3.get(tree_type, None)
        if not dict4:
            dict4 = {}
            dict3[tree_type] = dict4

        the_list = dict4.get(obj_type)
        if not the_list:
            the_list = []
            dict4[obj_type] = the_list
        the_list.append(obj)

    def get(self, lifecycle_state: str, src_or_dst: str, tree_type: int, obj_type: str) -> Any:
        dict2 = self.all_dict.get(lifecycle_state, None)
        dict3 = dict2.get(src_or_dst, None)
        dict4 = dict3.get(tree_type, None)
        return dict4.get(obj_type, None)

    def entries(self) -> List[Tuple[str, str, int, str, List]]:
        tuple_list: List[Tuple[str, str, int, str, List]] = []

        for lifecycle_state, dict2 in self.all_dict.items():
            for src_or_dst, dict3 in dict2.items():
                for tree_type, dict4 in dict3.items():
                    for obj_type, final_list in dict4.items():
                        tuple_list.append((lifecycle_state, src_or_dst, tree_type, obj_type, final_list))

        logger.debug(f'Returning {len(tuple_list)} entry sets for multimap')
        return tuple_list


# CLASS TableListCollection
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
class TableListCollection:
    def __init__(self):
        self.all_dict: TableMultiMap = TableMultiMap()
        """ {PENDING or ARCHIVE} -> {SRC or DST} -> {tree type} -> {obj type} -> """

        self.all: List[LiveTable] = []
        self.local_file: List[LiveTable] = []
        self.local_dir: List[LiveTable] = []
        self.gdrive_file: List[LiveTable] = []
        self.gdrive_dir: List[LiveTable] = []
        self.src_pending: List[LiveTable] = []
        self.dst_pending: List[LiveTable] = []
        self.src_archive: List[LiveTable] = []
        self.dst_archive: List[LiveTable] = []
        self.tuple_to_obj_func_map: Dict[str, Callable[[Dict[UID, DisplayNode], Tuple], Any]] = {}
        # self.obj_to_tuple_func_map: Dict[str, Callable[[Any, UID], Tuple]] = {}

    def put_table(self, lifecycle_state: str, src_or_dst: str, tree_type: int, obj_type: str, table):
        self.all_dict.put(lifecycle_state, src_or_dst, tree_type, obj_type, table)

    def get_table(self, lifecycle_state: str, src_or_dst: str, tree_type: int, obj_type: str) -> LiveTable:
        return self.all_dict.get(lifecycle_state, src_or_dst, tree_type, obj_type)


def _ensure_uid(val):
    """Converts val to UID but allows for null"""
    if val:
        return UID(val)
    return None


def _add_gdrive_parent_cols(table: Table):
    table.cols.update({('parent_uid', 'INTEGER'),
                       ('parent_goog_id', 'TEXT')})
    table.cols.move_to_end('parent_uid', last=True)
    table.cols.move_to_end('parent_goog_id', last=True)


# CLASS OpDatabase
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼

class OpDatabase(MetaDatabase):
    TABLE_PENDING_CHANGE = Table(name='pending_op', cols=OrderedDict([
        ('uid', 'INTEGER PRIMARY KEY'),
        ('batch_uid', 'INTEGER'),
        ('op_type', 'INTEGER'),
        ('src_node_uid', 'INTEGER'),
        ('dst_node_uid', 'INTEGER'),
        ('create_ts', 'INTEGER')
    ]))

    TABLE_COMPLETED_CHANGE = Table(name='completed_op',
                                   cols=OrderedDict([
                                       ('uid', 'INTEGER PRIMARY KEY'),
                                       ('batch_uid', 'INTEGER'),
                                       ('op_type', 'INTEGER'),
                                       ('src_node_uid', 'INTEGER'),
                                       ('dst_node_uid', 'INTEGER'),
                                       ('create_ts', 'INTEGER'),
                                       ('complete_ts', 'INTEGER')
                                   ]))

    TABLE_FAILED_CHANGE = Table(name='failed_op',
                                cols=OrderedDict([
                                    ('uid', 'INTEGER PRIMARY KEY'),
                                    ('batch_uid', 'INTEGER'),
                                    ('op_type', 'INTEGER'),
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
        # We do not use OpRef to Tuple, because we convert Op to Tuple instead
        self.table_pending_op = LiveTable(OpDatabase.TABLE_PENDING_CHANGE, self.conn, None, _tuple_to_op_ref)
        self.table_completed_op = LiveTable(OpDatabase.TABLE_COMPLETED_CHANGE, self.conn, None, _tuple_to_op_ref)
        self.table_failed_op = LiveTable(OpDatabase.TABLE_FAILED_CHANGE, self.conn, None, _tuple_to_op_ref)

        # pending ...
        self._copy_and_augment_table(LocalDiskDatabase.TABLE_LOCAL_FILE, PENDING, SRC)
        self._copy_and_augment_table(LocalDiskDatabase.TABLE_LOCAL_FILE, PENDING, DST)
        self._copy_and_augment_table(LocalDiskDatabase.TABLE_LOCAL_DIR, PENDING, SRC)
        self._copy_and_augment_table(LocalDiskDatabase.TABLE_LOCAL_DIR, PENDING, DST)
        self._copy_and_augment_table(GDriveDatabase.TABLE_GRDIVE_FOLDER, PENDING, SRC)
        self._copy_and_augment_table(GDriveDatabase.TABLE_GRDIVE_FOLDER, PENDING, DST)
        self._copy_and_augment_table(GDriveDatabase.TABLE_GRDIVE_FILE, PENDING, SRC)
        self._copy_and_augment_table(GDriveDatabase.TABLE_GRDIVE_FILE, PENDING, DST)

        # archive ...
        self._copy_and_augment_table(LocalDiskDatabase.TABLE_LOCAL_FILE, ARCHIVE, SRC)
        self._copy_and_augment_table(LocalDiskDatabase.TABLE_LOCAL_FILE, ARCHIVE, DST)
        self._copy_and_augment_table(LocalDiskDatabase.TABLE_LOCAL_DIR, ARCHIVE, SRC)
        self._copy_and_augment_table(LocalDiskDatabase.TABLE_LOCAL_DIR, ARCHIVE, DST)
        self._copy_and_augment_table(GDriveDatabase.TABLE_GRDIVE_FOLDER, ARCHIVE, SRC)
        self._copy_and_augment_table(GDriveDatabase.TABLE_GRDIVE_FOLDER, ARCHIVE, DST)
        self._copy_and_augment_table(GDriveDatabase.TABLE_GRDIVE_FILE, ARCHIVE, SRC)
        self._copy_and_augment_table(GDriveDatabase.TABLE_GRDIVE_FILE, ARCHIVE, DST)

        # Add additional columns for GDrive tables:
        for table in itertools.chain(self.table_lists.gdrive_dir, self.table_lists.gdrive_file):
            _add_gdrive_parent_cols(table)

    def _verify_goog_id_consistency(self, goog_id: str, obj_uid: UID):
        if goog_id:
            # Sanity check: make sure pending change cache matches GDrive cache
            uid_from_cacheman = self.cache_manager.get_uid_for_goog_id(goog_id, obj_uid)
            if uid_from_cacheman != obj_uid:
                raise RuntimeError(f'UID from cacheman ({uid_from_cacheman}) does not match UID from change cache ({obj_uid}) '
                                   f'for goog_id "{goog_id}"')

    def _store_gdrive_object(self, obj: GDriveNode, goog_id: str, parent_uid_int: int, action_uid_int: int,
                             nodes_by_action_uid: Dict[UID, DisplayNode]):
        self._verify_goog_id_consistency(goog_id, obj.uid)

        if not parent_uid_int:
            raise RuntimeError(f'Cannot store GDrive object: it has no parent! Object: {obj}')
        obj.set_parent_uids(UID(parent_uid_int))
        action_uid = UID(action_uid_int)
        if nodes_by_action_uid.get(action_uid, None):
            raise RuntimeError(f'Duplicate node for action_uid: {action_uid}')
        nodes_by_action_uid[action_uid] = obj

    def _tuple_to_gdrive_folder(self, nodes_by_action_uid: Dict[UID, DisplayNode], row: Tuple) -> GDriveFolder:
        action_uid_int, uid_int, goog_id, item_name, item_trashed, drive_id, my_share, sync_ts, all_children_fetched, parent_uid_int, \
            parent_goog_id = row

        obj = GDriveFolder(GDriveIdentifier(uid=UID(uid_int), full_path=None), goog_id=goog_id, item_name=item_name, trashed=item_trashed,
                           drive_id=drive_id, my_share=my_share, sync_ts=sync_ts, all_children_fetched=all_children_fetched)

        self._store_gdrive_object(obj, goog_id, parent_uid_int, action_uid_int, nodes_by_action_uid)

        return obj

    def _tuple_to_gdrive_file(self, nodes_by_action_uid: Dict[UID, DisplayNode], row: Tuple) -> GDriveFile:
        action_uid_int, uid_int, goog_id, item_name, item_trashed, size_bytes, md5, create_ts, modify_ts, owner_id, drive_id, my_share, \
            version, head_revision_id, sync_ts, parent_uid_int, parent_goog_id = row

        obj = GDriveFile(GDriveIdentifier(uid=UID(uid_int), full_path=None), goog_id=goog_id, item_name=item_name,
                         trashed=item_trashed, drive_id=drive_id, my_share=my_share, version=version,
                         head_revision_id=head_revision_id, md5=md5,
                         create_ts=create_ts, modify_ts=modify_ts, size_bytes=size_bytes, owner_id=owner_id, sync_ts=sync_ts)

        self._store_gdrive_object(obj, goog_id, parent_uid_int, action_uid_int, nodes_by_action_uid)
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
        if uid != uid_int:
            raise RuntimeError(f'UID conflict! Cache man returned {uid} but op cache returned {uid_int} (from row: {row})')
        node_identifier = LocalFsIdentifier(uid=uid, full_path=full_path)
        obj = LocalFileNode(node_identifier, md5, sha256, size_bytes, sync_ts, modify_ts, change_ts, exists)
        action_uid = UID(action_uid_int)
        if nodes_by_action_uid.get(action_uid, None):
            raise RuntimeError(f'Duplicate node for action_uid: {action_uid}')
        nodes_by_action_uid[action_uid] = obj
        return obj

    def _action_node_to_tuple(self, obj: DisplayNode, action_uid: UID) -> Tuple:
        if not obj.has_tuple():
            raise RuntimeError(f'Node cannot be converted to tuple: {obj}')
        if obj.get_tree_type() == TREE_TYPE_GDRIVE:
            assert isinstance(obj, GDriveNode)
            parent_uid: Optional[UID] = None
            parent_goog_id: Optional[str] = None
            if obj.get_parent_uids():
                parent_uid = obj.get_parent_uids()[0]
                try:
                    parent_goog_id_list = self.cache_manager.get_goog_ids_for_uids([parent_uid])
                    if parent_goog_id_list:
                        parent_goog_id = parent_goog_id_list[0]
                except RuntimeError:
                    logger.debug(f'Could not resolve parent_uid to goog_id: {parent_uid}')
            return action_uid, *obj.to_tuple(), parent_uid, parent_goog_id

        return action_uid, *obj.to_tuple()

    def _copy_and_augment_table(self, src_table: Table, prefix: str, suffix: str) -> LiveTable:
        table: Table = copy.deepcopy(src_table)
        table.name = f'{prefix}_{table.name}_{suffix}'
        # uid is no longer primary key
        table.cols.update({'uid': 'INTEGER'})
        # primary key is also foreign key (not enforced) to Op (ergo, only one row per Op):
        table.cols.update({ACTION_UID_COL_NAME: 'INTEGER PRIMARY KEY'})
        # move to front:
        table.cols.move_to_end(ACTION_UID_COL_NAME, last=False)

        if src_table.name == LocalDiskDatabase.TABLE_LOCAL_FILE.name:
            live_table = LiveTable(table, self.conn, None, None)
            self.table_lists.put_table(prefix, suffix, TREE_TYPE_LOCAL_DISK, OBJ_TYPE_FILE, live_table)
            self.table_lists.local_file.append(live_table)
            self.table_lists.tuple_to_obj_func_map[live_table.name] = self._tuple_to_local_file
        elif src_table.name == LocalDiskDatabase.TABLE_LOCAL_DIR.name:
            live_table = LiveTable(table, self.conn, None, None)
            self.table_lists.put_table(prefix, suffix, TREE_TYPE_LOCAL_DISK, OBJ_TYPE_DIR, live_table)
            self.table_lists.local_dir.append(live_table)
            self.table_lists.tuple_to_obj_func_map[live_table.name] = self._tuple_to_local_dir
        elif src_table.name == GDriveDatabase.TABLE_GRDIVE_FILE.name:
            live_table = LiveTable(table, self.conn, None, None)
            self.table_lists.put_table(prefix, suffix, TREE_TYPE_GDRIVE, OBJ_TYPE_FILE, live_table)
            self.table_lists.gdrive_file.append(live_table)
            self.table_lists.tuple_to_obj_func_map[live_table.name] = self._tuple_to_gdrive_file
        elif src_table.name == GDriveDatabase.TABLE_GRDIVE_FOLDER.name:
            live_table = LiveTable(table, self.conn, None, None)
            self.table_lists.put_table(prefix, suffix, TREE_TYPE_GDRIVE, OBJ_TYPE_DIR, live_table)
            self.table_lists.gdrive_dir.append(live_table)
            self.table_lists.tuple_to_obj_func_map[live_table.name] = self._tuple_to_gdrive_folder
        else:
            raise RuntimeError(f'Unrecognized table name: {src_table.name}')

        self.table_lists.all.append(live_table)

        if prefix == PENDING:
            if suffix == SRC:
                self.table_lists.src_pending.append(live_table)
            elif suffix == DST:
                self.table_lists.dst_pending.append(live_table)
        elif prefix == ARCHIVE:
            if suffix == SRC:
                self.table_lists.src_archive.append(live_table)
            elif suffix == DST:
                self.table_lists.dst_archive.append(live_table)

        return live_table

    # PENDING_CHANGE operations ⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆

    def _get_all_in_table(self, table: LiveTable, nodes_by_action_uid: Dict[UID, DisplayNode]):
        # Look up appropriate ORM function and bind its first param to nodes_by_action_uid
        table_func: Callable = partial(self.table_lists.tuple_to_obj_func_map[table.name], nodes_by_action_uid)

        table.select_object_list(tuple_to_obj_func_override=table_func)

    def get_all_pending_ops(self) -> List[Op]:
        """ Gets all pending changes, filling int their src and dst nodes as well """
        entries: List[Op] = []

        if not self.table_pending_op.is_table():
            return entries

        src_node_by_action_uid: Dict[UID, DisplayNode] = {}
        for table in self.table_lists.src_pending:
            logger.debug(f'Getting src nodes from table: "{table.name}"')
            self._get_all_in_table(table, src_node_by_action_uid)

        dst_node_by_action_uid: Dict[UID, DisplayNode] = {}
        for table in self.table_lists.dst_pending:
            logger.debug(f'Getting dst nodes from table: "{table.name}"')
            self._get_all_in_table(table, dst_node_by_action_uid)

        rows = self.table_pending_op.get_all_rows()
        logger.debug(f'Found {len(rows)} pending ops in table {self.table_pending_op.name}')
        for row in rows:
            ref = OpRef(UID(row[0]), UID(row[1]), OpType(row[2]), UID(row[3]), _ensure_uid(row[4]), int(row[5]))
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

            entries.append(Op(ref.action_uid, ref.batch_uid, ref.op_type, src_node, dst_node))
        return entries

    def _make_tuple_list(self, entries: Iterable[Op], lifecycle_state: str) -> TableMultiMap:
        tuple_list_multimap: TableMultiMap = TableMultiMap()

        for e in entries:
            assert isinstance(e, Op), f'Expected Op; got instead: {e}'

            node = e.src_node
            node_tuple = self._action_node_to_tuple(node, e.action_uid)
            tuple_list_multimap.append(lifecycle_state, SRC, node.node_identifier.tree_type, node.get_obj_type(), node_tuple)

            if e.dst_node:
                node = e.dst_node
                node_tuple = self._action_node_to_tuple(node, e.action_uid)
                tuple_list_multimap.append(lifecycle_state, DST, node.node_identifier.tree_type, node.get_obj_type(), node_tuple)

        return tuple_list_multimap

    def _upsert_nodes_without_commit(self, entries: Iterable[Op], lifecycle_state: str):
        tuple_list_multimap = self._make_tuple_list(entries, lifecycle_state)
        for lifecycle_state, src_or_dst, tree_type, obj_type, tuple_list in tuple_list_multimap.entries():
            table: LiveTable = self.table_lists.get_table(lifecycle_state, src_or_dst, tree_type, obj_type)
            assert table, f'No table for values: {lifecycle_state}, {src_or_dst}, {tree_type}, {obj_type}'
            table.upsert_many(tuple_list, commit=False)

    def upsert_pending_ops(self, entries: Iterable[Op], overwrite, commit=True):
        """Inserts or updates a list of Ops (remember that each action's UID is its primary key).
        If overwrite=true, then removes all existing changes first."""

        if overwrite:
            self.table_pending_op.drop_table_if_exists(commit=False)
            for table in self.table_lists.all:
                table.drop_table_if_exists(commit=False)

        # create missing tables:
        self.table_pending_op.create_table_if_not_exist(commit=False)
        for table in self.table_lists.all:
            table.create_table_if_not_exist(commit=False)

        # Upsert src & dst nodes
        self._upsert_nodes_without_commit(entries, PENDING)

        # Upsert Ops
        change_tuple_list: List[Tuple] = []
        for e in entries:
            change_tuple_list.append(_pending_op_to_tuple(e))
        self.table_pending_op.upsert_many(change_tuple_list, commit)

    def delete_pending_ops(self, changes: Iterable[Op], commit=True):
        uid_tuple_list = list(map(lambda x: (x.action_uid,), changes))

        # Delete for all child tables (src and dst nodes):
        for table in itertools.chain(self.table_lists.src_pending, self.table_lists.dst_pending):
            if len(uid_tuple_list) == 1:
                table.delete_for_uid(uid_tuple_list[0][0], uid_col_name=ACTION_UID_COL_NAME, commit=False)
            else:
                table.delete_for_uid_list(uid_tuple_list, uid_col_name=ACTION_UID_COL_NAME, commit=False)

        # Finally delete the Ops
        self.table_pending_op.delete_for_uid_list(uid_tuple_list, commit=commit)

    # COMPLETED_CHANGE operations ⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆

    def _upsert_completed_ops(self, entries: Iterable[Op], commit=True):
        """Inserts or updates a list of Ops (remember that each action's UID is its primary key)."""

        self.table_completed_op.create_table_if_not_exist(commit=False)

        # Upsert src & dst nodes
        self._upsert_nodes_without_commit(entries, ARCHIVE)

        # Upsert Ops
        current_time = int(time.time())
        change_tuple_list = []
        for e in entries:
            assert isinstance(e, Op), f'Expected Op; got instead: {e}'
            change_tuple_list.append(_completed_op_to_tuple(e, current_time))
        self.table_completed_op.upsert_many(change_tuple_list, commit)

    # FAILED_CHANGE operations ⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆

    def _upsert_failed_ops(self, entries: Iterable[Op], error_msg: str, commit=True):
        """Inserts or updates a list of Op (remember that each action's UID is its primary key)."""

        self.table_failed_op.create_table_if_not_exist(commit=False)

        # Upsert src & dst nodes
        self._upsert_nodes_without_commit(entries, ARCHIVE)

        current_time = int(time.time())
        change_tuple_list = []
        for e in entries:
            assert isinstance(e, Op), f'Expected Op; got instead: {e}'
            change_tuple_list.append(_failed_op_to_tuple(e, current_time, error_msg))

        self.table_failed_op.upsert_many(change_tuple_list, commit)

    # Compound operations ⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆⯆

    def archive_completed_ops(self, entries: Iterable[Op]):
        self.delete_pending_ops(changes=entries, commit=False)
        self._upsert_completed_ops(entries)

    def archive_failed_ops(self, entries: Iterable[Op], error_msg: str):
        self.delete_pending_ops(changes=entries, commit=False)
        self._upsert_failed_ops(entries, error_msg)

