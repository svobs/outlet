import copy
import itertools
import logging
import pathlib
from collections import OrderedDict
from functools import partial
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple

from constants import OBJ_TYPE_DIR, OBJ_TYPE_FILE, TrashStatus, TREE_TYPE_GDRIVE, TREE_TYPE_LOCAL_DISK
from backend.store.sqlite.base_db import LiveTable, MetaDatabase, Table
from backend.store.sqlite.gdrive_db import GDriveDatabase
from backend.store.sqlite.local_db import LocalDiskDatabase
from model.uid import UID
from model.user_op import UserOp, UserOpType
from model.node.node import Node
from model.node.gdrive_node import GDriveFile, GDriveFolder, GDriveNode
from model.node.local_disk_node import LocalDirNode, LocalFileNode
from model.node_identifier import GDriveIdentifier, LocalNodeIdentifier
from util import time_util

logger = logging.getLogger(__name__)

PENDING = 'pending'
ARCHIVE = 'archive'
SRC = 'src'
DST = 'dst'

ACTION_UID_COL_NAME = 'op_uid'


# CLASS UserOpRef
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼

class UserOpRef:
    def __init__(self, op_uid: UID, batch_uid: UID, op_type: UserOpType, src_uid: UID, dst_uid: UID = None, create_ts: int = None):
        self.op_uid: UID = op_uid
        self.batch_uid: UID = batch_uid
        self.op_type: UserOpType = op_type
        self.src_uid: UID = src_uid
        self.dst_uid: UID = dst_uid
        self.create_ts: int = create_ts
        if not self.create_ts:
            self.create_ts = time_util.now_ms()

    def __repr__(self):
        return f'UserOpRef(uid={self.op_uid} type={self.op_type.name} src={self.src_uid} dst={self.dst_uid}'


def _pending_op_to_tuple(e: UserOp):
    assert isinstance(e, UserOp), f'Expected UserOp; got instead: {e}'
    src_uid = None
    dst_uid = None
    if e.src_node:
        src_uid = e.src_node.uid

    if e.dst_node:
        dst_uid = e.dst_node.uid

    return e.op_uid, e.batch_uid, e.op_type, src_uid, dst_uid, e.create_ts


def _completed_op_to_tuple(e: UserOp, current_time):
    assert isinstance(e, UserOp), f'Expected UserOp; got instead: {e}'
    src_uid = None
    dst_uid = None
    if e.src_node:
        src_uid = e.src_node.uid

    if e.dst_node:
        dst_uid = e.dst_node.uid

    return e.op_uid, e.batch_uid, e.op_type, src_uid, dst_uid, e.create_ts, current_time


def _failed_op_to_tuple(e: UserOp, current_time, error_msg):
    partial_tuple = _completed_op_to_tuple(e, current_time)
    return *partial_tuple, error_msg


def _tuple_to_op_ref(row: Tuple) -> UserOpRef:
    assert isinstance(row, Tuple), f'Expected Tuple; got instead: {row}'
    return UserOpRef(UID(row[0]), UID(row[1]), UserOpType(row[2]), UID(row[3]), _ensure_uid(row[4]), int(row[5]))


class TableMultiMap:
    """
    ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
    CLASS TableMultiMap
    ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼
    """
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


class TableListCollection:
    """
    ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
    CLASS TableListCollection
    ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼
    """
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
        self.tuple_to_obj_func_map: Dict[str, Callable[[Dict[UID, Node], Tuple], Any]] = {}
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


class OpDatabase(MetaDatabase):
    """
    ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
    CLASS OpDatabase
    ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼
    """

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

    def __init__(self, db_path, backend):
        super().__init__(db_path)
        self.cacheman = backend.cacheman

        self.table_lists: TableListCollection = TableListCollection()
        # We do not use UserOpRef to Tuple, because we convert UserOp to Tuple instead
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

    def _get_parent_uid(self, full_path: str) -> UID:
        parent_path = str(pathlib.Path(full_path).parent)
        return self.cacheman.get_uid_for_local_path(parent_path)

    def _verify_goog_id_consistency(self, goog_id: str, obj_uid: UID):
        if goog_id:
            # Sanity check: make sure pending change cache matches GDrive cache
            uid_from_cacheman = self.cacheman.get_uid_for_goog_id(goog_id, obj_uid)
            if uid_from_cacheman != obj_uid:
                raise RuntimeError(f'UID from cacheman ({uid_from_cacheman}) does not match UID from change cache ({obj_uid}) '
                                   f'for goog_id "{goog_id}"')

    def _collect_gdrive_object(self, obj: GDriveNode, goog_id: str, parent_uid_int: int, action_uid_int: int,
                               nodes_by_action_uid: Dict[UID, Node]):
        self._verify_goog_id_consistency(goog_id, obj.uid)

        if not parent_uid_int:
            raise RuntimeError(f'Invalid GDrive object in op database: it has no parent! Object: {obj}')
        obj.set_parent_uids(UID(parent_uid_int))
        op_uid = UID(action_uid_int)
        if nodes_by_action_uid.get(op_uid, None):
            raise RuntimeError(f'Duplicate node for op_uid: {op_uid}')
        nodes_by_action_uid[op_uid] = obj

    def _tuple_to_gdrive_folder(self, nodes_by_action_uid: Dict[UID, Node], row: Tuple) -> GDriveFolder:
        action_uid_int, uid_int, goog_id, node_name, item_trashed, create_ts, modify_ts, owner_uid, drive_id, is_shared, shared_by_user_uid, \
            sync_ts, all_children_fetched, parent_uid_int, parent_goog_id = row

        obj = GDriveFolder(GDriveIdentifier(uid=UID(uid_int), path_list=None), goog_id=goog_id, node_name=node_name, trashed=item_trashed,
                           create_ts=create_ts, modify_ts=modify_ts, owner_uid=owner_uid, drive_id=drive_id, is_shared=is_shared,
                           shared_by_user_uid=shared_by_user_uid, sync_ts=sync_ts, all_children_fetched=all_children_fetched)

        self._collect_gdrive_object(obj, goog_id, parent_uid_int, action_uid_int, nodes_by_action_uid)

        return obj

    def _tuple_to_gdrive_file(self, nodes_by_action_uid: Dict[UID, Node], row: Tuple) -> GDriveFile:
        action_uid_int, uid_int, goog_id, node_name, mime_type_uid, item_trashed, size_bytes, md5, create_ts, modify_ts, owner_uid, drive_id, \
            is_shared, shared_by_user_uid, version, sync_ts, parent_uid_int, parent_goog_id = row

        obj = GDriveFile(GDriveIdentifier(uid=UID(uid_int), path_list=None), goog_id=goog_id, node_name=node_name, mime_type_uid=mime_type_uid,
                         trashed=item_trashed, drive_id=drive_id, version=version, md5=md5, is_shared=is_shared,
                         create_ts=create_ts, modify_ts=modify_ts, size_bytes=size_bytes, owner_uid=owner_uid, shared_by_user_uid=shared_by_user_uid,
                         sync_ts=sync_ts)

        self._collect_gdrive_object(obj, goog_id, parent_uid_int, action_uid_int, nodes_by_action_uid)
        return obj

    def _tuple_to_local_dir(self, nodes_by_action_uid: Dict[UID, Node], row: Tuple) -> LocalDirNode:
        action_uid_int, uid_int, full_path, is_live = row

        uid = self.cacheman.get_uid_for_local_path(full_path, uid_int)
        assert uid == uid_int, f'UID conflict! Got {uid} but read {row}'
        parent_uid: UID = self._get_parent_uid(full_path)
        obj = LocalDirNode(LocalNodeIdentifier(uid=uid, path_list=full_path), parent_uid, TrashStatus.NOT_TRASHED, bool(is_live))
        op_uid = UID(action_uid_int)
        if nodes_by_action_uid.get(op_uid, None):
            raise RuntimeError(f'Duplicate node for op_uid: {op_uid}')
        nodes_by_action_uid[op_uid] = obj
        return obj

    def _tuple_to_local_file(self, nodes_by_action_uid: Dict[UID, Node], row: Tuple) -> LocalFileNode:
        action_uid_int, uid_int, md5, sha256, size_bytes, sync_ts, modify_ts, change_ts, full_path, trashed, is_live = row

        uid = self.cacheman.get_uid_for_local_path(full_path, uid_int)
        if uid != uid_int:
            raise RuntimeError(f'UID conflict! Cacheman returned {uid} but op cache returned {uid_int} (from row: {row})')
        parent_uid: UID = self._get_parent_uid(full_path)
        node_identifier = LocalNodeIdentifier(uid=uid, path_list=full_path)
        obj = LocalFileNode(node_identifier, parent_uid, md5, sha256, size_bytes, sync_ts, modify_ts, change_ts, trashed, is_live)
        op_uid = UID(action_uid_int)
        if nodes_by_action_uid.get(op_uid, None):
            raise RuntimeError(f'Duplicate node for op_uid: {op_uid}')
        nodes_by_action_uid[op_uid] = obj
        return obj

    def _action_node_to_tuple(self, node: Node, op_uid: UID) -> Tuple:
        if not node.has_tuple():
            raise RuntimeError(f'Node cannot be converted to tuple: {node}')
        if node.get_tree_type() == TREE_TYPE_GDRIVE:
            assert isinstance(node, GDriveNode)
            parent_uid: Optional[UID] = None
            parent_goog_id: Optional[str] = None
            if node.get_parent_uids():
                parent_uid = node.get_parent_uids()[0]
                try:
                    parent_goog_id = self.cacheman.get_goog_id_for_parent(node)
                except RuntimeError:
                    logger.debug(f'Could not resolve goog_id for UID {parent_uid}; assuming parent is not yet created')
            return op_uid, *node.to_tuple(), parent_uid, parent_goog_id

        return op_uid, *node.to_tuple()

    def _copy_and_augment_table(self, src_table: Table, prefix: str, suffix: str) -> LiveTable:
        table: Table = copy.deepcopy(src_table)
        table.name = f'{prefix}_{table.name}_{suffix}'
        # uid is no longer primary key
        table.cols.update({'uid': 'INTEGER'})
        # primary key is also foreign key (not enforced) to UserOp (ergo, only one row per UserOp):
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

    # PENDING_CHANGE operations
    # ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼

    def _get_all_in_table(self, table: LiveTable, nodes_by_action_uid: Dict[UID, Node]):
        # Look up appropriate ORM function and bind its first param to nodes_by_action_uid
        table_func: Callable = partial(self.table_lists.tuple_to_obj_func_map[table.name], nodes_by_action_uid)

        table.select_object_list(tuple_to_obj_func_override=table_func)

    def get_all_pending_ops(self) -> List[UserOp]:
        """ Gets all pending changes, filling int their src and dst nodes as well """
        entries: List[UserOp] = []

        if not self.table_pending_op.is_table():
            return entries

        src_node_by_action_uid: Dict[UID, Node] = {}
        for table in self.table_lists.src_pending:
            logger.debug(f'Getting src nodes from table: "{table.name}"')
            self._get_all_in_table(table, src_node_by_action_uid)

        dst_node_by_action_uid: Dict[UID, Node] = {}
        for table in self.table_lists.dst_pending:
            logger.debug(f'Getting dst nodes from table: "{table.name}"')
            self._get_all_in_table(table, dst_node_by_action_uid)

        rows = self.table_pending_op.get_all_rows()
        logger.debug(f'Found {len(rows)} pending ops in table {self.table_pending_op.name}')
        for row in rows:
            ref = UserOpRef(UID(row[0]), UID(row[1]), UserOpType(row[2]), UID(row[3]), _ensure_uid(row[4]), int(row[5]))
            src_node = src_node_by_action_uid.get(ref.op_uid, None)
            dst_node = dst_node_by_action_uid.get(ref.op_uid, None)

            if not src_node:
                raise RuntimeError(f'No src node found for: {ref}')
            if src_node.uid != ref.src_uid:
                raise RuntimeError(f'Src node UID ({src_node.uid}) does not match ref in: {ref}')
            if ref.dst_uid:
                if not dst_node:
                    raise RuntimeError(f'No dst node found for: {ref}')

                if dst_node.uid != ref.dst_uid:
                    raise RuntimeError(f'Dst node UID ({dst_node.uid}) does not match ref in: {ref}')

            entries.append(UserOp(ref.op_uid, ref.batch_uid, ref.op_type, src_node, dst_node))
        return entries

    def _make_tuple_list(self, entries: Iterable[UserOp], lifecycle_state: str) -> TableMultiMap:
        tuple_list_multimap: TableMultiMap = TableMultiMap()

        for e in entries:
            assert isinstance(e, UserOp), f'Expected UserOp; got instead: {e}'

            node = e.src_node
            assert not node.has_no_parents(), f'Src node has no parents: {node}'
            node_tuple = self._action_node_to_tuple(node, e.op_uid)
            tuple_list_multimap.append(lifecycle_state, SRC, node.node_identifier.tree_type, node.get_obj_type(), node_tuple)

            if e.dst_node:
                node = e.dst_node
                assert not node.has_no_parents(), f'Dst node has no parents: {node}'
                node_tuple = self._action_node_to_tuple(node, e.op_uid)
                tuple_list_multimap.append(lifecycle_state, DST, node.node_identifier.tree_type, node.get_obj_type(), node_tuple)

        return tuple_list_multimap

    def _upsert_nodes_without_commit(self, entries: Iterable[UserOp], lifecycle_state: str):
        tuple_list_multimap = self._make_tuple_list(entries, lifecycle_state)
        for lifecycle_state, src_or_dst, tree_type, obj_type, tuple_list in tuple_list_multimap.entries():
            table: LiveTable = self.table_lists.get_table(lifecycle_state, src_or_dst, tree_type, obj_type)
            assert table, f'No table for values: {lifecycle_state}, {src_or_dst}, {tree_type}, {obj_type}'
            table.upsert_many(tuple_list, commit=False)

    def upsert_pending_ops(self, entries: Iterable[UserOp], overwrite, commit=True):
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

    def delete_pending_ops(self, changes: Iterable[UserOp], commit=True):
        uid_tuple_list = list(map(lambda x: (x.op_uid,), changes))

        # Delete for all child tables (src and dst nodes):
        for table in itertools.chain(self.table_lists.src_pending, self.table_lists.dst_pending):
            if len(uid_tuple_list) == 1:
                table.delete_for_uid(uid_tuple_list[0][0], uid_col_name=ACTION_UID_COL_NAME, commit=False)
            else:
                table.delete_for_uid_list(uid_tuple_list, uid_col_name=ACTION_UID_COL_NAME, commit=False)

        # Finally delete the Ops
        self.table_pending_op.delete_for_uid_list(uid_tuple_list, commit=commit)

    # COMPLETED_CHANGE operations
    # ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼

    def _upsert_completed_ops(self, entries: Iterable[UserOp], commit=True):
        """Inserts or updates a list of Ops (remember that each action's UID is its primary key)."""

        self.table_completed_op.create_table_if_not_exist(commit=False)

        # Upsert src & dst nodes
        self._upsert_nodes_without_commit(entries, ARCHIVE)

        # Upsert Ops
        current_time = time_util.now_sec()
        change_tuple_list = []
        for e in entries:
            assert isinstance(e, UserOp), f'Expected UserOp; got instead: {e}'
            change_tuple_list.append(_completed_op_to_tuple(e, current_time))
        self.table_completed_op.upsert_many(change_tuple_list, commit)

    # FAILED_CHANGE operations
    # ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼

    def _upsert_failed_ops(self, entries: Iterable[UserOp], error_msg: str, commit=True):
        """Inserts or updates a list of UserOp (remember that each action's UID is its primary key)."""

        self.table_failed_op.create_table_if_not_exist(commit=False)

        # Upsert src & dst nodes
        self._upsert_nodes_without_commit(entries, ARCHIVE)

        current_time = time_util.now_sec()
        change_tuple_list = []
        for e in entries:
            assert isinstance(e, UserOp), f'Expected UserOp; got instead: {e}'
            change_tuple_list.append(_failed_op_to_tuple(e, current_time, error_msg))

        self.table_failed_op.upsert_many(change_tuple_list, commit)

    # Compound operations
    # ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼

    def archive_completed_ops(self, entries: Iterable[UserOp]):
        self.delete_pending_ops(changes=entries, commit=False)
        self._upsert_completed_ops(entries)

    def archive_failed_ops(self, entries: Iterable[UserOp], error_msg: str):
        self.delete_pending_ops(changes=entries, commit=False)
        self._upsert_failed_ops(entries, error_msg)

