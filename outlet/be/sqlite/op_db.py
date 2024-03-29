import copy
import itertools
import logging
from collections import OrderedDict
from functools import partial
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple

from be.sqlite.base_db import LiveTable, MetaDatabase, Table
from be.sqlite.gdrive_db import GDriveDatabase
from be.sqlite.local_db import LocalDiskDatabase
from constants import OBJ_TYPE_DIR, OBJ_TYPE_FILE, TreeType
from model.node.gdrive_node import GDriveFile, GDriveFolder, GDriveNode
from model.node.locald_node import LocalDirNode, LocalFileNode
from model.node.node import TNode
from model.node_identifier import GDriveIdentifier, LocalNodeIdentifier
from model.uid import UID
from model.user_op import UserOp, UserOpResult, UserOpStatus, UserOpCode
from util import time_util

logger = logging.getLogger(__name__)

PENDING = 'pending'
ARCHIVE = 'archive'
SRC = 'src'
DST = 'dst'

OP_UID_COL_NAME = 'op_uid'


# CLASS UserOpRef
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼

class UserOpRef:
    def __init__(self, op_uid: UID, batch_uid: UID, op_type: UserOpCode, status: UserOpStatus, src_uid: UID,
                 dst_uid: UID = None, create_ts: int = None, detail_msg: str = ''):
        self.op_uid: UID = op_uid
        self.batch_uid: UID = batch_uid
        self.op_type: UserOpCode = op_type
        self.status: UserOpStatus = status
        self.src_uid: UID = src_uid
        self.dst_uid: UID = dst_uid
        self.create_ts: int = create_ts
        if not self.create_ts:
            self.create_ts = time_util.now_ms()
        self.detail_msg: str = detail_msg

    def __repr__(self):
        return f'UserOpRef(uid={self.op_uid} type={self.op_type.name} status={self.status.name} src={self.src_uid} dst={self.dst_uid} ' \
               f'msg="{self.detail_msg}"'


def _pending_op_to_tuple(e: UserOp):
    assert isinstance(e, UserOp), f'Expected UserOp; got instead: {e}'
    src_uid = None
    dst_uid = None
    if e.src_node:
        src_uid = e.src_node.uid

    if e.dst_node:
        dst_uid = e.dst_node.uid

    if e.is_stopped_on_error():
        detail_msg = str(e.result.error)
    else:
        detail_msg = ''

    return e.op_uid, e.batch_uid, e.op_type, e.get_status(), src_uid, dst_uid, e.create_ts, detail_msg


def _completed_op_to_tuple(e: UserOp, current_time: int, detail_msg: str = ''):
    assert isinstance(e, UserOp), f'Expected UserOp; got instead: {e}'
    src_uid = None
    dst_uid = None
    if e.src_node:
        src_uid = e.src_node.uid

    if e.dst_node:
        dst_uid = e.dst_node.uid

    # I may come to regret this little block at some point...
    if e.result and e.result.status == UserOpStatus.STOPPED_ON_ERROR and e.result.error:
        detail_msg = e.result.error

    return e.op_uid, e.batch_uid, e.op_type, e.get_status(), src_uid, dst_uid, e.create_ts, current_time, detail_msg


def _failed_op_to_tuple(e: UserOp, current_time: int, error_msg: str):
    return _completed_op_to_tuple(e, current_time, detail_msg=error_msg)


def _tuple_to_op_ref(row: Tuple) -> UserOpRef:
    assert isinstance(row, Tuple), f'Expected Tuple; got instead: {row}'
    return UserOpRef(UID(row[0]), UID(row[1]), UserOpCode(row[2]), UserOpStatus(row[3]), UID(row[4]), _ensure_uid(row[5]), int(row[6]), row[7])


class TableMegaMap:
    """
    ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
    CLASS TableMegaMap
    ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼
    """

    def __init__(self):
        # lifecycle_state -> src_or_dst -> tree_type -> obj_type
        self.all_dict: Dict[str, Dict[str, Dict[TreeType, Dict[str, Any]]]] = {}

    def put(self, lifecycle_state: str, src_or_dst: str, tree_type: TreeType, obj_type: str, obj):
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

    def append(self, lifecycle_state: str, src_or_dst: str, tree_type: TreeType, obj_type: str, obj):
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

    def get(self, lifecycle_state: str, src_or_dst: str, tree_type: TreeType, obj_type: str) -> Any:
        dict2 = self.all_dict.get(lifecycle_state, None)
        dict3 = dict2.get(src_or_dst, None)
        dict4 = dict3.get(tree_type, None)
        return dict4.get(obj_type, None)

    def entries(self) -> List[Tuple[str, str, TreeType, str, List]]:
        tuple_list: List[Tuple[str, str, TreeType, str, List]] = []

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
        self.all_dict: TableMegaMap = TableMegaMap()
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
        self.tuple_to_obj_func_map: Dict[str, Callable[[Dict[UID, TNode], Tuple], Any]] = {}
        # self.obj_to_tuple_func_map: Dict[str, Callable[[Any, UID], Tuple]] = {}

    def put_table(self, lifecycle_state: str, src_or_dst: str, tree_type: TreeType, obj_type: str, table):
        self.all_dict.put(lifecycle_state, src_or_dst, tree_type, obj_type, table)

    def get_table(self, lifecycle_state: str, src_or_dst: str, tree_type: TreeType, obj_type: str) -> LiveTable:
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

    TABLE_PENDING_OP = Table(name='op_pending', cols=OrderedDict([
        ('uid', 'INTEGER PRIMARY KEY'),
        ('batch_uid', 'INTEGER'),
        ('op_type', 'INTEGER'),
        ('status', 'INTEGER'),
        ('src_node_uid', 'INTEGER'),
        ('dst_node_uid', 'INTEGER'),
        ('create_ts', 'INTEGER'),
        ('detail_msg', 'TEXT')
    ]))

    TABLE_COMPLETED_OP = Table(name='op_completed',
                               cols=OrderedDict([
                                   ('uid', 'INTEGER PRIMARY KEY'),
                                   ('batch_uid', 'INTEGER'),
                                   ('op_type', 'INTEGER'),
                                   ('status', 'INTEGER'),
                                   ('src_node_uid', 'INTEGER'),
                                   ('dst_node_uid', 'INTEGER'),
                                   ('create_ts', 'INTEGER'),
                                   ('complete_ts', 'INTEGER'),
                                   ('detail_msg', 'TEXT')
                               ]))

    def __init__(self, db_path, backend):
        super().__init__(db_path)
        self.cacheman = backend.cacheman

        self.table_lists: TableListCollection = TableListCollection()
        # We do not use UserOpRef to Tuple, because we convert UserOp to Tuple instead
        self.table_pending_op = LiveTable(OpDatabase.TABLE_PENDING_OP, self.conn, None, _tuple_to_op_ref)
        self.table_completed_op = LiveTable(OpDatabase.TABLE_COMPLETED_OP, self.conn, None, _tuple_to_op_ref)

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

    def _verify_goog_id_consistency(self, goog_id: str, device_uid: UID, node_uid: UID):
        if goog_id:
            # Sanity check: make sure pending change cache matches GDrive cache
            uid_from_cacheman = self.cacheman.get_uid_for_goog_id(device_uid, goog_id, node_uid)
            if uid_from_cacheman != node_uid:
                raise RuntimeError(f'UID from cacheman ({uid_from_cacheman}) does not match UID from change cache ({node_uid}) '
                                   f'for goog_id "{goog_id}"')

    def _collect_gdrive_object(self, obj: GDriveNode, goog_id: str, parent_uid_int: int, op_uid_int: int,
                               nodes_by_action_uid: Dict[UID, TNode]):
        self._verify_goog_id_consistency(goog_id, obj.device_uid, obj.uid)

        if not parent_uid_int:
            raise RuntimeError(f'Invalid GDrive object in op database: it has no parent! Object: {obj}')
        obj.set_parent_uids(UID(parent_uid_int))
        op_uid = UID(op_uid_int)
        if nodes_by_action_uid.get(op_uid, None):
            raise RuntimeError(f'Duplicate node for op_uid: {op_uid}')
        nodes_by_action_uid[op_uid] = obj

    def _tuple_to_gdrive_folder(self, nodes_by_action_uid: Dict[UID, TNode], row: Tuple) -> GDriveFolder:
        op_uid_int, device_uid, uid_int, goog_id, node_name, item_trashed, create_ts, modify_ts, owner_uid, drive_id, \
            is_shared, shared_by_user_uid, sync_ts, all_children_fetched, parent_uid_int, parent_goog_id = row

        obj = GDriveFolder(GDriveIdentifier(uid=UID(uid_int), device_uid=UID(device_uid), path_list=None),
                           goog_id=goog_id, node_name=node_name, trashed=item_trashed,
                           create_ts=create_ts, modify_ts=modify_ts, owner_uid=owner_uid, drive_id=drive_id, is_shared=is_shared,
                           shared_by_user_uid=shared_by_user_uid, sync_ts=sync_ts, all_children_fetched=all_children_fetched)

        self._collect_gdrive_object(obj, goog_id, parent_uid_int, op_uid_int, nodes_by_action_uid)

        return obj

    def _tuple_to_gdrive_file(self, nodes_by_action_uid: Dict[UID, TNode], row: Tuple) -> GDriveFile:
        op_uid_int, device_uid, uid_int, goog_id, content_uid, size_bytes, node_name, mime_type_uid, item_trashed, create_ts, modify_ts, \
            owner_uid, drive_id, is_shared, shared_by_user_uid, version, sync_ts, parent_uid_int, parent_goog_id = row

        content_meta = self.cacheman.get_content_meta_for_uid(content_uid)
        obj = GDriveFile(GDriveIdentifier(uid=UID(uid_int), device_uid=UID(device_uid), path_list=None),
                         goog_id=goog_id, node_name=node_name, mime_type_uid=mime_type_uid,
                         trashed=item_trashed, drive_id=drive_id, version=version, content_meta=content_meta, size_bytes=size_bytes,
                         is_shared=is_shared,  create_ts=create_ts, modify_ts=modify_ts, owner_uid=owner_uid,
                         shared_by_user_uid=shared_by_user_uid, sync_ts=sync_ts)

        self._collect_gdrive_object(obj, goog_id, parent_uid_int, op_uid_int, nodes_by_action_uid)
        return obj

    def _tuple_to_local_dir(self, nodes_by_action_uid: Dict[UID, TNode], row: Tuple) -> LocalDirNode:
        op_uid_int, device_uid, uid_int, parent_uid, full_path, trashed_status, is_live, sync_ts, create_ts, modify_ts, change_ts, \
            all_children_fetched = row

        uid = self.cacheman.get_uid_for_local_path(full_path, uid_int)
        assert uid == uid_int, f'UID conflict! Got {uid} but read {row}'
        obj = LocalDirNode(LocalNodeIdentifier(uid=uid, device_uid=UID(device_uid),
                           full_path=full_path), parent_uid, trashed_status, bool(is_live),
                           sync_ts, create_ts, modify_ts, change_ts,  bool(all_children_fetched))
        op_uid = UID(op_uid_int)
        if nodes_by_action_uid.get(op_uid, None):
            raise RuntimeError(f'Duplicate node for op_uid: {op_uid}')
        nodes_by_action_uid[op_uid] = obj
        return obj

    def _tuple_to_local_file(self, nodes_by_action_uid: Dict[UID, TNode], row: Tuple) -> LocalFileNode:
        op_uid_int, device_uid, uid_int, parent_uid, content_uid, size_bytes, sync_ts, create_ts, modify_ts, change_ts, full_path, trashed, \
            is_live = row

        uid = self.cacheman.get_uid_for_local_path(full_path, uid_int)
        if uid != uid_int:
            raise RuntimeError(f'UID conflict! Cacheman returned {uid} but op cache returned {uid_int} (from row: {row})')
        node_identifier = LocalNodeIdentifier(uid=uid, device_uid=UID(device_uid), full_path=full_path)
        content_meta = self.cacheman.get_content_meta_for_uid(content_uid)
        obj = LocalFileNode(node_identifier, parent_uid, content_meta, size_bytes, sync_ts, create_ts, modify_ts, change_ts, trashed, is_live)
        op_uid = UID(op_uid_int)
        if nodes_by_action_uid.get(op_uid, None):
            raise RuntimeError(f'Duplicate node for op_uid: {op_uid}')
        nodes_by_action_uid[op_uid] = obj
        return obj

    def _action_node_to_tuple(self, node: TNode, op_uid: UID) -> Tuple:
        if not node.has_tuple():
            raise RuntimeError(f'TNode cannot be converted to tuple: {node}')
        if node.tree_type == TreeType.GDRIVE:
            assert isinstance(node, GDriveNode)
            parent_uid: Optional[UID] = None
            parent_goog_id: Optional[str] = None
            if node.get_parent_uids():
                parent_uid = node.get_parent_uids()[0]
                parent_goog_id_list = self.cacheman.get_parent_goog_id_list(node)
                if len(parent_goog_id_list) < 1:
                    # this is currently the only way the previous method will return empty list without failing
                    logger.debug(f'Could not resolve goog_id for {node.device_uid}:{parent_uid}; assuming parent is GDrive root')
                else:
                    parent_goog_id = parent_goog_id_list[0]
                    if len(parent_goog_id_list) > 1:
                        # FIXME: need to add support for storing multiple parents!
                        # (however in theory this shouldn't result in a bug, because the load process should favor the correct copy from main cache)
                        logger.error(f'TNode {node.device_uid}:{node.uid} has more than one parent: only first one will be stored!')
            return op_uid, node.device_uid, *node.to_tuple(), parent_uid, parent_goog_id

        assert node.tree_type == TreeType.LOCAL_DISK
        return op_uid, node.device_uid, *node.to_tuple()

    def _copy_and_augment_table(self, src_table: Table, prefix: str, suffix: str) -> LiveTable:
        table: Table = copy.deepcopy(src_table)
        table.name = f'{prefix}_{table.name}_{suffix}'
        # Rename column 'uid' to 'node_uid', & it is no longer primary key
        table.cols.pop('uid')
        table.cols['node_uid'] = 'INTEGER'
        # move to front:
        table.cols.move_to_end('node_uid', last=False)

        # add device_uid:
        table.cols.update({('device_uid', 'INTEGER')})
        # move to front:
        table.cols.move_to_end('device_uid', last=False)

        # primary key is also foreign key (not enforced) to UserOp (ergo, only one row per UserOp):
        table.cols.update({OP_UID_COL_NAME: 'INTEGER PRIMARY KEY'})
        # move to front:
        table.cols.move_to_end(OP_UID_COL_NAME, last=False)

        if src_table.name == LocalDiskDatabase.TABLE_LOCAL_FILE.name:
            live_table = LiveTable(table, self.conn, None, None)
            self.table_lists.put_table(prefix, suffix, TreeType.LOCAL_DISK, OBJ_TYPE_FILE, live_table)
            self.table_lists.local_file.append(live_table)
            self.table_lists.tuple_to_obj_func_map[live_table.name] = self._tuple_to_local_file
        elif src_table.name == LocalDiskDatabase.TABLE_LOCAL_DIR.name:
            live_table = LiveTable(table, self.conn, None, None)
            self.table_lists.put_table(prefix, suffix, TreeType.LOCAL_DISK, OBJ_TYPE_DIR, live_table)
            self.table_lists.local_dir.append(live_table)
            self.table_lists.tuple_to_obj_func_map[live_table.name] = self._tuple_to_local_dir
        elif src_table.name == GDriveDatabase.TABLE_GRDIVE_FILE.name:
            live_table = LiveTable(table, self.conn, None, None)
            self.table_lists.put_table(prefix, suffix, TreeType.GDRIVE, OBJ_TYPE_FILE, live_table)
            self.table_lists.gdrive_file.append(live_table)
            self.table_lists.tuple_to_obj_func_map[live_table.name] = self._tuple_to_gdrive_file
        elif src_table.name == GDriveDatabase.TABLE_GRDIVE_FOLDER.name:
            live_table = LiveTable(table, self.conn, None, None)
            self.table_lists.put_table(prefix, suffix, TreeType.GDRIVE, OBJ_TYPE_DIR, live_table)
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

    # PENDING_OP operations
    # ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼

    def _get_all_in_table(self, table: LiveTable, nodes_by_op_uid: Dict[UID, TNode]):
        # Look up appropriate ORM function and bind its first param to nodes_by_op_uid
        table_func: Callable = partial(self.table_lists.tuple_to_obj_func_map[table.name], nodes_by_op_uid)

        table.select_object_list(tuple_to_obj_func_override=table_func)

    def get_pending_ops(self, where_clause: Optional[str] = '', where_tuple: Tuple = None) -> List[UserOp]:
        """ Gets all pending changes, filling int their src and dst nodes as well """
        entries: List[UserOp] = []

        if not self.table_pending_op.is_table():
            return entries

        src_node_by_op_uid: Dict[UID, TNode] = {}
        for table in self.table_lists.src_pending:
            logger.debug(f'Getting src nodes from table: "{table.name}"')
            self._get_all_in_table(table, src_node_by_op_uid)

        dst_node_by_op_uid: Dict[UID, TNode] = {}
        for table in self.table_lists.dst_pending:
            logger.debug(f'Getting dst nodes from table: "{table.name}"')
            self._get_all_in_table(table, dst_node_by_op_uid)

        op_tuple_list = self.table_pending_op.select(where_clause, where_tuple)
        logger.debug(f'Found {len(op_tuple_list)} pending ops in table {self.table_pending_op.name}')
        for op_tuple in op_tuple_list:
            ref = _tuple_to_op_ref(op_tuple)
            src_node = src_node_by_op_uid.get(ref.op_uid, None)
            dst_node = dst_node_by_op_uid.get(ref.op_uid, None)

            if not src_node:
                raise RuntimeError(f'No src node found for: {ref}')
            if src_node.uid != ref.src_uid:
                raise RuntimeError(f'Src node UID ({src_node.uid}) does not match ref in: {ref}')
            if ref.dst_uid:
                if not dst_node:
                    raise RuntimeError(f'No dst node found for: {ref}')

                if dst_node.uid != ref.dst_uid:
                    raise RuntimeError(f'Dst node UID ({dst_node.uid}) does not match ref in: {ref}')

            op = UserOp(ref.op_uid, ref.batch_uid, ref.op_type, src_node, dst_node)

            error_msg = ref.detail_msg if ref.status == UserOpStatus.STOPPED_ON_ERROR else None
            op.result = UserOpResult(status=ref.status, error=error_msg)

            entries.append(op)
        return entries

    def get_all_pending_ops(self) -> List[UserOp]:
        return self.get_pending_ops()

    def get_all_pending_ops_for_batch_uid(self, batch_uid: UID) -> List[UserOp]:
        return self.get_pending_ops(where_clause=f'WHERE batch_uid = ?', where_tuple=(batch_uid,))

    def _make_tuple_list(self, entries: Iterable[UserOp], lifecycle_state: str) -> TableMegaMap:
        tuple_list_multimap: TableMegaMap = TableMegaMap()

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

    def upsert_pending_op_list(self, entries: Iterable[UserOp], truncate_table_first: bool = False, commit: bool = True):
        """Inserts or updates a list of Ops (remember that each action's UID is its primary key).
        If overwrite=true, then removes all existing changes first."""

        if truncate_table_first:
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
        tuple_list: List[Tuple] = []
        for e in entries:
            tuple_list.append(_pending_op_to_tuple(e))
        self.table_pending_op.upsert_many(tuple_list, commit)

    def delete_pending_ops(self, changes: Iterable[UserOp], commit=True):
        uid_tuple_list = list(map(lambda x: (x.op_uid,), changes))

        # Delete for all child tables (src and dst nodes):
        for table in itertools.chain(self.table_lists.src_pending, self.table_lists.dst_pending):
            if len(uid_tuple_list) == 1:
                table.delete_for_uid(uid_tuple_list[0][0], uid_col_name=OP_UID_COL_NAME, commit=False)
            else:
                table.delete_for_uid_list(uid_tuple_list, uid_col_name=OP_UID_COL_NAME, commit=False)

        # Finally delete the ops
        self.table_pending_op.delete_for_uid_list(uid_tuple_list, commit=commit)

    # COMPLETED_OP operations
    # ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼

    def _upsert_completed_ops(self, entries: Iterable[UserOp], detail_msg='', commit=True):
        """Inserts or updates a list of Ops (remember that each action's UID is its primary key)."""

        self.table_completed_op.create_table_if_not_exist(commit=False)

        # Upsert src & dst nodes
        self._upsert_nodes_without_commit(entries, ARCHIVE)

        # Upsert Ops
        current_time = time_util.now_sec()
        tuple_list = []
        for user_op in entries:
            assert isinstance(user_op, UserOp), f'Expected UserOp; got instead: {user_op}'
            tuple_list.append(_completed_op_to_tuple(user_op, current_time, detail_msg))
        self.table_completed_op.upsert_many(tuple_list, commit)

    # Compound operations
    # ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼

    def archive_completed_op_list(self, entries: Iterable[UserOp]):
        self.delete_pending_ops(changes=entries, commit=False)
        self._upsert_completed_ops(entries)

    def archive_failed_op_list(self, entries: Iterable[UserOp], error_msg: str):
        # TODO: ideally we would verify that the given ops are present while doing this, so that we don't have bogus op_failed entries
        self.delete_pending_ops(changes=entries, commit=False)
        self._upsert_completed_ops(entries, error_msg)

    def archive_completed_op_and_batch(self, op: UserOp):
        self.upsert_pending_op_list([op], commit=False)  # easier to do this than query by batch_uid first then sort things out in memory
        batch_uid = op.batch_uid
        op_list = self.get_all_pending_ops_for_batch_uid(batch_uid)
        self.delete_pending_ops(changes=op_list, commit=False)
        self._upsert_completed_ops(op_list)
