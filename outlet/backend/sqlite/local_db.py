import logging
import pathlib
from collections import OrderedDict
from typing import List, Optional, Tuple

from backend.sqlite.base_db import LiveTable, MetaDatabase, Table
from model.uid import UID
from model.node.local_disk_node import LocalDirNode, LocalFileNode, LocalNode
from model.node_identifier import LocalNodeIdentifier

logger = logging.getLogger(__name__)


def _file_to_tuple(f: LocalFileNode):
    assert isinstance(f, LocalFileNode), f'Expected LocalFileNode; got instead: {f}'
    return f.to_tuple()


def _dir_to_tuple(d):
    assert isinstance(d, LocalDirNode), f'Expected LocalDirNode; got instead: {d}'
    return d.to_tuple()


class LocalDiskDatabase(MetaDatabase):
    """
    ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
    CLASS LocalDiskDatabase
    ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼
    """
    TABLE_LOCAL_FILE = Table(name='local_file', cols=OrderedDict([
        ('uid', 'INTEGER PRIMARY KEY'),
        ('parent_uid', 'INTEGER'),
        ('md5', 'TEXT'),
        ('sha256', 'TEXT'),
        ('size_bytes', 'INTEGER'),
        ('sync_ts', 'INTEGER'),
        ('modify_ts', 'INTEGER'),
        ('change_ts', 'INTEGER'),
        ('full_path', 'TEXT'),
        ('trashed', 'INTEGER'),
        ('live', 'INTEGER')
    ]))

    # 2020-06, So far this is really just a mapping of UIDs to paths, to keep things consistent across runs.
    TABLE_LOCAL_DIR = Table(name='local_dir', cols=OrderedDict([
        ('uid', 'INTEGER PRIMARY KEY'),
        ('parent_uid', 'INTEGER'),
        ('full_path', 'TEXT'),
        ('trashed', 'INTEGER'),
        ('live', 'INTEGER'),
        ('all_children_fetched', 'INTEGER')
    ]))

    def __init__(self, db_path, backend, device_uid: UID):
        super().__init__(db_path)
        self.cacheman = backend.cacheman
        self.device_uid: UID = device_uid
        self.table_local_file = LiveTable(LocalDiskDatabase.TABLE_LOCAL_FILE, self.conn, _file_to_tuple, self._tuple_to_file)
        self.table_local_dir = LiveTable(LocalDiskDatabase.TABLE_LOCAL_DIR, self.conn, _dir_to_tuple, self._tuple_to_dir)

    def _get_parent_uid(self, full_path: str) -> UID:
        parent_path = str(pathlib.Path(full_path).parent)
        return self.cacheman.get_uid_for_local_path(parent_path)

    # LOCAL_FILE operations
    # ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼

    def _tuple_to_file(self, row: Tuple) -> LocalFileNode:
        uid_int, parent_uid_int, md5, sha256, size_bytes, sync_ts, modify_ts, change_ts, full_path, trashed, is_live = row

        # make sure we call get_uid_for_local_path() for both the node's path and its parent's path, so that UID mapper has a chance to store it
        uid = self.cacheman.get_uid_for_local_path(full_path, uid_int)
        assert uid == uid_int, f'UID conflict! Got {uid} but read {uid_int} in row: {row}'
        node_identifier = LocalNodeIdentifier(uid=uid, device_uid=self.device_uid, full_path=full_path)
        parent_uid: UID = self._get_parent_uid(full_path)
        assert parent_uid == parent_uid_int, f'UID conflict! Got {uid} but read {parent_uid_int} in row: {row}'
        return LocalFileNode(node_identifier, parent_uid, md5, sha256, size_bytes, sync_ts, modify_ts, change_ts, trashed, is_live)

    def has_local_files(self):
        return self.table_local_file.has_rows()

    def get_local_files(self) -> List[LocalFileNode]:
        return self.table_local_file.select_object_list()

    def insert_local_files(self, entries: List[LocalFileNode], overwrite, commit=True):
        self.table_local_file.insert_object_list(entries, overwrite=overwrite, commit=commit)

    def upsert_local_file(self, node: LocalFileNode, commit=True):
        if not node.is_live():
            # These don't belong here; they belong in the op DB
            logger.warning(f'Saving node with is_live=False! Check code for bug: {node}')
        self.table_local_file.upsert_object(node, commit=commit)

    def upsert_local_file_list(self, file_list: List[LocalFileNode], commit=True):
        self.table_local_file.create_table_if_not_exist(commit=False)
        self.table_local_file.upsert_object_list(file_list, commit=commit)

    def delete_local_file_with_uid(self, uid: UID, commit=True):
        self.table_local_file.delete_for_uid(uid, commit=commit)

    def delete_local_files_for_uid_list(self, uid_list: List[UID], commit=True):
        uid_tuple_list = list(map(lambda uid: (uid,), uid_list))
        self.table_local_file.delete_for_uid_list(uid_tuple_list, commit=commit)

    def truncate_local_files(self, commit=True):
        self.table_local_file.truncate_table(commit=commit)

    # LOCAL_DIR operations
    # ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼

    def _tuple_to_dir(self, row: Tuple) -> LocalDirNode:
        uid_int, parent_uid_int, full_path, trashed, is_live, all_children_fetched = row
        uid = self.cacheman.get_uid_for_local_path(full_path, uid_int)
        assert uid == uid_int, f'UID conflict! Got {uid} from memstore but read from disk: {uid_int}'
        parent_uid: UID = self._get_parent_uid(full_path)
        assert parent_uid == parent_uid_int, f'UID conflict! Got {parent_uid} from memstore but read from disk: {parent_uid_int}'
        return LocalDirNode(LocalNodeIdentifier(uid=uid, device_uid=self.device_uid, full_path=full_path), parent_uid=parent_uid,
                            trashed=trashed, is_live=bool(is_live), all_children_fetched=bool(all_children_fetched))

    def has_local_dirs(self):
        return self.table_local_dir.has_rows()

    def get_local_dirs(self) -> List[LocalDirNode]:
        return self.table_local_dir.select_object_list()

    def insert_local_dirs(self, entries: List[LocalDirNode], overwrite, commit=True):
        self.table_local_dir.insert_object_list(entries, overwrite=overwrite, commit=commit)

    def upsert_local_dir(self, node: LocalDirNode, commit=True):
        if not node.is_live():
            # These don't belong here; they belong in the op DB
            logger.warning(f'Saving node with is_live=False! Check code for bug: {node}')
        self.table_local_dir.upsert_object(node, commit=commit)

    def upsert_local_dir_list(self, dir_list: List[LocalDirNode], commit=True):
        self.table_local_dir.create_table_if_not_exist(commit=False)
        self.table_local_dir.upsert_object_list(dir_list, commit=commit)

    def delete_local_dir_with_uid(self, uid: UID, commit=True):
        self.table_local_dir.delete_for_uid(uid, commit=commit)

    def delete_local_dirs_for_uid_list(self, uid_list: List[UID], commit=True):
        uid_tuple_list = list(map(lambda uid: (uid,), uid_list))
        self.table_local_dir.delete_for_uid_list(uid_tuple_list, commit=commit)

    def truncate_local_dirs(self, commit=True):
        self.table_local_dir.truncate_table(commit=commit)

    def get_child_list_for_node_uid(self, node_uid: UID) -> List[LocalNode]:
        child_dir_list = self.table_local_dir.select_object_list(where_clause='WHERE parent_uid = ?', where_tuple=(node_uid,))
        child_file_list = self.table_local_file.select_object_list(where_clause='WHERE parent_uid = ?', where_tuple=(node_uid,))
        return child_dir_list + child_file_list

    # Mixed type operations
    # ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼

    def upsert_single_node(self, node: LocalNode, commit=True):
        if node.is_dir():
            assert isinstance(node, LocalDirNode)
            self.table_local_dir.create_table_if_not_exist(commit=False)
            self.upsert_local_dir(node, commit)
        else:
            assert isinstance(node, LocalFileNode)
            self.table_local_file.create_table_if_not_exist(commit=False)
            self.upsert_local_file(node, commit)

    def delete_single_node(self, node: LocalNode, commit=True):
        if node.is_dir():
            self.delete_local_dir_with_uid(node.uid, commit)
        else:
            self.delete_local_file_with_uid(node.uid, commit)

    def upsert_files_and_dirs(self, node_list: List[LocalNode], commit=True):
        """Sorts files from dirs in the given list and batches them. Much faster than single-node operations."""
        dir_list: List[LocalDirNode] = []
        file_list: List[LocalFileNode] = []
        for node in node_list:
            if node.is_dir():
                if not isinstance(node, LocalDirNode):
                    logger.debug('TODO')  # TODO
                assert isinstance(node, LocalDirNode), f'Not a LocalDirNode: {node}'
                dir_list.append(node)
            else:
                assert isinstance(node, LocalFileNode), f'Not a LocalFileNode: {node}'
                file_list.append(node)
        if dir_list:
            self.upsert_local_dir_list(dir_list, commit=False)
        if file_list:
            self.upsert_local_file_list(file_list, commit=commit)

    def delete_files_and_dirs(self, node_list: List[LocalNode], commit=True):
        """Sorts files from dirs in the given list and batches them. Much faster than single-node operations."""
        dir_uid_list: List[UID] = []
        file_uid_list: List[UID] = []
        for node in node_list:
            if node.is_dir():
                dir_uid_list.append(node.uid)
            else:
                file_uid_list.append(node.uid)

        if dir_uid_list:
            self.delete_local_dirs_for_uid_list(dir_uid_list, commit=False)
        if file_uid_list:
            self.delete_local_files_for_uid_list(file_uid_list, commit=commit)

    def get_file_or_dir_for_uid(self, uid: UID) -> Optional[LocalNode]:
        dir_list = self.table_local_dir.select_object_list(where_clause='WHERE uid = ?', where_tuple=(uid,))
        if dir_list:
            return dir_list[0]

        file_list = self.table_local_file.select_object_list(where_clause='WHERE uid = ?', where_tuple=(uid,))
        if file_list:
            return file_list[0]

        return None

    def get_file_or_dir_for_path(self, full_path: str) -> Optional[LocalNode]:
        dir_list = self.table_local_dir.select_object_list(where_clause='WHERE full_path = ?', where_tuple=(full_path,))
        if dir_list:
            return dir_list[0]

        file_list = self.table_local_file.select_object_list(where_clause='WHERE full_path = ?', where_tuple=(full_path,))
        if file_list:
            return file_list[0]

        return None
