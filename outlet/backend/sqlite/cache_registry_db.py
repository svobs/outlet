import logging
from collections import OrderedDict
from typing import List, Tuple, Union

from constants import TREE_TYPE_LOCAL_DISK
from model.device import Device
from model.node_identifier import LocalNodeIdentifier, SinglePathNodeIdentifier
from util import file_util
from model.cache_info import CacheInfoEntry
from backend.sqlite.base_db import LiveTable, MetaDatabase, Table

logger = logging.getLogger(__name__)


def _cache_info_to_tuple(d: CacheInfoEntry) -> Tuple:
    assert isinstance(d, CacheInfoEntry), f'Expected CacheInfoEntry; got instead: {d}'
    return d.to_tuple()


def _tuple_to_cache_info(a_tuple: Tuple) -> CacheInfoEntry:
    assert isinstance(a_tuple, Tuple), f'Expected Tuple; got instead: {a_tuple}'
    return CacheInfoEntry(*a_tuple)


def _device_to_tuple(d: Device) -> Tuple:
    assert isinstance(d, Device), f'Expected Device; got instead: {d}'
    return d.to_tuple()


def _tuple_to_device(a_tuple: Tuple) -> Device:
    assert isinstance(a_tuple, Tuple), f'Expected Tuple; got instead: {a_tuple}'
    return Device(*a_tuple)



class CacheRegistry(MetaDatabase):
    """
    ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
    CLASS CacheRegistry
    ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼
    """
    TABLE_CACHE_REGISTRY = Table(name='cache_registry', cols=OrderedDict([
        ('cache_location', 'TEXT'),
        ('cache_type', 'INTEGER'),
        ('subtree_root_path', 'TEXT'),
        ('subtree_root_uid', 'INTEGER'),
        ('sync_ts', 'INTEGER'),
        ('complete', 'INTEGER')
    ]))

    TABLE_DEVICE = Table(name='device', cols=OrderedDict([
        ('uid', 'INTEGER PRIMARY KEY'),
        ('device_id', 'TEXT'),
        ('tree_type', 'INTEGER'),
        ('friendly_name', 'TEXT'),
        ('sync_ts', 'INTEGER'),
    ]))

    def __init__(self, main_registry_path: str, node_identifier_factory):
        super().__init__(main_registry_path)
        self.node_identifier_factory = node_identifier_factory
        self.table_cache_registry = LiveTable(CacheRegistry.TABLE_CACHE_REGISTRY, self.conn, _cache_info_to_tuple, _tuple_to_cache_info)
        self.table_device = LiveTable(CacheRegistry.TABLE_DEVICE, self.conn, _device_to_tuple, _tuple_to_device)

    def has_cache_info(self):
        return self.table_cache_registry.has_rows()

    def create_cache_registry_if_not_exist(self):
        self.table_cache_registry.create_table_if_not_exist(self.conn)

    def get_cache_info_list(self) -> List[CacheInfoEntry]:
        rows = self.table_cache_registry.get_all_rows()
        entries = []
        for row in rows:
            cache_location, cache_type, subtree_root_path, subtree_root_uid, sync_ts, is_complete = row
            subtree_root_path = file_util.normalize_path(subtree_root_path)
            if cache_type == TREE_TYPE_LOCAL_DISK:
                node_identifier = LocalNodeIdentifier(uid=subtree_root_uid, path_list=subtree_root_path)
            else:
                node_identifier = SinglePathNodeIdentifier(uid=subtree_root_uid, path_list=subtree_root_path, tree_type=cache_type)
            entries.append(CacheInfoEntry(cache_location=cache_location, subtree_root=node_identifier,
                                          sync_ts=sync_ts, is_complete=is_complete))
        return entries

    # Takes a list of CacheInfoEntry objects:
    def insert_cache_info(self, entries: Union[CacheInfoEntry, List[CacheInfoEntry]], append: bool, overwrite: bool):
        rows = []
        if type(entries) == list:
            for entry in entries:
                rows.append(entry.to_tuple())
        else:
            rows.append(entries.to_tuple())

        has_existing = self.has_cache_info()
        if has_existing:
            if overwrite:
                self.table_cache_registry.drop_table_if_exists(commit=False)
            elif not append:
                raise RuntimeError('Cannot insert CacheInfo into a non-empty table (overwrite=False, append=False)')

        self.table_cache_registry.create_table_if_not_exist(commit=False)
        self.table_cache_registry.insert_many(rows)

    def get_device_list(self) -> List[CacheInfoEntry]:
        return self.table_device.select_object_list()

    def upsert_device(self, device: Device):
        return self.table_device.upsert_object(device)
