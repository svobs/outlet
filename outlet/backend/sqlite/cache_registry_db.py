import logging
from collections import OrderedDict
from typing import List, Optional, Tuple, Union

from backend.sqlite.base_db import LiveTable, MetaDatabase, Table
from model.cache_info import CacheInfoEntry
from model.device import Device
from model.uid import UID
from util import file_util, time_util

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
    return Device(a_tuple[0], a_tuple[1], a_tuple[2], a_tuple[3])


class CacheRegistryDatabase(MetaDatabase):
    """
    ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
    CLASS CacheRegistryDatabase
    ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼
    """
    TABLE_CACHE_REGISTRY = Table(name='cache_registry', cols=OrderedDict([
        ('cache_location', 'TEXT'),
        ('device_uid', 'INTEGER'),
        ('subtree_root_path', 'TEXT'),
        ('subtree_root_uid', 'INTEGER'),
        ('sync_ts', 'INTEGER'),
        ('complete', 'INTEGER')
    ]))

    TABLE_DEVICE = Table(name='device', cols=OrderedDict([
        ('uid', 'INTEGER PRIMARY KEY AUTOINCREMENT'),
        ('device_id', 'TEXT'),
        ('tree_type', 'INTEGER'),
        ('friendly_name', 'TEXT'),
        ('sync_ts', 'INTEGER'),
    ]))

    def __init__(self, main_registry_path: str, node_identifier_factory):
        super().__init__(main_registry_path)
        self.node_identifier_factory = node_identifier_factory
        self.table_cache_registry = LiveTable(CacheRegistryDatabase.TABLE_CACHE_REGISTRY, self.conn, _cache_info_to_tuple, _tuple_to_cache_info)
        self.table_device = LiveTable(CacheRegistryDatabase.TABLE_DEVICE, self.conn, _device_to_tuple, _tuple_to_device)

    def has_cache_info(self):
        return self.table_cache_registry.has_rows()

    def create_cache_registry_if_not_exist(self):
        self.table_cache_registry.create_table_if_not_exist(self.conn)

    @staticmethod
    def _find_device_with_uid(device_list: List[Device], device_uid: UID) -> Optional[Device]:
        for device in device_list:
            if device.uid == device_uid:
                return device

        raise RuntimeError(f'Could not find device with UID: {device_uid}')

    def get_cache_info_list(self) -> List[CacheInfoEntry]:
        rows = self.table_cache_registry.get_all_rows()
        entries = []
        for row in rows:
            cache_location, device_uid, subtree_root_path, subtree_root_uid, sync_ts, is_complete = row
            subtree_root_path = file_util.normalize_path(subtree_root_path)
            node_identifier = self.node_identifier_factory.for_values(uid=subtree_root_uid, device_uid=device_uid,
                                                                      path_list=subtree_root_path, must_be_single_path=True)
            assert node_identifier.is_spid(), f'Not a SPID: {node_identifier}'
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

    def get_device_list(self) -> List[Device]:
        device_list = self.table_device.select_object_list()
        return device_list

    def upsert_device(self, device: Device):
        self.table_device.create_table_if_not_exist()
        if not device.uid:
            self.insert_device(device)
            return
        # this *will* commit the tx
        device_tuple = (*device.to_tuple(), time_util.now_sec())
        self.table_device.upsert_one(device_tuple)

    def insert_device(self, device: Device):
        self.table_device.create_table_if_not_exist()
        if not device.uid:
            device.uid = None  # make sure it is really null
            logger.debug(f'Got nextval for uid in table device: {device.uid}')
        # this *will* commit the tx
        device_tuple = (*device.to_tuple(), time_util.now_sec())
        self.table_device.insert_one(device_tuple)
        device.uid = UID(self.table_device.get_last_insert_rowid())
