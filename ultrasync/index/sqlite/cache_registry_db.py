import logging
from typing import List, Union

import file_util
from index.cache_info import CacheInfoEntry
from index.sqlite.base_db import MetaDatabase, Table

logger = logging.getLogger(__name__)


def ensure_int(val):
    try:
        if type(val) == str:
            return int(val)
    except ValueError:
        # just kidding! (need to support UIDs for local files which use strings instead)
        pass
    return val


class CacheRegistry(MetaDatabase):
    TABLE_CACHE_REGISTRY = Table(name='cache_registry', cols={'cache_location': 'TEXT',
                                                              'cache_type': 'INTEGER',
                                                              'subtree_root_path': 'TEXT',
                                                              'subtree_root_uid': 'INTEGER',
                                                              'sync_ts': 'INTEGER',
                                                              'complete': 'INTEGER'})

    def __init__(self, main_registry_path: str, node_identifier_factory):
        super().__init__(main_registry_path)
        self.node_identifier_factory = node_identifier_factory

    def has_cache_info(self):
        return self.has_rows(self.TABLE_CACHE_REGISTRY)

    def create_cache_registry_if_not_exist(self):
        self.create_table_if_not_exist(self.TABLE_CACHE_REGISTRY)

    def get_cache_info(self) -> List[CacheInfoEntry]:
        # Gets all changes in the table
        rows = self.get_all_rows(self.TABLE_CACHE_REGISTRY)
        entries = []
        for row in rows:
            cache_location, cache_type, subtree_root_path, subtree_root_uid, sync_ts, is_complete = row
            subtree_root_path = file_util.normalize_path(subtree_root_path)
            node_identifier = self.node_identifier_factory.for_values(tree_type=cache_type, full_path=subtree_root_path,
                                                                      uid=ensure_int(subtree_root_uid))
            entries.append(CacheInfoEntry(cache_location=cache_location, subtree_root=node_identifier, sync_ts=sync_ts, is_complete=is_complete))
        return entries

    # Takes a list of LocalFileNode objects:
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
                self.drop_table_if_exists(self.TABLE_CACHE_REGISTRY, commit=False)
            elif not append:
                raise RuntimeError('Cannot insert CacheInfo into a non-empty table (overwrite=False, append=False)')

        self.create_table_if_not_exist(self.TABLE_CACHE_REGISTRY, commit=False)
        self.insert_many(self.TABLE_CACHE_REGISTRY, rows)
