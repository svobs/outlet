import logging

from index.cache_info import CacheInfoEntry
from index.sqlite.base_db import MetaDatabase

logger = logging.getLogger(__name__)


def ensure_int(val):
    if type(val) == str:
        return int(val)
    return val


class CacheRegistry(MetaDatabase):
    TABLE_CACHE_REGISTRY = {
        'name': 'cache_registry',
        'cols': (('cache_location', 'TEXT'),
                 ('cache_type', 'INTEGER'),
                 ('subtree_root', 'TEXT'),
                 ('sync_ts', 'INTEGER'),
                 ('complete', 'INTEGER'))
    }

    def __init__(self, main_registry_path):
        super().__init__(main_registry_path)

    def has_cache_info(self):
        return self.has_rows(self.TABLE_CACHE_REGISTRY)

    def create_cache_registry_if_not_exist(self):
        self.create_table_if_not_exist(self.TABLE_CACHE_REGISTRY)

    def get_cache_info(self):
        # Gets all changes in the table
        rows = self.get_all_rows(self.TABLE_CACHE_REGISTRY)
        entries = []
        for row in rows:
            entries.append(CacheInfoEntry(*row))
        return entries

    # Takes a list of FMeta objects:
    def insert_cache_info(self, entries, append, overwrite):
        rows = []
        if type(entries) == list:
            for entry in entries:
                rows.append(entry.to_tuple())
        else:
            rows.append(entries.to_tuple())

        has_existing = self.has_cache_info()
        if has_existing:
            if overwrite:
                self.drop_table_if_exists(self.TABLE_CACHE_REGISTRY)
                self.create_table_if_not_exist(self.TABLE_CACHE_REGISTRY)
            elif not append:
                raise RuntimeError('Cannot insert CacheInfo into a non-empty table (overwrite=False, append=False)')

        self.insert_many(self.TABLE_CACHE_REGISTRY, rows)
