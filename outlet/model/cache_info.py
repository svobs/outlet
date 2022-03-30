import pathlib

from logging_constants import SUPER_DEBUG_ENABLED
from model.node_identifier import SinglePathNodeIdentifier
from util.ensure import ensure_int
import logging
logger = logging.getLogger(__name__)


class CacheInfoEntry:
    """
    ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
    CLASS CacheInfoEntry
    ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼
    """
    def __init__(self, cache_location, subtree_root: SinglePathNodeIdentifier, sync_ts, is_complete):
        self.cache_location: str = cache_location
        self.subtree_root: SinglePathNodeIdentifier = subtree_root
        self.sync_ts = ensure_int(sync_ts)
        self.is_complete = is_complete

    def to_tuple(self, cache_path_prefix: str):
        cache_location = self.convert_abs_path_to_relative(self.cache_location, cache_path_prefix)

        return cache_location, self.subtree_root.device_uid, self.subtree_root.get_single_path(), self.subtree_root.node_uid, \
            self.sync_ts, self.is_complete

    def __repr__(self):
        return f'CacheInfoEntry(location="{self.cache_location}" subtree_root={self.subtree_root} is_complete={self.is_complete})'

    @staticmethod
    def convert_abs_path_to_relative(cache_path: str, cache_path_prefix: str) -> str:
        if pathlib.PurePosixPath(cache_path).is_relative_to(cache_path_prefix):
            cache_path = cache_path.replace(cache_path_prefix, '.', 1)
            if SUPER_DEBUG_ENABLED:
                logger.debug(f'Converted cache path from abs to rel (base: "{cache_path_prefix}"); result = {cache_path}')
        return cache_path

    @staticmethod
    def convert_relative_path_to_abs(cache_path: str, cache_path_prefix: str) -> str:
        if cache_path.startswith('./'):
            cache_path = cache_path.replace('.', cache_path_prefix, 1)
            if SUPER_DEBUG_ENABLED:
                logger.debug(f'Converted cache path from rel to abs (base: "{cache_path_prefix}"); result = {cache_path}')
        return cache_path


class PersistedCacheInfo(CacheInfoEntry):
    """
    ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
    CLASS PersistedCacheInfo
    ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼
    """
    def __init__(self, base: CacheInfoEntry):
        super().__init__(cache_location=base.cache_location, subtree_root=base.subtree_root,
                         sync_ts=base.sync_ts, is_complete=base.is_complete)
        self.is_loaded = False
        """Indicates the data needs to be loaded from disk cache into memory cache"""

        self.needs_refresh = False
        """Indicates the data needs to be synced from source into disk/memory caches"""

        self.needs_save = False
        """Indicates the data needs to be saved to disk cache again"""

    def __repr__(self):
        return f'PersistedCacheInfo(location="{self.cache_location}" subtree_root={self.subtree_root} ' \
               f'complete={self.is_complete} loaded={self.is_loaded} needs_refresh={self.needs_refresh})'
