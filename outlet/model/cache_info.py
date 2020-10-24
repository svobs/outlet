from model.node_identifier import ensure_int, NodeIdentifier

# CLASS CacheInfoEntry
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼


class CacheInfoEntry:
    def __init__(self, cache_location, subtree_root: NodeIdentifier, sync_ts, is_complete):
        self.cache_location: str = cache_location
        self.subtree_root: NodeIdentifier = subtree_root
        self.sync_ts = ensure_int(sync_ts)
        self.is_complete = is_complete

    def to_tuple(self):
        return self.cache_location, self.subtree_root.tree_type, self.subtree_root.get_single_path(), self.subtree_root.uid, \
               self.sync_ts, self.is_complete

    def __repr__(self):
        return f'CacheInfoEntry(location="{self.cache_location}" subtree_root={self.subtree_root} is_complete={self.is_complete})'


# CLASS PersistedCacheInfo
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼


class PersistedCacheInfo(CacheInfoEntry):
    def __init__(self, base: CacheInfoEntry):
        super().__init__(cache_location=base.cache_location, subtree_root=base.subtree_root, sync_ts=base.sync_ts,
                         is_complete=base.is_complete)
        self.is_loaded = False
        """Indicates the data needs to be loaded from disk again"""

        self.needs_refresh = True
        """Indicates the data needs to be synced with disk"""

        self.needs_save = False
        """Indicates the data needs to be saved to disk again"""

    def __repr__(self):
        return f'PersistedCacheInfo(location="{self.cache_location}" subtree_root={self.subtree_root} ' \
               f'complete={self.is_complete} loaded={self.is_loaded} needs_refresh={self.needs_refresh})'

