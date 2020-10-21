from store.gdrive.master_gdrive_memory import GDriveMemoryCache


class GDriveDiskCache:
    def __init__(self, app, memcache: GDriveMemoryCache):
        self.app = app
        self._memcache = memcache

