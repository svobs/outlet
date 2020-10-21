from store.gdrive.master_gdrive_memory import GDriveMemoryStore


class GDriveDiskCache:
    def __init__(self, app, memstore: GDriveMemoryStore):
        self.app = app
        self._memstore = memstore

