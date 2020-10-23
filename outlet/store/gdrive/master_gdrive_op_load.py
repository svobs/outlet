from abc import ABC, abstractmethod
from typing import List, Optional
import logging

from model.gdrive_meta import GDriveUser, MimeType
from store.gdrive.master_gdrive_memory import GDriveMemoryStore
from store.sqlite.gdrive_db import GDriveDatabase

logger = logging.getLogger(__name__)


# ABSTRACT CLASS GDriveDiskLoadOp
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
class GDriveDiskLoadOp(ABC):
    @abstractmethod
    def load_from_diskstore(self, cache: GDriveDatabase):
        pass

    @abstractmethod
    def update_memstore(self, memstore: GDriveMemoryStore):
        pass


# CLASS GDriveDiskLoadOp
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
class GDriveLoadAllMetaOp(GDriveDiskLoadOp):
    def __init__(self):
        self.users: Optional[List[GDriveUser]] = None
        self.mime_types: Optional[List[MimeType]] = None

    def load_from_diskstore(self, cache: GDriveDatabase):
        logger.debug(f'Loading GDrive users and MIME types from diskstore...')
        self.users = cache.get_all_users()
        self.mime_types = cache.get_all_mime_types()

    def update_memstore(self, memstore: GDriveMemoryStore):
        logger.debug(f'Replacing GDrive memstore meta with {len(self.users)} users and {len(self.mime_types)} MIME types...')
        memstore.replace_all_users(self.users)
        memstore.replace_all_mime_types(self.mime_types)

