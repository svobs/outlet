from typing import Optional
import logging

from backend.sqlite.content_meta_db import ContentMeta, ContentMetaDatabase
from model.uid import UID
from util.has_lifecycle import HasLifecycle

logger = logging.getLogger(__name__)


class ContentMetaManager(HasLifecycle):

    def __init__(self, backend, cache_path: str):
        HasLifecycle.__init__(self)
        self.backend = backend
        self.content_meta_db: ContentMetaDatabase = ContentMetaDatabase(cache_path, backend)

    def start(self):
        logger.debug(f'[ContentMetaManager] Startup started')
        HasLifecycle.start(self)
        logger.debug(f'[ContentMetaManager] Startup done')

    def shutdown(self):
        logger.debug(f'[ContentMetaManager] Shutdown started')
        HasLifecycle.shutdown(self)
        logger.debug(f'[ContentMetaManager] Shutdown done')

    def get_content_meta_for_uid(self, content_uid: UID) -> ContentMeta:
        # TODO
        pass

    def get_content_meta_for(self, size_bytes: int, md5: Optional[str] = None, sha256: Optional[str] = None) -> ContentMeta:
        # TODO
        pass
