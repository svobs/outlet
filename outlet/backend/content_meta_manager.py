import threading
from typing import Dict, List, Optional
import logging

from backend.sqlite.content_meta_db import ContentMeta, ContentMetaDatabase
from constants import NULL_UID
from logging_constants import SUPER_DEBUG_ENABLED, TRACE_ENABLED
from model.uid import UID
from util.has_lifecycle import HasLifecycle, start_func, stop_func

logger = logging.getLogger(__name__)


class ContentMetaManager(HasLifecycle):
    """
    ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
    CLASS ContentMetaManager
    ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼
    """

    def __init__(self, backend, cache_path: str):
        HasLifecycle.__init__(self)
        self.backend = backend
        self.content_meta_db: ContentMetaDatabase = ContentMetaDatabase(backend, cache_path)

        self._struct_lock = threading.Lock()
        self.meta_dict: Dict[UID, ContentMeta] = {}

    @start_func
    def start(self):
        meta_list: List[ContentMeta] = self.content_meta_db.get_all()
        if meta_list:
            meta_dict = {}
            for meta in meta_list:
                meta_dict[meta.uid] = meta
            self.meta_dict = meta_dict
            logger.debug(f'Size of ContentMeta dict: {len(self.meta_dict)}')
        else:
            logger.debug(f'No ContentMeta in diskstore; assuming we are starting fresh')
            self.content_meta_db.create_table_if_not_exist()

    @stop_func
    def shutdown(self):
        try:
            if self.content_meta_db:
                self.content_meta_db.close()
                self.content_meta_db = None
        except (AttributeError, NameError):
            pass

        try:
            self.meta_dict = None
            self.backend = None
        except (AttributeError, NameError):
            pass

    def get_content_meta_for_uid(self, content_uid: UID) -> Optional[ContentMeta]:
        """If content_uid is 0 or None, returns None (it was just cleaner to put this logic here rather than duplicate it many places).
        Raises exception if content_uid is non-zero but no ContentMeta found with that UID."""
        if not content_uid:
            if TRACE_ENABLED:
                logger.debug(f'get_content_meta_for_uid(): Returning None because content_uid={content_uid}')
            return None

        with self._struct_lock:
            meta = self.meta_dict.get(content_uid)
            if not meta:
                raise RuntimeError(f'get_content_meta_for_uid(): no ContentMeta found in memstore for UID: {content_uid}')

            if TRACE_ENABLED:
                logger.debug(f'get_content_meta_for_uid(): Returning {meta} for content_uid {content_uid}')
            return meta

    def get_or_create_content_meta_for(self, size_bytes: int, md5: Optional[str] = None, sha256: Optional[str] = None) -> ContentMeta:
        with self._struct_lock:
            if md5:
                for meta in self.meta_dict.values():
                    if meta.md5 == md5:
                        return meta

                return self._insert_new_content_meta(size_bytes, md5, sha256)
            elif sha256:
                for meta in self.meta_dict.values():
                    if meta.sha256 == sha256:
                        return meta

                return self._insert_new_content_meta(size_bytes, md5, sha256)
            else:
                # faux-ContentMeta
                return ContentMeta(uid=NULL_UID, md5=None, sha256=None, size_bytes=size_bytes)

    def _insert_new_content_meta(self, size_bytes: int, md5: Optional[str] = None, sha256: Optional[str] = None) -> ContentMeta:
        uid = self.backend.uid_generator.next_uid()
        meta = ContentMeta(uid=uid, md5=md5, sha256=sha256, size_bytes=size_bytes)
        self.content_meta_db.insert_content_meta(meta)
        self.meta_dict[uid] = meta
        return meta
