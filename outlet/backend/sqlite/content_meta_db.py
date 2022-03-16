import logging
from collections import OrderedDict
from typing import List, Optional, Tuple

from backend.sqlite.base_db import LiveTable, MetaDatabase, Table
from model.uid import UID
from util import time_util

logger = logging.getLogger(__name__)


class ContentMeta:
    """
    Represents a unique piece of content, as identified by its "signature", which can be an MD5, an SHA-256, or both.

    Any node which has content (e.g. a file node) must reference one of these, which corresponds to a row in the content_meta table.
    A node whose signature has not been calculated should reference a ContentMeta which has null values for MD5 and SHA-256 and for
    which the node is its only referencing object. When its signature is calculated and is found to correspond to some existing
    ContentMeta, the previous ContentMeta is then defunct and should be deleted.
    """
    def __init__(self, uid: UID, md5: Optional[str], sha256: Optional[str], size_bytes: int):
        self.uid: UID = uid
        self.md5: Optional[str] = md5
        self.sha256: Optional[str] = sha256
        self.size_bytes: int = size_bytes
        # Don't bother with sync_ts

    def __eq__(self, other):
        return isinstance(other, ContentMeta) and self.size_bytes == other.size_bytes and self.md5 == other.md5 and self.sha256 == other.sha256

    def has_signature(self) -> bool:
        return self.md5 is not None and self.sha256 is not None

    def to_tuple(self) -> Tuple:
        return self.uid, self.md5, self.sha256, self.size_bytes, time_util.now_sec()  # presumably we are calling this to do an insert


class ContentMetaDatabase(MetaDatabase):
    """
    ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
    CLASS ContentMetaDatabase
    ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼
    """
    TABLE_CONTENT = Table(name='content', cols=OrderedDict([
        ('uid', 'INTEGER PRIMARY KEY'),
        ('md5', 'TEXT'),
        ('sha256', 'TEXT'),
        ('size_bytes', 'INTEGER'),
        ('sync_ts', 'INTEGER')  # this is actually the insert_ts, since we never update
    ]))

    def __init__(self, backend, db_path: str):
        super().__init__(db_path)
        self.table_content = LiveTable(ContentMetaDatabase.TABLE_CONTENT, self.conn, obj_to_tuple_func=self._content_to_tuple,
                                       tuple_to_obj_func=self._tuple_to_content)

    @staticmethod
    def _content_to_tuple(o: ContentMeta) -> Tuple:
        assert isinstance(o, ContentMeta), f'Expected ContentMeta; got instead: {o}'
        return o.to_tuple()

    @staticmethod
    def _tuple_to_content(row: Tuple) -> ContentMeta:
        uid_int, md5, sha256, size_bytes, sync_ts = row
        if md5 == '':
            md5 = None
        if sha256 == '':
            sha256 = None
        return ContentMeta(uid=UID(uid_int), md5=md5, sha256=sha256, size_bytes=size_bytes)

    # CONTENT operations
    # ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼

    def get_all(self) -> List[ContentMeta]:
        return self.table_content.select_object_list()

    def insert_content_meta(self, content_meta: ContentMeta, commit=True):
        self.table_content.insert_object(content_meta, commit=commit)

    def delete_content_meta_with_uid(self, uid: UID, commit=True):
        self.table_content.delete_for_uid(uid, commit=commit)

    def delete_content_meta_for_uid_list(self, uid_list: List[UID], commit=True):
        uid_tuple_list = list(map(lambda uid: (uid,), uid_list))
        self.table_content.delete_for_uid_list(uid_tuple_list, commit=commit)

    def get_content_meta_for_md5(self, md5: str) -> Optional[ContentMeta]:
        content_list = self.table_content.select_object_list(where_clause='WHERE md5 = ?', where_tuple=(md5,))
        if content_list:
            assert len(content_list) == 1, f'Expected exactly 1 but found {len(content_list)} entries for MD5: {md5}'
            return content_list[0]

        return None

    def get_content_meta_for_sha256(self, sha256: str) -> Optional[ContentMeta]:
        content_list = self.table_content.select_object_list(where_clause='WHERE sha256 = ?', where_tuple=(sha256,))
        if content_list:
            assert len(content_list) == 1, f'Expected exactly 1 but found {len(content_list)} entries for sha256: {sha256}'
            return content_list[0]

        return None

    def get_meta_for_uid(self, content_uid: UID) -> ContentMeta:
        """Raises RuntimeEerror if no meta is found with given UID"""
        return self.table_content.select_object_for_uid(uid=content_uid)

    def get_meta_for(self, size_bytes: int, md5: Optional[str] = None, sha256: Optional[str] = None) -> Optional[ContentMeta]:
        """If either md5 or sha256 are supplied, this method returns either None or a single object. If only size_bytes is supplied,
        this method will likewise return either None or a single ContentMeta, based on whether an actual entry exists with the given
        size_bytes & null md5 & null sha256."""
        if md5:
            meta = self.get_content_meta_for_md5(md5)
        elif sha256:
            meta = self.get_content_meta_for_sha256(sha256)
        else:
            assert size_bytes > 0, f'get_meta_for(): invalid request: md5 is null, sha256 is null, and size_bytes is not positive!'
            where_clause = 'WHERE md5 IS NULL AND sha256 IS NULL AND size_bytes = ?'
            meta_list = self.table_content.select_object_list(where_clause=where_clause, where_tuple=(size_bytes,))
            if meta_list:
                assert len(meta_list) == 1, f'Expected exactly 1 but found {len(meta_list)} entries for null md5 & sha256, and size_bytes={size_bytes}'
                meta = meta_list[0]
            else:
                meta = None

        if meta:
            assert meta.size_bytes == size_bytes and meta.md5 == md5 and meta.sha256 == sha256, \
                f'Inconsistency detected: query = [size_bytes={size_bytes} md5={md5} sha256={sha256}]; ' \
                f'result = [size_bytes={meta.size_bytes} md5={meta.md5} sha256={meta.sha256}, uid={meta.uid}]'
        return meta
