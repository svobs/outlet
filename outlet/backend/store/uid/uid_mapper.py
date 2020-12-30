import threading
import logging
from typing import Dict, List, Optional, Tuple

from model.user_op import UserOpType
from backend.store.sqlite.uid_path_mapper_db import UidPathMapperDb
from util import file_util
from constants import CACHE_WRITE_HOLDOFF_TIME_MS, LOCAL_ROOT_UID, ROOT_PATH
from model.uid import UID
from util.holdoff_timer import HoldOffTimer
from util.stopwatch_sec import Stopwatch

logger = logging.getLogger(__name__)


class UidPathMapper:
    """
    ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
    CLASS UidPathMapper

    Maps a UID (int) to a file tree path (string)
    # TODO: need to account for possiblity of missing entries in cache (due to holdoff timer being used to batch writes)
    ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼
    """
    def __init__(self, backend, uid_path_cache_path):
        self.backend = backend
        self._uid_lock = threading.Lock()
        self.uid_generator = backend.uid_generator
        self.uid_path_cache_path = uid_path_cache_path
        # Every unique path must map to one unique UID
        self._full_path_uid_dict: Dict[str, UID] = {ROOT_PATH: LOCAL_ROOT_UID}
        self._to_cache: List[Tuple[UID, str]] = []
        self._cache_write_timer = HoldOffTimer(holdoff_time_ms=CACHE_WRITE_HOLDOFF_TIME_MS, task_func=self._append_to_cache)

    def start(self):
        self._load_cached_uids()

    def get_uid_for_path(self, full_path: str, uid_suggestion: Optional[UID] = None) -> UID:
        if not full_path and isinstance(full_path, str):
            raise RuntimeError(f'get_uid_for_path(): full_path is not str: {full_path}')

        needs_write = False
        with self._uid_lock:
            path = file_util.normalize_path(full_path)
            uid = self._full_path_uid_dict.get(path, None)
            if not uid:
                if uid_suggestion:
                    self._full_path_uid_dict[path] = uid_suggestion
                    uid = uid_suggestion
                else:
                    uid = self.uid_generator.next_uid()
                self._to_cache.append((uid, path))
                needs_write = True
            elif uid_suggestion and uid_suggestion != uid:
                logger.warning(f'UID was requested ({uid_suggestion}) but found existing UID ({uid}) for key: "{path}"')

        if needs_write:
            self._cache_write_timer.start_or_delay()
        return uid

    def _enqueue_to_cache(self, path, uid):
        self._full_path_uid_dict[path] = uid

    def _load_cached_uids(self):
        sw = Stopwatch()
        with UidPathMapperDb(self.uid_path_cache_path, self.backend) as db:
            # 0=uid, 1=full_path:
            mapping_list: List[Tuple[str, str]] = db.get_all_uid_path_mappings()

        for mapping in mapping_list:
            self._full_path_uid_dict[mapping[1]] = UID(mapping[0])

        logger.debug(f'{sw} Loaded {len(mapping_list)} UID-path mappings from disk cache')

    def _append_to_cache(self):
        with self._uid_lock:
            to_write = self._to_cache
            self._to_cache = []

        with UidPathMapperDb(self.uid_path_cache_path, self.backend) as db:
            db.upsert_uid_path_mapping_list(to_write)

        logger.debug(f'Wrote {len(to_write)} UID-path mappings to disk cache')


class UidGoogIdMapper:
    """
    ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
    CLASS UidGoogIdMapper

    Maps a UID (int) to a GoogId (hash string)
    ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼
    """
    def __init__(self, backend):
        self._uid_lock = threading.Lock()
        self.uid_generator = backend.uid_generator
        # Every unique GoogId must map to one unique UID
        self._goog_uid_dict: Dict[str, UID] = {}
        self._uid_goog_dict: Dict[UID, str] = {}

    def get_uid_for_goog_id(self, goog_id: str, uid_suggestion: Optional[UID] = None) -> UID:
        with self._uid_lock:
            uid = self._goog_uid_dict.get(goog_id, None)
            if not uid:
                if uid_suggestion:
                    uid = uid_suggestion
                else:
                    uid = self.uid_generator.next_uid()
                self._goog_uid_dict[goog_id] = uid
                self._uid_goog_dict[uid] = goog_id
            elif uid_suggestion and uid_suggestion != uid:
                logger.warning(f'UID was requested ({uid_suggestion}) but found existing UID ({uid}) for key: "{goog_id}"')
            return uid

    def get_goog_id_for_uid(self, uid: UID) -> str:
        with self._uid_lock:
            return self._uid_goog_dict.get(uid, None)


class UidChangeTreeMapper:
    """
    ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
    CLASS UidChangeTreeMapper

    Maps a UID (int) to a change tree string
    ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼
    """
    def __init__(self, backend):
        self._uid_lock = threading.Lock()
        self.uid_generator = backend.uid_generator
        self._nid_uid_dict: Dict[str, UID] = {}

    @staticmethod
    def _build_tree_nid(tree_type: int, single_path: str, op: UserOpType) -> str:
        if op:
            return f'{tree_type}:{op.name}:{single_path}'
        else:
            return f'{tree_type}'

    def get_uid_for(self, tree_type: int, single_path: Optional[str], op: Optional[UserOpType]) -> UID:
        if op:
            nid = self._build_tree_nid(tree_type, single_path, op)
        else:
            assert not single_path
            nid = str(tree_type)
        return self._get(nid)

    def _get(self, nid):
        with self._uid_lock:
            uid = self._nid_uid_dict.get(nid, None)
            if not uid:
                uid = self.uid_generator.next_uid()
                self._nid_uid_dict[nid] = uid
            return uid
