import threading
import logging
from typing import Dict, List, Optional, Tuple

from model.user_op import UserOpType
from backend.sqlite.uid_path_mapper_db import UidPathMapperDb
from util import file_util
from constants import CACHE_WRITE_HOLDOFF_TIME_MS, ROOT_PATH, ROOT_PATH_UID
from model.uid import UID
from util.has_lifecycle import HasLifecycle
from util.holdoff_timer import HoldOffTimer
from util.stopwatch_sec import Stopwatch

logger = logging.getLogger(__name__)


class UidPathMapper(HasLifecycle):
    """
    ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
    CLASS UidPathMapper

    Bidirectionally maps a UID (int) to a file tree path (string)
    Note: root path ("/") will have a UID of ROOT_PATH_UID (which equals LOCAL_PATH_UID, though it may not actually represent a local file path)

    # TODO: need to account for possiblity of missing entries in cache (due to holdoff timer being used to batch writes)
    ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼
    """
    def __init__(self, backend, uid_path_cache_path):
        super().__init__()
        self.backend = backend
        self.uid_generator = backend.uid_generator

        self._uid_lock = threading.Lock()

        # Every unique path must map to one unique UID
        self._path_uid_dict: Dict[str, UID] = {ROOT_PATH: ROOT_PATH_UID}
        self._uid_path_dict: Dict[UID, str] = {ROOT_PATH_UID: ROOT_PATH}

        self.uid_path_cache_path = uid_path_cache_path
        self._to_write: List[Tuple[UID, str]] = []
        self._write_timer = HoldOffTimer(holdoff_time_ms=CACHE_WRITE_HOLDOFF_TIME_MS, task_func=self._write_to_disk)

    def start(self):
        self._load_cached_uids()

    def shutdown(self):
        self._write_to_disk()

    def _add(self, path: str, uid: UID):
        self._path_uid_dict[path] = uid
        self._uid_path_dict[uid] = path
        self._to_write.append((uid, path))

    def get_uid_for_path(self, full_path: str, uid_suggestion: Optional[UID] = None) -> UID:
        if not full_path:
            raise RuntimeError(f'get_uid_for_path(): full_path is empty!')

        if not isinstance(full_path, str):
            raise RuntimeError(f'get_uid_for_path(): full_path is not str: {full_path}')

        needs_write = False
        with self._uid_lock:
            path = file_util.normalize_path(full_path)
            uid = self._path_uid_dict.get(path, None)
            if not uid:
                if uid_suggestion:
                    uid = uid_suggestion
                else:
                    uid = self.uid_generator.next_uid()
                self._add(path, uid_suggestion)
                needs_write = True
            elif uid_suggestion and uid_suggestion != uid:
                logger.warning(f'UID was requested ({uid_suggestion}) but found existing UID ({uid}) for key: "{path}"')

        if needs_write:
            self._write_timer.start_or_delay()
        return uid

    def get_path_for_uid(self, uid: UID) -> str:
        if not uid:
            raise RuntimeError(f'get_path_for_uid(): UID is empty or zero!')

        if not isinstance(uid, UID):
            raise RuntimeError(f'get_uid_for_path(): not a UID: {uid}')

        needs_write = False
        with self._uid_lock:
            path = self._uid_path_dict.get(uid, None)
            if not path:
                raise RuntimeError(f'No path mapping found for UID: {uid}')

        if needs_write:
            self._write_timer.start_or_delay()
        return path

    def _load_cached_uids(self):
        with self._uid_lock:
            sw = Stopwatch()
            with UidPathMapperDb(self.uid_path_cache_path, self.backend) as db:
                # 0=uid, 1=full_path:
                mapping_list: List[Tuple[str, str]] = db.get_all_uid_path_mappings()
                max_uid: UID = db.get_last_uid()

            for mapping in mapping_list:
                self._path_uid_dict[mapping[1]] = UID(mapping[0])
                self._uid_path_dict[UID(mapping[0])] = mapping[1]

            self.uid_generator.ensure_next_uid_greater_than(max_uid)

            logger.debug(f'{sw} Loaded {len(mapping_list)} UID-path mappings from disk cache')

    def _write_to_disk(self):
        with self._uid_lock:
            to_write = self._to_write
            self._to_write = []

            if self._to_write:
                with UidPathMapperDb(self.uid_path_cache_path, self.backend) as db:
                    db.upsert_uid_path_mapping_list(to_write)

                logger.debug(f'Wrote {len(to_write)} UID-path mappings to disk cache')


class UidGoogIdMapper:
    """
    ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
    CLASS UidGoogIdMapper

    Bidirectionally maps a UID (int) to a GoogId (hash string)
    ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼
    """
    def __init__(self, backend):
        self.uid_generator = backend.uid_generator

        self._uid_lock = threading.Lock()
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
    def _build_tree_nid(device_uid: UID, single_path: str, op: UserOpType) -> str:
        if op:
            return f'{device_uid}:{op.name}:{single_path}'
        else:
            return f'{device_uid}'

    def get_uid_for(self, device_uid: UID, single_path: Optional[str], op: Optional[UserOpType]) -> UID:
        if op:
            nid = self._build_tree_nid(device_uid, single_path, op)
        else:
            assert not single_path
            nid = str(device_uid)
        return self._get(nid)

    def _get(self, nid):
        with self._uid_lock:
            uid = self._nid_uid_dict.get(nid, None)
            if not uid:
                uid = self.uid_generator.next_uid()
                self._nid_uid_dict[nid] = uid
            return uid
