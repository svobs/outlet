import threading
import logging
from abc import ABC
from typing import Dict, Generic, List, Optional, Tuple, TypeVar

from backend.sqlite.base_db import Table
from backend.sqlite.uid_mapper_db import UidMapperDb
from util import file_util
from constants import CACHE_WRITE_HOLDOFF_TIME_MS, GoogID, ROOT_PATH, ROOT_PATH_UID
from logging_constants import SUPER_DEBUG_ENABLED
from model.uid import UID
from util.ensure import ensure_uid
from util.has_lifecycle import HasLifecycle
from util.holdoff_timer import HoldOffTimer
from util.stopwatch_sec import Stopwatch

logger = logging.getLogger(__name__)

MappingT = TypeVar('MappingT')


class UidPersistedMapper(HasLifecycle, Generic[MappingT], ABC):
    """Abstract base class.

       TODO: need to account for possiblity of missing entries in cache (due to holdoff timer being used to batch writes)
    """
    def __init__(self, backend, cache_path: str, table: Table):
        super().__init__()
        self.backend = backend
        self.table = table
        self.uid_generator = backend.uid_generator

        self._uid_lock = threading.Lock()

        # Every unique path must map to one unique UID
        self._uid_forward_dict: Dict[MappingT, UID] = {}
        self._uid_reverse_dict: Dict[UID, MappingT] = {}

        self.cache_path = cache_path
        self._to_write: List[Tuple[UID, MappingT]] = []
        self._write_timer = HoldOffTimer(holdoff_time_ms=CACHE_WRITE_HOLDOFF_TIME_MS, task_func=self._write_to_disk)

    def start(self):
        logger.debug(f'[{self.__class__.__name__}] Startup started')
        self._load_cached_uids()
        logger.debug(f'[{self.__class__.__name__}] Startup done')

    def shutdown(self):
        logger.debug(f'[{self.__class__.__name__}] Shutdown started')
        self._write_to_disk()
        logger.debug(f'[{self.__class__.__name__}] Shutdown done')

    def _add(self, value: MappingT, uid: UID):
        assert value and uid, f'Missing param: value={value}, uid={uid}'
        assert isinstance(uid, UID), f'Not a UID: {uid}'
        self._uid_forward_dict[value] = uid
        self._uid_reverse_dict[uid] = value
        self._to_write.append((uid, value))

    def get_uid_for_mapping(self, val: MappingT, uid_suggestion: Optional[UID] = None) -> UID:
        needs_write = False
        with self._uid_lock:
            uid = self._uid_forward_dict.get(val, None)
            if not uid:
                if uid_suggestion:
                    uid = ensure_uid(uid_suggestion)
                else:
                    uid = self.uid_generator.next_uid()
                if SUPER_DEBUG_ENABLED:
                    logger.debug(f'New UID generated: {uid} = "{val}"')
                self._add(val, uid)
                needs_write = True
            elif uid_suggestion and uid_suggestion != uid:
                logger.warning(f'UID was requested ({uid_suggestion}) but found existing UID ({uid}) for key: "{val}"')

        if needs_write:
            self._write_timer.start_or_delay()
        return uid

    def get_mapping_for_uid(self, uid: UID) -> MappingT:
        if not uid:
            raise RuntimeError(f'get_mapping_for_uid(): UID is empty or zero!')

        uid = ensure_uid(uid)
        if not isinstance(uid, UID):
            raise RuntimeError(f'get_mapping_for_uid(): not a UID: {uid}')

        needs_write = False
        with self._uid_lock:
            val = self._uid_reverse_dict.get(uid, None)
            if not val:
                raise RuntimeError(f'No val found for UID: {uid}')

        if needs_write:
            self._write_timer.start_or_delay()
        return val

    def _load_cached_uids(self):
        logger.debug(f'[{self.__class__.__name__}] Loading UID mappings from disk cache')

        with self._uid_lock:
            sw = Stopwatch()
            with UidMapperDb(self.cache_path, self.backend, self.table) as db:
                # 0=uid, 1=full_path:
                mapping_list: List[Tuple[str, str]] = db.get_all_uid_mappings()
                max_uid: UID = db.get_last_uid()

            for mapping in mapping_list:
                self._uid_forward_dict[mapping[1]] = UID(mapping[0])
                self._uid_reverse_dict[UID(mapping[0])] = mapping[1]

            self.uid_generator.ensure_next_uid_greater_than(max_uid)

            logger.debug(f'[{self.__class__.__name__}] {sw} Loaded {len(mapping_list)} UID mappings from disk cache')

    def _write_to_disk(self):
        with self._uid_lock:
            logger.debug(f'[{self.__class__.__name__}] Writing {len(self._to_write)} UID mappings to disk cache: {self.cache_path}')
            to_write = self._to_write
            self._to_write = []

            if to_write:
                with UidMapperDb(self.cache_path, self.backend, self.table) as db:
                    db.upsert_uid_str_mapping_list(to_write)

                logger.info(f'[{self.__class__.__name__}] Wrote {len(to_write)} UID-str mappings to disk cache ({self.cache_path})')


class UidPathMapper(UidPersistedMapper[str]):
    """
    ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
    CLASS UidPathMapper

    Bidirectionally maps a UID (int) to a file tree path (string)
    Note: root path ("/") will have a UID of ROOT_PATH_UID (which equals LOCAL_PATH_UID, though it may not actually represent a local file path)

    ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼
    """
    def __init__(self, backend, uid_path_cache_path: str):
        super().__init__(backend, uid_path_cache_path, UidMapperDb.TABLE_UID_PATH)

        # Every unique path must map to one unique UID
        self._uid_forward_dict[ROOT_PATH] = ROOT_PATH_UID
        self._uid_reverse_dict[ROOT_PATH_UID] = ROOT_PATH

    def get_uid_for_path(self, full_path: str, uid_suggestion: Optional[UID] = None) -> UID:
        if not full_path:
            raise RuntimeError(f'get_uid_for_path(): full_path is empty!')

        if not isinstance(full_path, str):
            raise RuntimeError(f'get_uid_for_path(): full_path is not str: {full_path}')

        if not full_path.startswith('/'):
            raise RuntimeError(f'get_uid_for_path(): not a valid path: "{full_path}"')

        path = file_util.normalize_path(full_path)
        return self.get_uid_for_mapping(path, uid_suggestion)

    def get_path_for_uid(self, uid: UID) -> str:
        return self.get_mapping_for_uid(uid)


class UidGoogIdMapper(UidPersistedMapper[GoogID]):
    """
    ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
    CLASS UidGoogIdMapper

    Bidirectionally maps a UID (int) to a GoogId (hash string)
    ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼
    """
    def __init__(self, backend, uid_path_cache_path: str):
        super().__init__(backend, uid_path_cache_path, UidMapperDb.TABLE_UID_GOOG_ID)

    def get_uid_for_goog_id(self, goog_id: GoogID, uid_suggestion: Optional[UID] = None) -> UID:
        if not goog_id:
            raise RuntimeError(f'get_uid_for_goog_id(): goog_id is empty!')

        if not isinstance(goog_id, str):
            raise RuntimeError(f'get_uid_for_goog_id(): goog_id is not str: {goog_id}')

        return self.get_uid_for_mapping(goog_id, uid_suggestion)

    def get_goog_id_for_uid(self, uid: UID) -> GoogID:
        return self.get_mapping_for_uid(uid)
