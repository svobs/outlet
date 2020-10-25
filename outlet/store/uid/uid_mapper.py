import threading
import logging
from typing import Dict, List, Optional

from util import file_util
from constants import LOCAL_ROOT_UID, ROOT_PATH
from model.uid import UID

logger = logging.getLogger(__name__)


# CLASS UidPathMapper
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼

class UidPathMapper:
    """
    Maps a UID (int) to a file tree path (string)
    """
    def __init__(self, app):
        self._uid_lock = threading.Lock()
        self.uid_generator = app.uid_generator
        # Every unique path must map to one unique UID
        self._full_path_uid_dict: Dict[str, UID] = {ROOT_PATH: LOCAL_ROOT_UID}

    def get_uid_for_path_list(self, path_list: List[str], uid_suggestion: Optional[UID] = None):
        if len(path_list) != 1:
            # sanity check
            raise RuntimeError(f'get_uid_for_path_list(): too many paths supplied: {path_list}')

        return self.get_uid_for_path(path_list[0], uid_suggestion)

    def get_uid_for_path(self, full_path: str, uid_suggestion: Optional[UID] = None) -> UID:
        if not full_path and isinstance(full_path, str):
            raise RuntimeError(f'get_uid_for_path(): full_path is not str: {full_path}')

        with self._uid_lock:
            path = file_util.normalize_path(full_path)
            uid = self._full_path_uid_dict.get(path, None)
            if not uid:
                if uid_suggestion:
                    self._full_path_uid_dict[path] = uid_suggestion
                    uid = uid_suggestion
                else:
                    uid = self.uid_generator.next_uid()
                    self._full_path_uid_dict[path] = uid
            elif uid_suggestion and uid_suggestion != uid:
                logger.warning(f'UID was requested ({uid_suggestion}) but found existing UID ({uid}) for key: "{path}"')
            return uid


# CLASS UidGoogIdMapper
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼

class UidGoogIdMapper:
    """
    Maps a UID (int) to a GoogId (hash string)
    """
    def __init__(self, app):
        self._uid_lock = threading.Lock()
        self.uid_generator = app.uid_generator
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