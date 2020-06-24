import threading
import logging
from typing import Dict, Optional

import file_util
from constants import ROOT_PATH
from index.uid_generator import ROOT_UID, UID

logger = logging.getLogger(__name__)


# CLASS UidPathMapper
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼

class UidPathMapper:
    """
    Maps a UID (int) to a file tree path (string)
    """
    def __init__(self, application):
        self._uid_lock = threading.Lock()
        self.uid_generator = application.uid_generator
        # Every unique path must map to one unique UID
        self._full_path_uid_dict: Dict[str, UID] = {ROOT_PATH: ROOT_UID}

    def get_uid_for_path(self, path: str, uid_suggestion: Optional[UID] = None) -> UID:
        with self._uid_lock:
            path = file_util.normalize_path(path)
            uid = self._full_path_uid_dict.get(path, None)
            if not uid:
                if uid_suggestion:
                    self._full_path_uid_dict[path] = uid_suggestion
                    uid = uid_suggestion
                else:
                    uid = self.uid_generator.next_uid()
                    self._full_path_uid_dict[path] = uid
            elif uid_suggestion and uid_suggestion != uid:
                logger.warning(f'UID was requested ({uid_suggestion}) but an existing UID was found ({uid}) for key: "{path}"')
            return uid


# CLASS UidGoogIdMapper
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼

class UidGoogIdMapper:
    """
    Maps a UID (int) to a GoogId (hash string)
    """
    def __init__(self, application):
        self._uid_lock = threading.Lock()
        self.uid_generator = application.uid_generator
        # Every unique GoogId must map to one unique UID
        self._goog_uid_dict: Dict[str, UID] = {}

    def get_uid_for_goog_id(self, goog_id: str, uid_suggestion: Optional[UID] = None) -> UID:
        with self._uid_lock:
            uid = self._goog_uid_dict.get(goog_id, None)
            if not uid:
                if uid_suggestion:
                    self._goog_uid_dict[goog_id] = uid_suggestion
                    uid = uid_suggestion
                else:
                    uid = self.uid_generator.next_uid()
                    self._goog_uid_dict[goog_id] = uid
            elif uid_suggestion and uid_suggestion != uid:
                logger.warning(f'UID was requested ({uid_suggestion}) but an existing UID was found ({uid}) for key: "{goog_id}"')
            return uid

