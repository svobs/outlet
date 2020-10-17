import threading
from abc import ABC, abstractmethod

import logging

from constants import NULL_UID, GDRIVE_ROOT_UID
from model.uid import UID

logger = logging.getLogger(__name__)

CONFIG_KEY_ENABLE_LAST_UID = 'cache.enable_uid_lastval_persistence'
"""If true, read and write the last allocated UID value to 'transient.global.last_uid' so that duplicate UIDs aren't assigned across startups"""

CONFIG_KEY_UID_RESERVATION_BLOCK_SIZE = 'cache.uid_reservation_block_size'
"""The number of sequential UIDs to reserve each time we persist to disk. Setting to a higher number will mean less disk access, but
the UID numbers will get larger faster if there are a lot of program restarts, which is somewhere between annoying and inconvenient
when debugging"""

CONFIG_KEY_LAST_UID = 'transient.global.last_uid'


# ABSTRACT CLASS UidGenerator
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼

class UidGenerator(ABC):
    def __init__(self):
        pass

    @abstractmethod
    def next_uid(self) -> UID:
        pass

    @abstractmethod
    def ensure_next_uid_greater_than(self, uid: UID):
        pass


# CLASS NullUidGenerator
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼

class NullUidGenerator(UidGenerator):
    def __init__(self):
        super().__init__()

    def next_uid(self) -> UID:
        return NULL_UID

    def ensure_next_uid_greater_than(self, uid: UID):
        pass


# CLASS PersistentAtomicIntUidGenerator
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼

class PersistentAtomicIntUidGenerator(UidGenerator):
    def __init__(self, config):
        super().__init__()
        self._config = config
        self._enable_uid_persistence: bool = self._config.get(CONFIG_KEY_ENABLE_LAST_UID)
        if self._enable_uid_persistence:
            self._last_uid_written = self._config.get(CONFIG_KEY_LAST_UID, GDRIVE_ROOT_UID + 1)
            self._uid_reservation_block_size = self._config.get(CONFIG_KEY_UID_RESERVATION_BLOCK_SIZE)
        else:
            self._last_uid_written = GDRIVE_ROOT_UID + 1
        self._value = self._last_uid_written + 1
        self._lock = threading.Lock()

    def _set(self, new_value):
        self._value = new_value
        if self._enable_uid_persistence and self._value > self._last_uid_written:
            # skip ahead and write a larger number. This will cause us to burn through numbers quicker, but will really speed things up
            self._last_uid_written = self._value + self._uid_reservation_block_size
            self._config.write(CONFIG_KEY_LAST_UID, self._last_uid_written)
        return self._value

    def next_uid(self) -> UID:
        with self._lock:
            return UID(self._set(self._value + 1))

    def ensure_next_uid_greater_than(self, uid: int):
        with self._lock:
            if uid > self._value:
                new_val = self._set(uid)
                logger.debug(f'Set next_uid to {new_val}')
            else:
                logger.debug(f'Ignoring request to set next_uid ({uid}); it is smaller than the present value ({self._value})')
