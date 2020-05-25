import threading
from abc import ABC, abstractmethod

import logging
from index.atomic_counter import AtomicCounter

logger = logging.getLogger(__name__)
CONFIG_KEY = 'transient.global.last_uid'
WRITE_OUT_UID_EVERY_N = 1000


class UID(int):
    def __new__(cls, val, *args, **kwargs):
        return super(UID, cls).__new__(cls, val)


class AUID(int):
    def __new__(cls, val, *args, **kwargs):
        return super(AUID, cls).__new__(cls, val)


ROOT_UID = UID(1)
NULL_UID = UID(0)


class UidGenerator(ABC):
    def __init__(self):
        pass

    @abstractmethod
    def get_new_uid(self) -> UID:
        pass

    @abstractmethod
    def set_next_uid(self, uid: UID):
        pass


class NullUidGenerator(UidGenerator):
    def __init__(self):
        super().__init__()

    def get_new_uid(self) -> UID:
        return NULL_UID

    def set_next_uid(self, uid: UID):
        pass


class PersistentAtomicIntUidGenerator(UidGenerator):
    def __init__(self, config):
        super().__init__()
        self._config = config
        self._last_uid_written = self._config.get(CONFIG_KEY, ROOT_UID + 1)
        self._value = self._last_uid_written + 1
        self._lock = threading.Lock()

    def _set(self, new_value):
        self._value = new_value
        if self._value > self._last_uid_written:
            # skip ahead and write a larger number. This will cause us to burn through numbers quicker, but will really speed things up
            self._last_uid_written = self._value + WRITE_OUT_UID_EVERY_N
            self._config.write(CONFIG_KEY, self._last_uid_written)
        return self._value

    def get_new_uid(self) -> UID:
        with self._lock:
            return UID(self._set(self._value + 1))

    def set_next_uid(self, uid: int):
        with self._lock:
            new_val = self._set(uid)
        logger.debug(f'Set next_uid to {new_val}')
