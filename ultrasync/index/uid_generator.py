from abc import ABC, abstractmethod

import logging
from index.atomic_counter import AtomicCounter

logger = logging.getLogger(__name__)
CONFIG_KEY = 'transient.global.last_uid'


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


class AtomicIntUidGenerator(UidGenerator):
    def __init__(self):
        super().__init__()
        self._counter = AtomicCounter(ROOT_UID + 1)

    def get_new_uid(self) -> UID:
        return UID(self._counter.increment())

    def set_next_uid(self, uid: int):
        new_val = self._counter.set_at_least(uid)
        logger.debug(f'Set next_uid to {new_val}')


class PersistentAtomicIntUidGenerator(AtomicIntUidGenerator):
    def __init__(self, config):
        super().__init__()
        self._config = config
        self.set_next_uid(self._config.get(CONFIG_KEY, ROOT_UID + 1))

    def get_new_uid(self) -> UID:
        new_uid = super().get_new_uid()
        #TODO self._config.write(CONFIG_KEY, new_uid)
        return new_uid

    def set_next_uid(self, uid: int):
        new_val = self._counter.set_at_least(uid)
        logger.debug(f'Set next_uid to {new_val}')
