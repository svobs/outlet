from abc import ABC, abstractmethod

import logging
from index.atomic_counter import AtomicCounter

logger = logging.getLogger(__name__)


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


class AtomicIntIdGenerator(UidGenerator):
    def __init__(self):
        super().__init__()
        self._counter = AtomicCounter(ROOT_UID + 1)

    def get_new_uid(self) -> UID:
        return UID(self._counter.increment())

    def set_next_uid(self, uid: int):
        new_val = self._counter.set_at_least(uid)
        logger.debug(f'Set next_uid to {new_val}')


class ApplicationUidGenerator(UidGenerator):
    def __init__(self):
        super().__init__()
        self._counter = AtomicCounter(ROOT_UID + 1)

    def get_new_uid(self) -> AUID:
        return AUID(self._counter.increment())

    def set_next_uid(self, uid: int):
        new_val = self._counter.set_at_least(uid)
        logger.debug(f'Set next_uid to {new_val}')
