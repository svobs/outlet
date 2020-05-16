from abc import ABC, abstractmethod

import constants
import logging
from index.atomic_counter import AtomicCounter

logger = logging.getLogger(__name__)


class IdGenerator(ABC):
    def __init__(self):
        pass

    @abstractmethod
    def get_new_uid(self) -> int:
        pass

    @abstractmethod
    def set_next_uid(self, uid: int):
        pass


class NullIdGenerator(IdGenerator):
    def __init__(self):
        super().__init__()

    def get_new_uid(self) -> int:
        return constants.NULL_UID

    def set_next_uid(self, uid: int):
        pass


class AtomicIntIdGenerator(IdGenerator):
    def __init__(self):
        super().__init__()
        self._next_uid = AtomicCounter(constants.ROOT_UID + 1)

    def get_new_uid(self) -> int:
        return self._next_uid.increment()

    def set_next_uid(self, uid: int):
        new_val = self._next_uid.set_at_least(uid)
        logger.debug(f'Set next_uid to {new_val}')
