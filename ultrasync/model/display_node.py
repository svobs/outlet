from abc import ABC, abstractmethod


def ensure_int(self, val):
    if type(val) == str:
        return int(val)
    return val


class DisplayNode(ABC):
    def __init__(self):
        pass

    @classmethod
    @abstractmethod
    def is_leaf(cls):
        return False

    @abstractmethod
    def get_name(self):
        return None

    @abstractmethod
    def category(self):
        return None
