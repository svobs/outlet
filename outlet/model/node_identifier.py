import logging
from abc import ABC, abstractmethod
from typing import List, Optional, Union

from constants import NULL_UID, TREE_TYPE_DISPLAY, TREE_TYPE_GDRIVE, TREE_TYPE_LOCAL_DISK, TREE_TYPE_NA
from model.uid import UID
from util import file_util

logger = logging.getLogger(__name__)


def ensure_int(val):
    try:
        if type(val) == str:
            return int(val)
    except ValueError:
        logger.error(f'Bad value: {val}')
    return val


def ensure_bool(val):
    try:
        return bool(val)
    except ValueError:
        pass
    return val


def ensure_list(full_path):
    if full_path:
        if type(full_path) == list:
            return full_path
        else:
            return [full_path]
    else:
        return []


"""
◤━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━◥
    ABSTRACT CLASS NodeIdentifier
◣━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━◢
"""


class NodeIdentifier(ABC):
    """
    Represents a unique node_identifier that can be used across trees and tree types to identify a node.
    Still a work in progress and may change greatly.
    """

    def __init__(self, uid: UID, path_list: Optional[Union[str, List[str]]]):
        if uid and not isinstance(uid, UID):
            uid = UID(ensure_int(uid))
        self.uid: UID = uid
        self._path_list: Optional[List[str]] = None
        self.set_path_list(path_list)

    @property
    @abstractmethod
    def tree_type(self) -> int:
        return TREE_TYPE_NA

    def get_single_path(self) -> str:
        """Do not use this unless you really mean it"""
        path_list = self.get_path_list()
        if len(path_list) != 1:
            raise RuntimeError(f'get_single_path(): expected exactly one path for node_identifier: {self}')
        return path_list[0]

    def get_path_list(self) -> List[str]:
        if self._path_list:
            if isinstance(self._path_list, list):
                return self._path_list
            elif isinstance(self._path_list, str):
                return [self._path_list]
            assert False, f'Expected list or str for path_list but got: type={type(self._path_list)}; val={self._path_list} '
        return []

    def set_path_list(self, path_list: Optional[Union[str, List[str]]]):
        """Can be None, a single full path, or a list of full paths"""
        if not path_list:
            self._path_list = None
        elif isinstance(path_list, list):
            if len(path_list) == 1:
                assert isinstance(path_list[0], str), f'Found instead: {path_list[0]}, type={type(path_list[0])}'
                self._path_list = path_list[0]
            else:
                self._path_list = path_list
                # Need this for equals operator to function properly
                self._path_list.sort()
        else:
            assert isinstance(path_list, str), f'Found instead: {path_list}, type={type(path_list)}'
            self._path_list = path_list

    def normalize_paths(self):
        path_list = self.get_path_list()
        for index, full_path in enumerate(path_list):
            if not file_util.is_normalized(full_path):
                path_list[index] = file_util.normalize_path(full_path)
                logger.debug(f'Normalized path: {full_path}')
        self.set_path_list(path_list)

    def __repr__(self):
        # should never be displayed
        return f'∣{TREE_TYPE_DISPLAY[self.tree_type]}-{self.uid}⩨{self.get_path_list()}∣'

    def __eq__(self, other):
        if isinstance(other, NodeIdentifier):
            return self._path_list == other._path_list and self.uid == other.uid and self.tree_type == other.tree_type
        return False

    def __ne__(self, other):
        return not self.__eq__(other)


"""
◤━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━◥
    CLASS NullNodeIdentifier
◣━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━◢
"""


class NullNodeIdentifier(NodeIdentifier):
    def __init__(self):
        """Used for EphemeralNodes."""
        super().__init__(NULL_UID, None)

    @property
    def tree_type(self) -> int:
        return TREE_TYPE_NA


"""
◤━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━◥
    CLASS SinglePathNodeIdentifier
◣━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━◢
"""


class SinglePathNodeIdentifier(NodeIdentifier):
    def __init__(self, uid: UID, path_list: Optional[Union[str, List[str]]], tree_type: int):
        """Has only one path. We still name the variable 'path_list' for consistency with the class hierarchy."""
        super().__init__(uid, path_list)
        if len(self.get_path_list()) != 1:
            raise RuntimeError(f'SinglePathNodeIdentifier must have exactly 1 path, but was given: {path_list}')
        self._tree_type = tree_type

    @property
    def tree_type(self) -> int:
        return self._tree_type


"""
◤━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━◥
    CLASS GDriveIdentifier
◣━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━◢
"""


class GDriveIdentifier(NodeIdentifier):
    def __init__(self, uid: UID, path_list: Optional[Union[str, List[str]]]):
        super().__init__(uid, path_list)

    @property
    def tree_type(self) -> int:
        return TREE_TYPE_GDRIVE


"""
◤━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━◥
    CLASS LocalNodeIdentifier
◣━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━◢
"""


class LocalNodeIdentifier(SinglePathNodeIdentifier):
    def __init__(self, uid: UID, path_list: Optional[Union[str, List[str]]]):
        super().__init__(uid, path_list, TREE_TYPE_LOCAL_DISK)
