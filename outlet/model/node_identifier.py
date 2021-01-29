import logging
import pathlib
from abc import ABC, abstractmethod
from typing import List, Optional, Union

from constants import NULL_UID, SUPER_DEBUG, TREE_TYPE_DISPLAY, TREE_TYPE_GDRIVE, TREE_TYPE_LOCAL_DISK, TREE_TYPE_NA
from error import InvalidOperationError
from model.uid import UID
from util import file_util
from util.ensure import ensure_uid

logger = logging.getLogger(__name__)


class NodeIdentifier(ABC):
    """
    ◤━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━◥
    ABSTRACT CLASS NodeIdentifier

    Represents a unique node_identifier that can be used across trees and tree types to identify a node.
    Still a work in progress and may change greatly.
    ◣━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━◢
    """
    def __init__(self, uid: UID, path_list: Optional[Union[str, List[str]]]):
        self.uid: UID = ensure_uid(uid)
        self._path_list: Optional[List[str]] = None
        self.set_path_list(path_list)

    @property
    @abstractmethod
    def tree_type(self) -> int:
        return TREE_TYPE_NA

    def get_tree_type(self) -> int:
        return self.tree_type

    @staticmethod
    def is_spid():
        return False

    def get_single_path(self) -> str:
        raise InvalidOperationError(f'Cannot call get_single_path() for {type(self)}')

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
                assert isinstance(path_list[0], str), f'set_path_list(): Found instead: {path_list[0]}, type={type(path_list[0])}'
                self._path_list = path_list[0]
            else:
                self._path_list = path_list
                # Need this for equals operator to function properly
                self._path_list.sort()
        else:
            assert isinstance(path_list, str), f'set_path_list(): Found instead: {path_list}, type={type(path_list)}'
            self._path_list = path_list

    def add_path_if_missing(self, single_path: str):
        path_list = self.get_path_list()
        if single_path not in path_list:
            path_list.append(single_path)
            if SUPER_DEBUG:
                logger.debug(f'Added path: {single_path} to node UID {self.uid}')
        self.set_path_list(path_list)

    def has_path(self, path: str) -> bool:
        return path in self.get_path_list()

    def normalize_paths(self):
        path_list = self.get_path_list()
        for index, full_path in enumerate(path_list):
            if not file_util.is_normalized(full_path):
                path_list[index] = file_util.normalize_path(full_path)
                logger.debug(f'Normalized path: {full_path}')
        self.set_path_list(path_list)

    def has_path_in_subtree(self, subtree_path: str) -> bool:
        for path in self.get_path_list():
            if path.startswith(subtree_path):
                return True
        return False

    def __repr__(self):
        return f'∣{TREE_TYPE_DISPLAY[self.tree_type]}-{self.uid}⩨{self.get_path_list()}∣'

    def __eq__(self, other):
        if isinstance(other, NodeIdentifier):
            return self.get_path_list() == other.get_path_list() and self.uid == other.uid and self.tree_type == other.tree_type
        return False

    def __ne__(self, other):
        return not self.__eq__(other)


class NullNodeIdentifier(NodeIdentifier):
    """
    ◤━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━◥
    CLASS NullNodeIdentifier

    Used for EphemeralNodes.
    ◣━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━◢
    """
    def __init__(self):
        super().__init__(NULL_UID, None)

    @property
    def tree_type(self) -> int:
        return TREE_TYPE_NA


class SinglePathNodeIdentifier(NodeIdentifier):
    """
    ◤━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━◥
    CLASS SinglePathNodeIdentifier

    AKA "SPID"
    ◣━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━◢
    """
    def __init__(self, uid: UID, path_list: Optional[Union[str, List[str]]], tree_type: int):
        """Has only one path. We still name the variable 'path_list' for consistency with the class hierarchy."""
        super().__init__(uid, path_list)
        if len(self.get_path_list()) != 1:
            raise RuntimeError(f'SinglePathNodeIdentifier must have exactly 1 path, but was given: {path_list}')
        self._tree_type = tree_type

    @property
    def tree_type(self) -> int:
        return self._tree_type

    @staticmethod
    def is_spid():
        return True

    def get_single_path(self) -> str:
        """This will only work for SPIDs"""
        path_list = self.get_path_list()
        if len(path_list) != 1:
            raise RuntimeError(f'get_single_path(): expected exactly one path for node_identifier: {self}')
        return path_list[0]

    def get_single_parent_path(self) -> str:
        return str(pathlib.Path(self.get_single_path()).parent)

    def __repr__(self):
        return f'∣{TREE_TYPE_DISPLAY[self.tree_type]}-{self.uid}⩨{self.get_single_path()}∣'

    @staticmethod
    def from_node_identifier(node_identifier, single_path: str):
        if single_path not in node_identifier.get_path_list():
            raise RuntimeError('bad!')
        return SinglePathNodeIdentifier(uid=node_identifier.uid, path_list=single_path, tree_type=node_identifier.get_tree_type())


class GDriveIdentifier(NodeIdentifier):
    """
    ◤━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━◥
        CLASS GDriveIdentifier
    ◣━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━◢
    """
    def __init__(self, uid: UID, path_list: Optional[Union[str, List[str]]]):
        super().__init__(uid, path_list)

    @property
    def tree_type(self) -> int:
        return TREE_TYPE_GDRIVE


class LocalNodeIdentifier(SinglePathNodeIdentifier):
    """
    ◤━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━◥
        CLASS LocalNodeIdentifier
    ◣━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━◢
    """
    def __init__(self, uid: UID, path_list: Optional[Union[str, List[str]]]):
        super().__init__(uid, path_list, TREE_TYPE_LOCAL_DISK)
