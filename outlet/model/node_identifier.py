import logging
import pathlib
from abc import ABC, abstractmethod
from typing import List, Optional, Union

from constants import ChangeTreeCategory, NodeIdentifierType, NULL_UID, TREE_TYPE_DISPLAY, TreeType
from error import InvalidOperationError
from logging_constants import TRACE_ENABLED
from model.uid import UID
from util import file_util
from util.ensure import ensure_uid

logger = logging.getLogger(__name__)

# Explicit type alias:
GUID = str
DN_UID = str


class NodeIdentifier(ABC):
    """
    ◤━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━◥
    ABSTRACT CLASS NodeIdentifier

    Represents a unique node_identifier that can be used across trees and tree types to identify a node.
    Still a work in progress and may change greatly.
    ◣━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━◢
    """
    def __init__(self, node_uid: UID, device_uid: UID, path_list: Optional[Union[str, List[str]]]):
        assert node_uid is not None, 'NodeIdentifier(): node_uid is empty!'
        assert device_uid is not None, 'NodeIdentifier(): device_uid is empty!'

        self.node_uid: UID = ensure_uid(node_uid)

        self.device_uid: UID = ensure_uid(device_uid)

        self._path_list: Optional[List[str]] = None
        self.set_path_list(path_list)

    @property
    @abstractmethod
    def tree_type(self) -> TreeType:
        return TreeType.NA

    @property
    def dn_uid(self) -> DN_UID:
        """Device+TNode UID (expressed as a str).
        This guarantees a unique identifier for the node across all devices, but DOES NOT guarantee uniqueness for all of its path instances.
        (i.e. this is sometimes the same as the node's GUID, but not for all tree types)"""
        return f'{self.device_uid}:{self.node_uid}'

    @staticmethod
    def get_type() -> NodeIdentifierType:
        return NodeIdentifierType.GENERIC_MULTI_PATH  # the default; most classes should override

    @staticmethod
    def is_spid():
        return False

    @property
    def guid(self) -> GUID:
        """SPID-only"""
        assert not self.is_spid(), f'Cannot call get_single_path() for {type(self)}: it is not a SPID'
        raise InvalidOperationError(f'Cannot call .guid for {type(self)}: it is not a SPID')

    def get_single_path(self) -> str:
        """SPID-only"""
        assert not self.is_spid(), f'Cannot call get_single_path() for {type(self)}: it is not a SPID'
        raise InvalidOperationError(f'Cannot call get_single_path() for {type(self)}: it is not a SPID')

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
        if path_list is None:
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
            if TRACE_ENABLED:
                logger.debug(f'Added path: {single_path} to node UID {self.node_uid}')
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
            if pathlib.PurePosixPath(path).is_relative_to(subtree_path):
                return True
        return False

    def __repr__(self):
        return f'∣{TREE_TYPE_DISPLAY[self.tree_type]}⩨{self.guid}⩨{self.get_path_list()}∣'

    def __eq__(self, other):
        # All NodeIdentifiers can be identified uniquely by their device_uid + node_uid (unless they are SPIDs, which are stricter)
        if isinstance(other, NodeIdentifier):
            return self.get_path_list() == other.get_path_list() and self.node_uid == other.node_uid and self.device_uid == other.device_uid
        return False

    def __ne__(self, other):
        return not self.__eq__(other)


class SinglePathNodeIdentifier(NodeIdentifier, ABC):
    """
    ◤━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━◥
    CLASS SinglePathNodeIdentifier

    AKA "SPID"
    ◣━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━◢
    """
    def __init__(self, node_uid: UID, device_uid: UID, full_path: str, parent_guid: Optional[GUID] = None):
        """Has only one path. We still name the variable 'path_list' for consistency with the class hierarchy."""
        super().__init__(node_uid, device_uid, full_path)

        self.parent_guid: Optional[GUID] = parent_guid
        """This is only guaranteed to be present for certain operations"""

        if len(self.get_path_list()) != 1:
            raise RuntimeError(f'SinglePathNodeIdentifier must have exactly 1 path, but was given: {self.get_path_list()}')

    @staticmethod
    def get_type() -> NodeIdentifierType:
        return NodeIdentifierType.GENERIC_SPID

    # Currently this is only exposed for gRPC
    @property
    def path_uid(self) -> UID:
        # default for LocalDisk, etc
        return self.node_uid

    @property
    def tree_type(self) -> TreeType:
        raise RuntimeError(f'Cannot use SinglePathNodeIdentifier directly!')

    @staticmethod
    def is_spid():
        return True

    @property
    def guid(self) -> GUID:
        """Override if more info is required to ensure uniqueness in a SPID-based tree"""
        return f'{self.device_uid}:{self.path_uid}'

    def get_single_path(self) -> str:
        """This will only work for SPIDs"""
        path_list = self.get_path_list()
        if len(path_list) != 1:
            raise RuntimeError(f'get_single_path(): expected exactly one path for node_identifier: {self}')
        return path_list[0]

    def get_single_parent_path(self) -> str:
        return str(pathlib.Path(self.get_single_path()).parent)

    def __repr__(self):
        return f'∣{TREE_TYPE_DISPLAY[self.tree_type]}⩨{self.guid}⩨{repr(self.get_single_path())}∣'

    def __eq__(self, other):
        if isinstance(other, SinglePathNodeIdentifier):
            return self.get_single_path() == other.get_single_path() and self.guid == other.guid
        return False


class GDriveIdentifier(NodeIdentifier):
    """
    ◤━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━◥
        CLASS GDriveIdentifier
    ◣━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━◢
    """
    def __init__(self, uid: UID, device_uid: UID, path_list: Optional[Union[str, List[str]]]):
        super().__init__(uid, device_uid, path_list)

    @property
    def guid(self) -> GUID:
        # this is impossible without more information to identify the path
        raise RuntimeError('Cannot generate GUID for GDriveIdentifier!')

    def __repr__(self):
        return f'∣{TREE_TYPE_DISPLAY[self.tree_type]}⩨{self.device_uid}:{self.node_uid}:X⩨{self.get_path_list()}∣'

    @staticmethod
    def get_type() -> NodeIdentifierType:
        return NodeIdentifierType.GDRIVE_MPID

    @property
    def tree_type(self) -> TreeType:
        return TreeType.GDRIVE


class EphemeralNodeIdentifier(SinglePathNodeIdentifier):
    """
    ◤━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━◥
    CLASS EphemeralNodeIdentifier

    Used for EphemeralNodes.
    ◣━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━◢
    """
    def __init__(self):
        # Note: GTK3 doesn't care about GUIDs, so we can just enter junk data here (unlike Mac version)
        super().__init__(NULL_UID, NULL_UID, ".")  # note: need to make this a non-None value

    @property
    def tree_type(self) -> TreeType:
        return TreeType.NA


class GDriveSPID(SinglePathNodeIdentifier):
    """
    ◤━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━◥
        CLASS GDriveSPID
    ◣━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━◢
    """
    def __init__(self, node_uid: UID, device_uid: UID, path_uid: UID, full_path: str, parent_guid: Optional[GUID] = None):
        assert node_uid != path_uid, f'Invalid: node_uid ({node_uid}) cannot be the same as path_uid ({path_uid}) for GDriveSPID! ' \
                                     f'(full_path={full_path})'
        super().__init__(node_uid, device_uid, full_path, parent_guid)
        self._path_uid: UID = path_uid

    # Need to expose this property so that we can transmit to FE via gRPC, so it can generate GUIDs also
    @property
    def path_uid(self) -> UID:
        # default
        return self._path_uid

    @property
    def tree_type(self) -> TreeType:
        return TreeType.GDRIVE

    @property
    def guid(self) -> GUID:
        return f'{self.device_uid}:{self.node_uid}:{self._path_uid}'

    @staticmethod
    def get_type() -> NodeIdentifierType:
        return NodeIdentifierType.GDRIVE_SPID

    def __repr__(self):
        return f'∣{TREE_TYPE_DISPLAY[self.tree_type]}⩨{self.guid}⩨{repr(self.get_single_path())}∣'


class MixedTreeSPID(SinglePathNodeIdentifier):
    """
    ◤━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━◥
        CLASS MixedTreeSPID

        Currently only used for the super-root node in a ChangeTree.
    ◣━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━◢
    """
    def __init__(self, node_uid: UID, device_uid: UID, path_uid: UID, full_path: str, parent_guid: Optional[GUID] = None):
        super().__init__(node_uid, device_uid, full_path, parent_guid)
        self._path_uid: UID = path_uid

    # Need to expose this property so that we can transmit to FE via gRPC, so it can generate GUIDs also
    @property
    def path_uid(self) -> UID:
        # default
        return self._path_uid

    @property
    def tree_type(self) -> TreeType:
        return TreeType.MIXED

    @property
    def guid(self) -> GUID:
        return f'{self.device_uid}:{self.node_uid}:{self._path_uid}'

    @staticmethod
    def get_type() -> NodeIdentifierType:
        return NodeIdentifierType.MIXED_TREE_SPID

    def __repr__(self):
        return f'∣{TREE_TYPE_DISPLAY[self.tree_type]}⩨{self.guid}⩨{repr(self.get_single_path())}∣'


class LocalNodeIdentifier(SinglePathNodeIdentifier):
    """
    ◤━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━◥
        CLASS LocalNodeIdentifier
        TODO: change name to LocalDiskSPID
    ◣━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━◢
    """
    def __init__(self, uid: UID, device_uid: UID, full_path: str, parent_guid: Optional[GUID] = None):
        super().__init__(uid, device_uid, full_path, parent_guid)

    @staticmethod
    def get_type() -> NodeIdentifierType:
        return NodeIdentifierType.LOCAL_DISK_SPID

    @property
    def tree_type(self) -> TreeType:
        return TreeType.LOCAL_DISK


class ChangeTreeSPID(SinglePathNodeIdentifier):
    """
    NOTE: path_uid is stored as node_uid for ChangeTreeSPIDs, but node_uid is not used and should not be assumed to be the same value as
    the underlying TNode. ChangeTreeSPIDs do not correspond to actual node_uids because their nodes are usually not live
    TODO: consider including tree_id in here so that global lookup is possible
    """
    def __init__(self, path_uid: UID, device_uid: UID, full_path: str, category: ChangeTreeCategory, parent_guid: Optional[GUID] = None):
        super().__init__(path_uid, device_uid, full_path, parent_guid)
        self.category: ChangeTreeCategory = category

    @staticmethod
    def guid_for(path_uid: UID, device_uid: UID, category: ChangeTreeCategory) -> GUID:
        return f'{device_uid}:{category.name}:{path_uid}'

    @property
    def path_uid(self) -> UID:
        # default
        return self.node_uid

    def get_type(self) -> NodeIdentifierType:
        # Both enums have 1:1 mappings with identical values, so we can do this:
        return NodeIdentifierType(self.category)

    @property
    def tree_type(self) -> TreeType:
        # TODO: deprecate tree_type: this is a bad API
        raise RuntimeError('ChangeTreeSPID does not have tree_type!')

    @property
    def guid(self) -> GUID:
        return f'{self.device_uid}:{self.category.name}:{self.path_uid}'

    def __lt__(self, other):
        if self.device_uid == other.device_uid:
            if self.category == other.category:
                return self.device_uid < other.device_uid
            else:
                return self.category < other.category
        else:
            return self.device_uid < other.device_uid

    def __gt__(self, other):
        if self.device_uid == other.device_uid:
            if self.category == other.category:
                return self.device_uid > other.device_uid
            else:
                return self.category > other.category
        else:
            return self.device_uid > other.device_uid

    def __eq__(self, other):
        if isinstance(other, ChangeTreeSPID):
            return self.device_uid == other.device_uid and self.category == other.category and self.device_uid == other.device_uid
        return False

    def __ne__(self, other):
        return not self == other

    def __repr__(self):
        return f'∣{self.guid}⩨{repr(self.get_single_path())}∣'
