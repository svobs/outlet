import collections
import logging
import os
from abc import ABC, abstractmethod
from typing import List, Optional, Tuple, Union

from constants import IconId, IS_MACOS, OBJ_TYPE_DIR, TrashStatus, TreeType
from error import InvalidOperationError
from model.node.trait import HasParentList
from model.node_identifier import DN_UID, NodeIdentifier
from model.uid import UID
from util import time_util

logger = logging.getLogger(__name__)

# TYPEDEF SPIDNodePair
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
SPIDNodePair = collections.namedtuple('SPIDNodePair', 'spid node')


class AbstractNode(ABC):
    """
    ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
    CLASS AbstractNode
    ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼
    """

    def __init__(self):
        pass

    @property
    @abstractmethod
    def identifier(self):
        pass

    def get_tag(self) -> str:
        return ''

    def __lt__(self, other):
        return self.identifier < other.identifier


class TNode(AbstractNode, HasParentList, ABC):
    """
    ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
    ABSTRACT CLASS TNode

    Base class for all data nodes.
    ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼
    """

    def __init__(self,
                 node_identifier: NodeIdentifier,
                 parent_uids: Optional[Union[UID, List[UID]]] = None):
        AbstractNode.__init__(self)
        HasParentList.__init__(self, parent_uids)
        self.node_identifier: NodeIdentifier = node_identifier

        self._icon: Optional[IconId] = None

    @property
    def identifier(self):
        return self.node_identifier.node_uid

    @abstractmethod
    def is_parent_of(self, potential_child_node) -> bool:
        raise InvalidOperationError('is_parent_of')

    @property
    def tree_type(self) -> TreeType:
        return self.node_identifier.tree_type

    @property
    def device_uid(self) -> UID:
        return self.node_identifier.device_uid

    @property
    def dn_uid(self) -> DN_UID:
        """Device+TNode UID (expressed as a str).
        This guarantees a unique identifier for the node across all devices, but DOES NOT guarantee uniqueness for all of its path instances.
        (i.e. this is sometimes the same as the node's GUID, but not for all tree types)"""
        return f'{self.device_uid}:{self.uid}'

    @staticmethod
    def format_dn_uid(device_uid, node_uid):
        return f'{device_uid}:{node_uid}'

    def get_tag(self) -> str:
        return str(self.node_identifier)

    def __lt__(self, other):
        return self.name < other.name

    @staticmethod
    def is_container_node() -> bool:
        return False

    @classmethod
    @abstractmethod
    def get_obj_type(cls):
        return None

    @classmethod
    def is_file(cls):
        return False

    @classmethod
    @abstractmethod
    def is_dir(cls):
        return False

    @classmethod
    def is_ephemereal(cls) -> bool:
        return False

    @classmethod
    def is_live(cls) -> bool:
        """Whether the object represented by this node actually exists currently; or it is just e.g. planned to exist or is an ephemeral node."""
        return False

    @classmethod
    def set_is_live(cls, is_live: bool):
        raise InvalidOperationError('Cannot call set_is_live() for base class TNode!')

    @classmethod
    def has_tuple(cls) -> bool:
        return False

    @property
    def is_shared(self):
        return False

    def to_tuple(self) -> Tuple:
        raise RuntimeError('Operation not supported for this object: "to_tuple()"')

    def set_node_identifier(self, node_identifier: NodeIdentifier):
        self.node_identifier = node_identifier

    @property
    def name(self):
        assert self.node_identifier.get_path_list(), f'Oops - for {self}'
        return os.path.basename(self.node_identifier.get_path_list()[0])

    def get_trashed_status(self) -> TrashStatus:
        return TrashStatus.NOT_TRASHED

    @staticmethod
    def get_etc() -> Optional[str]:
        return None

    @property
    def md5(self) -> Optional[str]:
        return None

    @property
    def sha256(self) -> Optional[str]:
        return None

    @property
    def size_bytes(self) -> Optional[int]:
        return self.get_size_bytes()

    def get_size_bytes(self) -> Optional[int]:
        return None

    def set_size_bytes(self, size_bytes: int):
        pass

    @property
    def sync_ts(self) -> Optional[int]:
        raise InvalidOperationError('sync_ts(): if you are seeing this msg you forgot to implement this in subclass of TNode!')

    @sync_ts.setter
    def sync_ts(self, sync_ts: int):
        raise InvalidOperationError('sync_ts(): if you are seeing this msg you forgot to implement this in subclass of TNode!')

    @property
    def create_ts(self) -> Optional[int]:
        return None

    @property
    def modify_ts(self) -> Optional[int]:
        return None

    @property
    def change_ts(self) -> Optional[int]:
        return None

    def get_path_list(self) -> List[str]:
        return self.node_identifier.get_path_list()

    def get_single_path(self) -> str:
        return self.node_identifier.get_single_path()

    @property
    def uid(self) -> UID:
        return self.node_identifier.node_uid

    @uid.setter
    def uid(self, uid: UID):
        self.node_identifier.node_uid = uid

    def get_icon(self) -> IconId:
        if self._icon:
            return self._icon
        return self.get_default_icon()

    def get_custom_icon(self) -> Optional[IconId]:
        return self._icon

    def set_icon(self, icon: IconId):
        self._icon = icon

    def get_default_icon(self) -> IconId:
        if self.is_live():
            return IconId.ICON_GENERIC_FILE
        return IconId.ICON_FILE_CP_DST

    def _get_valid_create_ts_for_macos(self, is_seconds_precision_enough) -> Optional[int]:
        if self.create_ts:
            if self.modify_ts:
                if self.create_ts > self.modify_ts:
                    if is_seconds_precision_enough and logger.isEnabledFor(logging.DEBUG):
                        logger.debug(f'(MacOS): create_ts ({self.create_ts}) > modify_ts ({self.modify_ts}); '
                                     f'using modify_ts for both in: {self.node_identifier}')
                    else:
                        logger.warning(f'(MacOS): create_ts ({self.create_ts}) is AFTER its modify_ts ({self.modify_ts}); '
                                       f'using modify_ts for both in: {self.node_identifier}')
                    return self.modify_ts
            return self.create_ts
        return None

    def is_meta_equal(self, other_node, is_seconds_precision_enough: bool) -> bool:
        # try:
        other_modify_ts = other_node.modify_ts
        self_modify_ts = self.modify_ts
        if IS_MACOS:
            assert isinstance(other_node, TNode)
            other_create_ts = other_node._get_valid_create_ts_for_macos(is_seconds_precision_enough)
            self_create_ts = self._get_valid_create_ts_for_macos(is_seconds_precision_enough)
        else:
            other_create_ts = other_node.create_ts
            self_create_ts = self.create_ts

        if is_seconds_precision_enough:
            if self_create_ts:
                self_create_ts = int(self_create_ts/1000)
            if self_modify_ts:
                self_modify_ts = int(self_modify_ts/1000)

            if other_create_ts:
                other_create_ts = int(other_create_ts/1000)
            if other_modify_ts:
                other_modify_ts = int(other_modify_ts/1000)

        if not self_create_ts or not self_modify_ts:
            raise RuntimeError(f'Failed to get either create_ts ({self_create_ts}) or modify_ts ({self_modify_ts}) from other: {self}')
        if not other_create_ts or not other_modify_ts:
            raise RuntimeError(f'Failed to get either create_ts ({other_create_ts}) or modify_ts ({other_modify_ts}) from other: {other_node}')

        # Note that change_ts is not included, since this cannot be changed easily (and doesn't seem to be crucial to our purposes anyway)
        return other_create_ts == self_create_ts and other_modify_ts == self_modify_ts and \
            (not self.is_file() or (other_node.get_size_bytes() == self.get_size_bytes()))

    def how_is_meta_not_equal(self, other_node) -> str:
        """For use with debugging why is_meta_equal() returned false"""
        problem_list: List[str] = []
        if IS_MACOS:
            # Workaround for Mac create_ts bug
            assert isinstance(other_node, TNode)
            problem_list += self._print_field_diff('create_ts', self._get_valid_create_ts_for_macos(), other_node._get_valid_create_ts_for_macos())
        else:
            problem_list += self._print_field_diff('create_ts', self.create_ts, other_node.create_ts)
        problem_list += self._print_field_diff('modify_ts', self.modify_ts, other_node.modify_ts)
        if self.is_file() and other_node.is_file():
            problem_list += self._print_field_diff('size_bytes', self.get_size_bytes(), other_node.get_size_bytes())
        return f'Comparison of this ({self.node_identifier}) to other ({other_node.node_identifier}) = [{", ".join(problem_list)}]'

    @staticmethod
    def _print_field_diff(field_name: str, val_self, val_other) -> List[str]:
        if val_self > val_other:
            return [f'{field_name} is greater ({val_self} > {val_other}; '
                    f'"{time_util.ts_to_str_with_millis(val_self)}" > "{time_util.ts_to_str_with_millis(val_other)}")']
        elif val_self < val_other:
            return [f'{field_name} is smaller ({val_self} < {val_other}; '
                    f'"{time_util.ts_to_str_with_millis(val_self)}" < "{time_util.ts_to_str_with_millis(val_other)}")']
        return []

    @classmethod
    def has_signature(cls) -> bool:
        return False

    def is_signature_equal(self, other_node) -> bool:
        assert isinstance(other_node, TNode), f'Invalid type: {type(other_node)}'
        if other_node.device_uid == self.device_uid and other_node.uid == self.uid:
            # Same identity -> signature matches by default
            return True

        if other_node.md5 and self.md5:
            return other_node.md5 == self.md5

        if other_node.sha256 and self.sha256:
            return other_node.sha256 == self.sha256

        logger.error(f'is_signature_equal(): not enough info to compare signatures for this ({self}) and other ({other_node})')
        raise RuntimeError(f'Cannot not compare signatures for nodes: neeed either MD5 or SHA256 from both this ({self}) and other ({other_node})')

    @abstractmethod
    def update_from(self, other_node):
        assert isinstance(other_node, TNode), f'Invalid type: {type(other_node)}'
        assert other_node.node_identifier.node_uid == self.node_identifier.node_uid \
               and other_node.node_identifier.device_uid == self.node_identifier.device_uid, \
               f'Other identifier ({other_node.node_identifier}) does not match: {self.node_identifier}'
        HasParentList.update_from(self, other_node)
        # do not change UID or tree type
        self.node_identifier.set_path_list(other_node.get_path_list())
        self._icon = other_node._icon

    def is_substantially_equal(self, other_node, is_seconds_precision_enough: bool) -> bool:
        return self.name == other_node.name and self.is_meta_equal(other_node, is_seconds_precision_enough) \
               and self.is_signature_equal(other_node)


class NonexistentDirNode(TNode):
    """
    ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
    CLASS NonexistentDirNode

    Represents a directory which does not exist. Use this in SPIDNodePair objects when the SPID points to something which doesn't exist.
    It's much safer to use this class rather than remembering to deal with null/nil/None.
    ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼
    """

    def __init__(self, node_identifier: NodeIdentifier, name: str):
        super().__init__(node_identifier)
        self._name = name

    @property
    def name(self):
        return self._name

    @classmethod
    def is_dir(cls):
        return True

    @classmethod
    def get_obj_type(cls):
        return OBJ_TYPE_DIR

    @property
    def sync_ts(self):
        return None

    def update_from(self, other_node):
        assert isinstance(other_node, NonexistentDirNode), f'Invalid type: {type(other_node)}'
        TNode.update_from(self, other_node)
        self._name = other_node.name

    def is_parent_of(self, potential_child_node) -> bool:
        # never a parent of anything. still waiting for that adoption paperwork
        return False
