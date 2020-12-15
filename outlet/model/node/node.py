import collections
import logging
import os
from abc import ABC, abstractmethod
from typing import List, Optional, Tuple, Union

from constants import IconId, TrashStatus
from error import InvalidOperationError
from model.node.trait import HasParentList
from model.node_identifier import NodeIdentifier
from store.uid.uid_generator import UID
from util.ensure import ensure_uid
from util.simple_tree import BaseNode

logger = logging.getLogger(__name__)

# TYPEDEF SPIDNodePair
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
SPIDNodePair = collections.namedtuple('SPIDNodePair', 'spid node')


class Node(BaseNode, HasParentList, ABC):
    """
    ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
    ABSTRACT CLASS Node

    Base class for all data nodes.
    ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼
    """
    def __init__(self, node_identifier: NodeIdentifier, nid: UID = None, parent_uids: Optional[Union[UID, List[UID]]] = None,
                 trashed: TrashStatus = TrashStatus.NOT_TRASHED):
        # Look at these next 3 lines. They are very important.
        if not nid:
            nid = node_identifier.uid
        else:
            nid = ensure_uid(nid)
        BaseNode.__init__(self, identifier=nid)
        HasParentList.__init__(self, parent_uids)
        self.node_identifier: NodeIdentifier = node_identifier

        if not trashed:
            self._trashed: TrashStatus = TrashStatus.NOT_TRASHED
        elif trashed < TrashStatus.NOT_TRASHED or trashed > TrashStatus.DELETED:
            raise RuntimeError(f'Invalid value for "trashed": {trashed}')
        else:
            self._trashed: TrashStatus = TrashStatus(trashed)

        self._icon: Optional[IconId] = None

    @abstractmethod
    def is_parent_of(self, potential_child_node) -> bool:
        raise InvalidOperationError('is_parent_of')

    def get_tree_type(self) -> int:
        return self.node_identifier.tree_type

    def get_tag(self) -> str:
        return str(self.node_identifier)

    def __lt__(self, other):
        return self.name < other.name

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
    def is_display_only(cls):
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
        raise InvalidOperationError('Cannot call set_is_live() for base class Node!')

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
        self.identifier = node_identifier.uid

    @property
    def name(self):
        assert self.node_identifier.get_path_list(), f'Oops - for {self}'
        return os.path.basename(self.node_identifier.get_path_list()[0])

    def get_trashed_status(self) -> TrashStatus:
        return self._trashed

    @staticmethod
    def get_etc():
        return None

    @property
    def md5(self):
        return None

    @property
    def sha256(self):
        return None

    def get_size_bytes(self):
        return None

    def set_size_bytes(self, size_bytes: int):
        pass

    @property
    @abstractmethod
    def sync_ts(self):
        raise RuntimeError('sync_ts(): if you are seeing this msg you forgot to implement this in subclass of Node!')

    @property
    def modify_ts(self):
        return None

    @property
    def change_ts(self):
        return None

    def get_path_list(self):
        return self.node_identifier.get_path_list()

    def get_single_path(self):
        return self.node_identifier.get_single_path()

    @property
    def uid(self) -> UID:
        return self.node_identifier.uid

    @uid.setter
    def uid(self, uid: UID):
        self.node_identifier.uid = uid
        self.identifier = uid

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

    @abstractmethod
    def update_from(self, other_node):
        assert isinstance(other_node, Node), f'Invalid type: {type(other_node)}'
        assert other_node.node_identifier.uid == self.node_identifier.uid and other_node.node_identifier.tree_type == self.node_identifier.tree_type,\
            f'Other identifier ({other_node.node_identifier}) does not match: {self.node_identifier}'
        HasParentList.update_from(self, other_node)
        # do not change UID or tree type
        self.node_identifier.set_path_list(other_node.get_path_list())
        self.identifier = other_node.identifier
        self._trashed = other_node._trashed
