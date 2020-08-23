import logging
from abc import ABC, abstractmethod
from typing import Optional

from constants import ROOT_PATH, LOCAL_ROOT_UID, TREE_TYPE_GDRIVE, TREE_TYPE_LOCAL_DISK
from index.uid.uid import UID

logger = logging.getLogger(__name__)


def ensure_int(val):
    try:
        if type(val) == str:
            return int(val)
    except ValueError:
        logger.error(f'Bad value: {val}')
    return val


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

    def __init__(self, uid: UID, full_path: str):
        if uid and not isinstance(uid, UID):
            uid = UID(ensure_int(uid))
        self.uid: UID = uid
        self.full_path: str = full_path

    @property
    @abstractmethod
    def tree_type(self) -> int:
        return 0

    def __repr__(self):
        # should never be displayed
        return f'∣✪-{self.uid}⩨{self.full_path}∣'

    def __eq__(self, other):
        return self.full_path == other.full_path and self.uid == other.uid and self.tree_type == other.tree_type

    def __ne__(self, other):
        return not self.__eq__(other)


"""
◤━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━◥
    CLASS LogicalNodeIdentifier
◣━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━◢
"""


class LogicalNodeIdentifier(NodeIdentifier):
    def __init__(self, uid: UID, full_path: str, tree_type: int):
        """Object has a path, but does not represent a physical item"""
        super().__init__(uid, full_path)
        self._tree_type = tree_type

    @property
    def tree_type(self) -> int:
        return self._tree_type

    def __repr__(self):
        return f'∣∅-{self.uid}⩨{self.full_path}∣'


"""
◤━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━◥
    CLASS GDriveIdentifier
◣━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━◢
"""


class GDriveIdentifier(NodeIdentifier):
    def __init__(self, uid: UID, full_path: Optional[str]):
        super().__init__(uid, full_path)

    @property
    def tree_type(self) -> int:
        return TREE_TYPE_GDRIVE

    def __repr__(self):
        return f'∣G-{self.uid}⩨{self.full_path}∣'


"""
◤━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━◥
    CLASS LocalFsIdentifier
◣━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━◢
"""


class LocalFsIdentifier(NodeIdentifier):
    def __init__(self, full_path: str, uid: UID):
        super().__init__(uid, full_path)

    @property
    def tree_type(self) -> int:
        return TREE_TYPE_LOCAL_DISK

    def __repr__(self):
        return f'∣L-{self.uid}⩨{self.full_path}∣'

