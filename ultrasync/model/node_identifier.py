import logging
from abc import ABC, abstractmethod
from typing import Optional

from constants import ROOT_PATH, ROOT_UID, TREE_TYPE_GDRIVE, TREE_TYPE_LOCAL_DISK
from index.uid import UID
from model.category import Category

logger = logging.getLogger(__name__)


def ensure_category(val):
    if type(val) == str:
        return Category(int(val))
    elif type(val) == int:
        return Category(val)
    return val


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

    def __init__(self, uid: UID, full_path: str, category: Category):
        # TODO: remove category entirely. PlanningNodes will encapsulate Add/Move/Update. LocalFileNode, LocalDirNode, GoogNode will have to_delete flag
        # assert full_path is None or (type(full_path) == str and full_path.find('/') >= 0), f'full_path does not look like a path: {full_path}'
        if uid and not isinstance(uid, UID):
            uid = UID(ensure_int(uid))
        self.uid: UID = uid
        self.full_path: str = full_path
        self.category: Category = ensure_category(category)

    @property
    @abstractmethod
    def tree_type(self) -> int:
        return 0

    def __repr__(self):
        # should never be displayed
        return f'∣✪-{self.uid}∣{self.category.name[0]}⩨{self.full_path}∣'

    def __eq__(self, other):
        if isinstance(other, str):
            return other == ROOT_PATH and self.uid == ROOT_UID
        return self.full_path == other.full_path and self.uid == other.uid and self.tree_type == other.tree_type

    def __ne__(self, other):
        return not self.__eq__(other)


"""
◤━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━◥
    CLASS LogicalNodeIdentifier
◣━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━◢
"""


class LogicalNodeIdentifier(NodeIdentifier):
    def __init__(self, uid: UID, full_path: str, tree_type: int, category: Category = Category.NA):
        """Object has a path, but does not represent a physical item"""
        super().__init__(uid, full_path, category)
        self._tree_type = tree_type

    @property
    def tree_type(self) -> int:
        return self._tree_type

    def __repr__(self):
        return f'∣∅-{self.uid}∣{self.category.name[0]}⩨{self.full_path}∣'


"""
◤━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━◥
    CLASS GDriveIdentifier
◣━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━◢
"""


class GDriveIdentifier(NodeIdentifier):
    def __init__(self, uid: UID, full_path: Optional[str], category: Category = Category.NA):
        super().__init__(uid, full_path, category)

    @property
    def tree_type(self) -> int:
        return TREE_TYPE_GDRIVE

    def __repr__(self):
        return f'∣G-{self.uid}∣{self.category.name[0]}⩨{self.full_path}∣'


"""
◤━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━◥
    CLASS LocalFsIdentifier
◣━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━◢
"""


class LocalFsIdentifier(NodeIdentifier):
    def __init__(self, full_path: str, uid: UID, category: Category = Category.NA):
        super().__init__(uid, full_path, category)

    @property
    def tree_type(self) -> int:
        return TREE_TYPE_LOCAL_DISK

    def __repr__(self):
        return f'∣L-{self.uid}∣{self.category.name[0]}⩨{self.full_path}∣'

