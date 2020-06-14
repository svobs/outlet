from abc import ABC, abstractmethod
from typing import Optional
import logging

from constants import GDRIVE_PATH_PREFIX, TREE_TYPE_GDRIVE, TREE_TYPE_LOCAL_DISK, TREE_TYPE_MIXED, ROOT_PATH

from index import uid_generator
from index.uid_generator import UID
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
        logger.debug(f'Bad value: {val}')
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
        return f'∣✪-{self.uid}∣Ⓒ{self.category.name[0]}⩨{self.full_path}∣'

    def __eq__(self, other):
        if isinstance(other, str):
            return other == ROOT_PATH and self.uid == uid_generator.ROOT_UID
        return self.full_path == other.full_path and self.uid == other.uid and self.tree_type == other.tree_type

    def __ne__(self, other):
        return not self.__eq__(other)


"""
◤━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━◥
    CLASS LogicalNodeIdentifier
◣━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━◢
"""


class LogicalNodeIdentifier(NodeIdentifier):
    def __init__(self, uid: UID, full_path: str, category: Category, tree_type: int):
        """Object has a path, but does not represent a physical item"""
        super().__init__(uid, full_path, category)
        self._tree_type = tree_type

    @property
    def tree_type(self) -> int:
        return self._tree_type

    def __repr__(self):
        return f'∣∅-{self.uid}∣Ⓒ{self.category.name[0]}⩨{self.full_path}∣'


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
        return f'∣G-{self.uid}∣Ⓒ{self.category.name[0]}⩨{self.full_path}∣'


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
        return f'∣L-{self.uid}∣Ⓒ{self.category.name[0]}⩨{self.full_path}∣'


class NodeIdentifierFactory:
    def __init__(self, application):
        self.application = application

    @staticmethod
    def nid(uid, tree_type, category=Category.NA):
        if category == Category.NA:
            return f'{tree_type}-{uid}'
        return f'{tree_type}-{uid}-{category.value}'

    @staticmethod
    def parse_nid(nid: str) -> NodeIdentifier:
        assert nid
        parsed = nid.split('-')
        if len(parsed) > 2:
            category = Category(parsed[2])
        else:
            category = Category.NA
        tree_type = int(parsed[0])
        if tree_type == TREE_TYPE_LOCAL_DISK:
            return LocalFsIdentifier(uid=UID(parsed[1]), full_path=None, category=category)
        elif tree_type == TREE_TYPE_GDRIVE:
            return GDriveIdentifier(uid=UID(parsed[1]), full_path=None, category=category)
        else:
            return LogicalNodeIdentifier(uid=UID(parsed[1]), full_path=None, category=category)

    @staticmethod
    def get_gdrive_root_constant_identifier() -> GDriveIdentifier:
        return GDriveIdentifier(uid=uid_generator.ROOT_UID, full_path=ROOT_PATH)

    def for_values(self, tree_type: int = None,
                   full_path: str = None,
                   uid: UID = None,
                   category: Category = Category.NA):
        if not tree_type:
            if full_path:
                if full_path.startswith(GDRIVE_PATH_PREFIX):
                    gdrive_path = full_path[len(GDRIVE_PATH_PREFIX):]
                    if gdrive_path != '/' and gdrive_path.endswith('/'):
                        gdrive_path = gdrive_path[:-1]
                    if not gdrive_path:
                        # can happen if the use enters "gdrive:/". Just return the root
                        return NodeIdentifierFactory.get_gdrive_root_constant_identifier()
                    return GDriveIdentifier(uid=uid, full_path=gdrive_path, category=category)
                else:
                    if not uid:
                        uid = self.application.cache_manager.get_uid_for_path(full_path)
                    return LocalFsIdentifier(uid=uid, full_path=full_path, category=category)
            else:
                raise RuntimeError('no tree_type and no full_path supplied')
        elif tree_type == TREE_TYPE_LOCAL_DISK:
            if not uid:
                uid = self.application.cache_manager.get_uid_for_path(full_path)
            return LocalFsIdentifier(uid=uid, full_path=full_path, category=category)
        elif tree_type == TREE_TYPE_GDRIVE:
            if full_path == ROOT_PATH and not uid:
                uid = uid_generator.ROOT_UID
            elif uid == uid_generator.ROOT_UID and not full_path:
                full_path = ROOT_PATH
            return GDriveIdentifier(uid=uid, full_path=full_path, category=category)
        elif tree_type == TREE_TYPE_MIXED:
            logger.warning(f'Creating a node identifier of type MIXED for uid={uid}, full_path={full_path}, category={category}')
            return LogicalNodeIdentifier(full_path=full_path, uid=uid, tree_type=tree_type, category=category)
        else:
            raise RuntimeError('bad')
