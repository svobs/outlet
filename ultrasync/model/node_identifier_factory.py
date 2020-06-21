import logging

from constants import GDRIVE_PATH_PREFIX, ROOT_UID, TREE_TYPE_GDRIVE, TREE_TYPE_LOCAL_DISK, TREE_TYPE_MIXED, ROOT_PATH
from index.uid import UID

from model.category import Category
from model.node_identifier import GDriveIdentifier, LocalFsIdentifier, LogicalNodeIdentifier, NodeIdentifier

logger = logging.getLogger(__name__)

# CLASS NodeIdentifierFactory
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼


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
        return GDriveIdentifier(uid=ROOT_UID, full_path=ROOT_PATH)

    def for_values(self, tree_type: int = None, full_path: str = None, uid: UID = None, category: Category = Category.NA) \
            -> NodeIdentifier:
        """Big factory method for creating a new identifier (for example when you intend to create a new node"""
        if not tree_type:
            return self._and_deriving_tree_type_from_path(full_path, uid, category)

        elif tree_type == TREE_TYPE_LOCAL_DISK:
            return self._for_tree_type_local(full_path, uid, category)

        elif tree_type == TREE_TYPE_GDRIVE:
            return self._for_tree_type_gdrive(full_path, uid, category)

        elif tree_type == TREE_TYPE_MIXED:
            logger.warning(f'Creating a node identifier of type MIXED for uid={uid}, full_path={full_path}, category={category}')
            return LogicalNodeIdentifier(full_path=full_path, uid=uid, tree_type=tree_type, category=category)
        else:
            raise RuntimeError('bad')

    def _and_deriving_tree_type_from_path(self, full_path: str, uid: UID, category: Category):
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

    def _for_tree_type_local(self, full_path: str = None, uid: UID = None, category: Category = Category.NA) -> LocalFsIdentifier:
        if not uid:
            uid = self.application.cache_manager.get_uid_for_path(full_path)

        return LocalFsIdentifier(uid=uid, full_path=full_path, category=category)

    def _for_tree_type_gdrive(self, full_path: str = None, uid: UID = None, category: Category = Category.NA) -> GDriveIdentifier:
        if not uid:
            if full_path == ROOT_PATH:
                uid = ROOT_UID
            else:
                uid = self.application.uid_generator.get_new_uid()
        elif uid == ROOT_UID and not full_path:
            full_path = ROOT_PATH

        return GDriveIdentifier(uid=uid, full_path=full_path, category=category)

