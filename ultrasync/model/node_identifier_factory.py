import logging

from model.change_action import ChangeType
from constants import GDRIVE_PATH_PREFIX, ROOT_UID, TREE_TYPE_GDRIVE, TREE_TYPE_LOCAL_DISK, TREE_TYPE_MIXED, ROOT_PATH
from index.uid.uid import UID

from model.node_identifier import GDriveIdentifier, LocalFsIdentifier, LogicalNodeIdentifier, NodeIdentifier

logger = logging.getLogger(__name__)

# CLASS NodeIdentifierFactory
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼


class NodeIdentifierFactory:
    def __init__(self, application):
        self.application = application

    @staticmethod
    def nid(uid: UID, tree_type: int, change_type: ChangeType):
        return f'{tree_type}-{uid}-{change_type.name}'

    @staticmethod
    def get_gdrive_root_constant_identifier() -> GDriveIdentifier:
        return GDriveIdentifier(uid=ROOT_UID, full_path=ROOT_PATH)

    def for_values(self, tree_type: int = None, full_path: str = None, uid: UID = None) -> NodeIdentifier:
        """Big factory method for creating a new identifier (for example when you intend to create a new node"""
        if not tree_type:
            return self._and_deriving_tree_type_from_path(full_path, uid)

        elif tree_type == TREE_TYPE_LOCAL_DISK:
            return self._for_tree_type_local(full_path, uid)

        elif tree_type == TREE_TYPE_GDRIVE:
            return self._for_tree_type_gdrive(full_path, uid)

        elif tree_type == TREE_TYPE_MIXED:
            logger.warning(f'Creating a node identifier of type MIXED for uid={uid}, full_path={full_path}')
            return LogicalNodeIdentifier(full_path=full_path, uid=uid, tree_type=tree_type)
        else:
            raise RuntimeError('bad')

    def _and_deriving_tree_type_from_path(self, full_path: str, uid: UID):
        if full_path:
            if full_path.startswith(GDRIVE_PATH_PREFIX):
                gdrive_path = full_path[len(GDRIVE_PATH_PREFIX):]
                if gdrive_path != '/' and gdrive_path.endswith('/'):
                    gdrive_path = gdrive_path[:-1]
                if not gdrive_path:
                    # can happen if the use enters "gdrive:/". Just return the root
                    return NodeIdentifierFactory.get_gdrive_root_constant_identifier()
                return GDriveIdentifier(uid=uid, full_path=gdrive_path)
            else:
                if not uid:
                    uid = self.application.cache_manager.get_uid_for_path(full_path)

                return LocalFsIdentifier(uid=uid, full_path=full_path)
        else:
            raise RuntimeError('no tree_type and no full_path supplied')

    def _for_tree_type_local(self, full_path: str = None, uid: UID = None) -> LocalFsIdentifier:
        if not uid:
            uid = self.application.cache_manager.get_uid_for_path(full_path)

        return LocalFsIdentifier(uid=uid, full_path=full_path)

    def _for_tree_type_gdrive(self, full_path: str = None, uid: UID = None) -> GDriveIdentifier:
        if not uid:
            if full_path == ROOT_PATH:
                uid = ROOT_UID
            else:
                uid = self.application.uid_generator.next_uid()
        elif uid == ROOT_UID and not full_path:
            full_path = ROOT_PATH

        return GDriveIdentifier(uid=uid, full_path=full_path)

