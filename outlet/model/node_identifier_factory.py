import logging
from typing import List, Optional, Union

from model.op import OpType
from constants import GDRIVE_PATH_PREFIX, GDRIVE_ROOT_UID, TREE_TYPE_GDRIVE, TREE_TYPE_LOCAL_DISK, TREE_TYPE_MIXED, ROOT_PATH
from model.uid import UID

from model.node_identifier import ensure_list, GDriveIdentifier, LocalNodeIdentifier, LogicalNodeIdentifier, NodeIdentifier

logger = logging.getLogger(__name__)

# CLASS NodeIdentifierFactory
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼


class NodeIdentifierFactory:
    def __init__(self, app):
        self.app = app

    @staticmethod
    def nid(uid: UID, tree_type: int, op_type: OpType):
        return f'{tree_type}-{uid}-{op_type.name}'

    @staticmethod
    def get_gdrive_root_constant_identifier() -> GDriveIdentifier:
        return GDriveIdentifier(uid=GDRIVE_ROOT_UID, path_list=ROOT_PATH)

    def for_values(self, tree_type: int = None, path_list:  Union[str, List[str]] = None, uid: UID = None) -> NodeIdentifier:
        """Big factory method for creating a new identifier (for example when you intend to create a new node"""
        full_path_list = ensure_list(path_list)
        if not tree_type:
            return self._and_deriving_tree_type_from_path(full_path_list, uid)

        elif tree_type == TREE_TYPE_LOCAL_DISK:
            return self._for_tree_type_local(full_path_list, uid)

        elif tree_type == TREE_TYPE_GDRIVE:
            return self._for_tree_type_gdrive(full_path_list, uid)

        elif tree_type == TREE_TYPE_MIXED:
            logger.warning(f'Creating a node identifier of type MIXED for uid={uid}, path_list={full_path_list}')
            return LogicalNodeIdentifier(path_list=full_path_list, uid=uid, tree_type=tree_type)
        else:
            raise RuntimeError('bad')

    @staticmethod
    def _strip_gdrive(path):
        stripped = path[len(GDRIVE_PATH_PREFIX):]
        if stripped != '/' and stripped.endswith('/'):
            stripped = stripped[:-1]
        return stripped

    @staticmethod
    def _derive_gdrive_path_list(full_path_list):
        derived_list = []
        for path in ensure_list(full_path_list):
            derived_list.append(NodeIdentifierFactory._strip_gdrive(path))
        return derived_list

    def _and_deriving_tree_type_from_path(self, full_path_list: Optional[List[str]], uid: UID) -> NodeIdentifier:
        if full_path_list:
            if full_path_list[0].startswith(GDRIVE_PATH_PREFIX):
                derived_list = NodeIdentifierFactory._derive_gdrive_path_list(full_path_list)
                if not derived_list or not derived_list[0]:
                    return NodeIdentifierFactory.get_gdrive_root_constant_identifier()
                return GDriveIdentifier(path_list=derived_list, uid=uid)
            else:
                if not uid:
                    uid = self.app.cacheman.get_uid_for_path(full_path_list[0])

                return LocalNodeIdentifier(uid=uid, path_list=full_path_list)
        else:
            raise RuntimeError('no tree_type and no full_path supplied')

    def _for_tree_type_local(self, full_path_list: Optional[List[str]] = None, uid: UID = None) -> LocalNodeIdentifier:
        if full_path_list:
            uid = self.app.cacheman.get_uid_for_path(full_path_list[0], uid)

            return LocalNodeIdentifier(uid=uid, path_list=full_path_list)
        else:
            raise RuntimeError('no full_path supplied for local file')

    def _for_tree_type_gdrive(self, full_path_list: Optional[List[str]] = None, uid: UID = None) -> GDriveIdentifier:
        if not uid:
            if full_path_list and full_path_list[0] == ROOT_PATH:
                uid = GDRIVE_ROOT_UID
            else:
                uid = self.app.uid_generator.next_uid()
        elif uid == GDRIVE_ROOT_UID and not full_path_list:
            full_path_list = [ROOT_PATH]

        return GDriveIdentifier(uid=uid, path_list=full_path_list)

