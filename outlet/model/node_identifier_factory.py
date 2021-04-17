import logging
from typing import List, Optional, Union

from constants import GDRIVE_PATH_PREFIX, GDRIVE_ROOT_UID, LOCAL_ROOT_UID, NULL_UID, ROOT_PATH, ROOT_PATH_UID, SUPER_ROOT_UID, TreeType
from model.node_identifier import GDriveIdentifier, GDriveSPID, GUID, LocalNodeIdentifier, MixedTreeSPID, NodeIdentifier, SinglePathNodeIdentifier
from model.uid import UID
from util.ensure import ensure_list, ensure_uid

logger = logging.getLogger(__name__)


class NodeIdentifierFactory:
    """
    ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
    CLASS NodeIdentifierFactory
    ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼
    """
    def __init__(self, backend):
        self.backend = backend

    def get_root_constant_gdrive_identifier(self, device_uid: UID) -> GDriveIdentifier:
        assert self._get_tree_type_for_device_uid(device_uid) == TreeType.GDRIVE, f'Device UID {device_uid} is not GDrive!'
        return GDriveIdentifier(uid=GDRIVE_ROOT_UID, device_uid=device_uid, path_list=ROOT_PATH)

    @staticmethod
    def get_root_constant_spid(tree_type: TreeType, device_uid: UID) -> SinglePathNodeIdentifier:
        if tree_type == TreeType.GDRIVE:
            return NodeIdentifierFactory.get_root_constant_gdrive_spid(device_uid)

        if tree_type == TreeType.LOCAL_DISK:
            return NodeIdentifierFactory.get_root_constant_local_disk_spid(device_uid)

        if tree_type == TreeType.MIXED:
            assert device_uid == NULL_UID, f'Expected NULL_UID for device_uid but found: {device_uid}'
            return MixedTreeSPID(uid=SUPER_ROOT_UID, device_uid=device_uid, path_uid=ROOT_PATH_UID, full_path=ROOT_PATH)

        raise RuntimeError(f'get_root_constant_spid(): invalid tree type: {tree_type}')

    @staticmethod
    def get_root_constant_gdrive_spid(device_uid: UID) -> SinglePathNodeIdentifier:
        return GDriveSPID(uid=GDRIVE_ROOT_UID, device_uid=device_uid, path_uid=ROOT_PATH_UID, full_path=ROOT_PATH)

    @staticmethod
    def get_root_constant_local_disk_spid(device_uid: UID) -> SinglePathNodeIdentifier:
        return LocalNodeIdentifier(uid=LOCAL_ROOT_UID, device_uid=device_uid, full_path=ROOT_PATH)

    def from_guid(self, guid: GUID) -> SinglePathNodeIdentifier:
        """
        BACKEND ONLY! Returns a SPID corresponding to the given GUID.
        Obvioiusly GUIDs are ineffcient to use, but in practice the BE will only use them to record user selection & expanded nodes.
        (GUIDs are also used by the Mac frontend)
        """
        uid_list = guid.split(':')
        if len(uid_list) < 2:
            raise RuntimeError(f'Invalid GUID: "{guid}"')

        device_uid = UID(uid_list[0])
        node_uid = UID(uid_list[1])

        tree_type = self._get_tree_type_for_device_uid(device_uid)

        if tree_type == TreeType.LOCAL_DISK:
            full_path = self.backend.cacheman.get_path_for_uid(node_uid)
            return LocalNodeIdentifier(node_uid, device_uid=device_uid, full_path=full_path)
        elif tree_type == TreeType.GDRIVE or tree_type == TreeType.MIXED:
            if len(uid_list) != 3:
                raise RuntimeError(f'Tree type ({tree_type.name}) does not contain path_uid!')
            path_uid = UID(uid_list[2])
            full_path = self.backend.cacheman.get_path_for_uid(path_uid)
            return GDriveSPID(node_uid, device_uid=device_uid, path_uid=path_uid, full_path=full_path)
        else:
            raise RuntimeError(f'Invalid tree_type: {tree_type.name}')

    def from_path(self, full_path: str, device_uid: UID) -> NodeIdentifier:
        # FIXME: clean this up! See note for ActiveTreeManager._resolve_root_meta_from_path()
        full_path_list = ensure_list(full_path)
        return self._and_deriving_tree_type_from_path(full_path_list, node_uid=None, device_uid=device_uid, must_be_single_path=True)

    def for_values(self,
                   device_uid: Optional[UID] = None,
                   tree_type: Optional[TreeType] = None,
                   path_list: Optional[Union[str, List[str]]] = None,
                   uid: Optional[UID] = None,
                   path_uid: Optional[UID] = None,
                   must_be_single_path: bool = False) -> NodeIdentifier:
        """Big factory method for creating a new identifier (for example when you intend to create a new node.
        May be called either from FE or BE. For FE, it may be quite slow due to network overhead."""
        uid = ensure_uid(uid)
        device_uid = ensure_uid(device_uid)
        full_path_list = ensure_list(path_list)

        if not tree_type:
            if device_uid:
                tree_type = self._get_tree_type_for_device_uid(device_uid)
            else:
                return self._and_deriving_tree_type_from_path(full_path_list, uid, device_uid, must_be_single_path)

        if tree_type == TreeType.LOCAL_DISK:
            assert device_uid, f'No device_uid provided!'
            return self._for_tree_type_local(device_uid, full_path_list, uid)

        elif tree_type == TreeType.GDRIVE:
            assert device_uid, f'No device_uid provided!'
            return self._for_tree_type_gdrive(device_uid, full_path_list, uid, path_uid, must_be_single_path)

        elif tree_type == TreeType.MIXED:
            logger.warning(f'Creating a node identifier of type MIXED for uid={uid}, path={full_path_list}')
            if len(full_path_list) > 1:
                raise RuntimeError(f'Too many paths for tree_type MIXED: {full_path_list}')
            if not path_uid:
                path_uid = self.backend.get_uid_for_local_path(full_path_list[0])
            return MixedTreeSPID(uid=uid, device_uid=device_uid, path_uid=path_uid, full_path=full_path_list[0])
        else:
            raise RuntimeError('bad')

    def _get_tree_type_for_device_uid(self, device_uid: UID) -> TreeType:
        for device in self.backend.get_device_list():
            if device.uid == device_uid:
                return device.tree_type
        raise RuntimeError(f'Could not find device with UID: {device_uid}')

    @staticmethod
    def strip_gdrive(path):
        stripped = path[len(GDRIVE_PATH_PREFIX):]
        if stripped.endswith('/'):
            stripped = stripped[:-1]
        if not stripped.startswith('/', 0):
            # this happens if either the path is '/' or the user mistyped
            stripped = f'/{stripped}'
        return stripped

    @staticmethod
    def _derive_gdrive_path_list(full_path_list):
        derived_list = []
        for path in full_path_list:
            derived_list.append(NodeIdentifierFactory.strip_gdrive(path))
        return derived_list

    def _and_deriving_tree_type_from_path(self, full_path_list: Optional[List[str]], node_uid: Optional[UID], device_uid: Optional[UID],
                                          must_be_single_path: bool = False) -> NodeIdentifier:
        if full_path_list:
            if full_path_list[0].startswith(GDRIVE_PATH_PREFIX):
                # GDrive

                derived_list: List[str] = NodeIdentifierFactory._derive_gdrive_path_list(full_path_list)
                if must_be_single_path:
                    if not derived_list or not derived_list[0] or derived_list[0] == ROOT_PATH:
                        return NodeIdentifierFactory.get_root_constant_gdrive_spid(device_uid)
                    if len(derived_list) > 1:
                        raise RuntimeError(f'Could not make GDrive identifier: must_be_single_path=True but given too many paths:'
                                           f' {derived_list}')
                    path_uid = self.backend.get_uid_for_local_path(derived_list[0])
                    return GDriveSPID(uid=node_uid, device_uid=device_uid, path_uid=path_uid, full_path=derived_list[0])
                if not derived_list or not derived_list[0]:
                    return self.backend.node_identifier_factory.get_root_constant_gdrive_identifier(device_uid)
                assert node_uid, f'Null value for node_uid!'
                assert device_uid, f'Null value for device_uid!'
                return GDriveIdentifier(path_list=derived_list, uid=node_uid, device_uid=device_uid)
            else:
                # LocalDisk

                if not node_uid:
                    node_uid = self.backend.get_uid_for_local_path(full_path_list[0])

                return LocalNodeIdentifier(uid=node_uid, device_uid=device_uid, full_path=full_path_list[0])
        else:
            raise RuntimeError('Neither tree_type nor full_path supplied for GDriveIdentifier!')

    def _for_tree_type_local(self, device_uid: UID, full_path_list: Optional[List[str]] = None, node_uid: UID = None) -> LocalNodeIdentifier:
        if node_uid and full_path_list:
            return LocalNodeIdentifier(uid=node_uid, device_uid=device_uid, full_path=full_path_list[0])

        if full_path_list:
            node_uid = self.backend.get_uid_for_local_path(full_path_list[0], node_uid)

            return LocalNodeIdentifier(uid=node_uid, device_uid=device_uid, full_path=full_path_list[0])
        elif node_uid:
            node = self.backend.get_node_for_uid(node_uid, device_uid)
            if node:
                full_path_list = node.get_path_list()
                return LocalNodeIdentifier(uid=node_uid, device_uid=device_uid, full_path=full_path_list[0])
        else:
            raise RuntimeError('Neither "uid" nor "full_path" supplied for LocalNodeIdentifier!')

    def _for_tree_type_gdrive(self, device_uid: UID, full_path_list: Optional[List[str]] = None, node_uid: UID = None, path_uid: Optional[UID] = None,
                              must_be_single_path: bool = False) \
            -> Union[GDriveIdentifier, SinglePathNodeIdentifier]:
        if not node_uid:
            if full_path_list and full_path_list[0] == ROOT_PATH:
                node_uid = GDRIVE_ROOT_UID
            else:
                node_uid = self.backend.next_uid()
        elif node_uid == GDRIVE_ROOT_UID and not full_path_list:
            full_path_list = [ROOT_PATH]

        if must_be_single_path:
            if len(full_path_list) > 1:
                raise RuntimeError(f'Could not make identifier: must_be_single_path=True but given too many paths: {full_path_list}')
            if not path_uid:
                path_uid = self.backend.get_uid_for_local_path(full_path_list[0])
            return GDriveSPID(uid=node_uid, device_uid=device_uid, path_uid=path_uid, full_path=full_path_list[0])
        return GDriveIdentifier(uid=node_uid, device_uid=device_uid, path_list=full_path_list)

