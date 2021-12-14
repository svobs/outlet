import logging
from typing import List, Optional, Tuple, Union

from constants import GDRIVE_ROOT_UID, GRPC_CHANGE_TREE_NO_OP, LOCAL_ROOT_UID, ROOT_PATH, ROOT_PATH_UID, \
    SUPER_ROOT_DEVICE_UID, SUPER_ROOT_UID, \
    TreeType
from model.node_identifier import ChangeTreeSPID, GDriveIdentifier, GDriveSPID, GUID, LocalNodeIdentifier, MixedTreeSPID, NodeIdentifier, \
    SinglePathNodeIdentifier
from model.uid import UID
from model.user_op import UserOpType
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
        if self._get_tree_type_for_device_uid(device_uid) != TreeType.GDRIVE:
            raise RuntimeError(f'Cannot get GDrive root: device UID {device_uid} is not GDrive!')
        return GDriveIdentifier(uid=GDRIVE_ROOT_UID, device_uid=device_uid, path_list=ROOT_PATH)

    def get_device_root_spid(self, device_uid: UID) -> SinglePathNodeIdentifier:
        tree_type = self._get_tree_type_for_device_uid(device_uid)
        return NodeIdentifierFactory.get_root_constant_spid(tree_type=tree_type, device_uid=device_uid)

    @staticmethod
    def get_root_constant_spid(tree_type: TreeType, device_uid: UID) -> SinglePathNodeIdentifier:
        if tree_type == TreeType.GDRIVE:
            return NodeIdentifierFactory.get_root_constant_gdrive_spid(device_uid)

        if tree_type == TreeType.LOCAL_DISK:
            return NodeIdentifierFactory.get_root_constant_local_disk_spid(device_uid)

        if tree_type == TreeType.MIXED:
            assert device_uid == SUPER_ROOT_DEVICE_UID, f'Expected {SUPER_ROOT_DEVICE_UID} for MixedTreeSPID device_uid but found: {device_uid}'
            return MixedTreeSPID(node_uid=SUPER_ROOT_UID, device_uid=device_uid, path_uid=ROOT_PATH_UID, full_path=ROOT_PATH)

        raise RuntimeError(f'get_root_constant_spid(): invalid tree type: {tree_type}')

    @staticmethod
    def get_root_constant_gdrive_spid(device_uid: UID) -> SinglePathNodeIdentifier:
        return GDriveSPID(node_uid=GDRIVE_ROOT_UID, device_uid=device_uid, path_uid=ROOT_PATH_UID, full_path=ROOT_PATH)

    @staticmethod
    def get_root_constant_local_disk_spid(device_uid: UID) -> SinglePathNodeIdentifier:
        return LocalNodeIdentifier(uid=LOCAL_ROOT_UID, device_uid=device_uid, full_path=ROOT_PATH)

    def from_guid(self, guid: GUID) -> SinglePathNodeIdentifier:
        """
        BACKEND ONLY! Returns a SPID corresponding to the given GUID.
        Obvioiusly GUIDs are ineffcient to use, but in practice the BE will only use them to record user selection & expanded nodes.
        (GUIDs are also used by the Mac frontend)
        """
        # FIXME: this doesn't work for ChangeTreeSPID
        uid_list = guid.split(':')
        if len(uid_list) < 2:
            raise RuntimeError(f'Invalid GUID: "{guid}"')

        device_uid = UID(uid_list[0])
        node_uid = UID(uid_list[1])

        tree_type = self._get_tree_type_for_device_uid(device_uid)

        if tree_type == TreeType.LOCAL_DISK:
            full_path = self.backend.cacheman.get_path_for_uid(node_uid)
            return LocalNodeIdentifier(node_uid, device_uid=device_uid, full_path=full_path)
        elif tree_type == TreeType.GDRIVE:
            if len(uid_list) != 3:
                raise RuntimeError(f'Tree type ({tree_type.name}) does not contain path_uid!')
            path_uid = UID(uid_list[2])
            full_path = self.backend.cacheman.get_path_for_uid(path_uid)
            return GDriveSPID(node_uid=node_uid, device_uid=device_uid, path_uid=path_uid, full_path=full_path)
        elif tree_type == TreeType.MIXED:
            if len(uid_list) != 3:
                raise RuntimeError(f'Tree type ({tree_type.name}) does not contain path_uid!')
            path_uid = UID(uid_list[2])
            full_path = self.backend.cacheman.get_path_for_uid(path_uid)
            return MixedTreeSPID(node_uid=node_uid, device_uid=device_uid, path_uid=path_uid, full_path=full_path)
        else:
            raise RuntimeError(f'Invalid tree_type: {tree_type.name}')

    def for_values(self,
                   device_uid: UID,
                   path_list: Optional[Union[str, List[str]]] = None,
                   uid: Optional[UID] = None,
                   path_uid: Optional[UID] = None,
                   op_type: Optional[int] = None,
                   must_be_single_path: bool = False,
                   parent_guid: Optional[GUID] = None) -> NodeIdentifier:
        """Big factory method for creating a new identifier (for example when you intend to create a new node.
        May be called either from FE or BE. For FE, it may be quite slow due to network overhead."""

        uid = ensure_uid(uid)
        device_uid = ensure_uid(device_uid)
        full_path_list = ensure_list(path_list)

        # we may be coming from gRPC
        parent_guid = None if parent_guid == "" else parent_guid

        if not device_uid:
            raise RuntimeError('No device_uid provided!')

        tree_type = self._get_tree_type_for_device_uid(device_uid)

        if op_type:
            # ChangeTreeSPID (we must be coming from gRPC)
            if op_type == GRPC_CHANGE_TREE_NO_OP:
                op_type = None
            else:
                op_type = UserOpType(op_type)
            return ChangeTreeSPID(path_uid=path_uid, device_uid=device_uid, full_path=path_list, op_type=op_type, parent_guid=parent_guid)

        if tree_type == TreeType.LOCAL_DISK:
            return self._for_tree_type_local(device_uid, full_path_list, uid, parent_guid)

        elif tree_type == TreeType.GDRIVE:
            return self._for_tree_type_gdrive(device_uid, full_path_list, uid, path_uid, must_be_single_path, parent_guid)

        elif tree_type == TreeType.MIXED:
            logger.debug(f'Creating a node identifier of type MIXED for uid={uid}, device_uid={device_uid}, path={full_path_list}, '
                         f'parent_guid={parent_guid}')
            if len(full_path_list) > 1:
                raise RuntimeError(f'Too many paths for tree_type MIXED: {full_path_list}')
            if not path_uid:
                path_uid = self.backend.get_uid_for_local_path(full_path_list[0])
            if device_uid != SUPER_ROOT_DEVICE_UID:
                raise RuntimeError(f'Invalid device_uid for TreeType MIXED: expected {SUPER_ROOT_DEVICE_UID} but found: {device_uid}')
            return MixedTreeSPID(node_uid=uid, device_uid=device_uid, path_uid=path_uid, full_path=full_path_list[0], parent_guid=parent_guid)
        else:
            raise RuntimeError('bad')

    def _get_tree_type_for_device_uid(self, device_uid: UID) -> TreeType:
        if device_uid == SUPER_ROOT_DEVICE_UID:
            return TreeType.MIXED

        for device in self.backend.get_device_list():
            if device.uid == device_uid:
                return device.tree_type

        raise RuntimeError(f'Could not find device with UID: {device_uid}')

    def _for_tree_type_local(self, device_uid: UID, full_path_list: Optional[List[str]] = None, node_uid: Optional[UID] = None,
                             parent_guid: Optional[GUID] = None) -> LocalNodeIdentifier:
        if full_path_list:
            if not node_uid:
                node_uid = self.backend.get_uid_for_local_path(full_path_list[0])

            return LocalNodeIdentifier(uid=node_uid, device_uid=device_uid, full_path=full_path_list[0], parent_guid=parent_guid)
        elif node_uid:
            node = self.backend.get_node_for_uid(node_uid, device_uid)
            if node:
                full_path_list = node.get_path_list()
                return LocalNodeIdentifier(uid=node_uid, device_uid=device_uid, full_path=full_path_list[0], parent_guid=parent_guid)
        else:
            raise RuntimeError('Neither "uid" nor "full_path" supplied for LocalNodeIdentifier!')

    def _for_tree_type_gdrive(self, device_uid: UID, full_path_list: Optional[List[str]] = None, node_uid: UID = None, path_uid: Optional[UID] = None,
                              must_be_single_path: bool = False, parent_guid: Optional[GUID] = None) \
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
            full_path = full_path_list[0]
            if not full_path:
                raise RuntimeError(f'Could not make identifier: must_be_single_path=True but full_path is empty ({full_path})')
            if not path_uid:
                path_uid = self.backend.get_uid_for_local_path(full_path_list[0])
            return GDriveSPID(node_uid=node_uid, device_uid=device_uid, path_uid=path_uid, full_path=full_path_list[0], parent_guid=parent_guid)
        return GDriveIdentifier(uid=node_uid, device_uid=device_uid, path_list=full_path_list)
