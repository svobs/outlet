import logging
from typing import List, Optional

from constants import ChangeTreeCategory, GDRIVE_ROOT_UID, LOCAL_ROOT_UID, NodeIdentifierType, ROOT_PATH, ROOT_PATH_UID, \
    SUPER_ROOT_DEVICE_UID, SUPER_ROOT_UID, \
    TreeType
from model.node_identifier import ChangeTreeSPID, GDriveIdentifier, GDriveSPID, GUID, LocalNodeIdentifier, MixedTreeSPID, NodeIdentifier, \
    SinglePathNodeIdentifier
from model.uid import UID
from model.user_op import ChangeTreeCategoryMeta
from util.ensure import ensure_uid

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

    def _get_tree_type_for_device_uid(self, device_uid: UID) -> TreeType:
        if device_uid == SUPER_ROOT_DEVICE_UID:
            return TreeType.MIXED

        for device in self.backend.get_device_list():
            if device.uid == device_uid:
                return device.tree_type

        raise RuntimeError(f'Could not find device with UID: {device_uid}')

    def _derive_spid_type_from_device_uid(self, device_uid: UID) -> NodeIdentifierType:
        if not device_uid:
            raise RuntimeError('No device_uid provided!')

        tree_type = self._get_tree_type_for_device_uid(device_uid)
        if tree_type == TreeType.LOCAL_DISK:
            return NodeIdentifierType.LOCAL_DISK_SPID
        elif tree_type == TreeType.GDRIVE:
            return NodeIdentifierType.GDRIVE_SPID
        elif tree_type == TreeType.MIXED:
            return NodeIdentifierType.MIXED_TREE_SPID
        else:
            raise RuntimeError(f'Could not derive identifier_type for device_uid ({device_uid})!')

    def _derive_identifier_type_from_device_uid(self, device_uid: UID) -> NodeIdentifierType:
        """This favors MPIDs over SPIDs."""
        if not device_uid:
            raise RuntimeError('No device_uid provided!')

        tree_type = self._get_tree_type_for_device_uid(device_uid)
        if tree_type == TreeType.LOCAL_DISK:
            return NodeIdentifierType.LOCAL_DISK_SPID
        elif tree_type == TreeType.GDRIVE:
            return NodeIdentifierType.GDRIVE_MPID
        elif tree_type == TreeType.MIXED:
            return NodeIdentifierType.MIXED_TREE_SPID
        else:
            raise RuntimeError(f'Could not derive identifier_type for device_uid ({device_uid})!')

    def from_guid(self, guid: GUID) -> SinglePathNodeIdentifier:
        """
        BACKEND ONLY! Returns a SPID corresponding to the given GUID.
        Any SPID is derivable from its GUID (assuming access to the CacheMan for secondary fields like full_path).
        Obvioiusly GUIDs are ineffcient to use, but in practice the BE will only use them to record user selection & expanded nodes.
        (GUIDs are also used by the Mac frontend)
        """
        uid_list = guid.split(':')
        if len(uid_list) < 2:
            raise RuntimeError(f'Invalid GUID: not enough segments "{guid}"')

        device_uid = UID(uid_list[0])

        if len(uid_list) == 3:
            if uid_list[1].isdigit():
                # Is GDriveSPID or MixedSPID
                node_uid = UID(uid_list[1])
                path_uid = UID(uid_list[2])
            else:
                # Is ChangeTreeSPID
                change_tree_category = ChangeTreeCategoryMeta.category_for_name(uid_list[1])
                if not change_tree_category:
                    raise RuntimeError(f'Invalid GUID (could not parse category name): "{guid}"')
                path_uid = UID(uid_list[2])
                full_path = self.backend.cacheman.get_path_for_uid(path_uid)

                # Derive parent GUID (not too hard):
                parent_path = self.backend.cacheman.derive_parent_path(full_path)
                parent_path_uid = self.backend.cacheman.get_uid_for_local_path(parent_path)
                parent_guid = f'{uid_list[0]}:{uid_list[1]:{parent_path_uid}}'

                return ChangeTreeSPID(path_uid=path_uid, device_uid=device_uid, full_path=full_path,
                                      category=change_tree_category, parent_guid=parent_guid)
        else:  # len(uid_list) == 2
            # Is LocalNodeIdentifier
            node_uid = UID(uid_list[1])
            path_uid = node_uid

        try:
            full_path = self.backend.cacheman.get_path_for_uid(path_uid)
        except RuntimeError as err:
            logger.error(f'from_guid(): get_path_for_uid() returned exception: {repr(err)}')
            raise RuntimeError(f'Failed to resolve node_uid ({node_uid}) for GUID ({guid})')

        tree_type = self._get_tree_type_for_device_uid(device_uid)

        if tree_type == TreeType.LOCAL_DISK:
            return LocalNodeIdentifier(node_uid, device_uid=device_uid, full_path=full_path)
        elif tree_type == TreeType.GDRIVE:
            if len(uid_list) != 3:
                raise RuntimeError(f'Tree type ({tree_type.name}) does not contain path_uid!')
            return GDriveSPID(node_uid=node_uid, device_uid=device_uid, path_uid=path_uid, full_path=full_path)
        elif tree_type == TreeType.MIXED:
            if len(uid_list) != 3:
                raise RuntimeError(f'Tree type ({tree_type.name}) does not contain path_uid!')
            return MixedTreeSPID(node_uid=node_uid, device_uid=device_uid, path_uid=path_uid, full_path=full_path)
        else:
            raise RuntimeError(f'Invalid tree_type: {tree_type.name}')

    def build_node_id(self,
                      node_uid: UID,
                      device_uid: UID,
                      identifier_type: Optional[NodeIdentifierType] = None,
                      path_list: List[str] = None
                      ) -> NodeIdentifier:
        node_uid = ensure_uid(node_uid)
        if node_uid is None:  # although ==0 is allowed for legacy behavior
            raise RuntimeError('No node_uid provided!')

        device_uid = ensure_uid(device_uid)
        if not device_uid:
            raise RuntimeError('No device_uid provided!')

        if not identifier_type:
            identifier_type = self._derive_identifier_type_from_device_uid(device_uid)

        if identifier_type == NodeIdentifierType.GDRIVE_MPID:
            return GDriveIdentifier(uid=node_uid, device_uid=device_uid, path_list=path_list)
        elif not path_list or len(path_list) == 1:
            single_path = None if not path_list else path_list[0]
            return self.build_spid(node_uid=node_uid, device_uid=device_uid, identifier_type=identifier_type, single_path=single_path)
        else:
            raise RuntimeError(f'Invalid multi-path identifier type: {identifier_type}')

    def build_spid(self,
                   node_uid: UID,
                   device_uid: UID,
                   identifier_type: Optional[NodeIdentifierType] = None,  # Required for CategorySPIDs. Else will be derived from device_uid
                   single_path: Optional[str] = None,  # either single_path or path_uid (or both) must be provided
                   path_uid: Optional[UID] = None,  # either single_path or path_uid (or both) must be provided
                   parent_guid: Optional[GUID] = None
                   ) -> SinglePathNodeIdentifier:

        device_uid = ensure_uid(device_uid)
        if not device_uid:
            raise RuntimeError('build_spid(): No device_uid provided!')

        node_uid = ensure_uid(node_uid)
        if node_uid is None:  # although ==0 is allowed for legacy behavior
            raise RuntimeError('build_spid(): No node_uid provided!')

        if not identifier_type:
            identifier_type = self._derive_spid_type_from_device_uid(device_uid)

        if not path_uid or not single_path:
            if not path_uid and single_path:
                path_uid = self.backend.cacheman.get_uid_for_local_path(single_path)
            elif path_uid and not single_path:
                single_path = self.backend.cacheman.get_path_for_uid(path_uid)
            elif identifier_type == NodeIdentifierType.LOCAL_DISK_SPID:
                path_uid = node_uid
                single_path = self.backend.cacheman.get_path_for_uid(path_uid)
            else:
                raise RuntimeError('build_spid(): Neither path_uid nor single_path provided!')

        # We may be coming from gRPC
        if parent_guid == "":
            parent_guid = None

        if identifier_type == NodeIdentifierType.LOCAL_DISK_SPID:
            return LocalNodeIdentifier(uid=node_uid, device_uid=device_uid, full_path=single_path)
        elif identifier_type == NodeIdentifierType.GDRIVE_SPID:
            return GDriveSPID(node_uid=node_uid, device_uid=device_uid, path_uid=path_uid, full_path=single_path, parent_guid=parent_guid)
        elif identifier_type == NodeIdentifierType.MIXED_TREE_SPID:
            if device_uid != SUPER_ROOT_DEVICE_UID:
                raise RuntimeError(f'Invalid device_uid for IdentifierType MIXED_TREE_SPID: expected {SUPER_ROOT_DEVICE_UID} but found: {device_uid}')
            return MixedTreeSPID(node_uid=node_uid, device_uid=device_uid, path_uid=path_uid, full_path=single_path, parent_guid=parent_guid)
        else:
            # must be ChangeTreeSPID for some category
            try:
                category = ChangeTreeCategory(identifier_type)
            except (KeyError, TypeError):
                raise RuntimeError(f'Invalid SPID identifier type: {identifier_type}')

            return ChangeTreeSPID(path_uid=path_uid, device_uid=device_uid, full_path=single_path, category=category, parent_guid=parent_guid)
