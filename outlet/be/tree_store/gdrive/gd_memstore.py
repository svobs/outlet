import logging
from typing import Dict, List, Optional, Tuple

from be.tree_store.cache_write_op import NodeUpdateInfo, NodeUpdateInfoFactory
from be.tree_store.gdrive.gd_tree import GDriveWholeTree
from be.uid.uid_mapper import UidGoogIdMapper
from constants import GDRIVE_FOLDER_MIME_TYPE_UID, GDRIVE_ME_USER_UID
from logging_constants import SUPER_DEBUG_ENABLED, TRACE_ENABLED
from model.gdrive_meta import GDriveUser, MimeType
from model.node.gdrive_node import GDriveFolder, GDriveNode
from model.uid import UID

logger = logging.getLogger(__name__)


class GDriveMemoryStore:
    """
    ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
    CLASS GDriveMemoryStore
    ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼
    """
    def __init__(self, backend, uid_mapper: UidGoogIdMapper, device_uid: UID):
        self.backend = backend
        self.master_tree: Optional[GDriveWholeTree] = None
        self._uid_mapper: UidGoogIdMapper = uid_mapper
        self.device_uid: UID = device_uid

        self._mime_type_for_str_dict: Dict[str, MimeType] = {}
        self._mime_type_for_uid_dict: Dict[UID, MimeType] = {}
        self._mime_type_uid_nextval: int = GDRIVE_FOLDER_MIME_TYPE_UID + 1

        self._user_for_permission_id_dict: Dict[str, GDriveUser] = {}
        self._user_for_uid_dict: Dict[UID, GDriveUser] = {}
        self._user_uid_nextval: int = GDRIVE_ME_USER_UID + 1

    def is_loaded(self) -> bool:
        return self.master_tree is not None

    def upsert_single_node(self, node: GDriveNode, update_only: bool = False) -> NodeUpdateInfo:

        if SUPER_DEBUG_ENABLED:
            logger.debug(f'Upserting to memstore: {node}')

        assert self.master_tree

        # Validate UID:
        if node.goog_id:
            uid_from_mapper = self._uid_mapper.get_uid_for_goog_id(goog_id=node.goog_id)
            if node.uid != uid_from_mapper:
                logger.warning(f'Found node in cache with same GoogID ({node.goog_id}) but different UID ('
                               f'{uid_from_mapper}). Changing UID of node (was: {node.uid}) to match and overwrite previous node')
                node.uid = uid_from_mapper

        self.backend.cacheman.update_node_icon(node)
        if TRACE_ENABLED:
            logger.debug(f'TNode {node.device_uid}:{node.uid} has icon: {node.get_icon().name}, custom_icon: {node.get_custom_icon()}')

        cached_node: GDriveNode = self.master_tree.get_node_for_uid(node.uid)
        if cached_node:
            if cached_node.is_dir() and not node.is_dir():
                # This should never happen, because GDrive does not allow a node's type to be changed:
                raise RuntimeError(f'Invalid request: cannot replace a GDrive folder with a file: "{node.get_path_list()}"')

            # it is ok if we have an existing node which doesn't have a goog_id; that will be replaced
            if cached_node.goog_id and cached_node.goog_id != node.goog_id:
                raise RuntimeError(f'Serious error: cache already contains UID {node.uid} but Google ID does not match '
                                   f'(existing="{cached_node.goog_id}"; new="{node.goog_id}")')

            if cached_node.is_live() and not node.is_live():
                if cached_node.get_icon() != node.get_icon():
                    cached_node.set_icon(node.get_icon())
                    logger.info(f'Will not overwrite existing node with non-existing, but will copy its icon: {node}')
                    return NodeUpdateInfo(node, False, True)

                # In the future, let's close this hole with more elegant logic
                logger.info(f'Will not replace a node which exists with one which does not exist; ignoring: {node}')
                return NodeUpdateInfoFactory.no_update()

            if cached_node == node:
                logger.debug(f'TNode is identical to cached node; skipping memstore update (CachedNode={cached_node}; NewNode={node}')
                if SUPER_DEBUG_ENABLED:
                    logger.debug(f'Existing node: {cached_node}')
                return NodeUpdateInfoFactory.no_update()

            if SUPER_DEBUG_ENABLED:
                logger.debug(f'Merging node UID={cached_node.uid} into cached node')

            if cached_node.is_dir() and node.is_dir():
                assert isinstance(cached_node, GDriveFolder)
                if cached_node.all_children_fetched and not node.all_children_fetched:
                    if TRACE_ENABLED:
                        logger.debug(f'Merging into existing node which has all_children_fetched=True; will set new node to True')
                    node.all_children_fetched = True
                elif not cached_node.all_children_fetched and node.all_children_fetched:
                    logger.debug(f'Overwriting node with all_children_fetched=False with one which is True: {node}')

        elif update_only:
            if SUPER_DEBUG_ENABLED:
                logger.debug(f'Skipping update for node because it is not in the memory cache: {node}')
            return NodeUpdateInfoFactory.no_update()

        # Finally, update in-memory cache (tree). If an existing node is found with the same UID, it will update and return that instead:
        # FIXME: determine if nodes were removed from parents. If so, send notifications to ATM
        node = self.master_tree.upsert_node(node)

        return NodeUpdateInfo(node, True, True)

    def remove_single_node(self, node: GDriveNode, to_trash: bool = False):
        """Note: this is not allowed for non-empty directories."""
        if SUPER_DEBUG_ENABLED:
            logger.debug(f'Removing GDriveNode from memory cache: {node}')

        if to_trash:
            # TODO
            raise RuntimeError(f'Not supported: to_trash=true!')

        if node.is_dir():
            children: List[GDriveNode] = self.master_tree.get_child_list_for_node(node)
            if children:
                raise RuntimeError(f'Cannot remove GDrive folder from cache: it contains {len(children)} children!')

        cached_node = self.master_tree.get_node_for_uid(node.uid)
        if cached_node:
            self.master_tree.remove_node(cached_node)

    # Meta operations:

    def replace_all_users(self, user_list: List[GDriveUser]):
        self.delete_all_users()

        for user in user_list:
            if user.uid > self._user_uid_nextval:
                self._user_uid_nextval = user.uid + 1
            self._user_for_permission_id_dict[user.permission_id] = user
            self._user_for_uid_dict[user.uid] = user

    def upsert_user(self, user: GDriveUser):
        if not user.permission_id:
            raise RuntimeError(f'User is missing permission_id: {user}')
        existing_user = self._user_for_permission_id_dict.get(user.permission_id, None)
        if existing_user:
            existing_user.update_from(user)
            if user.uid and user.uid != existing_user.uid:
                raise RuntimeError(f'upsert_user(): user being inserted has unexpected UID! (UID={user.uid}; expected={existing_user.uid})')
            else:
                user.uid = existing_user.uid
        else:
            self.create_user(user)
            if user.uid > self._user_uid_nextval:
                self._user_uid_nextval = user.uid + 1
            self._user_for_permission_id_dict[user.permission_id] = user
            self._user_for_uid_dict[user.uid] = user
    
    def create_user(self, user: GDriveUser):
        if user.uid:
            raise RuntimeError(f'create_gdrive_user(): user already has UID! (UID={user.uid})')
        if user.is_me:
            if not user.uid:
                user.uid = GDRIVE_ME_USER_UID
            elif user.uid != GDRIVE_ME_USER_UID:
                raise RuntimeError(f'create_gdrive_user(): cannot set is_me=true AND UID={user.uid}')

        user_from_permission_id = self._user_for_permission_id_dict.get(user.permission_id, None)
        if user_from_permission_id:
            assert user_from_permission_id.permission_id == user.permission_id and user_from_permission_id.uid
            user.uid = user_from_permission_id.uid
            return
        if not user.is_me:
            user.uid = UID(self._user_uid_nextval)

        if not user.is_me:
            self._user_uid_nextval += 1
        self._user_for_permission_id_dict[user.permission_id] = user
        self._user_for_uid_dict[user.uid] = user

    def get_gdrive_user_for_permission_id(self, permission_id: str) -> GDriveUser:
        return self._user_for_permission_id_dict.get(permission_id, None)

    def get_gdrive_user_for_user_uid(self, uid: UID) -> GDriveUser:
        return self._user_for_uid_dict.get(uid, None)

    def replace_all_mime_types(self, mime_type_list: List[MimeType]):
        self.delete_all_mime_types()

        for mime_type in mime_type_list:
            if mime_type.uid > self._mime_type_uid_nextval:
                self._mime_type_uid_nextval = mime_type.uid + 1
            self._mime_type_for_str_dict[mime_type.type_string] = mime_type
            self._mime_type_for_uid_dict[mime_type.uid] = mime_type

    def get_mime_type_for_uid(self, uid: UID) -> Optional[MimeType]:
        return self._mime_type_for_uid_dict.get(uid, None)

    def get_or_create_mime_type(self, mime_type_string: str) -> Tuple[MimeType, bool]:
        mime_type: Optional[MimeType] = self._mime_type_for_str_dict.get(mime_type_string, None)
        if mime_type:
            is_new = False
        else:
            is_new = True
            mime_type = MimeType(UID(self._mime_type_uid_nextval), mime_type_string)
            self._mime_type_uid_nextval += 1
            self._mime_type_for_str_dict[mime_type_string] = mime_type
            self._mime_type_for_uid_dict[mime_type.uid] = mime_type
        return mime_type, is_new

    def delete_all_mime_types(self):
        self._mime_type_for_str_dict.clear()
        self._mime_type_for_uid_dict.clear()
        self._mime_type_uid_nextval = GDRIVE_FOLDER_MIME_TYPE_UID + 1

    def delete_all_users(self):
        self._user_for_permission_id_dict.clear()
        self._user_for_uid_dict.clear()
        self._user_uid_nextval = GDRIVE_ME_USER_UID + 1

    def delete_all_gdrive_data(self):
        self.delete_all_mime_types()
        self.delete_all_users()
