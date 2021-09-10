import logging
from typing import Dict, Optional

import dateutil.parser

from constants import TrashStatus
from model.gdrive_meta import GDriveUser, MimeType
from model.node.gdrive_node import GDriveFile, GDriveFolder
from model.node_identifier import GDriveIdentifier
from model.uid import UID
from util import time_util

logger = logging.getLogger(__name__)


class GDriveAPIConverter:
    def __init__(self, gdrive_store):
        self._gdrive_store = gdrive_store

    @staticmethod
    def _convert_trashed(result) -> Optional[TrashStatus]:
        x_trashed = result.get('explicitlyTrashed', None)
        trashed = result.get('trashed', None)
        if x_trashed is None and trashed is None:
            return None

        if x_trashed:
            return TrashStatus.EXPLICITLY_TRASHED
        elif trashed:
            return TrashStatus.IMPLICITLY_TRASHED
        else:
            return TrashStatus.NOT_TRASHED

    @staticmethod
    def _parse_gdrive_date(result, field_name) -> Optional[int]:
        timestamp = result.get(field_name, None)
        if timestamp:
            timestamp = dateutil.parser.parse(timestamp)
            timestamp = int(timestamp.timestamp() * 1000)
        return timestamp

    def get_or_store_user(self, user: Dict) -> GDriveUser:
        permission_id = user.get('permissionId', None)
        gdrive_user: Optional[GDriveUser] = self._gdrive_store.get_gdrive_user_for_permission_id(permission_id)
        if not gdrive_user:
            # Completely new user
            user_name = user.get('displayName', None)
            user_email = user.get('emailAddress', None)
            user_photo_link = user.get('photoLink', None)
            user_is_me = user.get('me', None)
            gdrive_user: GDriveUser = GDriveUser(display_name=user_name, permission_id=permission_id, email_address=user_email,
                                                 photo_link=user_photo_link, is_me=user_is_me)
            self._gdrive_store.create_gdrive_user(gdrive_user)
        return gdrive_user

    def dict_to_gdrive_folder(self, item: Dict, sync_ts: int = 0, uid: UID = None) -> GDriveFolder:
        # 'driveId' only populated for items which someone has shared with me

        if not sync_ts:
            sync_ts = time_util.now_sec()

        goog_id = item['id']
        uid = self._gdrive_store.get_uid_for_goog_id(goog_id, uid_suggestion=uid)

        owners = item.get('owners', None)
        if owners:
            user = self.get_or_store_user(owners[0])
            owner_uid = user.uid
        else:
            owner_uid = None

        sharing_user = item.get('sharingUser', None)
        if sharing_user:
            user = self.get_or_store_user(sharing_user)
            sharing_user_uid = user.uid
        else:
            sharing_user_uid = None

        create_ts = GDriveAPIConverter._parse_gdrive_date(item, 'createdTime')

        modify_ts = GDriveAPIConverter._parse_gdrive_date(item, 'modifiedTime')

        goog_node = GDriveFolder(GDriveIdentifier(uid=uid, device_uid=self._gdrive_store.device_uid, path_list=None), goog_id=goog_id, node_name=item['name'],
                                 trashed=GDriveAPIConverter._convert_trashed(item), create_ts=create_ts, modify_ts=modify_ts, owner_uid=owner_uid,
                                 drive_id=item.get('driveId', None), is_shared=item.get('shared', None), shared_by_user_uid=sharing_user_uid,
                                 sync_ts=sync_ts, all_children_fetched=False)

        parent_goog_ids = item.get('parents', [])
        parent_uids = self._gdrive_store.get_uid_list_for_goog_id_list(parent_goog_ids)
        goog_node.set_parent_uids(parent_uids)

        return goog_node

    def dict_to_gdrive_file(self, item: Dict, sync_ts: int = 0, uid: UID = None) -> GDriveFile:
        if not sync_ts:
            sync_ts = time_util.now_sec()

        owners = item.get('owners', None)
        if owners:
            user = self.get_or_store_user(owners[0])
            owner_uid = user.uid
        else:
            owner_uid = None

        sharing_user = item.get('sharingUser', None)
        if sharing_user:
            user = self.get_or_store_user(sharing_user)
            sharing_user_uid = user.uid
        else:
            sharing_user_uid = None

        create_ts = GDriveAPIConverter._parse_gdrive_date(item, 'createdTime')

        modify_ts = GDriveAPIConverter._parse_gdrive_date(item, 'modifiedTime')

        size_str = item.get('size', None)
        size = None if size_str is None else int(size_str)
        version = item.get('version', None)
        mime_type_string = item.get('mimeType', None)
        mime_type: MimeType = self._gdrive_store.get_or_create_gdrive_mime_type(mime_type_string)

        goog_id = item['id']

        uid = self._gdrive_store.get_uid_for_goog_id(goog_id, uid_suggestion=uid)
        assert isinstance(uid, UID), f'Not a UID: {uid}'
        goog_node: GDriveFile = GDriveFile(node_identifier=GDriveIdentifier(uid=uid, device_uid=self._gdrive_store.device_uid, path_list=None),
                                           goog_id=goog_id, node_name=item["name"],
                                           mime_type_uid=mime_type.uid, trashed=GDriveAPIConverter._convert_trashed(item),
                                           drive_id=item.get('driveId', None), version=version,
                                           md5=item.get('md5Checksum', None), is_shared=item.get('shared', None), create_ts=create_ts,
                                           modify_ts=modify_ts, size_bytes=size, shared_by_user_uid=sharing_user_uid, owner_uid=owner_uid,
                                           sync_ts=sync_ts)

        parent_goog_ids = item.get('parents', [])
        parent_uids = self._gdrive_store.get_uid_list_for_goog_id_list(parent_goog_ids)
        goog_node.set_parent_uids(parent_uids)

        return goog_node
