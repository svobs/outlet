import io
import logging
import os.path
import pickle
import socket
import time
from typing import Callable, Dict, List, Optional, Tuple, Union

import dateutil.parser
import humanfriendly
from google.auth.transport.requests import Request
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build, Resource
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaFileUpload, MediaIoBaseDownload
from pydispatch import dispatcher

from backend.tree_store.gdrive.query_observer import GDriveQueryObserver, SimpleNodeCollector
from constants import GDRIVE_AUTH_SCOPES, GDRIVE_CLIENT_REQUEST_MAX_RETRIES, GDRIVE_CLIENT_SLEEP_ON_FAILURE_SEC, GDRIVE_FILE_FIELDS, \
    GDRIVE_FOLDER_FIELDS, \
    GDRIVE_MY_DRIVE_ROOT_GOOG_ID, MIME_TYPE_FOLDER, QUERY_FOLDERS_ONLY, QUERY_NON_FOLDERS_ONLY, SUPER_DEBUG_ENABLED, TrashStatus, \
    TreeID
from backend.tree_store.gdrive.change_observer import GDriveChangeObserver, GDriveNodeChange, GDriveRM
from model.uid import UID
from model.gdrive_meta import GDriveUser, MimeType
from model.node.gdrive_node import GDriveFile, GDriveFolder, GDriveNode
from model.node_identifier import GDriveIdentifier
from signal_constants import Signal
from util import file_util, time_util
from util.has_lifecycle import HasLifecycle
from util.stopwatch_sec import Stopwatch

logger = logging.getLogger(__name__)


class MemoryCache:
    """
    ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
    CLASS MemoryCache

    Workaround for bug in Google code:
    See: https://github.com/googleapis/google-api-python-client/issues/325#issuecomment-274349841
    ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼
    """
    _CACHE = {}

    def get(self, url):
        return MemoryCache._CACHE.get(url)

    def set(self, url, content):
        MemoryCache._CACHE[url] = content


class GDriveClient(HasLifecycle):
    """
    ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
    CLASS GDriveClient
    ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼
    """
    def __init__(self, backend, gdrive_store, device_uid: UID, tree_id=None):
        HasLifecycle.__init__(self)
        self.backend = backend
        self.gdrive_store = gdrive_store
        self.device_uid: UID = device_uid
        self.tree_id: TreeID = tree_id
        self.page_size: int = self.backend.get_config('gdrive.page_size')
        self.service: Optional[Resource] = None

    def start(self):
        logger.debug(f'Starting GDriveClient')
        HasLifecycle.start(self)

        # The file token.pickle stores the user's access and refresh tokens, and is
        # created automatically when the authorization flow completes for the first
        # time.
        token_file_path = file_util.get_resource_path(self.backend.get_config('gdrive.auth.token_file_path'))
        creds_file_path = file_util.get_resource_path(self.backend.get_config('gdrive.auth.credentials_file_path'))
        self.service = GDriveClient._load_google_client_service(token_file_path, creds_file_path)

    def shutdown(self):
        HasLifecycle.shutdown(self)
        
        if self.service:
            self.service = None

    @staticmethod
    def _load_google_client_service(token_file_path: str, creds_file_path: str):
        def request():
            logger.debug('Trying to authenticate against GDrive API...')
            creds = None
            if os.path.exists(token_file_path):
                with open(token_file_path, 'rb') as token:
                    creds = pickle.load(token)
            # If there are no (valid) credentials available, let the user log in.
            if not creds or not creds.valid:
                if creds and creds.expired and creds.refresh_token:
                    creds.refresh(Request())
                else:
                    if not os.path.exists(creds_file_path):
                        raise RuntimeError(f'Could not find credentials file at the specified path ({creds_file_path})! This file is required to run')
                    flow = InstalledAppFlow.from_client_secrets_file(creds_file_path, GDRIVE_AUTH_SCOPES)
                    creds = flow.run_local_server(port=0)
                # Save the credentials for the next run
                with open(token_file_path, 'wb') as token:
                    pickle.dump(creds, token)

            service: Resource = build('drive', 'v3', credentials=creds, cache=MemoryCache())
            return service

        result = GDriveClient._try_repeatedly(request)
        logger.debug('Authentication done!')
        return result

    @staticmethod
    def _try_repeatedly(request_func):
        retries_remaining = GDRIVE_CLIENT_REQUEST_MAX_RETRIES
        while True:
            try:
                return request_func()
            except Exception as err:
                logger.debug(f'Error type: {type(err)}')
                if isinstance(err, socket.timeout):
                    if retries_remaining == 0:
                        raise
                    logger.error(f'Request timed out: sleeping {GDRIVE_CLIENT_SLEEP_ON_FAILURE_SEC} sec (retries remaining: {retries_remaining})')
                else:
                    if isinstance(err, HttpError):
                        try:
                            if err.resp and err.resp.status == 403 or err.resp.status == 404:
                                # TODO: custom error class
                                raise
                        except AttributeError as err2:
                            logger.error(f'Additional error: {err2}')

                    if retries_remaining == 0:
                        raise
                    # Typically a transport error (socket timeout, name server problem...)
                    logger.error(f'Request failed: {repr(err)}: sleeping {GDRIVE_CLIENT_SLEEP_ON_FAILURE_SEC} sec '
                                 f'(retries remaining: {retries_remaining})')
                time.sleep(GDRIVE_CLIENT_SLEEP_ON_FAILURE_SEC)
                retries_remaining -= 1

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

    def _store_user(self, user: Dict) -> GDriveUser:
        permission_id = user.get('permissionId', None)
        gdrive_user: Optional[GDriveUser] = self.gdrive_store.get_gdrive_user_for_permission_id(permission_id)
        if not gdrive_user:
            # Completely new user
            user_name = user.get('displayName', None)
            user_email = user.get('emailAddress', None)
            user_photo_link = user.get('photoLink', None)
            user_is_me = user.get('me', None)
            gdrive_user: GDriveUser = GDriveUser(display_name=user_name, permission_id=permission_id, email_address=user_email,
                                                 photo_link=user_photo_link, is_me=user_is_me)
            self.gdrive_store.create_gdrive_user(gdrive_user)
        return gdrive_user

    def _convert_dict_to_gdrive_folder(self, item: Dict, sync_ts: int = 0, uid: UID = None) -> GDriveFolder:
        # 'driveId' only populated for items which someone has shared with me

        if not sync_ts:
            sync_ts = time_util.now_sec()

        goog_id = item['id']
        uid = self.gdrive_store.get_uid_for_goog_id(goog_id, uid_suggestion=uid)

        owners = item.get('owners', None)
        if owners:
            user = self._store_user(owners[0])
            owner_uid = user.uid
        else:
            owner_uid = None

        sharing_user = item.get('sharingUser', None)
        if sharing_user:
            user = self._store_user(sharing_user)
            sharing_user_uid = user.uid
        else:
            sharing_user_uid = None

        create_ts = GDriveClient._parse_gdrive_date(item, 'createdTime')

        modify_ts = GDriveClient._parse_gdrive_date(item, 'modifiedTime')

        goog_node = GDriveFolder(GDriveIdentifier(uid=uid, device_uid=self.device_uid, path_list=None), goog_id=goog_id, node_name=item['name'],
                                 trashed=GDriveClient._convert_trashed(item), create_ts=create_ts, modify_ts=modify_ts, owner_uid=owner_uid,
                                 drive_id=item.get('driveId', None), is_shared=item.get('shared', None), shared_by_user_uid=sharing_user_uid,
                                 sync_ts=sync_ts, all_children_fetched=False)

        parent_goog_ids = item.get('parents', [])
        parent_uids = self.gdrive_store.get_uid_list_for_goog_id_list(parent_goog_ids)
        goog_node.set_parent_uids(parent_uids)

        return goog_node

    def _convert_dict_to_gdrive_file(self, item: Dict, sync_ts: int = 0, uid: UID = None) -> GDriveFile:
        if not sync_ts:
            sync_ts = time_util.now_sec()

        owners = item.get('owners', None)
        if owners:
            user = self._store_user(owners[0])
            owner_uid = user.uid
        else:
            owner_uid = None

        sharing_user = item.get('sharingUser', None)
        if sharing_user:
            user = self._store_user(sharing_user)
            sharing_user_uid = user.uid
        else:
            sharing_user_uid = None

        create_ts = GDriveClient._parse_gdrive_date(item, 'createdTime')

        modify_ts = GDriveClient._parse_gdrive_date(item, 'modifiedTime')

        size_str = item.get('size', None)
        size = None if size_str is None else int(size_str)
        version = item.get('version', None)
        mime_type_string = item.get('mimeType', None)
        mime_type: MimeType = self.gdrive_store.get_or_create_gdrive_mime_type(mime_type_string)

        goog_id = item['id']

        uid = self.gdrive_store.get_uid_for_goog_id(goog_id, uid_suggestion=uid)
        goog_node: GDriveFile = GDriveFile(node_identifier=GDriveIdentifier(uid=uid, device_uid=self.device_uid, path_list=None),
                                           goog_id=goog_id, node_name=item["name"],
                                           mime_type_uid=mime_type.uid, trashed=GDriveClient._convert_trashed(item),
                                           drive_id=item.get('driveId', None), version=version,
                                           md5=item.get('md5Checksum', None), is_shared=item.get('shared', None), create_ts=create_ts,
                                           modify_ts=modify_ts, size_bytes=size, shared_by_user_uid=sharing_user_uid, owner_uid=owner_uid,
                                           sync_ts=sync_ts)

        parent_goog_ids = item.get('parents', [])
        parent_uids = self.gdrive_store.get_uid_list_for_goog_id_list(parent_goog_ids)
        goog_node.set_parent_uids(parent_uids)

        return goog_node

    def _execute_query(self, query: str, fields: str, initial_page_token: Optional[str], sync_ts: int, observer: GDriveQueryObserver):
        """Gets a list of files and/or folders which match the query criteria."""

        # Google Drive only; not backend data or Google Photos:
        spaces = 'drive'

        if initial_page_token:
            logger.info('Found a page token. Attempting to resume previous download')

        def request():
            m = f'Sending request for GDrive items, page {request.page_count}...'
            logger.debug(m)
            if self.tree_id:
                dispatcher.send(signal=Signal.SET_PROGRESS_TEXT, sender=self.tree_id, msg=m)

            # Call the Drive v3 API
            response = self.service.files().list(q=query, fields=fields, spaces=spaces, pageSize=self.page_size,
                                                 # include items from shared drives
                                                 includeItemsFromAllDrives=True, supportsAllDrives=True,
                                                 pageToken=request.page_token).execute()
            request.page_count += 1
            return response

        request.page_token = initial_page_token
        request.page_count = 0
        item_count = 0

        stopwatch_retrieval = Stopwatch()

        while True:
            results: dict = GDriveClient._try_repeatedly(request)

            # TODO: how will we know if the token is invalid?

            if results.get('incompleteSearch', False):
                # Not clear when this would happen, but fail fast if so
                raise RuntimeError(f'Results are incomplete! (page {request.page_count})')

            items: list = results.get('files', [])
            if not items:
                logger.debug('Request returned no files')
                break

            msg = f'Received {len(items)} items'
            logger.debug(msg)
            if self.tree_id:
                dispatcher.send(Signal.SET_PROGRESS_TEXT, sender=self.tree_id, msg=msg)

            for item in items:
                mime_type = item['mimeType']
                if mime_type == MIME_TYPE_FOLDER:
                    goog_node: GDriveFolder = self._convert_dict_to_gdrive_folder(item, sync_ts=sync_ts)
                else:
                    goog_node: GDriveFile = self._convert_dict_to_gdrive_file(item, sync_ts=sync_ts)

                observer.node_received(goog_node, item)
                item_count += 1

            request.page_token = results.get('nextPageToken')

            observer.end_of_page(request.page_token)

            if not request.page_token:
                logger.debug('Done!')
                break

        logger.debug(f'{stopwatch_retrieval} Query returned {item_count} nodes')

    # API CALLS
    # ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼

    def get_about(self) -> GDriveUser:
        """
        self.service.about().get()
        Returns: info about the current user and its storage usage
        """
        msg = 'Getting info for user...'
        logger.debug(msg)
        if self.tree_id:
            dispatcher.send(Signal.SET_PROGRESS_TEXT, sender=self.tree_id, msg=msg)

        fields = 'user, storageQuota'

        def request():
            return self.service.about().get(fields=fields).execute()

        about = GDriveClient._try_repeatedly(request)
        logger.debug(f'ABOUT: {about}')

        user: GDriveUser = self._store_user(about['user'])
        logger.info(f'Logged in as user {user.display_name} <{user.email_address}> (user_id={user.permission_id})')

        storage_quota = about['storageQuota']
        storage_total = storage_quota['limit']
        total = humanfriendly.format_size(int(storage_total))
        storage_used = storage_quota['usage']
        used = humanfriendly.format_size(int(storage_used))
        storage_used_in_drive = storage_quota['usageInDrive']
        drive_used = humanfriendly.format_size(int(storage_used_in_drive))
        storage_used_in_drive_trash = storage_quota['usageInDriveTrash']
        drive_trash_used = humanfriendly.format_size(int(storage_used_in_drive_trash))

        logger.info(f'{used} of {total} used (including {drive_used} for Drive files; of which {drive_trash_used} is trash)')
        return user

    def get_my_drive_root(self, sync_ts: int) -> GDriveFolder:
        """
        Returns: a GDriveFolder representing the user's GDrive root node.
        """

        def request():
            return self.service.files().get(fileId=GDRIVE_MY_DRIVE_ROOT_GOOG_ID, fields=fields).execute()

        fields = GDRIVE_FOLDER_FIELDS

        result = GDriveClient._try_repeatedly(request)

        root_node = self._convert_dict_to_gdrive_folder(result, sync_ts)
        logger.debug(f'Drive root: {root_node}')
        return root_node

    def get_all_shared_with_me(self) -> List[GDriveNode]:
        query = f"sharedWithMe"
        fields = f'nextPageToken, incompleteSearch, files({GDRIVE_FILE_FIELDS}, parents)'

        sync_ts = time_util.now_sec()
        observer = SimpleNodeCollector()
        self._execute_query(query, fields, None, sync_ts, observer)
        return observer.nodes

    def get_all_children_for_parent(self, parent_goog_id: str) -> List[GDriveNode]:
        if not parent_goog_id:
            logger.debug(f'get_all_children_for_parent(): parent_goog_id is null: setting it to "root"')
            parent_goog_id = 'root'
            # raise RuntimeError(f'Cannot query GDrive for children: parent_goog_id is empty!')

        """Gets all nodes (files, folders, etc) for the given parent"""
        query = f"'{parent_goog_id}' in parents"
        fields = f'nextPageToken, incompleteSearch, files({GDRIVE_FILE_FIELDS}, parents)'

        sync_ts = time_util.now_sec()
        observer = SimpleNodeCollector()
        self._execute_query(query, fields, None, sync_ts, observer)
        return observer.nodes

    def get_single_node_with_parent_and_name_and_criteria(self, node: GDriveNode, match_func: Callable[[GDriveNode], bool] = None) \
            -> Optional[GDriveNode]:
        src_parent_goog_id_list: List[str] = self.backend.cacheman.get_parent_goog_id_list(node)
        if not src_parent_goog_id_list:
            raise RuntimeError(f'Node has no parents: "{node.name}" ({node.device_uid}:{node.uid}')

        result: SimpleNodeCollector = self.get_existing_nodes_with_parent_and_name(parent_goog_id=src_parent_goog_id_list[0], name=node.name)
        logger.debug(f'Found {len(result.nodes)} matching GDrive nodes with parent={src_parent_goog_id_list[0]} and name={node.name}')

        if len(result.nodes) > 0:
            for found_node, found_raw in zip(result.nodes, result.raw_items):
                if match_func:
                    if match_func(found_node):
                        return found_node
                else:
                    return found_node

        return None

    def get_existing_nodes_with_parent_and_name(self, parent_goog_id: str, name: str) -> SimpleNodeCollector:
        query = f"name='{name}' AND '{parent_goog_id}' in parents"
        fields = f'nextPageToken, incompleteSearch, files({GDRIVE_FILE_FIELDS}, parents)'

        logger.debug(f'Getting existing nodes named "{name}" with parent "{parent_goog_id}"')

        sync_ts = time_util.now_sec()
        observer = SimpleNodeCollector()
        self._execute_query(query, fields, None, sync_ts, observer)

        return observer

    def get_existing_node_by_id(self, goog_id: str) -> Optional[GDriveNode]:
        if not goog_id:
            raise RuntimeError('GDriveClient.get_existing_node_by_id(): no goog_id specified!')
        fields = f'{GDRIVE_FILE_FIELDS}, parents'

        sync_ts = time_util.now_sec()

        def request():
            logger.debug(f'Getting node with goog_id "{goog_id}"')

            # Call the Drive v3 API
            return self.service.files().get(fileId=goog_id, fields=fields, supportsAllDrives=True).execute()

        item: dict = GDriveClient._try_repeatedly(request)

        if not item:
            logger.debug('Request returned no files')
            return None

        mime_type = item['mimeType']
        if mime_type == MIME_TYPE_FOLDER:
            goog_node: GDriveFolder = self._convert_dict_to_gdrive_folder(item, sync_ts=sync_ts)
        else:
            goog_node: GDriveFile = self._convert_dict_to_gdrive_file(item, sync_ts=sync_ts)

        if SUPER_DEBUG_ENABLED:
            logger.debug(f'Request returned {goog_node}')

        return goog_node

    # FILES
    # ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼

    def get_existing_file(self, node: GDriveNode) -> Optional[GDriveNode]:
        if not node.goog_id:
            raise RuntimeError(f'Cannot get existing file: node is missing goog_id: {node}')

        found_node, found_raw = self.get_single_node_with_parent_and_name_and_criteria(node, lambda x: x.goog_id == node.goog_id)
        return found_node

    def get_single_file_with_parent_and_name_and_criteria(self, node: GDriveNode, match_func: Callable[[GDriveNode], bool] = None) \
            -> Tuple[Optional[GDriveNode], Optional[Dict]]:
        """Important note: if the given node has several parents, the first one found in the cache will be used in the query"""
        src_parent_goog_id_list: List[str] = self.backend.cacheman.get_parent_goog_id_list(node)
        if not src_parent_goog_id_list:
            raise RuntimeError(f'Node has no parents: "{node.name}" ({node.device_uid}:{node.uid}')

        result: SimpleNodeCollector = self.get_existing_file_with_parent_and_name(parent_goog_id=src_parent_goog_id_list[0], name=node.name)
        logger.debug(f'Found {len(result.nodes)} matching GDrive files with parent={src_parent_goog_id_list[0]} and name="{node.name}"')

        if len(result.nodes) > 0:
            for found_node, found_raw in zip(result.nodes, result.raw_items):
                assert isinstance(found_node, GDriveFile)
                if match_func:
                    if match_func(found_node):
                        return found_node, found_raw
                else:
                    return found_node, found_raw

        return None, None

    def get_existing_file_with_parent_and_name(self, parent_goog_id: str, name: str) -> SimpleNodeCollector:
        query = f"{QUERY_NON_FOLDERS_ONLY} AND name='{name}' AND '{parent_goog_id}' in parents"
        fields = f'nextPageToken, incompleteSearch, files({GDRIVE_FILE_FIELDS}, parents)'

        logger.debug(f'Getting existing files named "{name}" with parent "{parent_goog_id}"')

        sync_ts = time_util.now_sec()
        observer = SimpleNodeCollector()
        self._execute_query(query, fields, None, sync_ts, observer)

        return observer

    def get_all_non_folders(self, initial_page_token: Optional[str], sync_ts: int, observer: GDriveQueryObserver):
        query = QUERY_NON_FOLDERS_ONLY
        fields = f'nextPageToken, incompleteSearch, files({GDRIVE_FILE_FIELDS}, parents)'

        logger.info('Getting list of ALL NON DIRS in Google Drive...')

        return self._execute_query(query, fields, initial_page_token, sync_ts, observer)

    def copy_existing_file(self, src_goog_id: str, new_name: str, new_parent_goog_ids: List[str]) -> Optional[GDriveNode]:
        if not src_goog_id:
            raise RuntimeError('GDriveClient.copy_existing_file(): no goog_id specified!')
        fields = f'{GDRIVE_FILE_FIELDS}, parents'

        file_metadata = {'name': new_name, 'parents': new_parent_goog_ids}

        sync_ts = time_util.now_sec()

        def request():
            logger.debug(f'Copying node with goog_id "{src_goog_id}" to new node with name="{new_name}" and parents={new_parent_goog_ids}')

            # Call the Drive v3 API
            return self.service.files().copy(body=file_metadata, fileId=src_goog_id, fields=fields, supportsAllDrives=True).execute()

        item: dict = GDriveClient._try_repeatedly(request)

        if not item:
            logger.error(f'Copy request returned no files! For copied goog_id: {src_goog_id}')
            return None

        goog_node: GDriveFile = self._convert_dict_to_gdrive_file(item, sync_ts=sync_ts)

        if SUPER_DEBUG_ENABLED:
            logger.debug(f'Request returned {goog_node}')

        return goog_node

    # FOLDERS
    # ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼

    def get_all_folders(self, initial_page_token: Optional[str], sync_ts: int, observer: GDriveQueryObserver):
        """
        Downloads all of the directory nodes from the user's GDrive and puts them into a
        GDriveMeta object.

        Assume 99.9% of items will have only one parent, and perhaps 0.001% will have no parent.
        he below solution optimizes with these assumptions.
        """
        fields = f'nextPageToken, incompleteSearch, files({GDRIVE_FOLDER_FIELDS}, parents)'

        self._execute_query(QUERY_FOLDERS_ONLY, fields, initial_page_token, sync_ts, observer)

    def get_folders_with_parent_and_name(self, parent_goog_id: str, name: str) -> SimpleNodeCollector:
        query = f"{QUERY_FOLDERS_ONLY} AND name='{name}' AND '{parent_goog_id}' in parents"
        fields = f'nextPageToken, incompleteSearch, files({GDRIVE_FOLDER_FIELDS}, parents)'

        sync_ts = time_util.now_sec()
        observer = SimpleNodeCollector()
        self._execute_query(query, fields, None, sync_ts, observer)
        return observer

    def create_folder(self, name: str, parent_goog_ids: List[str], uid: UID) -> GDriveFolder:
        """Create a folder with the given name. If successful, returns a new Google ID for the created folder"""
        if not name:
            raise RuntimeError(f'No name specified for folder!')

        file_metadata = {'name': name, 'parents': parent_goog_ids, 'mimeType': MIME_TYPE_FOLDER}

        def request():
            logger.debug(f'Creating folder: {name}')

            response = self.service.files().create(body=file_metadata, fields=f'{GDRIVE_FOLDER_FIELDS}, parents').execute()
            return response

        item = GDriveClient._try_repeatedly(request)

        goog_node: GDriveFolder = self._convert_dict_to_gdrive_folder(item, uid=uid)
        if not goog_node.goog_id:
            raise RuntimeError(f'Folder creation failed (no ID returned)!')

        logger.debug(f'Folder "{name}" created successfully! Returned id={goog_node.goog_id}')
        return goog_node

    # BINARIES
    # ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼

    def download_file(self, file_id: str, dest_path: str):
        """Download a single file based on Google ID and destination path"""
        logger.debug(f'Downloading GDrive goog_id="{file_id}" to "{dest_path}"')

        # only set this to True if you need to. Otherwise it will cause the download to fail...
        download_abusive_file = False

        def download():
            request = self.service.files().get_media(fileId=file_id, acknowledgeAbuse=download_abusive_file)
            fh = io.BytesIO()
            downloader = MediaIoBaseDownload(fh, request)
            done = False
            while done is False:
                status, done = downloader.next_chunk()
                logger.debug(f'Download {status.progress() * 100}%')

            with io.open(dest_path, 'wb') as f:
                fh.seek(0)
                f.write(fh.read())

            logger.info(f'GDrive download successful: dest="{dest_path}"')

        GDriveClient._try_repeatedly(download)

    def upload_new_file(self, local_file_full_path: str, parent_goog_ids: Union[str, List[str]], uid: UID) -> GDriveFile:
        """Upload a single file based on its path. If successful, returns the newly created GDriveFile"""
        if not local_file_full_path:
            raise RuntimeError(f'No path specified for file!')

        if isinstance(parent_goog_ids, str):
            parent_goog_ids = [parent_goog_ids]

        parent_path, file_name = os.path.split(local_file_full_path)
        file_metadata = {'name': file_name, 'parents': parent_goog_ids}

        media = MediaFileUpload(filename=local_file_full_path, resumable=True)

        def request():
            logger.debug(f'Uploading local file: "{local_file_full_path}" to parents: {parent_goog_ids}')

            response = self.service.files().create(body=file_metadata, media_body=media, fields=f'{GDRIVE_FILE_FIELDS}, parents').execute()
            return response

        file_meta = GDriveClient._try_repeatedly(request)
        gdrive_file = self._convert_dict_to_gdrive_file(file_meta, uid=uid)

        logger.info(
            f'File uploaded successfully! Returning {gdrive_file}",')

        return gdrive_file

    def update_existing_file(self, name: str, mime_type: str, goog_id: str, local_file_full_path: str) -> GDriveFile:
        if not local_file_full_path:
            raise RuntimeError(f'No path specified for file!')

        file_metadata = {'name': name, 'mimeType': mime_type}

        media = MediaFileUpload(filename=local_file_full_path, resumable=True)

        def request():
            logger.debug(f'Updating node "{goog_id}" with local file: "{local_file_full_path}"')

            # Send the request to the API.
            return self.service.files().update(fileId=goog_id, body=file_metadata, media_body=media,
                                               fields=f'{GDRIVE_FILE_FIELDS}, parents').execute()

        updated_file_meta = GDriveClient._try_repeatedly(request)
        gdrive_file: GDriveFile = self._convert_dict_to_gdrive_file(updated_file_meta)

        logger.info(
            f'File update uploaded successfully) Returned name="{gdrive_file.name}", version="{gdrive_file.version}", '
            f'goog_id="{gdrive_file.goog_id}"')

        return gdrive_file

    # MISC
    # ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼

    def modify_meta(self, goog_id: str, remove_parents: List[str], add_parents: List[str], name: str = None) -> GDriveNode:
        assert isinstance(add_parents, list), f'For goog_id={goog_id}: {add_parents}'
        assert isinstance(remove_parents, list), f'For goog_id={goog_id}: {remove_parents}'

        if name:
            meta = None
        else:
            meta = {'name': name}

        def request():
            # Move the file to the new folder
            file = self.service.files().update(fileId=goog_id, body=meta, addParents=add_parents,
                                               removeParents=remove_parents, fields=f'{GDRIVE_FILE_FIELDS}, parents').execute()
            return file

        item = GDriveClient._try_repeatedly(request)

        mime_type = item['mimeType']
        if mime_type == MIME_TYPE_FOLDER:
            goog_node = self._convert_dict_to_gdrive_folder(item)
        else:
            goog_node = self._convert_dict_to_gdrive_file(item)
        return goog_node

    def trash(self, goog_id: str):
        """Changes the trashed status of the given goog_id"""
        logger.debug(f'Sending request to trash file with goog_id="{goog_id}"')

        file_metadata = {'trashed': True}

        def request():
            return self.service.files().update(fileId=goog_id, body=file_metadata, fields=f'{GDRIVE_FILE_FIELDS}, parents').execute()

        file_meta = GDriveClient._try_repeatedly(request)
        gdrive_file: GDriveFile = self._convert_dict_to_gdrive_file(file_meta)

        logger.debug(f'Successfully trashed GDriveNode: {goog_id}: trashed={gdrive_file.get_trashed_status()}')
        return gdrive_file

    def hard_delete(self, goog_id: str):
        """Deletes the node with the given goog_id from Google Drive, skipping the trash. If the node is a folder, then it and all its descendents
        will be deleted."""
        logger.debug(f'Sending request to delete node with goog_id="{goog_id}"')

        def request():
            file = self.service.files().delete(fileId=goog_id).execute()
            return file

        GDriveClient._try_repeatedly(request)
        logger.debug(f'Successfully deleted GDriveNode: {goog_id}')

    def get_changes_start_token(self) -> str:
        logger.debug(f'Sending request to get startPageToken from Changes API"')

        def request():
            return self.service.changes().getStartPageToken().execute()

        response: Dict = GDriveClient._try_repeatedly(request)

        token = response.get('startPageToken', None)

        logger.debug(f'Got token: "{token}"')
        return token

    def get_changes_list(self, start_page_token: str, sync_ts: int, observer: GDriveChangeObserver):
        logger.debug(f'Sending request to get changes from start_page_token: "{start_page_token}"')

        # Google Drive only; not backend data or Google Photos:
        spaces = 'drive'

        def request():
            m = f'Sending request for changes, page {request.page_count} (token: {request.page_token})...'
            logger.debug(m)
            if self.tree_id:
                dispatcher.send(signal=Signal.SET_PROGRESS_TEXT, sender=self.tree_id, msg=m)

            # Call the Drive v3 API
            response = self.service.changes().list(pageToken=request.page_token, fields=f'nextPageToken, newStartPageToken, '
                                                                                        f'changes(changeType, time, removed, fileId, driveId, '
                                                                                        f'file({GDRIVE_FILE_FIELDS}, parents))', spaces=spaces,
                                                   # include changes from shared drives
                                                   includeItemsFromAllDrives=True, supportsAllDrives=True,
                                                   pageSize=self.page_size).execute()
            request.page_count += 1
            return response

        request.page_token = start_page_token
        request.page_count = 0

        stopwatch_retrieval = Stopwatch()

        count: int = 0
        while True:
            response_dict: dict = GDriveClient._try_repeatedly(request)

            # TODO: how will we know if the token is invalid?

            items: list = response_dict.get('changes', [])
            if not items:
                logger.debug('Request returned no changes')
                observer.new_start_token = response_dict.get('newStartPageToken', None)
                break

            count += len(items)
            msg = f'Received {len(items)} changes'
            logger.debug(msg)
            if self.tree_id:
                dispatcher.send(Signal.SET_PROGRESS_TEXT, sender=self.tree_id, msg=msg)

            for item in items:
                if SUPER_DEBUG_ENABLED:
                    logger.debug(f'CHANGE: {item}')

                goog_id = item['fileId']
                change_ts = item['time']

                is_removed = item['removed']
                if is_removed:
                    # Fill in node for removal change;
                    node = self.gdrive_store.get_node_for_goog_id(goog_id)
                    change: GDriveRM = GDriveRM(change_ts, goog_id, node)
                else:
                    if item['changeType'] == 'file':
                        file = item['file']
                        mime_type = file['mimeType']
                        if mime_type == MIME_TYPE_FOLDER:
                            goog_node: GDriveFolder = self._convert_dict_to_gdrive_folder(file, sync_ts=sync_ts)
                        else:
                            goog_node: GDriveFile = self._convert_dict_to_gdrive_file(file, sync_ts=sync_ts)
                        change: GDriveNodeChange = GDriveNodeChange(change_ts, goog_id, goog_node)
                    else:
                        logger.error(f'Strange item: {item}')
                        raise RuntimeError(f'is_removed==true but changeType is not "file" (got "{item["changeType"]}" instead')

                observer.change_received(change, item)

            request.page_token = response_dict.get('nextPageToken', None)

            observer.end_of_page(request.page_token)

            if not request.page_token:
                observer.new_start_token = response_dict.get('newStartPageToken', None)
                break

        logger.debug(f'{stopwatch_retrieval} Requests returned {count} changes '
                     f'(newStartPageToken="{observer.new_start_token}")')
