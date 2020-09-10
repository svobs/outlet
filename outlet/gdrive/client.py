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
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaFileUpload, MediaIoBaseDownload
from pydispatch import dispatcher

from constants import EXPLICITLY_TRASHED, GDRIVE_AUTH_SCOPES, GDRIVE_CLIENT_REQUEST_MAX_RETRIES, GDRIVE_FILE_FIELDS, GDRIVE_FOLDER_FIELDS, \
    IMPLICITLY_TRASHED, MIME_TYPE_FOLDER, NOT_TRASHED, QUERY_FOLDERS_ONLY, QUERY_NON_FOLDERS_ONLY
from gdrive.query_observer import GDriveQueryObserver, SimpleNodeCollector
from index.uid.uid import UID
from model.gdrive_whole_tree import UserMeta
from model.node.gdrive_node import GDriveFile, GDriveFolder, GDriveNode
from model.node_identifier import GDriveIdentifier
from ui import actions
from util import file_util
from util.stopwatch_sec import Stopwatch

logger = logging.getLogger(__name__)


def _load_google_client_service(config):
    def request():
        logger.debug('Trying to authenticate against GDrive API...')
        creds = None
        # The file token.pickle stores the user's access and refresh tokens, and is
        # created automatically when the authorization flow completes for the first
        # time.
        token_file_path = file_util.get_resource_path(config.get('auth.token_file_path'))
        if os.path.exists(token_file_path):
            with open(token_file_path, 'rb') as token:
                creds = pickle.load(token)
        # If there are no (valid) credentials available, let the user log in.
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
            else:
                creds_path = file_util.get_resource_path(config.get('auth.credentials_file_path'))
                if not os.path.exists(creds_path):
                    raise RuntimeError(f'Could not find credentials file at the specified path ({creds_path})! This file is required to run.')
                flow = InstalledAppFlow.from_client_secrets_file(creds_path, GDRIVE_AUTH_SCOPES)
                creds = flow.run_local_server(port=0)
            # Save the credentials for the next run
            with open(token_file_path, 'wb') as token:
                pickle.dump(creds, token)

        service = build('drive', 'v3', credentials=creds, cache=MemoryCache())
        return service

    result = _try_repeatedly(request)
    logger.debug('Authentication done!')
    return result


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
                logger.error(f'Request timed out: sleeping 3 secs (retries remaining: {retries_remaining})')
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
                logger.error(f'Request failed: {repr(err)}: sleeping 3 secs (retries remaining: {retries_remaining})')
            time.sleep(3)
            retries_remaining -= 1


def _convert_trashed(result):
    x_trashed = result.get('explicitlyTrashed', None)
    trashed = result.get('trashed', None)
    if x_trashed is None and trashed is None:
        return None

    if x_trashed:
        return EXPLICITLY_TRASHED
    elif trashed:
        return IMPLICITLY_TRASHED
    else:
        return NOT_TRASHED


# CLASS MemoryCache
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼

class MemoryCache:
    """
    Workaround for bug in Google code:
    See: https://github.com/googleapis/google-api-python-client/issues/325#issuecomment-274349841
    """
    _CACHE = {}

    def get(self, url):
        return MemoryCache._CACHE.get(url)

    def set(self, url, content):
        MemoryCache._CACHE[url] = content


# CLASS GDriveClient
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼


class GDriveClient:
    def __init__(self, application, tree_id=None):
        self.config = application.config
        self.cache_manager = application.cache_manager
        self.tree_id: str = tree_id
        self.page_size: int = self.config.get('gdrive.page_size')
        self.service = _load_google_client_service(self.config)

    def __del__(self):
        self.shutdown()

    def shutdown(self):
        if self.service:
            # Try to suppress warning
            logger.debug(f'Closing GDriveClient')
            self.service._http.http.close()
            self.service = None

    def _convert_dict_to_gdrive_folder(self, item: Dict, sync_ts: int = 0, uid: UID = None) -> GDriveFolder:
        # 'driveId' only populated for items which someone has shared with me
        # 'shared' only populated for items which are owned by me

        if not sync_ts:
            sync_ts = int(time.time())

        goog_id = item['id']
        uid = self.cache_manager.get_uid_for_goog_id(goog_id, uid_suggestion=uid)

        goog_node = GDriveFolder(GDriveIdentifier(uid=uid, full_path=None), goog_id=goog_id, node_name=item['name'], trashed=_convert_trashed(item),
                                 drive_id=item.get('driveId', None), my_share=item.get('shared', None), sync_ts=sync_ts, all_children_fetched=False)

        parent_goog_ids = item.get('parents', [])
        parent_uids = self.cache_manager.get_uid_list_for_goog_id_list(parent_goog_ids)
        goog_node.set_parent_uids(parent_uids)

        return goog_node

    def _convert_dict_to_gdrive_file(self, item: Dict, sync_ts: int = 0, uid: UID = None) -> GDriveFile:
        if not sync_ts:
            sync_ts = int(time.time())

        owners = item['owners']
        owner = None if len(owners) == 0 else owners[0]
        if owner:
            owner_id = owner.get('permissionId', None)
        else:
            owner_id = None

        create_ts = item.get('createdTime', None)
        if create_ts:
            create_ts = dateutil.parser.parse(create_ts)
            create_ts = int(create_ts.timestamp() * 1000)

        modify_ts = item.get('modifiedTime', None)
        if modify_ts:
            modify_ts = dateutil.parser.parse(modify_ts)
            modify_ts = int(modify_ts.timestamp() * 1000)

        head_revision_id = item.get('headRevisionId', None)
        size_str = item.get('size', None)
        size = None if size_str is None else int(size_str)
        version = item.get('version', None)
        if version:
            version = int(version)

        goog_id = item['id']
        uid = self.cache_manager.get_uid_for_goog_id(goog_id, uid_suggestion=uid)
        goog_node: GDriveFile = GDriveFile(node_identifier=GDriveIdentifier(uid=uid, full_path=None), goog_id=goog_id, node_name=item["name"],
                                           trashed=_convert_trashed(item), drive_id=item.get('driveId', None), version=version,
                                           head_revision_id=head_revision_id, md5=item.get('md5Checksum', None),
                                           my_share=item.get('shared', None), create_ts=create_ts, modify_ts=modify_ts, size_bytes=size,
                                           owner_id=owner_id, sync_ts=sync_ts)

        parent_goog_ids = item.get('parents', [])
        parent_uids = self.cache_manager.get_uid_list_for_goog_id_list(parent_goog_ids)
        goog_node.set_parent_uids(parent_uids)

        return goog_node

    def _execute_query(self, query: str, fields: str, initial_page_token: Optional[str], sync_ts: int, observer: GDriveQueryObserver):
        """Gets a list of files and/or folders which match the query criteria."""

        # Google Drive only; not app data or Google Photos:
        spaces = 'drive'

        if initial_page_token:
            logger.info('Found a page token. Attempting to resume previous download')

        def request():
            m = f'Sending request for files, page {request.page_count}...'
            logger.debug(m)
            if self.tree_id:
                dispatcher.send(signal=actions.SET_PROGRESS_TEXT, sender=self.tree_id, msg=m)

            # Call the Drive v3 API
            response = self.service.files().list(q=query, fields=fields, spaces=spaces, pageSize=self.page_size,
                                                 pageToken=request.page_token).execute()
            request.page_count += 1
            return response

        request.page_token = initial_page_token
        request.page_count = 0
        item_count = 0

        stopwatch_retrieval = Stopwatch()

        while True:
            results: dict = _try_repeatedly(request)

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
                actions.get_dispatcher().send(actions.SET_PROGRESS_TEXT, sender=self.tree_id, msg=msg)

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

        logger.debug(f'{stopwatch_retrieval} Query returned {item_count} files')

    # API CALLS
    # ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼

    def get_about(self) -> UserMeta:
        """
        self.service.about().get()
        Returns: info about the current user and its storage usage
        """
        if self.tree_id:
            msg = 'Getting info for user...'
            actions.get_dispatcher().send(actions.SET_PROGRESS_TEXT, sender=self.tree_id, msg=msg)

        fields = 'user, storageQuota'

        def request():
            return self.service.about().get(fields=fields).execute()

        about = _try_repeatedly(request)
        logger.debug(f'ABOUT: {about}')

        user = about['user']
        display_name = user['displayName']
        photo_link = user['photoLink']
        is_me = user['me']
        owner_id = user['permissionId']
        email_address = user['emailAddress']
        logger.info(f'Logged in as user {display_name} <{email_address}> (owner_id={owner_id})')
        logger.debug(f'User photo link: {photo_link}')
        user_meta = UserMeta(display_name=display_name, permission_id=owner_id, email_address=email_address, photo_link=photo_link)

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
        return user_meta

    def get_my_drive_root(self, sync_ts: int) -> GDriveFolder:
        """
        Returns: a GDriveFolder representing the user's GDrive root node.
        """

        def request():
            return self.service.files().get(fileId='root', fields=fields).execute()

        fields = 'id, name, trashed, explicitlyTrashed, shared, driveId'

        result = _try_repeatedly(request)

        root_node = self._convert_dict_to_gdrive_folder(result, sync_ts)
        logger.debug(f'Drive root: {root_node}')
        return root_node

    def get_all_children_for_parent(self, parent_goog_id: str) -> List[GDriveNode]:
        """Gets all nodes (files, folders, etc) for the given parent"""
        query = f"'{parent_goog_id}' in parents"
        fields = f'nextPageToken, incompleteSearch, files({GDRIVE_FILE_FIELDS}, parents)'

        sync_ts = int(time.time())
        observer = SimpleNodeCollector()
        self._execute_query(query, fields, None, sync_ts, observer)
        return observer.nodes

    def get_single_node_with_parent_and_name_and_criteria(self, node: GDriveNode, match_func: Callable[[GDriveNode], bool] = None) \
            -> Optional[GDriveNode]:
        src_parent_goog_id: str = self.cache_manager.get_goog_id_for_parent(node)
        result: SimpleNodeCollector = self.get_existing_node_with_parent_and_name(parent_goog_id=src_parent_goog_id, name=node.name)
        logger.debug(f'Found {len(result.nodes)} matching GDrive nodes with parent={src_parent_goog_id} and name={node.name}')

        if len(result.nodes) > 0:
            for found_node, found_raw in zip(result.nodes, result.raw_items):
                if match_func:
                    if match_func(found_node):
                        return found_node
                else:
                    return found_node

        return None

    def get_existing_node_with_parent_and_name(self, parent_goog_id: str, name: str) -> SimpleNodeCollector:
        query = f"name='{name}' AND '{parent_goog_id}' in parents"
        fields = f'nextPageToken, incompleteSearch, files({GDRIVE_FILE_FIELDS}, parents)'

        logger.debug(f'Getting existing nodes named "{name}" with parent "{parent_goog_id}"')

        sync_ts = int(time.time())
        observer = SimpleNodeCollector()
        self._execute_query(query, fields, None, sync_ts, observer)

        return observer

    # FILES
    # ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼

    def get_single_file_with_parent_and_name_and_criteria(self, node: GDriveNode, match_func: Callable[[GDriveNode], bool] = None) -> Tuple:
        src_parent_goog_id: str = self.cache_manager.get_goog_id_for_parent(node)
        result: SimpleNodeCollector = self.get_existing_file_with_parent_and_name(parent_goog_id=src_parent_goog_id, name=node.name)
        logger.debug(f'Found {len(result.nodes)} matching GDrive files with parent={src_parent_goog_id} and name={node.name}')

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

        sync_ts = int(time.time())
        observer = SimpleNodeCollector()
        self._execute_query(query, fields, None, sync_ts, observer)

        return observer

    def get_all_non_folders(self, initial_page_token: Optional[str], sync_ts: int, observer: GDriveQueryObserver):
        query = QUERY_NON_FOLDERS_ONLY
        fields = f'nextPageToken, incompleteSearch, files({GDRIVE_FILE_FIELDS}, parents)'

        logger.info('Getting list of ALL NON DIRS in Google Drive...')

        return self._execute_query(query, fields, initial_page_token, sync_ts, observer)

    # FOLDERS
    # ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼

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

        sync_ts = int(time.time())
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

        item = _try_repeatedly(request)

        goog_node: GDriveFolder = self._convert_dict_to_gdrive_folder(item, uid=uid)
        if not goog_node.goog_id:
            raise RuntimeError(f'Folder creation failed (no ID returned)!')

        logger.debug(f'Folder "{name}" created successfully! Returned id={goog_node.goog_id}')
        return goog_node

    # BINARIES
    # ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼

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

        _try_repeatedly(download)

    def upload_new_file(self, local_full_path: str, parent_goog_ids: Union[str, List[str]], uid: UID) -> GDriveFile:
        """Upload a single file based on its path. If successful, returns the newly created GDriveFile"""
        if not local_full_path:
            raise RuntimeError(f'No path specified for file!')

        if isinstance(parent_goog_ids, str):
            parent_goog_ids = [parent_goog_ids]

        parent_path, file_name = os.path.split(local_full_path)
        file_metadata = {'name': file_name, 'parents': parent_goog_ids}

        media = MediaFileUpload(filename=local_full_path, resumable=True)

        def request():
            logger.debug(f'Uploading local file: "{local_full_path}" to parents: {parent_goog_ids}')

            response = self.service.files().create(body=file_metadata, media_body=media, fields=f'{GDRIVE_FILE_FIELDS}, parents').execute()
            return response

        file_meta = _try_repeatedly(request)
        gdrive_file = self._convert_dict_to_gdrive_file(file_meta, uid=uid)

        logger.info(f'File uploaded successfully) Returned name="{gdrive_file.name}", version="{gdrive_file.version}", goog_id="{gdrive_file.goog_id}",')

        return gdrive_file

    def update_existing_file(self, name: str, mime_type: str, goog_id: str, local_full_path: str) -> GDriveFile:
        if not local_full_path:
            raise RuntimeError(f'No path specified for file!')

        file_metadata = {'name': name, 'mimeType': mime_type}

        media = MediaFileUpload(filename=local_full_path, resumable=True)

        def request():
            logger.debug(f'Updating node "{goog_id}" with local file: "{local_full_path}"')

            # Send the request to the API.
            return self.service.files().update(fileId=goog_id, body=file_metadata, media_body=media, fields=f'{GDRIVE_FILE_FIELDS}, parents').execute()

        updated_file_meta = _try_repeatedly(request)
        gdrive_file: GDriveFile = self._convert_dict_to_gdrive_file(updated_file_meta)

        logger.info(
            f'File update uploaded successfully) Returned name="{gdrive_file.name}", version="{gdrive_file.version}", goog_id="{gdrive_file.goog_id}",')

        return gdrive_file

    # MISC
    # ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼

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

        item = _try_repeatedly(request)

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

        file_meta = _try_repeatedly(request)
        gdrive_file: GDriveFile = self._convert_dict_to_gdrive_file(file_meta)

        logger.debug(f'Successfully trashed GDriveNode: {goog_id}: trashed={gdrive_file.trashed}')
        return gdrive_file

    def hard_delete(self, goog_id: str):
        logger.debug(f'Sending request to delete file with goog_id="{goog_id}"')

        def request():
            # Delete the item from Google Drive. Skips the trash
            file = self.service.files().delete(fileId=goog_id).execute()
            return file

        _try_repeatedly(request)
        logger.debug(f'Successfully deleted GDriveNode: {goog_id}')

    def get_changes_start_token(self) -> str:
        logger.debug(f'Sending request to get startPageToken from Changes API"')

        def request():
            return self.service.changes().getStartPageToken().execute()

        token = _try_repeatedly(request)

        logger.debug(f'Got token: "{token}"')
        return token

