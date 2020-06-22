import io
import logging
import os.path
import pickle
import socket
import time
from abc import ABC, abstractmethod
from typing import Dict, List, Optional, Tuple, Union

import dateutil.parser
import humanfriendly
from google.auth.transport.requests import Request
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaFileUpload, MediaIoBaseDownload
from pydispatch import dispatcher

import file_util
from constants import EXPLICITLY_TRASHED, GDRIVE_CLIENT_REQUEST_MAX_RETRIES, IMPLICITLY_TRASHED, NOT_TRASHED
from model.gdrive_whole_tree import UserMeta
from model.goog_node import GoogFile, GoogFolder, GoogNode
from stopwatch_sec import Stopwatch
from ui import actions

# IMPORTANT: If modifying these scopes, delete the file token.pickle.
# SCOPES = ['https://www.googleapis.com/auth/drive.readonly']
SCOPES = ['https://www.googleapis.com/auth/drive']

MIME_TYPE_SHORTCUT = 'application/vnd.google-apps.shortcut'
MIME_TYPE_FOLDER = 'application/vnd.google-apps.folder'

QUERY_FOLDERS_ONLY = f"mimeType='{MIME_TYPE_FOLDER}'"
QUERY_NON_FOLDERS_ONLY = f"not {QUERY_FOLDERS_ONLY}"

# Web view link takes the form:
WEB_VIEW_LINK = 'https://drive.google.com/file/d/{id}/view?usp=drivesdk'
WEB_CONTENT_LINK = 'https://drive.google.com/uc?id={id}&export=download'

DIR_FIELDS = 'id, name, trashed, explicitlyTrashed, driveId, shared'
FILE_FIELDS = 'id, name, trashed, explicitlyTrashed, driveId, shared, version, createdTime, ' \
              'modifiedTime, owners, md5Checksum, size, headRevisionId, shortcutDetails, mimeType, sharingUser'

logger = logging.getLogger(__name__)


# ABSTRACT CLASS MetaObserver
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼

class MetaObserver(ABC):
    """Observer interface, to be implemented with various strategies for processing downloaded Google Drive meta"""

    def __init__(self):
        pass

    @abstractmethod
    def meta_received(self, goog_node: GoogNode, item):
        pass

    @abstractmethod
    def end_of_page(self, next_page_token: str):
        pass


# CLASS SimpleNodeCollector
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼

class SimpleNodeCollector(MetaObserver):
    """Just collects Google nodes in its internal list, to be retreived all at once when meta download is done"""

    def __init__(self):
        super().__init__()
        self.nodes: List[GoogNode] = []
        self.raw_items = []

    def meta_received(self, goog_node: GoogNode, item):
        self.nodes.append(goog_node)
        self.raw_items.append(item)

    def end_of_page(self, next_page_token: str):
        pass

    def __repr__(self):
        return f'SimpleNodeCollector(nodes={len(self.nodes)} raw_items={len(self.raw_items)}'


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


def _load_google_client_service(config):
    def request():
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
                flow = InstalledAppFlow.from_client_secrets_file(creds_path, SCOPES)
                creds = flow.run_local_server(port=0)
            # Save the credentials for the next run
            with open(token_file_path, 'wb') as token:
                pickle.dump(creds, token)

        service = build('drive', 'v3', credentials=creds, cache=MemoryCache())
        return service

    return _try_repeatedly(request)


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


# CLASS GDriveClient
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼


class GDriveClient:
    def __init__(self, application, tree_id=None):
        self.config = application.config
        self.cache_manager = application.cache_manager
        self.tree_id = tree_id
        self.page_size = self.config.get('gdrive.page_size')
        self.service = _load_google_client_service(self.config)

    def _convert_to_goog_folder(self, result, sync_ts: int = 0) -> GoogFolder:
        # 'driveId' only populated for items which someone has shared with me
        # 'shared' only populated for items which are owned by me

        if not sync_ts:
            sync_ts = int(time.time())

        goog_id = result['id']
        uid = self.cache_manager.get_uid_for_goog_id(goog_id)
        return GoogFolder(uid=uid, goog_id=goog_id, item_name=result['name'], trashed=_convert_trashed(result),
                          drive_id=result.get('driveId', None), my_share=result.get('shared', None), sync_ts=sync_ts, all_children_fetched=False)

    def _convert_to_goog_file(self, item, sync_ts: int = 0) -> GoogFile:
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
        uid = self.cache_manager.get_uid_for_goog_id(goog_id)
        goog_node: GoogFile = GoogFile(uid=uid, goog_id=goog_id, item_name=item["name"],
                                       trashed=_convert_trashed(item),
                                       drive_id=item.get('driveId', None),
                                       version=version, head_revision_id=head_revision_id,
                                       md5=item.get('md5Checksum', None), my_share=item.get('shared', None), create_ts=create_ts,
                                       modify_ts=modify_ts, size_bytes=size, owner_id=owner_id, sync_ts=sync_ts)

        return goog_node

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

    def get_meta_my_drive_root(self, sync_ts: int) -> GoogFolder:
        """
        Returns: a GoogFolder representing the user's GDrive root node.
        """

        def request():
            return self.service.files().get(fileId='root', fields=fields).execute()

        fields = 'id, name, trashed, explicitlyTrashed, shared, driveId'

        result = _try_repeatedly(request)

        root_node = self._convert_to_goog_folder(result, sync_ts)
        logger.debug(f'Drive root: {root_node}')
        return root_node

    def get_existing_folder_with_parent_and_name(self, parent_goog_id: str, name: str) -> SimpleNodeCollector:
        query = f"{QUERY_FOLDERS_ONLY} AND name='{name}' AND '{parent_goog_id}' in parents"
        fields = f'nextPageToken, incompleteSearch, files({DIR_FIELDS}, parents)'

        sync_ts = int(time.time())
        observer = SimpleNodeCollector()
        self._get_meta_for_dirs(query, fields, None, sync_ts, observer)
        return observer

    def get_existing_file_with_parent_and_name(self, parent_goog_id: str, name: str) -> SimpleNodeCollector:
        query = f"{QUERY_NON_FOLDERS_ONLY} AND name='{name}' AND '{parent_goog_id}' in parents"
        fields = f'nextPageToken, incompleteSearch, files({FILE_FIELDS}, parents)'

        logger.debug(f'Getting existing files named "{name}" with parent "{parent_goog_id}"')

        sync_ts = int(time.time())
        observer = SimpleNodeCollector()
        self._get_meta_for_files(query, fields, None, sync_ts, observer)

        return observer

    def get_meta_all_files(self, initial_page_token: Optional[str], sync_ts: int, observer: MetaObserver):

        query = QUERY_NON_FOLDERS_ONLY
        fields = f'nextPageToken, incompleteSearch, files({FILE_FIELDS}, parents)'

        logger.info('Getting list of ALL NON DIRS in Google Drive...')

        return self._get_meta_for_files(query, fields, initial_page_token, sync_ts, observer)

    def _get_meta_for_files(self, query: str, fields: str, initial_page_token: Optional[str], sync_ts: int, observer: MetaObserver):
        """Generic version"""

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

        owner_dict: Dict[str, Tuple[str, str, str, bool]] = {}
        mime_types: Dict[str, GoogNode] = {}
        shortcuts: Dict[str, GoogNode] = {}

        while True:
            results: dict = _try_repeatedly(request)

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
                # Collect owners
                owners = item['owners']
                if len(owners) > 0:
                    owner = owners[0]
                    owner_id = owner.get('permissionId', None)
                    owner_name = owner.get('displayName', None)
                    owner_email = owner.get('emailAddress', None)
                    owner_photo_link = owner.get('photoLink', None)
                    owner_is_me = owner.get('me', None)
                    owner_dict[owner_id] = (owner_name, owner_email, owner_photo_link, owner_is_me)

                goog_node: GoogFile = self._convert_to_goog_file(item, sync_ts=sync_ts)

                # Collect MIME types
                mime_type = item['mimeType']
                mime_types[mime_type] = goog_node

                # web_view_link = item.get('webViewLink', None)
                # if web_view_link:
                #     logger.debug(f'Found webViewLink: "{web_view_link}" for goog_node: {goog_node}')
                #
                # web_content_link = item.get('webContentLink', None)
                # if web_content_link:
                #     logger.debug(f'Found webContentLink: "{web_content_link}" for goog_node: {goog_node}')

                sharing_user = item.get('sharingUser', None)
                if sharing_user:
                    logger.debug(f'Found sharingUser: "{sharing_user}" for goog_node: {goog_node}')

                is_shortcut = mime_type == MIME_TYPE_SHORTCUT
                if is_shortcut:
                    shortcut_details = item.get('shortcutDetails', None)
                    if not shortcut_details:
                        logger.error(f'Shortcut is missing shortcutDetails: id="{goog_node.uid}" name="{goog_node.name}"')
                    else:
                        target_id = shortcut_details.get('targetId')
                        if not target_id:
                            logger.error(f'Shortcut is missing targetId: id="{goog_node.uid}" name="{goog_node.name}"')
                        else:
                            logger.debug(f'Found shortcut: id="{goog_node.uid}" name="{goog_node.name}" -> target_id="{target_id}"')
                            shortcuts[goog_node.goog_id] = target_id

                observer.meta_received(goog_node, item)
                item_count += 1

            request.page_token = results.get('nextPageToken')

            observer.end_of_page(request.page_token)

            if not request.page_token:
                logger.debug('Done!')
                break

        logger.debug(f'{stopwatch_retrieval} Query returned {item_count} files')

        if logger.isEnabledFor(logging.DEBUG) and item_count > 0:
            logger.debug(f'Found {len(owner_dict)} distinct owners')
            for owner_id, owner in owner_dict.items():
                logger.debug(f'Found owner: id={owner_id} name={owner[0]} email={owner[1]} is_me={owner[2]}')

            logger.debug(f'Found {len(mime_types)} distinct MIME types')
            for mime_type, item in mime_types.items():
                logger.debug(f'MIME type: {mime_type} -> [{item.uid}] {item.name} {item.get_size_bytes()}')

        # TODO: save MIME types, owners, shortcuts

    # DIRECTORIES (folders)
    # ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼

    def get_meta_all_directories(self, initial_page_token: Optional[str], sync_ts: int, observer: MetaObserver):
        """
        Downloads all of the directory nodes from the user's GDrive and puts them into a
        GDriveMeta object.

        Assume 99.9% of items will have only one parent, and perhaps 0.001% will have no parent.
        he below solution optimizes with these assumptions.
        """
        fields = f'nextPageToken, incompleteSearch, files({DIR_FIELDS}, parents)'

        self._get_meta_for_dirs(QUERY_FOLDERS_ONLY, fields, initial_page_token, sync_ts, observer)

    def _get_meta_for_dirs(self, query: str, fields: str, initial_page_token: Optional[str], sync_ts: int, observer: MetaObserver):
        """Generic version of request for GoogFolders"""

        # Google Drive only; not app data or Google Photos:
        spaces = 'drive'

        def request():
            m = f'Sending request for dirs, page {request.page_count}...'
            logger.debug(m)
            if self.tree_id:
                actions.get_dispatcher().send(actions.SET_PROGRESS_TEXT, sender=self.tree_id, msg=m)

            # Call the Drive v3 API
            response = self.service.files().list(q=query, fields=fields, spaces=spaces, pageSize=self.page_size,
                                                 pageToken=request.page_token).execute()
            request.page_count += 1
            return response

        request.page_token = initial_page_token
        request.page_count = 0
        item_count = 0

        if request.page_token:
            logger.info('Found a page token. Attempting to resume previous download')

        stopwatch_retrieval = Stopwatch()

        while True:
            results: dict = _try_repeatedly(request)

            # TODO: how will we know if the token is invalid?

            if results.get('incompleteSearch', False):
                raise RuntimeError(f'Results are incomplete! (page {request.page_count})')

            items: list = results.get('files', [])
            if not items:
                if not items:
                    logger.debug('Request returned no folders')
                    break

            msg = f'Received {len(items)} items'
            logger.debug(msg)
            if self.tree_id:
                actions.get_dispatcher().send(actions.SET_PROGRESS_TEXT, sender=self.tree_id, msg=msg)

            for item in items:
                goog_node: GoogFolder = self._convert_to_goog_folder(item, sync_ts)

                observer.meta_received(goog_node, item)
                item_count += 1

            request.page_token = results.get('nextPageToken')

            observer.end_of_page(request.page_token)

            if not request.page_token:
                logger.debug('Done!')
                break

        logger.debug(f'{stopwatch_retrieval} Query returned {item_count} folders')

    # BINARIES
    # ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼

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

    def upload_new_file(self, local_full_path: str, parent_goog_ids: Union[str, List[str]]) -> GoogFile:
        """Upload a single file based on its path. If successful, returns the newly created Google ID"""
        if not local_full_path:
            raise RuntimeError(f'No path specified for file!')

        if isinstance(parent_goog_ids, str):
            parent_goog_ids = [parent_goog_ids]

        parent_path, file_name = os.path.split(local_full_path)
        file_metadata = {'name': file_name, 'parents': parent_goog_ids}
        media = MediaFileUpload(filename=local_full_path, resumable=True)

        def request():
            logger.debug(f'Uploading local file: "{local_full_path}" to parents: {parent_goog_ids}')

            response = self.service.files().create(body=file_metadata, media_body=media, fields=FILE_FIELDS).execute()
            return response

        file_meta = _try_repeatedly(request)
        goog_file = self._convert_to_goog_file(file_meta)

        logger.info(f'File uploaded successfully) Returned name="{goog_file.name}", version="{goog_file.version}", goog_id="{goog_file.goog_id}",')

        return goog_file

    def update_existing_file(self, raw_item, local_full_path: str) -> GoogFile:
        if not local_full_path:
            raise RuntimeError(f'No path specified for file!')

        file_metadata = {'name': raw_item['name'], 'mimeType': raw_item['mimeType']}

        media = MediaFileUpload(filename=local_full_path, resumable=True)

        def request():
            logger.debug(f'Updating node "{raw_item["id"]}" with local file: "{local_full_path}"')

            # Send the request to the API.
            return self.service.files().update(fileId=raw_item['id'], body=file_metadata, media_body=media, fields=FILE_FIELDS).execute()

        updated_file_meta = _try_repeatedly(request)
        goog_file: GoogFile = self._convert_to_goog_file(updated_file_meta)

        logger.info(f'File update uploaded successfully) Returned name="{goog_file.name}", version="{goog_file.version}", goog_id="{goog_file.goog_id}",')

        return goog_file

    def create_folder(self, name: str, parent_goog_ids: List[str]) -> GoogFolder:
        """Create a folder with the given name. If successful, returns a new Google ID for the created folder"""
        if not name:
            raise RuntimeError(f'No name specified for folder!')

        file_metadata = {'name': name, 'parents': parent_goog_ids, 'mimeType': MIME_TYPE_FOLDER}

        def request():
            logger.debug(f'Creating folder: {name}')

            response = self.service.files().create(body=file_metadata, fields=DIR_FIELDS).execute()
            return response

        item = _try_repeatedly(request)

        goog_node: GoogFolder = self._convert_to_goog_folder(item)
        if not goog_node.goog_id:
            raise RuntimeError(f'Folder creation failed (no ID returned)!')

        logger.debug(f'Folder "{name}" created successfully! Returned id={goog_node.goog_id}')
        return goog_node

    def modify_meta(self, goog_id: str, remove_parents: List[str], add_parents: List[str], name: str = None):
        assert isinstance(add_parents, list), f'For goog_id={goog_id}: {add_parents}'
        assert isinstance(remove_parents, list), f'For goog_id={goog_id}: {remove_parents}'

        if name:
            meta = None
        else:
            meta = {'name': name}

        def request():
            # Move the file to the new folder
            file = self.service.files().update(fileId=goog_id, body=meta, addParents=add_parents,
                                               removeParents=remove_parents, fields=FILE_FIELDS).execute()
            return file

        item = _try_repeatedly(request)

        mime_type = item['mimeType']
        if mime_type == MIME_TYPE_FOLDER:
            goog_node = self._convert_to_goog_folder(item)
        else:
            goog_node = self._convert_to_goog_file(item)
        return goog_node

    def trash(self, goog_id: str):
        logger.debug(f'Sending request to trash file with goog_id="{goog_id}"')

        file_metadata = {'trashed': True}

        def request():
            # Put the given file in the trash
            # Send the request to the API.
            return self.service.files().update(fileId=goog_id, body=file_metadata, fields=FILE_FIELDS).execute()

        file_meta = _try_repeatedly(request)
        goog_file: GoogFile = self._convert_to_goog_file(file_meta)

        logger.debug(f'Successfully trashed Goog node: {goog_id}: trashed={goog_file.trashed}')
        return goog_file

    def hard_delete(self, goog_id: str):
        logger.debug(f'Sending request to delete file with goog_id="{goog_id}"')

        def request():
            # Delete the item from Google Drive. Skips the trash
            file = self.service.files().delete(fileId=goog_id).execute()
            return file

        _try_repeatedly(request)
        logger.debug(f'Successfully deleted Goog node: {goog_id}')

