import io
import socket
from typing import List, Optional, Tuple, Union

from googleapiclient.errors import HttpError
from googleapiclient.http import MediaFileUpload, MediaIoBaseDownload

import file_util
import os.path
import pickle
import logging
import time
import humanfriendly
from datetime import datetime

from index.sqlite.gdrive_db import CurrentDownload, GDriveDatabase
from stopwatch_sec import Stopwatch
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request

from constants import EXPLICITLY_TRASHED, GDRIVE_CLIENT_REQUEST_MAX_RETRIES, GDRIVE_DOWNLOAD_STATE_GETTING_DIRS, \
    GDRIVE_DOWNLOAD_STATE_GETTING_NON_DIRS, GDRIVE_DOWNLOAD_STATE_NOT_STARTED, \
    GDRIVE_DOWNLOAD_STATE_READY_TO_COMPILE, IMPLICITLY_TRASHED, NOT_TRASHED
from model.gdrive_whole_tree import GDriveWholeTree, UserMeta
from model.goog_node import GoogFile, GoogFolder
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

ISO_8601_FMT = '%Y-%m-%dT%H:%M:%S.%f%z'

TOKEN_FILE_PATH = file_util.get_resource_path('token.pickle')
CREDENTIALS_FILE_PATH = file_util.get_resource_path('credentials.json')

logger = logging.getLogger(__name__)


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


def load_google_client_service():
    creds = None
    # The file token.pickle stores the user's access and refresh tokens, and is
    # created automatically when the authorization flow completes for the first
    # time.
    if os.path.exists(TOKEN_FILE_PATH):
        with open(TOKEN_FILE_PATH, 'rb') as token:
            creds = pickle.load(token)
    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(CREDENTIALS_FILE_PATH, SCOPES)
            creds = flow.run_local_server(port=0)
        # Save the credentials for the next run
        with open(TOKEN_FILE_PATH, 'wb') as token:
            pickle.dump(creds, token)

    service = build('drive', 'v3', credentials=creds, cache=MemoryCache())
    return service


def try_repeatedly(request_func):
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


def convert_trashed(result):
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


def convert_goog_folder(result, uid: int, sync_ts: int) -> GoogFolder:
    # 'driveId' only populated for items which someone has shared with me
    # 'shared' only populated for items which are owned by me
    return GoogFolder(uid=uid, goog_id=result['id'], item_name=result['name'], trashed=convert_trashed(result),
                      drive_id=result.get('driveId', None), my_share=result.get('shared', None), sync_ts=sync_ts, all_children_fetched=False)


def parent_mappings_tuples(item_uid: int, parent_goog_ids: List[str], sync_ts: int) -> List[Tuple[int, Optional[int], str, int]]:
    tuples = []
    for parent_id in parent_goog_ids:
        tuples.append((item_uid, None, parent_id, sync_ts))
    return tuples


# CLASS GDriveClient
# ⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟


class GDriveClient:
    def __init__(self, config, tree_id=None):
        self.service = load_google_client_service()
        self.config = config
        self.tree_id = tree_id
        self.page_size = config.get('gdrive.page_size')

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

        about = try_repeatedly(request)
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

    def get_my_drive_root(self, uid: int, sync_ts) -> GoogFolder:
        """
        Returns: a GoogFolder representing the user's GDrive root node.
        """

        def request():
            return self.service.files().get(fileId='root', fields=fields).execute()

        fields = 'id, name, trashed, explicitlyTrashed, shared, driveId'
        result = try_repeatedly(request)

        root_node = convert_goog_folder(result, uid, sync_ts)
        logger.debug(f'Drive root: {root_node}')
        return root_node

    def download_subtree_file_meta(self):
        pass
        # TODO
        # query = f"and '{subtree_root_gd_id}' in parents"

    def download_all_file_meta(self, meta: GDriveWholeTree, cache: GDriveDatabase, download: CurrentDownload):
        assert download.current_state == GDRIVE_DOWNLOAD_STATE_GETTING_NON_DIRS

        fields = f'nextPageToken, incompleteSearch, files({FILE_FIELDS}, parents)'
        # Google Drive only; not app data or Google Photos:
        spaces = 'drive'

        logger.info('Getting list of ALL NON DIRS in Google Drive...')

        if download.page_token:
            logger.info('Found a page token. Attempting to resume previous download')

        def request():
            msg = f'Sending request for files, page {request.page_count}...'
            logger.debug(msg)
            if self.tree_id:
                actions.get_dispatcher().send(actions.SET_PROGRESS_TEXT, sender=self.tree_id, msg=msg)

            # Call the Drive v3 API
            response = self.service.files().list(q=QUERY_NON_FOLDERS_ONLY, fields=fields, spaces=spaces, pageSize=self.page_size,
                                                 pageToken=download.page_token).execute()
            request.page_count += 1
            return response

        request.page_count = 0
        item_count = 0

        stopwatch_retrieval = Stopwatch()

        while True:
            results: dict = try_repeatedly(request)

            if results.get('incompleteSearch', False):
                # Not clear when this would happen, but fail fast if so
                raise RuntimeError(f'Results are incomplete! (page {request.page_count})')

            items: list = results.get('files', [])
            if not items:
                raise RuntimeError(f'No files returned from Drive API! (page {request.page_count})')

            msg = f'Received {len(items)} items'
            logger.debug(msg)
            if self.tree_id:
                actions.get_dispatcher().send(actions.SET_PROGRESS_TEXT, sender=self.tree_id, msg=msg)

            file_tuples: List[Tuple] = []
            id_parent_mappings: List[Tuple] = []
            for item in items:
                mime_type = item['mimeType']
                owners = item['owners']
                owner = None if len(owners) == 0 else owners[0]
                if owner:
                    owner_id = owner.get('permissionId', None)
                    owner_name = owner.get('displayName', None)
                    owner_email = owner.get('emailAddress', None)
                    owner_is_me = owner.get('me', None)
                    meta.owner_dict[owner_id] = (owner_name, owner_email, owner_is_me)
                else:
                    owner_id = None

                create_ts = item.get('createdTime', None)
                if create_ts:
                    create_ts = datetime.strptime(create_ts, ISO_8601_FMT)
                    create_ts = int(create_ts.timestamp() * 1000)

                modify_ts = item.get('modifiedTime', None)
                if modify_ts:
                    modify_ts = datetime.strptime(modify_ts, ISO_8601_FMT)
                    modify_ts = int(modify_ts.timestamp() * 1000)

                head_revision_id = item.get('headRevisionId', None)
                size_str = item.get('size', None)
                size = None if size_str is None else int(size_str)
                version = item.get('version', None)
                if version:
                    version = int(version)

                goog_node: GoogFile = GoogFile(uid=meta.get_new_uid(), goog_id=item['id'], item_name=item["name"], trashed=convert_trashed(item),
                                               drive_id=item.get('driveId', None),
                                               version=version, head_revision_id=head_revision_id,
                                               md5=item.get('md5Checksum', None), my_share=item.get('shared', None), create_ts=create_ts,
                                               modify_ts=modify_ts, size_bytes=size, owner_id=owner_id, sync_ts=download.update_ts)
                parent_google_ids = item.get('parents', [])
                meta.id_dict[goog_node.uid] = goog_node
                meta.mime_types[mime_type] = goog_node

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
                            meta.shortcuts[goog_node.uid] = target_id

                file_tuples.append(goog_node.to_tuple())
                id_parent_mappings += parent_mappings_tuples(goog_node.uid, parent_google_ids, sync_ts=download.update_ts)
                item_count += 1

            download.page_token = results.get('nextPageToken')

            if not download.page_token:
                # done
                assert download.current_state == GDRIVE_DOWNLOAD_STATE_GETTING_NON_DIRS
                download.current_state = GDRIVE_DOWNLOAD_STATE_READY_TO_COMPILE
                # fall through

            cache.insert_gdrive_files_and_parents(file_list=file_tuples, parent_mappings=id_parent_mappings, current_download=download)

            if not download.page_token:
                logger.debug('Done!')
                break

        logger.info(f'{stopwatch_retrieval} Query returned {item_count} files')

        if logger.isEnabledFor(logging.DEBUG):
            logger.debug(f'Found {len(meta.owner_dict)} distinct owners')
            for owner_id, owner in meta.owner_dict.items():
                logger.debug(f'Found owner: id={owner_id} name={owner[0]} email={owner[1]} is_me={owner[2]}')

            logger.debug(f'Found {len(meta.mime_types)} distinct MIME types')
            for mime_type, item in meta.mime_types.items():
                logger.debug(f'MIME type: {mime_type} -> [{item.uid}] {item.name} {item.size_bytes}')

        return meta

    def download_directory_structure(self, meta: GDriveWholeTree, cache: GDriveDatabase, download: CurrentDownload):
        """
        Downloads all of the directory nodes from the user's GDrive and puts them into a
        GDriveMeta object.

        Assume 99.9% of items will have only one parent, and perhaps 0.001% will have no parent.
        he below solution optimizes with these assumptions.
        """
        assert download.current_state == GDRIVE_DOWNLOAD_STATE_GETTING_DIRS

        fields = f'nextPageToken, incompleteSearch, files({DIR_FIELDS}, parents)'
        # Google Drive only; not app data or Google Photos:
        spaces = 'drive'

        logger.info('Getting list of ALL directories in Google Drive...')

        def request():
            msg = f'Sending request for dirs, page {request.page_count}...'
            logger.debug(msg)
            if self.tree_id:
                actions.get_dispatcher().send(actions.SET_PROGRESS_TEXT, sender=self.tree_id, msg=msg)

            # Call the Drive v3 API
            response = self.service.files().list(q=QUERY_FOLDERS_ONLY, fields=fields, spaces=spaces, pageSize=self.page_size,
                                                 pageToken=download.page_token).execute()
            request.page_count += 1
            return response

        request.page_count = 0
        item_count = 0

        if download.page_token:
            logger.info('Found a page token. Attempting to resume previous download')

        stopwatch_retrieval = Stopwatch()

        while True:
            results: dict = try_repeatedly(request)

            # TODO: how will we know if the token is invalid?

            if results.get('incompleteSearch', False):
                raise RuntimeError(f'Results are incomplete! (page {request.page_count})')

            items: list = results.get('files', [])
            if not items:
                raise RuntimeError(f'No files returned from Drive API! (page {request.page_count})')

            msg = f'Received {len(items)} items'
            logger.debug(msg)
            if self.tree_id:
                actions.get_dispatcher().send(actions.SET_PROGRESS_TEXT, sender=self.tree_id, msg=msg)

            dir_tuples: List[Tuple] = []
            id_parent_mappings: List[Tuple] = []
            for item in items:
                goog_node: GoogFolder = convert_goog_folder(item, meta.get_new_uid(), download.update_ts)
                parent_google_ids = item.get('parents', [])
                meta.id_dict[goog_node.uid] = goog_node
                dir_tuples.append(goog_node.to_tuple())

                id_parent_mappings += parent_mappings_tuples(goog_node.uid, parent_google_ids, sync_ts=download.update_ts)
                item_count += 1

            download.page_token = results.get('nextPageToken')

            if not download.page_token:
                # done
                assert download.current_state == GDRIVE_DOWNLOAD_STATE_GETTING_DIRS
                download.current_state = GDRIVE_DOWNLOAD_STATE_GETTING_NON_DIRS
                # fall through

            cache.insert_gdrive_dirs_and_parents(dir_list=dir_tuples, parent_mappings=id_parent_mappings, current_download=download)

            if not download.page_token:
                logger.debug('Done!')
                break

        logger.info(f'{stopwatch_retrieval} Query returned {item_count} directories')

        if logger.isEnabledFor(logging.DEBUG):
            for node in meta.roots:
                logger.debug(f'Found root:  [{node.uid}] {node.name}')

        return meta

    def download_file(self, file_id: str, dest_path: str):
        """Download a single file based on Google ID and destination path"""
        # logger.debug(f'Downloading file: "{file.identifier}" to "{dest_path}"')
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

            logger.info(f'Download complete: "{dest_path}"')

        try_repeatedly(download)

    def upload_file(self, full_path: str, parents: Union[str, List[str]]) -> str:
        """Upload a single file based on its path. If successful, returns the newly created Google ID"""
        if not full_path:
            raise RuntimeError(f'No path specified for file!')

        if isinstance(parents, str):
            parents = [parents]

        parent_path, file_name = os.path.split(full_path)
        file_metadata = {'name': file_name, 'parents': parents}
        media = MediaFileUpload(filename=full_path, resumable=True)

        def request():
            logger.debug(f'Uploading file: {full_path}')

            response = self.service.files().create(body=file_metadata, media_body=media, fields='id').execute()
            return response

        result = try_repeatedly(request)

        new_file_id = result.get('id', None)
        if not new_file_id:
            raise RuntimeError(f'Upload failed (no ID returned)!')

        logger.debug(f'File uploaded successfully) Returned id={new_file_id}')

        return new_file_id

    def create_folder(self, name: str, parents: Union[str, List[str]]) -> str:
        """Create a folder with the given name. If successful, returns a new Google ID for the created folder"""
        if not name:
            raise RuntimeError(f'No name specified for folder!')

        file_metadata = {'name': name, 'parents': parents, 'mimeType': MIME_TYPE_FOLDER}

        def request():
            logger.debug(f'Creating folder: {name}')

            response = self.service.files().create(body=file_metadata, fields='id').execute()
            return response

        result = try_repeatedly(request)

        new_folder_id = result.get('id', None)
        if not new_folder_id:
            raise RuntimeError(f'Folder creation failed (no ID returned)!')

        logger.debug(f'Folder created successfully) Returned id={new_folder_id}')
        return new_folder_id

    def move_file(self, file_id: str, dest_parent_id: str):
        # TODO
        def request():
            # Retrieve the existing parents to remove
            file = self.service.files().get(fileId=file_id, fields='parents').execute()
            previous_parents = ",".join(file.get('parents'))

            # Move the file to the new folder
            file = self.service.files().update(fileId=file_id, addParents=dest_parent_id,
                                               removeParents=previous_parents, fields='id, parents').execute()
