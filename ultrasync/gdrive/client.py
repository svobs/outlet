import file_util
import os.path
import pickle
import logging
import time
import humanfriendly
from datetime import datetime
from stopwatch_sec import Stopwatch
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request

# If modifying these scopes, delete the file token.pickle.
from constants import EXPLICITLY_TRASHED, IMPLICITLY_TRASHED, NOT_TRASHED
from model.gdrive_tree import UserMeta
from model.goog_node import GoogFile, GoogFolder
from ui import actions

SCOPES = ['https://www.googleapis.com/auth/drive.metadata.readonly']

MIME_TYPE_SHORTCUT = 'application/vnd.google-apps.shortcut'
MIME_TYPE_FOLDER = 'application/vnd.google-apps.folder'

QUERY_FOLDERS_ONLY = f"mimeType='{MIME_TYPE_FOLDER}'"
QUERY_NON_FOLDERS_ONLY = f"not {QUERY_FOLDERS_ONLY}"
MAX_RETRIES = 10

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
    retries_remaining = MAX_RETRIES
    while True:
        try:
            return request_func()
        except Exception as err:
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


def convert_goog_folder(result, sync_ts):
    # 'driveId' only populated for items which someone has shared with me
    # 'shared' only populated for items which are owned by me
    return GoogFolder(item_id=result['id'], item_name=result['name'], trashed=convert_trashed(result),
                      drive_id=result.get('driveId', None), my_share=result.get('shared', None), sync_ts=sync_ts, all_children_fetched=False)


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

    def get_my_drive_root(self, sync_ts):
        """
        Returns: a GoogFolder representing the user's GDrive root node.
        """

        def request():
            return self.service.files().get(fileId='root', fields=fields).execute()

        fields = 'id, name, trashed, explicitlyTrashed, shared, driveId'
        result = try_repeatedly(request)

        root_node = convert_goog_folder(result, sync_ts)
        logger.debug(f'Drive root: {root_node}')
        return root_node

    def download_subtree_file_meta(self):
        pass
        # TODO
        # query = f"and '{subtree_root_gd_id}' in parents"

    def download_all_file_meta(self, meta, sync_ts):
        fields = f'nextPageToken, incompleteSearch, files({FILE_FIELDS}, parents)'
        # Google Drive only; not app data or Google Photos:
        spaces = 'drive'

        logger.info('Getting list of ALL NON DIRS in Google Drive...')

        def request():
            msg = f'Sending request for files, page {request.page_count}...'
            logger.debug(msg)
            if self.tree_id:
                actions.get_dispatcher().send(actions.SET_PROGRESS_TEXT, sender=self.tree_id, msg=msg)

            # Call the Drive v3 API
            response = self.service.files().list(q=QUERY_NON_FOLDERS_ONLY, fields=fields, spaces=spaces, pageSize=self.page_size,
                                                 pageToken=request.next_token).execute()
            request.page_count += 1
            return response
        request.page_count = 0
        request.next_token = None
        item_count = 0

        stopwatch_retrieval = Stopwatch()

        while True:
            results = try_repeatedly(request)

            if results.get('incompleteSearch', False):
                # Not clear when this would happen, but fail fast if so
                raise RuntimeError(f'Results are incomplete! (page {request.page_count})')

            items = results.get('files', [])
            if not items:
                raise RuntimeError(f'No files returned from Drive API! (page {request.page_count})')

            msg = f'Received {len(items)} items'
            logger.debug(msg)
            if self.tree_id:
                actions.get_dispatcher().send(actions.SET_PROGRESS_TEXT, sender=self.tree_id, msg=msg)

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

                node = GoogFile(item_id=item['id'], item_name=item["name"], trashed=convert_trashed(item), drive_id=item.get('driveId', None),
                                version=version, head_revision_id=head_revision_id,
                                md5=item.get('md5Checksum', None), my_share=item.get('shared', None), create_ts=create_ts,
                                modify_ts=modify_ts, size_bytes=size, owner_id=owner_id, sync_ts=sync_ts)
                node.parents = item.get('parents', [])
                meta.add_item(node)
                meta.mime_types[mime_type] = node

                # web_view_link = item.get('webViewLink', None)
                # if web_view_link:
                #     logger.debug(f'Found webViewLink: "{web_view_link}" for node: {node}')
                #
                # web_content_link = item.get('webContentLink', None)
                # if web_content_link:
                #     logger.debug(f'Found webContentLink: "{web_content_link}" for node: {node}')

                sharing_user = item.get('sharingUser', None)
                if sharing_user:
                    logger.debug(f'Found sharingUser: "{sharing_user}" for node: {node}')

                is_shortcut = mime_type == MIME_TYPE_SHORTCUT
                if is_shortcut:
                    shortcut_details = item.get('shortcutDetails', None)
                    if not shortcut_details:
                        logger.error(f'Shortcut is missing shortcutDetails: id="{node.uid}" name="{node.name}"')
                    else:
                        target_id = shortcut_details.get('targetId')
                        if not target_id:
                            logger.error(f'Shortcut is missing targetId: id="{node.uid}" name="{node.name}"')
                        else:
                            logger.debug(f'Found shortcut: id="{node.uid}" name="{node.name}" -> target_id="{target_id}"')
                            meta.shortcuts[node.uid] = target_id

                item_count += 1

            request.next_token = results.get('nextPageToken')
            if not request.next_token:
                logger.debug('Done!')
                break

        logger.info(f'Query returned {item_count} files in {stopwatch_retrieval}')

        if logger.isEnabledFor(logging.DEBUG):
            logger.debug(f'Found {len(meta.owner_dict)} distinct owners')
            for owner_id, owner in meta.owner_dict.items():
                logger.debug(f'Found owner: id={owner_id} name={owner[0]} email={owner[1]} is_me={owner[2]}')

            logger.debug(f'Found {len(meta.mime_types)} distinct MIME types')
            for mime_type, item in meta.mime_types.items():
                logger.debug(f'MIME type: {mime_type} -> [{item.uid}] {item.name} {item.size_bytes}')

        return meta

    def download_directory_structure(self, meta, sync_ts):
        """
        Downloads all of the directory nodes from the user's GDrive and puts them into a
        GDriveMeta object.

        Assume 99.9% of items will have only one parent, and perhaps 0.001% will have no parent.
        he below solution optimizes with these assumptions.
        """

        # Need to make a special call to get the root node 'My Drive'. This node will not be included
        # in the "list files" call:
        drive_root = self.get_my_drive_root(sync_ts)
        meta.add_item(drive_root)

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
                                                 pageToken=request.next_token).execute()
            request.page_count += 1
            return response
        request.page_count = 0
        request.next_token = None
        item_count = 0

        stopwatch_retrieval = Stopwatch()

        while True:
            results = try_repeatedly(request)

            if results.get('incompleteSearch', False):
                raise RuntimeError(f'Results are incomplete! (page {request.page_count})')

            items = results.get('files', [])
            if not items:
                raise RuntimeError(f'No files returned from Drive API! (page {request.page_count})')

            msg = f'Received {len(items)} items'
            logger.debug(msg)
            if self.tree_id:
                actions.get_dispatcher().send(actions.SET_PROGRESS_TEXT, sender=self.tree_id, msg=msg)

            for item in items:
                dir_node = convert_goog_folder(item, sync_ts)
                dir_node.parents = item.get('parents', [])
                meta.add_item(dir_node)
                item_count += 1

            request.next_token = results.get('nextPageToken')
            if not request.next_token:
                logger.debug('Done!')
                break

        logger.info(f'Query returned {item_count} directories in {stopwatch_retrieval}')

        if logger.isEnabledFor(logging.DEBUG):
            for node in meta.roots:
                logger.debug(f'Found root:  [{node.uid}] {node.name}')

        return meta
