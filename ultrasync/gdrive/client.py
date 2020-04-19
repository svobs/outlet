import file_util
import os.path
import pickle
import logging
import time
import humanfriendly
from datetime import datetime
from stopwatch import Stopwatch
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from gdrive.model import DirNode, FileNode, IntermediateMeta

# If modifying these scopes, delete the file token.pickle.
SCOPES = ['https://www.googleapis.com/auth/drive.metadata.readonly']

FOLDERS_ONLY = "mimeType='application/vnd.google-apps.folder'"
NON_FOLDERS_ONLY = f"not {FOLDERS_ONLY}"
MAX_RETRIES = 10

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
            logger.error(f'Request failed: {repr(err)}: sleeping 3 secs (retries remaining: {retries_remaining}')
            time.sleep(3)
            retries_remaining -= 1


class GDriveClient:
    def __init__(self, config):
        self.service = load_google_client_service()
        self.config = config

        self.page_size = config.get('gdrive.page_size')

    def get_about(self):
        """
        self.service.about().get()
        Returns: info about the current user and its storage usage
        """
        fields = 'user, storageQuota'
        about = self.service.about().get(fields=fields).execute()
        logger.debug(f'ABOUT: {about}')

        user = about['user']
        display_name = user['displayName']
        photo_link = user['photoLink']
        is_me = user['me']
        owner_id = user['permissionId']
        email_address = user['emailAddress']
        logger.info(f'Logged in as user {display_name} <{email_address}> (owner_id={owner_id})')
        logger.debug(f'User photo link: {photo_link}')

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

    def get_my_drive_root(self):
        """
        Returns: a DirNode representing the user's GDrive root node.
        """
        fields = 'id, name, trashed, explicitlyTrashed'

        def request():
            return self.service.files().get(fileId='root', fields=fields).execute()

        result = try_repeatedly(request)

        root_node = DirNode(result['id'], result['name'], result['trashed'], result['explicitlyTrashed'])
        logger.debug(f'Drive root: {root_node.trash_status_str()} [{root_node.id}] "{root_node.name}"')
        return root_node

    def download_subtree_file_meta(self):
        pass
        # TODO
        # query = f"and '{subtree_root_gd_id}' in parents"

    def download_all_file_meta(self, meta):
        fields = 'nextPageToken, incompleteSearch, files(id, name, parents, trashed, explicitlyTrashed, version, createdTime, ' \
                 'modifiedTime, shared, owners, originalFilename, md5Checksum, size, headRevisionId, shortcutDetails, mimeType)'
        # Google Drive only; not app data or Google Photos:
        spaces = 'drive'

        logger.info('Getting list of ALL NON DIRS in Google Drive...')

        def request():
            logger.debug(f'Sending request for files, page {request.page_count}...')
            # Call the Drive v3 API
            response = self.service.files().list(q=NON_FOLDERS_ONLY, fields=fields, spaces=spaces, pageSize=self.page_size,
                                                 pageToken=request.next_token).execute()
            request.page_count += 1
            return response
        request.page_count = 0
        request.next_token = None
        item_count = 0

        stopwatch_retrieval = Stopwatch()
        owner_dict = {}
        mime_types = {}

        while True:
            results = try_repeatedly(request)

            if results.get('incompleteSearch', False):
                raise RuntimeError(f'Results are incomplete! (page {request.page_count})')

            items = results.get('files', [])
            if not items:
                raise RuntimeError(f'No files returned from Drive API! (page {request.page_count})')

            logger.debug(f'Received {len(items)} items')

            for item in items:
                owners = item['owners']
                owner = None if len(owners) == 0 else owners[0]
                owner_id = None
                if owner:
                    owner_id = owner['permissionId']
                    owner_name = owner['displayName']
                    owner_email = owner['emailAddress']
                    owner_is_me = owner['me']
                    owner_dict[owner_id] = (owner_name, owner_email, owner_is_me)

                created_ts_obj = datetime.strptime(item['createdTime'], ISO_8601_FMT)
                created_ts = int(created_ts_obj.timestamp() * 1000)
                modified_ts_obj = datetime.strptime(item['modifiedTime'], ISO_8601_FMT)
                modified_ts = int(modified_ts_obj.timestamp() * 1000)
                # TODO: find out if this is ever useful
                original_filename = item.get('originalFilename', None)
                # TODO: why is this sometimes absent?
                head_revision_id = item.get('headRevisionId', None)
                size_str = item.get('size', None)
                size = None if size_str is None else int(size_str)

                node = FileNode(item_id=item['id'], item_name=item["name"], original_filename=original_filename,
                                version=int(item['version']), head_revision_id=head_revision_id,
                                md5=item.get('md5Checksum', None), shared=item['shared'], created_ts=created_ts,
                                modified_ts=modified_ts, size_bytes=size, owner_id=owner_id,
                                trashed=item['trashed'], explicitly_trashed=item['explicitlyTrashed'])
                parents = item.get('parents', [])
                meta.add_item_with_parents(parents, node)
                item_count += 1
                mime_types[item['mimeType']] = node

            request.next_token = results.get('nextPageToken')
            if not request.next_token:
                logger.debug('Done!')
                break

        logger.info(f'Query returned {item_count} files in {stopwatch_retrieval}')

        if logger.isEnabledFor(logging.DEBUG):
            logger.debug(f'Found {len(owner_dict)} distinct owners')
            for owner_id, owner in owner_dict.items():
                logger.debug(f'Found owner: id={owner_id} name={owner[0]} email={owner[1]} is_me={owner[2]}')

            logger.debug(f'Found {len(mime_types)} distinct MIME types')
            for mime_type, item in mime_types.items():
                logger.debug(f'MIME type: {mime_type} -> [{item.id}] {item.name} {item.size_bytes}')

        return meta

    def download_directory_structure(self, meta):
        """
        Downloads all of the directory nodes from the user's GDrive and puts them into a
        IntermediateMeta object.

        Assume 99.9% of items will have only one parent, and perhaps 0.001% will have no parent.
        he below solution optimizes with these assumptions.
        """

        # Need to make a special call to get the root node 'My Drive'. This node will not be included
        # in the "list files" call:
        drive_root = self.get_my_drive_root()
        meta.add_root(drive_root)

        fields = 'nextPageToken, incompleteSearch, files(id, name, parents, trashed, explicitlyTrashed)'
        # Google Drive only; not app data or Google Photos:
        spaces = 'drive'

        logger.info('Getting list of ALL directories in Google Drive...')

        def request():
            logger.debug(f'Sending request for dirs, page {request.page_count}...')
            # Call the Drive v3 API
            response = self.service.files().list(q=FOLDERS_ONLY, fields=fields, spaces=spaces, pageSize=self.page_size,
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

            logger.debug(f'Received {len(items)} items')

            for item in items:
                dir_node = DirNode(item['id'], item["name"], item['trashed'], item['explicitlyTrashed'])
                parents = item.get('parents', [])
                meta.add_item_with_parents(parents, dir_node)
                item_count += 1

            request.next_token = results.get('nextPageToken')
            if not request.next_token:
                logger.debug('Done!')
                break

        logger.info(f'Query returned {item_count} directories in {stopwatch_retrieval}')

        if logger.isEnabledFor(logging.DEBUG):
            for node in meta.roots:
                logger.debug(f'Found root:  [{node.id}] {node.name}')

        return meta
