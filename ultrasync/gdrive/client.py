import file_util
import os.path
import pickle
import logging
import time
import humanfriendly
from stopwatch import Stopwatch
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from gdrive.model import DirNode, IntermediateMeta

# If modifying these scopes, delete the file token.pickle.
SCOPES = ['https://www.googleapis.com/auth/drive.metadata.readonly']

FOLDERS_ONLY = "mimeType='application/vnd.google-apps.folder'"
MAX_RETRIES = 10

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

    def get_about(self):
        """
        self.service.about().get()
        Returns: info about the current user and its storage usage
        """
        fields = 'user, storageQuota, maxUploadSize'
        about = self.service.about().get(fields=fields).execute()
        logger.debug(f'ABOUT: {about}')

        user = about['user']
        display_name = user['displayName']
        photo_link = user['photoLink']
        is_me = user['me']
        email_address = user['emailAddress']
        logger.info(f'Logged in as user {display_name} <{email_address}>')
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
        logger.debug(f'Drive root: {root_node.trash_status_str} [{root_node.id}] "{root_node.name}"')
        return root_node

    def download_subtree_file_list(self, subtree_root_gd_id):
        service = load_google_client_service()

        query = f"and '{subtree_root_gd_id}' in parents"

    def download_directory_structure(self):
        """
        Downloads all of the directory nodes from the user's GDrive and puts them into a
        IntermediateMeta object.

        Assume 99.9% of items will have only one parent, and perhaps 0.001% will have no parent.
        he below solution optimizes with these assumptions.
        """
        meta = IntermediateMeta()

        # Need to make a special call to get the root node 'My Drive'. This node will not be included
        # in the "list files" call:
        drive_root = self.get_my_drive_root()
        meta.add_root(drive_root)

        fields = 'nextPageToken, incompleteSearch, files(id, name, parents, trashed, explicitlyTrashed)'
        # Google Drive only; not app data or Google Photos:
        spaces = 'drive'
        page_size = 1000  # TODO

        logger.info('Getting list of all directories in Google Drive...')

        def request():
            logger.debug(f'Sending request for page {request.page_count}...')
            # Call the Drive v3 API
            response = self.service.files().list(q=FOLDERS_ONLY, fields=fields, spaces=spaces, pageSize=page_size,
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
                item_id = item['id']
                item_name = item["name"]
                trashed = item['trashed']
                explicitly_trashed = item['explicitlyTrashed']
                dir_node = DirNode(item_id, item_name, trashed, explicitly_trashed)
                parents = item.get('parents', [])
                #logger.debug(f'Item: {item_id} "{item_name}" par={parents} trashed={dir_node.trashed}')
                if len(parents) == 0:
                    meta.add_root(dir_node)
                else:
                    has_multiple_parents = (len(parents) > 1)
                    parent_index = 0
                    if has_multiple_parents:
                        logger.debug(f'Item has multiple parents:  [{item_id}] {item_name}')
                        meta.add_id_with_multiple_parents(dir_node)
                    for parent_id in parents:
                        meta.add_to_parent_dict(parent_id, dir_node)
                        if has_multiple_parents:
                            parent_index += 1
                            logger.debug(f'\tParent {parent_index}: [{parent_id}]')

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
