import file_util
import os.path
import pickle
import logging
import time
import humanfriendly
from queue import Queue
from stopwatch import Stopwatch
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from apiclient import errors
from ssl import SSLError
from treelib import Tree
from database import MetaDatabase

# If modifying these scopes, delete the file token.pickle.
SCOPES = ['https://www.googleapis.com/auth/drive.metadata.readonly']

FOLDERS_ONLY = "mimeType='application/vnd.google-apps.folder'"
MAX_RETRIES = 10

TOKEN_FILE_PATH = file_util.get_resource_path('token.pickle')
CREDENTIALS_FILE_PATH = file_util.get_resource_path('credentials.json')

logger = logging.getLogger(__name__)


class DirNode:
    def __init__(self, item_id, item_name):
        self.id = item_id
        self.name = item_name


class IntermediateMeta:
    def __init__(self):
        # Keep track of parentless nodes. These usually indicate shared folder roots,
        # but sometimes indicate something else screwy
        self.roots = []

        # 'parent_id' -> list of its DirNode children
        self.first_parent_dict = {}

        # List of item_ids which have more than 1 parent:
        self.ids_with_multiple_parents = []

    def add_to_parent_dict(self, parent_id, item_id, item_name):
        child_list = self.first_parent_dict.get(parent_id)
        if not child_list:
            child_list = []
            self.first_parent_dict[parent_id] = child_list
        child_list.append(DirNode(item_id, item_name))

    def add_id_with_multiple_parents(self, item_id):
        self.ids_with_multiple_parents.append((item_id,))

    def add_root(self, item_id, item_name):
        self.roots.append(DirNode(item_id, item_name))


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


# def do_test_old():
#     service = load_google_client_service()
#
#     # TODO: do not want trashed files included
#     # TODO: 'parents' field has parent ID
#
#     fields = 'nextPageToken, incompleteSearch, files(id, name, version, modifiedTime, originalFilename, md5Checksum, size, parents)'
#     # Google Drive only; not app data or Google Photos:
#     spaces = 'drive'
#     page_size = 20
#
#     logger.info('Listing files...')
#
#     count = 0

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


def get_about():
    fields = 'user, storageQuota, maxUploadSize'
    service = load_google_client_service()
    about = service.about().get(fields=fields).execute()
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


def get_my_drive_root(service=None):
    if not service:
        service = load_google_client_service()

    def request():
        return service.files().get(fileId='root').execute()

    result = try_repeatedly(request)

    root_node = DirNode(result['id'], result['name'])
    logger.debug(f'Drive root: [{root_node.id}] "{root_node.name}"')
    return root_node


def download_subtree_file_list(subtree_root_gd_id):
    service = load_google_client_service()

    query = f"and '{subtree_root_gd_id}' in parents"


def download_directory_structure():
    service = load_google_client_service()

    # TODO: do not want trashed files included

    fields = 'nextPageToken, incompleteSearch, files(id, name, parents)'
    # Google Drive only; not app data or Google Photos:
    spaces = 'drive'
    page_size = 1000  # TODO

    logger.info('Getting list of all directories in Google Drive...')

    # Assume 99.9% of items will have only one parent, and perhaps 0.001% will have no parent.
    # The below solution optimizes with these assumptions.

    meta = IntermediateMeta()

    # Need to make a special call to get the root node 'My Drive':
    drive_root = get_my_drive_root(service)
    meta.add_root(drive_root.id, drive_root.name)

    def request():
        logger.debug(f'Sending request for page {request.page_count}...')
        # Call the Drive v3 API
        response = service.files().list(q=FOLDERS_ONLY, fields=fields, spaces=spaces, pageSize=page_size, pageToken=request.next_token).execute()
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
            parents = item.get('parents', [])
            #logger.debug(f'Item: {item_id} "{item_name}" par={parents}')
            if len(parents) == 0:
                meta.add_root(item_id, item_name)
            else:
                has_multiple_parents = (len(parents) > 1)
                parent_index = 0
                if has_multiple_parents:
                    logger.debug(f'Item has multiple parents:  [{item_id}] {item_name}')
                    meta.add_id_with_multiple_parents(item_id)
                for parent_id in parents:
                    meta.add_to_parent_dict(parent_id, item_id, item_name)
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


def save_in_cache(cache_path, meta, overwrite):
    db = MetaDatabase(cache_path)

    # Convert to tuples for insert into DB:
    root_rows = []
    dir_rows = []
    for parent_id, item_list in meta.first_parent_dict.items():
        for item in item_list:
            if parent_id:
                dir_rows.append((item.id, item.name, parent_id))
            else:
                raise RuntimeError(f'Found root in first_parent_dict: {item}')

    for root in meta.roots:
        root_rows.append((root.id, root.name))

    db.insert_gdrive_dirs(root_rows, dir_rows, meta.ids_with_multiple_parents, overwrite)

    return meta


def load_dirs_from_cache(cache_path):
    db = MetaDatabase(cache_path)
    root_rows, dir_rows, ids_with_multiple_parents = db.get_gdrive_dirs()

    meta = IntermediateMeta()

    for item_id, item_name in root_rows:
        meta.add_root(item_id, item_name)

    for item_id, item_name, parent_id in dir_rows:
        meta.add_to_parent_dict(parent_id, item_id, item_name)

    meta.ids_with_multiple_parents = ids_with_multiple_parents

    return meta


def build_dir_trees(meta: IntermediateMeta):
    rows = []

    total = 0

    names = []
    for root in meta.roots:
        names.append(root.name)

    logger.debug(f'Root nodes: {names}')

    for root_node in meta.roots:
        tree_size = 0
        logger.debug(f'Building tree for GDrive root: [{root_node.id}] {root_node.name}')
        q = Queue()
        q.put((root_node.id, root_node.name, ''))

        while not q.empty():
            item_id, item_name, parent_path = q.get()
            path = os.path.join(parent_path, item_name)
            rows.append((item_id, item_name, path))
        #    logger.debug(f'DIR:  [{item_id}] {path}')
            tree_size += 1
            total += 1

            child_list = meta.first_parent_dict.get(item_id, None)
            if child_list:
                for child in child_list:
                    q.put((child.id, child.name, path))

        logger.debug(f'Root "{root_node.name}" has {tree_size} nodes')

    logger.debug(f'Finished with {total} items!')
