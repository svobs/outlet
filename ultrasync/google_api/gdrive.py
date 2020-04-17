import file_util
import os.path
import pickle
import logging
import time
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


def do_test():
    service = load_google_client_service()

    # TODO: do not want trashed files included

    fields = 'nextPageToken, incompleteSearch, files(id, name, parents)'
    # Google Drive only; not app data or Google Photos:
    spaces = 'drive'
    page_size = 1000

    logger.info('Listing files...')

    total = 0
    root_nodes = []

    parent_dict = {}
    next_token = None
    page_count = 0

    stopwatch_retrieval = Stopwatch()

    while True:
        retries = 0
        success = False
        results = {}
        while True:
            try:
                logger.debug(f'Making request for page {page_count}...')
                # Call the Drive v3 API
                results = service.files().list(q=FOLDERS_ONLY, fields=fields, spaces=spaces, pageSize=page_size, pageToken=next_token).execute()
                page_count += 1
                success = True
            except Exception as err:
                # Typically a transport error (socket timeout, name server problem...)
               # logger.error(f'Type: {type(err)}')
                logger.error(f'Request failed ("{repr(err)}"); sleeping 3 seconds...')
                time.sleep(3)
                retries += 1
            if success or retries > 10:
                break

        if results.get('incompleteSearch', False):
            raise RuntimeError(f'Results are incomplete! (page {page_count})')

        items = results.get('files', [])
        if not items:
            raise RuntimeError(f'No files returned from Drive API! (page {page_count})')

        for item in items:
            item_id = item['id']
            item_name = item["name"]
            this_node = (item_id, item_name)
            parents = item.get('parents', [])
            logger.debug(f'Item: {item_id} "{item_name}" par={parents}')
            if len(parents) == 0:
                root_nodes.append(this_node)
            else:
                if len(parents) > 1:
                    logger.warning(f'Item has multiple parents: {this_node}')
                for parent_id in parents:
                    child_list = parent_dict.get(parent_id)
                    if not child_list:
                        child_list = []
                        parent_dict[parent_id] = child_list
                    child_list.append(this_node)

            total += 1

        next_token = results.get('nextPageToken')
        if not next_token:
            logger.debug('Done!')
            break

    stopwatch_retrieval.stop()
    logger.info(f'Query returned {total} total items and found {len(root_nodes)} roots in {stopwatch_retrieval}')

    if len(root_nodes) == 0:
        raise RuntimeError(f"Did not get a root! Not even in {total} items!")

    trees = []  # TODO

    rows = []

    for root_item_id, root_item_name in root_nodes:
        logger.debug(f'Building tree for GDrive root: [{root_item_id}] {root_item_name}')
        q = Queue()
        q.put((root_item_id, root_item_name, ''))

        while not q.empty():
            item_id, item_name, parent_path = q.get()
            path = os.path.join(parent_path, item_name)
            rows.append((item_id, item_name, path))
            logger.debug(f'DIR:  [{item_id}] {path}')

            children = parent_dict.get(item_id, None)
            if children:
                for child_id, child_name in children:
                    q.put((child_id, child_name, path))

    db = MetaDatabase(file_util.get_resource_path('gdrive.db'))
    db.insert_gdrive_dirs(rows)

    logger.debug('DONE!')
