import io
import json
import logging
import os.path
import pickle
import socket
import time
from collections import deque
from functools import partial
from typing import Callable, Deque, Dict, List, Optional, Tuple, Union

import humanfriendly
from google.auth.transport.requests import Request
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build, Resource
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaFileUpload, MediaIoBaseDownload
from pydispatch import dispatcher

from be.tree_store.gdrive.client.change_observer import GDriveChangeObserver, GDriveNodeChange, GDriveRM
from be.tree_store.gdrive.client.conversion import GDriveAPIConverter
from be.tree_store.gdrive.client.query_observer import GDriveQueryObserver, SimpleNodeCollector
from constants import GDRIVE_AUTH_SCOPES, GDRIVE_CLIENT_REQUEST_MAX_RETRIES, GDRIVE_CLIENT_SLEEP_ON_FAILURE_SEC, GDRIVE_FILE_FIELDS, \
    GDRIVE_FOLDER_FIELDS, \
    GDRIVE_MY_DRIVE_ROOT_GOOG_ID, MIME_TYPE_FOLDER, QUERY_FOLDERS_ONLY, QUERY_NON_FOLDERS_ONLY, TreeID
from logging_constants import SUPER_DEBUG_ENABLED
from error import GDriveError, GDriveItemNotFoundError, GDriveNodePathNotFoundError
from model.gdrive_meta import GDriveUser
from model.node.gdrive_node import GDriveFile, GDriveFolder, GDriveNode
from model.uid import UID
from signal_constants import Signal
from util import file_util, time_util
from util.has_lifecycle import HasLifecycle
from util.stopwatch_sec import Stopwatch
from util.task_runner import Task

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


class QueryRequestState:
    def __init__(self, initial_page_token, sync_ts: int, observer: GDriveQueryObserver, parent_task: Optional[Task] = None):
        self.page_token: Optional[str] = initial_page_token
        self.page_count: int = 0
        self.item_count: int = 0
        self.sync_ts: int = sync_ts
        self.observer: GDriveQueryObserver = observer
        self.parent_task: Optional[Task] = parent_task
        self.stopwatch_retrieval = Stopwatch()


class GDriveClient(HasLifecycle):
    """
    ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
    CLASS GDriveClient
    ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼
    """

    def __init__(self, backend, gdrive_store):
        HasLifecycle.__init__(self)
        self.backend = backend
        self.gdrive_store = gdrive_store
        self.tree_id: Optional[TreeID] = None
        self.page_size: int = self.backend.get_config('gdrive.page_size')
        self.service: Optional[Resource] = None
        self._converter = GDriveAPIConverter(self.gdrive_store)

    @property
    def device_uid(self) -> UID:
        return self.gdrive_store.device_uid

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
                            if err.resp and err.resp.status in [403, 404]:
                                error_json = json.loads(err.content).get('error').get('errors')[0]
                                reason = error_json.get('reason')
                                message = error_json.get('message')
                                # TODO: something more slick
                                if err.resp.status == 404:
                                    raise GDriveItemNotFoundError(message)
                                raise GDriveError(f'Google Drive returned HTTP {err.resp.status}: Reason: "{reason}": "{message}"')
                        except AttributeError as err2:
                            logger.error(f'Additional error: {err2}')
                    else:
                        logger.exception(err)

                    if retries_remaining == 0:
                        raise
                    # Typically a transport error (socket timeout, name server problem...)
                    logger.info(f'Request failed: {repr(err)}: sleeping {GDRIVE_CLIENT_SLEEP_ON_FAILURE_SEC} sec '
                                f'(retries remaining: {retries_remaining})')
                time.sleep(GDRIVE_CLIENT_SLEEP_ON_FAILURE_SEC)
                retries_remaining -= 1

    def _exec_single_page_request(self, this_task: Optional[Task], make_request_func: Callable[[QueryRequestState], None],
                                  request_state: QueryRequestState):
        binded_request = partial(make_request_func, request_state)
        results: dict = GDriveClient._try_repeatedly(binded_request)

        # TODO: how will we know if the token is invalid?

        if results.get('incompleteSearch', False):
            # Not clear when this would happen, but fail fast if so.
            # If executing via the Central Executor, this will be caught and reported (and task.on_error() called if it exists)
            raise RuntimeError(f'Results are incomplete! (page {request_state.page_count}, token {request_state.page_token})')

        items: list = results.get('files', [])
        if not items:
            logger.debug('Request returned no files')
            return

        msg = f'Received {len(items)} items'
        logger.debug(msg)
        if self.tree_id:
            dispatcher.send(Signal.SET_PROGRESS_TEXT, sender=self.tree_id, msg=msg)

        for item in items:
            mime_type = item['mimeType']
            if mime_type == MIME_TYPE_FOLDER:
                goog_node: GDriveFolder = self._converter.dict_to_gdrive_folder(item, sync_ts=request_state.sync_ts)
            else:
                goog_node: GDriveFile = self._converter.dict_to_gdrive_file(item, sync_ts=request_state.sync_ts)

            request_state.observer.node_received(goog_node, item)
            request_state.item_count += 1

        request_state.page_token = results.get('nextPageToken')

        request_state.observer.end_of_page(request_state.page_token)

        if not request_state.page_token:
            logger.debug(f'{request_state.stopwatch_retrieval} Done. Query returned {request_state.item_count} nodes')
        elif request_state.parent_task:
            # essentially loop, with each loop being scheduled through the Central Executor as a child task of the parent:
            next_child_task = request_state.parent_task.create_child_task(self._exec_single_page_request, make_request_func, request_state)
            self.backend.executor.submit_async_task(next_child_task)

    def _execute_files_query(self, query: str, fields: str, initial_page_token: Optional[str], sync_ts: int, observer: GDriveQueryObserver,
                             this_task: Optional[Task] = None):
        """Gets a list of files and/or folders which match the query criteria."""

        # Google Drive only; not backend data or Google Photos:
        spaces = 'drive'

        def make_request(state: QueryRequestState):
            m = f'Sending request for GDrive items, page {state.page_count}...'
            logger.debug(f'{m} (q="{query}")')
            if self.tree_id:
                dispatcher.send(signal=Signal.SET_PROGRESS_TEXT, sender=self.tree_id, msg=m)

            # Call the Drive v3 API
            response = self.service.files().list(q=query, fields=fields, spaces=spaces, pageSize=self.page_size,
                                                 # include items from shared drives
                                                 includeItemsFromAllDrives=True, supportsAllDrives=True,
                                                 pageToken=state.page_token).execute()
            state.page_count += 1
            return response

        if initial_page_token:
            logger.info('Found a page token. Attempting to resume previous download')

        request_state = QueryRequestState(initial_page_token, sync_ts, observer, parent_task=this_task)

        if this_task:
            # Kick off first request as a child task:
            next_child_task = request_state.parent_task.create_child_task(self._exec_single_page_request, make_request, request_state)
            self.backend.executor.submit_async_task(next_child_task)
        else:
            while True:
                self._exec_single_page_request(None, make_request, request_state)
                if not request_state.page_token:
                    logger.debug('Done!')
                    break

    # VARIOUS GETTERS
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

        user: GDriveUser = self._converter.get_or_store_user(about['user'])
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

        root_node = self._converter.dict_to_gdrive_folder(result, sync_ts)
        logger.debug(f'Drive root: {root_node}')
        return root_node

    def get_all_shared_with_me(self) -> List[GDriveNode]:
        query = f"sharedWithMe"
        fields = f'nextPageToken, incompleteSearch, files({GDRIVE_FILE_FIELDS}, parents)'

        sync_ts = time_util.now_sec()
        observer = SimpleNodeCollector()
        self._execute_files_query(query, fields, None, sync_ts, observer)
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
        self._execute_files_query(query, fields, None, sync_ts, observer)
        return observer.nodes

    def get_subtree_bfs_node_list(self, parent_goog_id: str) -> List[GDriveNode]:
        """Very slow operation! Use sparingly!"""
        if not parent_goog_id:
            raise RuntimeError(f'get_subtree_bfs_node_list(): parent_goog_id cannot be empty!')

        bfs_list: List = []

        parent_goog_id_queue: Deque[str] = deque()
        parent_goog_id_queue.append(parent_goog_id)

        while len(parent_goog_id_queue) > 0:
            parent_goog_id = parent_goog_id_queue.popleft()
            for child_node in self.get_all_children_for_parent(parent_goog_id):
                bfs_list.append(child_node)
                if child_node.is_dir():
                    parent_goog_id_queue.append(child_node.goog_id)

        return bfs_list

    def get_single_node_with_parent_and_name_and_criteria(self, node: GDriveNode, match_func: Callable[[GDriveNode], bool] = None) \
            -> Optional[GDriveNode]:
        src_parent_goog_id_list: List[str] = self.backend.cacheman.get_parent_goog_id_list(node)
        if not src_parent_goog_id_list:
            raise RuntimeError(f'TNode has no parents: "{node.name}" ({node.device_uid}:{node.uid}')

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
        self._execute_files_query(query, fields, None, sync_ts, observer)

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

        try:
            item: dict = GDriveClient._try_repeatedly(request)
        except GDriveItemNotFoundError as err:
            logger.debug(f'Caught GDriveItemNotFoundError (will return None): {err}')
            return None

        if not item:
            logger.debug('Request returned no files')
            return None

        mime_type = item['mimeType']
        if mime_type == MIME_TYPE_FOLDER:
            goog_node: GDriveFolder = self._converter.dict_to_gdrive_folder(item, sync_ts=sync_ts)
        else:
            goog_node: GDriveFile = self._converter.dict_to_gdrive_file(item, sync_ts=sync_ts)

        if SUPER_DEBUG_ENABLED:
            logger.debug(f'Request returned {goog_node}')

        return goog_node

    # FILES
    # ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼

    def get_single_file_with_parent_and_name_and_criteria(self, node: GDriveNode, match_func: Callable[[GDriveNode], bool] = None) \
            -> Optional[GDriveNode]:
        """Important note: this method will raise an exception if more than one file is found with the specified criteria"""
        src_parent_goog_id_list: List[str] = self.backend.cacheman.get_parent_goog_id_list(node)
        if not src_parent_goog_id_list:
            raise RuntimeError(f'TNode has no parents: "{node.name}" ({node.device_uid}:{node.uid}')

        result: SimpleNodeCollector = self._get_existing_file_with_parent_and_name(parent_goog_id=src_parent_goog_id_list[0], name=node.name)
        logger.debug(f'Found {len(result.nodes)} matching GDrive files with parent={src_parent_goog_id_list[0]} and name="{node.name}"')

        if len(result.nodes) > 0:
            found_list = []
            for found_node in result.nodes:
                assert isinstance(found_node, GDriveFile)
                if match_func:
                    if match_func(found_node):
                        found_list.append(found_node)
                else:
                    found_list.append(found_node)

            if len(found_list) > 1:
                raise RuntimeError(f'Found {len(found_list)} GDrive files for the specified criteria! Expected to find only 0 or 1')
            elif len(found_list) == 1:
                return found_list[0]

        return None

    def _get_existing_file_with_parent_and_name(self, parent_goog_id: str, name: str) -> SimpleNodeCollector:
        query = f"{QUERY_NON_FOLDERS_ONLY} AND name='{name}' AND '{parent_goog_id}' in parents"
        fields = f'nextPageToken, incompleteSearch, files({GDRIVE_FILE_FIELDS}, parents)'

        logger.debug(f'Getting existing files named "{name}" with parent "{parent_goog_id}"')

        sync_ts = time_util.now_sec()
        observer = SimpleNodeCollector()
        self._execute_files_query(query, fields, None, sync_ts, observer)

        return observer

    def get_all_non_folders(self, initial_page_token: Optional[str], sync_ts: int, observer: GDriveQueryObserver,
                            this_task: Optional[Task] = None):
        query = QUERY_NON_FOLDERS_ONLY
        fields = f'nextPageToken, incompleteSearch, files({GDRIVE_FILE_FIELDS}, parents)'

        logger.info('Getting list of ALL NON DIRS in Google Drive...')

        return self._execute_files_query(query, fields, initial_page_token, sync_ts, observer, this_task)

    def copy_existing_file(self, src_goog_id: str, new_name: str, new_parent_goog_ids: List[str], uid: Optional[UID] = None) \
            -> Optional[GDriveNode]:
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

        goog_node: GDriveFile = self._converter.dict_to_gdrive_file(item, sync_ts=sync_ts, uid=uid)

        if SUPER_DEBUG_ENABLED:
            logger.debug(f'Request returned {goog_node}')

        return goog_node

    # FOLDERS
    # ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼

    def get_all_folders(self, initial_page_token: Optional[str], sync_ts: int, observer: GDriveQueryObserver,
                        this_task: Optional[Task] = None):
        """
        Downloads all of the directory nodes from the user's GDrive and puts them into a
        GDriveMeta object.

        Assume 99.9% of items will have only one parent, and perhaps 0.001% will have no parent.
        he below solution optimizes with these assumptions.
        """
        fields = f'nextPageToken, incompleteSearch, files({GDRIVE_FOLDER_FIELDS}, parents)'

        self._execute_files_query(QUERY_FOLDERS_ONLY, fields, initial_page_token, sync_ts, observer, this_task)

    def get_folders_with_parent_and_name(self, parent_goog_id: str, name: str) -> SimpleNodeCollector:
        query = f"{QUERY_FOLDERS_ONLY} AND name='{name}' AND '{parent_goog_id}' in parents"
        fields = f'nextPageToken, incompleteSearch, files({GDRIVE_FOLDER_FIELDS}, parents)'

        sync_ts = time_util.now_sec()
        observer = SimpleNodeCollector()
        self._execute_files_query(query, fields, None, sync_ts, observer)
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

        goog_node: GDriveFolder = self._converter.dict_to_gdrive_folder(item, uid=uid)
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
                logger.debug(f'GDrive [goog_id={file_id}] download progress: {status.progress() * 100}%')

            with io.open(dest_path, 'wb') as f:
                fh.seek(0)
                f.write(fh.read())

            logger.info(f'GDrive [goog_id={file_id}] download successful: dest="{dest_path}"')

        GDriveClient._try_repeatedly(download)

    def upload_new_file(self, local_file_full_path: str, parent_goog_ids: Union[str, List[str]], uid: UID, create_ts: int, modify_ts: int) \
            -> GDriveFile:
        """Upload a single file based on its path. If successful, returns the newly created GDriveFile"""
        if not local_file_full_path:
            raise RuntimeError(f'No path specified for file!')

        if isinstance(parent_goog_ids, str):
            parent_goog_ids = [parent_goog_ids]

        parent_path, file_name = os.path.split(local_file_full_path)
        meta = {'name': file_name, 'parents': parent_goog_ids}

        if create_ts:
            # formatted RFC 3339 timestamp
            meta['createdTime'] = time_util.ts_to_rfc_3339(create_ts)

        if modify_ts:
            meta['modifiedTime'] = time_util.ts_to_rfc_3339(modify_ts)

        media = MediaFileUpload(filename=local_file_full_path, resumable=True)

        def request():
            logger.debug(f'Uploading local file: "{local_file_full_path}" to parents: {parent_goog_ids}')

            # https://developers.google.com/drive/api/v3/reference/files/create
            response = self.service.files().create(body=meta, media_body=media, fields=f'{GDRIVE_FILE_FIELDS}, parents').execute()
            return response

        file_meta = GDriveClient._try_repeatedly(request)
        gdrive_file = self._converter.dict_to_gdrive_file(file_meta, uid=uid)

        logger.info(
            f'File uploaded successfully! Returning {gdrive_file}",')

        return gdrive_file

    def upload_update_to_existing_file(self, new_name: str, mime_type: str, goog_id: str, local_file_full_path: str,
                                       create_ts: Optional[int] = None, modify_ts: Optional[int] = None) -> GDriveFile:
        if not local_file_full_path:
            raise RuntimeError(f'No path specified for file!')
        if not create_ts:
            raise RuntimeError(f'No create_ts specified for file!')
        if not modify_ts:
            raise RuntimeError(f'No modify_ts specified for file!')
        if not mime_type:
            raise RuntimeError(f'No mime_type specified for file!')

        gdrive_file: GDriveNode = self.modify_meta(goog_id=goog_id, new_name=new_name, mime_type=mime_type,
                                                   local_file_full_path=local_file_full_path,
                                                   create_ts=create_ts, modify_ts=modify_ts, add_parents=[], remove_parents=[])
        assert isinstance(gdrive_file, GDriveFile), f'Not a GDriveFile: {gdrive_file}'

        logger.info(
            f'File update uploaded successfully) Returned name="{gdrive_file.name}", version="{gdrive_file.version}", '
            f'goog_id="{gdrive_file.goog_id}"')

        return gdrive_file

    # MISC
    # ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼

    def modify_meta(self, goog_id: str, remove_parents: List[str], add_parents: List[str], new_name: Optional[str] = None,
                    create_ts: Optional[int] = None, modify_ts: Optional[int] = None, mime_type: Optional[str] = None,
                    local_file_full_path: Optional[str] = None) -> GDriveNode:
        assert isinstance(add_parents, list), f'For goog_id={goog_id}: {add_parents}'
        assert isinstance(remove_parents, list), f'For goog_id={goog_id}: {remove_parents}'
        if not goog_id:
            raise RuntimeError(f'No goog_id specified for file!')

        meta = {}
        if new_name:
            meta['name'] = new_name

        if create_ts:
            # formatted RFC 3339 timestamp
            meta['createdTime'] = time_util.ts_to_rfc_3339(create_ts)

        if modify_ts:
            # formatted RFC 3339 timestamp
            meta['modifiedTime'] = time_util.ts_to_rfc_3339(modify_ts)
            modified_date_behavior = 'fromBody'
        else:
            modified_date_behavior = 'noChange'

        if mime_type:
            meta['mimeType'] = mime_type

        if local_file_full_path:
            logger.debug(f'Updating node "{goog_id}" with local file: "{local_file_full_path}"')
            media = MediaFileUpload(filename=local_file_full_path, resumable=True)
        else:
            media = None

        def request():
            # https://developers.google.com/drive/api/v3/reference/files/update
            file = self.service.files().update(fileId=goog_id, body=meta, addParents=add_parents, removeParents=remove_parents,
                                               media_body=media, newRevision=True,
                                               modifiedDateBehavior=modified_date_behavior,
                                               updateViewedDate=False, fields=f'{GDRIVE_FILE_FIELDS}, parents').execute()
            return file

        updated_meta = GDriveClient._try_repeatedly(request)

        mime_type = updated_meta['mimeType']
        # these will look up the UID which matches the goog_id:
        if mime_type == MIME_TYPE_FOLDER:
            goog_node = self._converter.dict_to_gdrive_folder(updated_meta)
        else:
            goog_node = self._converter.dict_to_gdrive_file(updated_meta)
        return goog_node

    def trash(self, goog_id: str):
        """Changes the trashed status of the given goog_id"""
        logger.debug(f'Sending request to trash file with goog_id="{goog_id}"')

        file_metadata = {'trashed': True}

        def request():
            return self.service.files().update(fileId=goog_id, body=file_metadata, modifiedDateBehavior='noChange',
                                               updateViewedDate=False, fields=f'{GDRIVE_FILE_FIELDS}, parents').execute()

        file_meta = GDriveClient._try_repeatedly(request)
        gdrive_file: GDriveFile = self._converter.dict_to_gdrive_file(file_meta)

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

    # CHANGES
    # ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼

    def get_changes_start_token(self) -> str:
        logger.debug(f'Sending request to get startPageToken from Changes API"')

        def request():
            return self.service.changes().getStartPageToken().execute()

        response: Dict = GDriveClient._try_repeatedly(request)

        token = response.get('startPageToken', None)

        logger.debug(f'Got token: "{token}"')
        return token

    def _get_one_page_of_changes(self, this_task: Task, make_request_func: Callable[[GDriveChangeObserver], None], observer: GDriveChangeObserver):
        binded_request = partial(make_request_func, observer)
        response_dict: dict = GDriveClient._try_repeatedly(binded_request)

        # TODO: how will we know if the token is invalid?

        items: list = response_dict.get('changes', [])
        if not items:
            logger.debug('Request returned no changes')
            observer.new_start_token = response_dict.get('newStartPageToken', None)
            return

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
                if not node:
                    # TODO: keep a list of recently deleted goog_ids in our database, so we can be sure.
                    logger.warning(f'GDrive changelist shows node with goog_id {goog_id} was removed from GDrive, but cannot find in our cache '
                                   f'(Did we delete it?) Skipping.')
                    continue

                change: GDriveRM = GDriveRM(change_ts, goog_id, node)
            else:
                if item['changeType'] == 'file':
                    file = item['file']
                    mime_type = file['mimeType']
                    if mime_type == MIME_TYPE_FOLDER:
                        goog_node: GDriveFolder = self._converter.dict_to_gdrive_folder(file, sync_ts=observer.sync_ts)
                    else:
                        goog_node: GDriveFile = self._converter.dict_to_gdrive_file(file, sync_ts=observer.sync_ts)
                    change: GDriveNodeChange = GDriveNodeChange(change_ts, goog_id, goog_node)
                else:
                    logger.error(f'Strange item: {item}')
                    raise RuntimeError(f'is_removed==true but changeType is not "file" (got "{item["changeType"]}" instead')

            observer.change_received(change, item)

        observer.new_start_token = response_dict.get('nextPageToken', None)

        observer.end_of_page()

        if observer.new_start_token:
            next_child_task = observer.parent_task.create_child_task(self._get_one_page_of_changes, make_request_func, observer)
            self.backend.executor.submit_async_task(next_child_task)

    def get_changes_list(self, observer: GDriveChangeObserver):
        logger.debug(f'Sending request to get changes from start_page_token: "{observer.new_start_token}"')

        # Google Drive only; not backend data or Google Photos:
        spaces = 'drive'

        def make_request_func(_observer: GDriveChangeObserver):
            m = f'Sending request for changes, page {_observer.page_count} (token: {_observer.new_start_token})...'
            logger.debug(m)
            if self.tree_id:
                dispatcher.send(signal=Signal.SET_PROGRESS_TEXT, sender=self.tree_id, msg=m)

            # Call the Drive v3 API
            response = self.service.changes().list(pageToken=_observer.new_start_token, fields=f'nextPageToken, newStartPageToken, '
                                                                                               f'changes(changeType, time, removed, fileId, driveId, '
                                                                                               f'file({GDRIVE_FILE_FIELDS}, parents))', spaces=spaces,
                                                   # include changes from shared drives
                                                   includeItemsFromAllDrives=True, supportsAllDrives=True,
                                                   pageSize=self.page_size).execute()
            return response

        next_child_task = observer.parent_task.create_child_task(self._get_one_page_of_changes, make_request_func, observer)
        self.backend.executor.submit_async_task(next_child_task)
