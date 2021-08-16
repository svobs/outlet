import logging
import os
from collections import deque
from functools import partial
from typing import Callable, List, Optional, Tuple

from pydispatch import dispatcher

from backend.tree_store.gdrive.query_observer import FileMetaPersister, FolderMetaPersister
from constants import GDRIVE_DOWNLOAD_STATE_COMPLETE, GDRIVE_DOWNLOAD_STATE_GETTING_DIRS, GDRIVE_DOWNLOAD_STATE_GETTING_NON_DIRS, \
    GDRIVE_DOWNLOAD_STATE_NOT_STARTED, \
    GDRIVE_DOWNLOAD_STATE_READY_TO_COMPILE, GDRIVE_DOWNLOAD_TYPE_CHANGES, GDRIVE_DOWNLOAD_TYPE_INITIAL_LOAD, GDRIVE_ROOT_UID, \
    ROOT_PATH, SUPER_DEBUG_ENABLED, \
    TRACE_ENABLED, TreeID
from backend.tree_store.gdrive.gdrive_whole_tree import GDriveWholeTree
from model.node.gdrive_node import GDriveFolder, GDriveNode
from model.node_identifier_factory import NodeIdentifierFactory
from model.uid import UID
from backend.tree_store.gdrive.gdrive_client import GDriveClient
from backend.tree_store.gdrive.master_gdrive_disk import GDriveDiskStore
from backend.tree_store.gdrive.master_gdrive_op_load import GDriveLoadAllMetaOp
from backend.sqlite.gdrive_db import CurrentDownload
from signal_constants import Signal
from util import time_util
from util.stopwatch_sec import Stopwatch
from util.task_runner import Task

logger = logging.getLogger(__name__)


class GDriveTreeLoader:
    """
    ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
    CLASS GDriveTreeLoader
    ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼
    """
    def __init__(self, backend, diskstore: GDriveDiskStore, gdrive_client: GDriveClient, device_uid: UID, tree_id: TreeID = None):
        self.backend = backend
        self._diskstore: GDriveDiskStore = diskstore
        self.gdrive_client: GDriveClient = gdrive_client
        self.device_uid: UID = device_uid
        self.tree_id: TreeID = tree_id

    def load_all(self, this_task: Optional[Task], invalidate_cache: bool, after_tree_loaded: Callable[[GDriveWholeTree], None]):
        logger.debug(f'GDriveTreeLoader.load_all() called with invalidate_cache={invalidate_cache}')

        try:
            # scroll down ⯆⯆⯆
            self._load_all(this_task, invalidate_cache, after_tree_loaded)
        finally:
            if self.tree_id:
                logger.debug(f'Sending STOP_PROGRESS for tree_id: {self.tree_id}')
                dispatcher.send(Signal.STOP_PROGRESS, sender=self.tree_id)

    def _load_all(self, this_task: Optional[Task], invalidate_cache: bool, after_tree_loaded: Callable[[GDriveWholeTree], None]):
        if self.tree_id:
            logger.debug(f'Sending START_PROGRESS_INDETERMINATE for tree_id: {self.tree_id}')
            dispatcher.send(Signal.START_PROGRESS_INDETERMINATE, sender=self.tree_id)

        sync_ts: int = time_util.now_sec()

        # Retrieve this here, so we can fail fast if not found, even though we won't do anything with this until the end
        master_tree_root = NodeIdentifierFactory.get_root_constant_gdrive_spid(self.device_uid)
        cache_info = self.backend.cacheman.get_cache_info_for_subtree(master_tree_root, create_if_not_found=False)

        changes_download: CurrentDownload = self._diskstore.get_current_download(GDRIVE_DOWNLOAD_TYPE_CHANGES)
        if not changes_download or invalidate_cache:
            logger.debug(f'Getting a new start token for changes (invalidate_cache={invalidate_cache})')
            token: str = self.gdrive_client.get_changes_start_token()
            changes_download = CurrentDownload(GDRIVE_DOWNLOAD_TYPE_CHANGES, GDRIVE_DOWNLOAD_STATE_NOT_STARTED, token, sync_ts)
            self._diskstore.create_or_update_download(changes_download)

        initial_download: CurrentDownload = self._diskstore.get_current_download(GDRIVE_DOWNLOAD_TYPE_INITIAL_LOAD)
        if not initial_download or invalidate_cache:
            logger.debug(f'Starting a fresh download for entire Google Drive tree meta (invalidate_cache={invalidate_cache})')
            initial_download = CurrentDownload(GDRIVE_DOWNLOAD_TYPE_INITIAL_LOAD, GDRIVE_DOWNLOAD_STATE_NOT_STARTED, None, sync_ts)
            self._diskstore.create_or_update_download(initial_download)

            # Notify UI trees that their old roots are invalid:
            dispatcher.send(signal=Signal.GDRIVE_RELOADED, sender=self.tree_id, device_uid=self.device_uid)

        if initial_download.current_state == GDRIVE_DOWNLOAD_STATE_NOT_STARTED:
            # completely fresh tree
            tree = GDriveWholeTree(self.backend, self.device_uid)
        else:
            # Start/resume: read cache
            msg = 'Reading disk cache...'
            logger.info(msg)

            if self.tree_id:
                dispatcher.send(Signal.SET_PROGRESS_TEXT, sender=self.tree_id, msg=msg)

            # Load all users and MIME types
            self.backend.cacheman.execute_gdrive_load_op(self.device_uid, GDriveLoadAllMetaOp())

            tree: GDriveWholeTree = self._diskstore.load_tree_from_cache(initial_download.is_complete(), self.tree_id)

        # TODO: do something with this data
        # tree.me = self.gdrive_client.get_about()

        # TODO: there must be a better way to do this...
        is_synchronous = this_task is None

        def launch_step_5(this_task_4):
            next_subtask = Task(this_task.priority, self._do_post_load_processing, tree, cache_info)
            self.backend.executor.submit_async_task(next_subtask, parent_task=this_task)

            def _after_tree_loaded(this_task_5):
                if SUPER_DEBUG_ENABLED:
                    logger.debug(f'Calling after_tree_loaded func ({after_tree_loaded}) async')
                after_tree_loaded(tree)
                if SUPER_DEBUG_ENABLED:
                    logger.debug(f'The after_tree_loaded func returned')
            next_subtask.add_next_task(_after_tree_loaded)

        if not initial_download.is_complete():
            state = 'Starting' if initial_download.current_state == GDRIVE_DOWNLOAD_STATE_NOT_STARTED else 'Resuming'
            logger.info(f'{state} download of all Google Drive tree (state={initial_download.current_state}, is_synchronous={is_synchronous})')

            # BEGIN STATE MACHINE:

            if is_synchronous:
                # Not inside an async task: just execute procedurally
                # It's a bit of a hack, using None like this...
                self._download_gdrive_root_meta(None, tree, initial_download, sync_ts)
                self._download_all_gdrive_dir_meta(None, tree, initial_download)
                self._download_all_gdrive_non_dir_meta(None, tree, initial_download)
                self._compile_downloaded_meta(None, tree, initial_download)
            else:
                # Create child task for each phase.
                # Each child's completion handler will call the next task in a chain.

                def launch_step_4(this_task_3):
                    next_subtask = Task(this_task.priority, self._compile_downloaded_meta, tree, initial_download)
                    self.backend.executor.submit_async_task(next_subtask, parent_task=this_task)
                    next_subtask.add_next_task(launch_step_5)

                def launch_step_3(this_task_2):
                    next_subtask = Task(this_task.priority, self._download_all_gdrive_non_dir_meta, tree, initial_download)
                    self.backend.executor.submit_async_task(next_subtask, parent_task=this_task)
                    next_subtask.add_next_task(launch_step_4)

                def launch_step_2(this_task_1):
                    next_subtask = Task(this_task.priority, self._download_all_gdrive_dir_meta, tree, initial_download)
                    self.backend.executor.submit_async_task(next_subtask, parent_task=this_task)
                    next_subtask.add_next_task(launch_step_3)

                first_subtask = Task(this_task.priority, self._download_gdrive_root_meta, tree, initial_download, sync_ts)
                first_subtask.add_next_task(launch_step_2)

                self.backend.executor.submit_async_task(first_subtask, parent_task=this_task)
        else:
            if not is_synchronous:
                launch_step_5(None)

        if is_synchronous:
            # for async case, this is done in launch_step_5()
            self._do_post_load_processing(None, tree, cache_info)
            logger.debug(f'Calling after_tree_loaded func ({after_tree_loaded})')
            after_tree_loaded(tree)

    def _download_gdrive_root_meta(self, this_task: Optional[Task], tree: GDriveWholeTree, initial_download: CurrentDownload, sync_ts: int):
        if initial_download.current_state == GDRIVE_DOWNLOAD_STATE_NOT_STARTED:
            self.backend.cacheman.delete_all_gdrive_data(self.device_uid)

            # this was already created with the tree: all its data is known
            gdrive_root_node = self.backend.cacheman.build_gdrive_root_node(self.device_uid, sync_ts=sync_ts)
            tree.uid_dict[GDRIVE_ROOT_UID] = gdrive_root_node
            tree.parent_child_dict[GDRIVE_ROOT_UID] = []

            # Need to make a special call to get the root node 'My Drive'. This node will not be included
            # in the "list files" call:
            initial_download.update_ts = sync_ts
            drive_root: GDriveFolder = self.gdrive_client.get_my_drive_root(initial_download.update_ts)
            tree.uid_dict[drive_root.uid] = drive_root

            initial_download.current_state = GDRIVE_DOWNLOAD_STATE_GETTING_DIRS
            initial_download.page_token = None
            self._diskstore.insert_gdrive_folder_list(folder_list=[gdrive_root_node, drive_root], commit=False)
            self._diskstore.create_or_update_download(download=initial_download)

    def _download_all_gdrive_dir_meta(self, this_task: Optional[Task], tree: GDriveWholeTree, initial_download: CurrentDownload):
        if initial_download.current_state == GDRIVE_DOWNLOAD_STATE_GETTING_DIRS:
            observer = FolderMetaPersister(tree, initial_download, self._diskstore, self.backend.cacheman)
            self.gdrive_client.get_all_folders(initial_download.page_token, initial_download.update_ts, observer)

    def _download_all_gdrive_non_dir_meta(self, this_task: Optional[Task], tree: GDriveWholeTree, initial_download: CurrentDownload):
        if initial_download.current_state == GDRIVE_DOWNLOAD_STATE_GETTING_NON_DIRS:
            observer = FileMetaPersister(tree, initial_download, self._diskstore, self.backend.cacheman)
            self.gdrive_client.get_all_non_folders(initial_download.page_token, initial_download.update_ts, observer)

    def _compile_downloaded_meta(self, this_task: Optional[Task], tree: GDriveWholeTree, initial_download: CurrentDownload):
        if initial_download.current_state == GDRIVE_DOWNLOAD_STATE_READY_TO_COMPILE:
            # Some post-processing needed...

            # (1) Update all_children_fetched state
            tuples: List[Tuple[int, bool]] = []
            for goog_node in tree.uid_dict.values():
                if goog_node.is_dir():
                    goog_node.all_children_fetched = True
                    tuples.append((goog_node.uid, True))

            # Updating the entire table is much faster than doing a 'where' clause:
            self._diskstore.update_folder_fetched_status(commit=False)

            # (2) Set UIDs for parents, and assemble parent dict:
            parent_mappings: List[Tuple] = self._diskstore.get_id_parent_mappings()
            parent_mappings = self._translate_parent_ids(tree, parent_mappings)
            self._diskstore.insert_id_parent_mappings(parent_mappings, overwrite=True, commit=False)
            logger.debug(f'Updated {len(parent_mappings)} id-parent mappings')

            # (3) mark download finished
            initial_download.current_state = GDRIVE_DOWNLOAD_STATE_COMPLETE
            self._diskstore.create_or_update_download(download=initial_download)
            logger.debug('GDrive data download complete.')

    def _do_post_load_processing(self, this_task: Optional[Task], tree: GDriveWholeTree, cache_info):
        # Still need to compute this in memory every time we load:
        self._fix_orphans(tree)
        self._compile_full_paths(tree)

        self._check_for_broken_nodes(tree)

        # set cache_info.is_loaded=True:
        cache_info.is_loaded = True

        logger.debug('GDrive: load_all() done')

    def _fix_orphans(self, tree: GDriveWholeTree):
        """Finds orphans (nodes with no parents) and sets them as children of root"""
        # TODO: can we roll this into the node loading?
        if SUPER_DEBUG_ENABLED:
            logger.debug(f'Determining topmost nodes for {len(tree.uid_dict)} GDrive nodes')

        count_found = 0

        max_uid = GDRIVE_ROOT_UID + 1
        for node in tree.uid_dict.values():
            if node.uid == GDRIVE_ROOT_UID:
                continue

            if not node.get_parent_uids():
                logger.info(f'Found GDrive orphan (attaching to root): {node.node_identifier}')
                node.add_parent(GDRIVE_ROOT_UID)
                tree.get_child_list_for_root().append(node)
                count_found += 1

            if node.uid >= max_uid:
                max_uid = node.uid

        logger.debug(f'Found {count_found} GDrive orphans')

        self.backend.uid_generator.ensure_next_uid_greater_than(max_uid + 1)

    def _translate_parent_ids(self, tree: GDriveWholeTree, id_parent_mappings: List[Tuple[UID, Optional[UID], str, int]]) -> List[Tuple]:
        """Fills in the parent_uid field ([1]) based on the parent's goog_id ([2]) for each downloaded mapping"""
        sw = Stopwatch()
        logger.debug(f'Translating parent IDs for {len(tree.uid_dict)} items...')

        new_mappings: List[Tuple] = []
        for mapping in id_parent_mappings:
            parent_uid = mapping[1]
            if not parent_uid:
                # [0]=item_uid, [1]=parent_uid, [2]=parent_goog_id, [3]=sync_ts
                parent_goog_id: str = mapping[2]

                # Add parent UID to tuple for later DB update:
                parent_uid = self.backend.cacheman.get_uid_for_goog_id(tree.device_uid, parent_goog_id)
                mapping = mapping[0], parent_uid, mapping[2], mapping[3]
            tree.add_parent_mapping(mapping[0], parent_uid)
            new_mappings.append(mapping)

        logger.debug(f'{sw} Filled in parent IDs')
        return new_mappings

    @staticmethod
    def _compile_full_paths(tree: GDriveWholeTree):
        if SUPER_DEBUG_ENABLED:
            logger.debug(f'Compiling paths for {len(tree.uid_dict)} GDrive nodes')

        full_path_stopwatch = Stopwatch()

        item_count: int = 0
        path_count: int = 0

        # set path list for root
        root_node = tree.get_root_node()
        root_node.node_identifier.set_path_list(ROOT_PATH)

        queue = deque()
        for root in tree.get_child_list_for_root():
            root.node_identifier.set_path_list(f'/{root.name}')
            queue.append(root)
            item_count += 1
            path_count += 1

        while len(queue) > 0:
            parent: GDriveNode = queue.popleft()
            children = tree.parent_child_dict.get(parent.uid, None)
            if children:
                for child in children:
                    child_path_list: List[str] = child.get_path_list()

                    # add paths for this parent:
                    parent_path_list = parent.get_path_list()
                    # if len(parent_path_list) > 1:
                    #     logger.info(f'MULTIPLE PARENT PATHS ({len(parent_path_list)}) for node {parent.uid} ("{parent.name}")')
                    # if len(parent_path_list) > 10:
                    #     logger.warning(f'Large number of parents for node {child.uid} (possible cycle?): {len(parent_path_list)}')
                    for parent_path in parent_path_list:
                        new_child_path = os.path.join(parent_path, child.name)
                        # if len(new_child_path) > 1000:
                        #     logger.warning(f'Very long path found (possible cycle?): {new_child_path}')
                        if TRACE_ENABLED:
                            logger.debug(f'[{path_count}] ({child.uid}) Adding path "{new_child_path}" to  paths ({child_path_list})')
                        if new_child_path not in child_path_list:
                            child_path_list.append(new_child_path)
                            path_count += 1

                    child.node_identifier.set_path_list(child_path_list)

                    item_count += 1

                    if child.is_dir():
                        queue.append(child)

        logger.debug(f'{full_path_stopwatch} Constructed {path_count:n} full paths for {item_count:n} items')

    @staticmethod
    def _check_for_broken_nodes(tree: GDriveWholeTree):
        error_count = 0
        broken_file_uid_list = []
        broken_folder_uid_list = []
        for node in tree.uid_dict.values():
            if not node.get_path_list():
                logger.error(f'Node has no paths: {node}')
                if node.is_dir():
                    broken_folder_uid_list.append(node.uid)
                else:
                    broken_file_uid_list.append(node.uid)
                error_count += 1

        if error_count:
            # This indicates a discontinuity in the node's ancestor graph
            # TODO: submit to adjudicator to fix
            logger.error(f'Found {error_count} broken nodes in tree!')
        else:
            logger.debug('No broken nodes detected in tree')
