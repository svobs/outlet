import errno
import logging
import os
from collections import deque
from typing import Callable, Deque, List, Optional, Tuple

from pydispatch import dispatcher

from constants import DISK_SCAN_MAX_ITEMS_PER_TASK
from logging_constants import SUPER_DEBUG_ENABLED, TRACE_ENABLED
from signal_constants import Signal
from model.node.local_disk_node import LocalDirNode, LocalNode
from backend.tree_store.local.locald_tree import LocalDiskTree
from model.node_identifier import LocalNodeIdentifier
from backend.tree_store.local.disk_tree_recurser import LocalTreeRecurser
from util.task_runner import Task

logger = logging.getLogger(__name__)


class FileCounter(LocalTreeRecurser):
    """
    ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
    CLASS LocalDiskScanner

    Does a quick walk of the filesystem and counts the files which are of interest
    ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼
    """

    def __init__(self, root_path):
        LocalTreeRecurser.__init__(self, root_path, valid_suffixes=None)
        self.files_to_scan = 0
        self.dirs_to_scan = 0

    def handle_target_file_type(self, file_path):
        self.files_to_scan += 1

    def handle_non_target_file(self, file_path):
        self.files_to_scan += 1

    def handle_dir(self, dir_path: str):
        self.dirs_to_scan += 1


class LocalDiskScanner:
    """
    ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
    CLASS LocalDiskScanner

    Walks the filesystem for a subtree (DisplayTree), using a cache if configured,
    to generate an up-to-date list of FMetas.
    ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼
    """

    def __init__(self, backend, master_local, root_node_identifer: LocalNodeIdentifier, tree_id=None):
        assert root_node_identifer.is_spid(), f'type={type(root_node_identifer)}, for {root_node_identifer}'
        self.backend = backend
        self.master_local = master_local
        self.cacheman = backend.cacheman
        self.root_node_identifier: LocalNodeIdentifier = root_node_identifer
        self.tree_id = tree_id  # For sending progress updates
        self.progress = 0
        self.total = 0

        self._dir_queue: Deque[str] = deque()
        # Put first entry in queue. We will iterate top-down
        self._dir_queue.append(self.root_node_identifier.get_single_path())

        self._local_tree: Optional[LocalDiskTree] = None

    def _find_total_files_to_scan(self):
        # First survey our local files:
        root_path = self.root_node_identifier.get_single_path()
        # TODO: this can be long-running: convert this to tasks
        logger.info(f'[{self.tree_id}] Preparing to scan. Counting files in path: {root_path}')
        file_counter = FileCounter(root_path)
        file_counter.recurse_through_dir_tree()

        logger.debug(f'[{self.tree_id}] Found {file_counter.files_to_scan} files and {file_counter.dirs_to_scan} dirs to scan.')
        return file_counter.files_to_scan

    @staticmethod
    def _list_dir_entries(target_dir: str, onerror: Optional[Callable] = None) -> Tuple[List[str], List[str]]:
        """Yanked from os._walk() and simplified"""
        dirs = []
        nondirs = []

        # We may not have read permission for target_dir, in which case we can't
        # get a list of the files the directory contains.  os.walk
        # always suppressed the exception then, rather than blow up for a
        # minor reason when (say) a thousand readable directories are still
        # left to visit.  That logic is copied here.
        try:
            # Note that scandir is global in this module due
            # to earlier import-*.
            scandir_it = os.scandir(target_dir)
        except OSError as error:
            if onerror is not None:
                onerror(error)
            return [], []

        with scandir_it:
            while True:
                try:
                    try:
                        entry: os.DirEntry = next(scandir_it)
                    except StopIteration:
                        break
                except OSError as error:
                    if onerror is not None:
                        onerror(error)
                    return [], []

                try:
                    is_dir = entry.is_dir()
                except OSError:
                    # If is_dir() raises an OSError, consider that the entry is not
                    # a directory, same behaviour than os.path.isdir().
                    is_dir = False

                if is_dir:
                    dirs.append(os.path.join(target_dir, entry.name))
                else:
                    nondirs.append(os.path.join(target_dir, entry.name))

        if SUPER_DEBUG_ENABLED:
            logger.debug(f'_list_dir_entries(): returning {len(dirs)} dirs, {len(nondirs)} nondirs')
        return dirs, nondirs

    def start_recursive_scan(self, this_task: Task):
        if not os.path.exists(self.root_node_identifier.get_single_path()):
            raise FileNotFoundError(errno.ENOENT, os.strerror(errno.ENOENT), self.root_node_identifier.get_single_path())

        self._local_tree = LocalDiskTree(self.backend)
        if not os.path.isdir(self.root_node_identifier.get_single_path()):
            logger.debug(f'[{self.tree_id}] Root of scan is a file!')
            self.master_local.overwrite_dir_entries_list(parent_full_path=self.root_node_identifier.get_single_path(), child_list=[])
            return

        self.total = self._find_total_files_to_scan()
        if self.tree_id:
            logger.debug(f'[{self.tree_id}] Sending START_PROGRESS with total={self.total}')
            dispatcher.send(signal=Signal.START_PROGRESS, sender=self.tree_id, total=self.total)

        this_task.add_next_task(self.scan_next_batch_of_dirs)

    def scan_next_batch_of_dirs(self, this_task: Task):
        nodes_scanned_this_task = 0
        dirs_scanned_this_task = 0

        while True:
            if len(self._dir_queue) == 0:
                logger.debug(f'Dir scan complete')

                if self.tree_id:
                    logger.debug(f'Sending STOP_PROGRESS for tree_id: {self.tree_id}')
                    dispatcher.send(Signal.STOP_PROGRESS, sender=self.tree_id)
                return

            target_dir: str = self._dir_queue.popleft()

            dirs_scanned_this_task += 1
            items_scanned_in_dir = len(self.scan_single_dir(target_dir))
            if SUPER_DEBUG_ENABLED:
                logger.debug(f'Scanned {items_scanned_in_dir} items from dir "{target_dir}". DirQueueSize: {len(self._dir_queue)}')
            nodes_scanned_this_task += items_scanned_in_dir

            # small or empty dirs will cause excessive overhead, so try to optimize by reusing tasks for these:
            if nodes_scanned_this_task >= DISK_SCAN_MAX_ITEMS_PER_TASK:
                logger.debug(f'Scanned {nodes_scanned_this_task} nodes from {dirs_scanned_this_task} dirs. '
                             f'DirQueueSize: {len(self._dir_queue)} BatchSize: {DISK_SCAN_MAX_ITEMS_PER_TASK} '
                             f'Progress: {self.progress} of {self.total}')
                break

        # Run next iteration next:
        this_task.add_next_task(self.scan_next_batch_of_dirs)

    def scan_single_dir(self, target_dir: str) -> List[LocalNode]:
        if SUPER_DEBUG_ENABLED:
            logger.debug(f'Scanning & building nodes for dir: "{target_dir}"')

        def on_error(error):
            logger.error(f'An error occured listing dir entries: {error}')

        (dir_list, nondir_list) = LocalDiskScanner._list_dir_entries(target_dir, onerror=on_error)

        child_node_list = []

        # FIXME: decide how to handle symlinks

        # DIRS
        self._dir_queue += dir_list
        for child_dir_path in dir_list:
            if child_dir_path != target_dir:  # Do not include parent dir in child list
                if SUPER_DEBUG_ENABLED:
                    logger.debug(f'[{self.tree_id}] Adding scanned dir: {child_dir_path}')

                dir_node = self.cacheman.build_local_dir_node(child_dir_path, is_live=True, all_children_fetched=True)
                if dir_node:
                    child_node_list.append(dir_node)

        # FILES
        for child_file_path in nondir_list:
            if child_file_path != target_dir:   # Do not include parent dir in child list (if it was actually a file)
                if SUPER_DEBUG_ENABLED:
                    logger.debug(f'[{self.tree_id}] Adding scanned file: {child_file_path}')
                file_node = self.cacheman.build_local_file_node(full_path=child_file_path)
                if file_node:
                    child_node_list.append(file_node)

                if self.tree_id:
                    dispatcher.send(Signal.PROGRESS_MADE, sender=self.tree_id, progress=1)
                    self.progress += 1
                    msg = f'Scanning file {self.progress} of {self.total}'
                    dispatcher.send(Signal.SET_PROGRESS_TEXT, sender=self.tree_id, msg=msg)

        if SUPER_DEBUG_ENABLED:
            logger.debug(f'[{self.tree_id}] scan_single_dir(): calling overwrite_dir_entries_list() with {len(child_node_list)} children')
        self.master_local.overwrite_dir_entries_list(parent_full_path=target_dir, child_list=child_node_list)

        return child_node_list
