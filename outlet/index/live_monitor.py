import threading
import time
from typing import Dict, Optional, Set

from watchdog.observers import Observer
from watchdog.observers.api import ObservedWatch

from constants import TREE_TYPE_GDRIVE, TREE_TYPE_LOCAL_DISK
from gdrive.gdrive_tree_loader import GDriveTreeLoader
from local.event_handler import LocalChangeEventHandler
from model.node_identifier import ensure_bool, ensure_int, NodeIdentifier
import logging

from model.node_identifier_factory import NodeIdentifierFactory
from ui import actions

logger = logging.getLogger(__name__)


# CLASS GDrivePollingThread
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼

class GDrivePollingThread(threading.Thread):
    def __init__(self, parent, thread_num):
        super().__init__(target=self._run_gdrive_polling_thread, name=f'GDrivePollingThread-{thread_num}', daemon=True)
        self._shutdown: bool = False
        self.app = parent.app
        self.gdrive_thread_polling_interval_sec: int = parent.gdrive_thread_polling_interval_sec

    def request_shutdown(self):
        logger.debug(f'Requesting shutdown of thread {self.name}')
        self._shutdown = True

    def _run_gdrive_polling_thread(self):
        logger.info('Starting GDrivePollingThread...')

        my_gdrive_root = NodeIdentifierFactory.get_gdrive_root_constant_identifier()
        cache_info = self.app.cache_manager.get_or_create_cache_info_entry(my_gdrive_root)
        gdrive_tree_loader = GDriveTreeLoader(application=self.app, cache_path=cache_info.cache_location, tree_id=actions.ID_GLOBAL_CACHE)

        while not self._shutdown:
            gdrive_tree_loader.sync_latest_changes()

            logger.debug(f'GDrivePollingThread: sleeping for {self.gdrive_thread_polling_interval_sec} sec')
            time.sleep(self.gdrive_thread_polling_interval_sec)


# CLASS LiveMonitor
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼

class LiveMonitor:
    def __init__(self, application):
        self.app = application

        self.enable_gdrive_polling_thread: bool = ensure_bool(self.app.config.get('cache.enable_gdrive_polling_thread'))
        self.gdrive_thread_polling_interval_sec: int = ensure_int(self.app.config.get('cache.gdrive_thread_polling_interval_sec'))

        self._struct_lock = threading.Lock()
        """Locks the following data structures"""

        self._active_tree_dict: Dict[str, NodeIdentifier] = {}
        """Dict for [tree_id -> NodeIdentifier]. Keeps track of what each tree is looking at"""

        self._active_gdrive_tree_set: Set[str] = set()
        """Keep track of how many displayed trees are currently using GDrive. It is up to the last GDrive user to shut off the lights"""

        self._local_tree_watcher_dict: Dict[str, ObservedWatch] = {}
        """The key to this dict is a local path"""

        self._active_local_tree_dict: Dict[str, Set[str]] = {}
        """A dict of [local_path -> set of tree_ids]"""

        self._gdrive_polling_thread: Optional[GDrivePollingThread] = None
        self._count_threads: int = 0

        self._watchdog_observer = Observer()

    def __del__(self):
        self.shutdown()

    def start(self):
        self._watchdog_observer.start()

    def shutdown(self):
        self._watchdog_observer.stop()
        self._stop_gdrive_capture()

    def _start_local_disk_capture(self, full_path: str, tree_id: str):
        logger.debug(f'[{tree_id}] Starting disk capture for path="{full_path}"')
        event_handler = LocalChangeEventHandler(self.app)
        watch: ObservedWatch = self._watchdog_observer.schedule(event_handler, full_path, recursive=True)
        self._local_tree_watcher_dict[full_path] = watch

        tree_id_set: Set[str] = self._active_local_tree_dict.get(full_path, None)
        if not tree_id_set:
            tree_id_set = {tree_id}
            self._active_local_tree_dict[full_path] = tree_id_set
        else:
            tree_id_set.add(tree_id)

    def _stop_local_disk_capture(self, full_path: str, tree_id: str):
        tree_id_set: Set[str] = self._active_local_tree_dict.get(full_path, None)
        assert tree_id_set, f'Expected non-empty tree_id_set for: {full_path}'
        tree_id_set.discard(tree_id)
        if not tree_id_set:
            # removed the last tree relying on this path: remove watcher
            self._active_local_tree_dict.pop(full_path)
            watch = self._local_tree_watcher_dict.pop(full_path, None)
            assert watch, f'Expected a watch for: {full_path}'
            self._watchdog_observer.unschedule(watch)

    def _start_gdrive_capture(self, tree_id: str):
        if self._gdrive_polling_thread and self._gdrive_polling_thread.is_alive():
            logger.warning('GDriveCaptureThread is already running!')
            return

        if not self.enable_gdrive_polling_thread:
            logger.debug(f'Not starting GDrivePollingThread: cache.enable_gdrive_polling_thread is disabled')
            return

        self._count_threads += 1
        self._gdrive_polling_thread = GDrivePollingThread(self, self._count_threads)
        self._gdrive_polling_thread.start()

    def _stop_gdrive_capture(self):
        if self._gdrive_polling_thread:
            self._gdrive_polling_thread.request_shutdown()
            self._gdrive_polling_thread = None

    def start_capture(self, node_identifier: NodeIdentifier, tree_id: str):
        """Also updates existing capture"""
        with self._struct_lock:
            prev_identifier: NodeIdentifier = self._active_tree_dict.get(tree_id, None)
            if prev_identifier:
                if prev_identifier == node_identifier:
                    logger.debug(f'[{tree_id}] Trying to replace tree capture with the same tree; ignoring: ({node_identifier})')
                    return

                logger.debug(f'[{tree_id}] Replacing prev capture tree ({node_identifier}) with new tree ({node_identifier})')

                if prev_identifier.tree_type == TREE_TYPE_GDRIVE:
                    self._active_gdrive_tree_set.discard(tree_id)

                    needs_gdrive_stop = len(self._active_gdrive_tree_set) == 0 and node_identifier.tree_type != TREE_TYPE_GDRIVE
                    if needs_gdrive_stop:
                        self._stop_gdrive_capture()
                else:
                    assert prev_identifier.tree_type == TREE_TYPE_LOCAL_DISK, f'Expected tree type LOCAL_DISK but is: {prev_identifier}'
                    self._stop_local_disk_capture(prev_identifier.full_path, tree_id)

            else:
                logger.debug(f'[{tree_id}] Starting capture of new tree ({node_identifier})')

            self._active_tree_dict[tree_id] = node_identifier

            if node_identifier.tree_type == TREE_TYPE_GDRIVE:
                # GDrive
                needs_gdrive_start = len(self._active_gdrive_tree_set) == 0
                self._active_gdrive_tree_set.add(tree_id)

                if not needs_gdrive_start:
                    logger.warning('Already capturing GDrive!')
                    return

                self._start_gdrive_capture(tree_id)
            else:
                # Local
                assert node_identifier.tree_type == TREE_TYPE_LOCAL_DISK, f'Expected tree type LOCAL_DISK but is: {node_identifier}'
                self._start_local_disk_capture(node_identifier.full_path, tree_id)

    def stop_capture(self, tree_id: str):
        with self._struct_lock:
            prev_identifier: NodeIdentifier = self._active_tree_dict.get(tree_id, None)
            if prev_identifier:
                logger.debug(f'[{tree_id}] Removing capture tree (was: ({prev_identifier})')
                assert tree_id in self._active_gdrive_tree_set or self._local_tree_watcher_dict.get(prev_identifier.full_path, None),\
                    f'Expected to find "{tree_id}" in active GDrive or local tree dicts!'

                if prev_identifier.tree_type == TREE_TYPE_GDRIVE:
                    # GDrive
                    self._active_gdrive_tree_set.discard(tree_id)
                    needs_gdrive_stop = len(self._active_gdrive_tree_set) == 0
                    if needs_gdrive_stop:
                        self._stop_gdrive_capture()
                else:
                    # Local
                    assert prev_identifier.tree_type == TREE_TYPE_LOCAL_DISK, f'Expected tree type LOCAL_DISK but is: {prev_identifier}'
                    self._stop_local_disk_capture(prev_identifier.full_path, tree_id)

            else:
                logger.error(f'[{tree_id}] Trying to remove capture which was not found!')
                assert tree_id not in self._active_gdrive_tree_set, \
                    f'Expected not to find "{self._active_gdrive_tree_set}" in active GDrive tree set!'

