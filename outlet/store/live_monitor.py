import logging
import os
import threading
import time
from typing import Dict, Optional, Set

from pydispatch import dispatcher
from watchdog.observers import Observer
from watchdog.observers.api import ObservedWatch

from constants import TREE_TYPE_GDRIVE, TREE_TYPE_LOCAL_DISK
from model.node.local_disk_node import LocalNode
from model.node_identifier import NodeIdentifier
from store.local.event_handler import LocalChangeEventHandler
from ui import actions
from util.ensure import ensure_bool, ensure_int
from util.has_lifecycle import HasLifecycle

logger = logging.getLogger(__name__)


# CLASS GDrivePollingThread
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼

class GDrivePollingThread(HasLifecycle, threading.Thread):
    def __init__(self, app, thread_num):
        super().__init__(target=self._run_gdrive_polling_thread, name=f'GDrivePollingThread-{thread_num}', daemon=True)
        self._shutdown: bool = False
        self.app = app
        self.gdrive_thread_polling_interval_sec: int = ensure_int(self.app.config.get('cache.gdrive_thread_polling_interval_sec'))

    def start(self):
        HasLifecycle.start(self)
        threading.Thread.start(self)

    def shutdown(self):
        logger.debug(f'Shutting down {self.name}')
        HasLifecycle.shutdown(self)
        self._shutdown = True

    def _run_gdrive_polling_thread(self):
        logger.info(f'Starting {self.name}...')

        while not self._shutdown:
            # FIXME: prevent possible buildup of requests if a sync runs longer than the polling interval
            dispatcher.send(signal=actions.SYNC_GDRIVE_CHANGES, sender=actions.ID_GDRIVE_POLLING_THREAD)

            logger.debug(f'{self.name}: sleeping for {self.gdrive_thread_polling_interval_sec} sec')
            time.sleep(self.gdrive_thread_polling_interval_sec)


# CLASS LocalFileChangeBatchingThread
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼

class LocalFileChangeBatchingThread(HasLifecycle, threading.Thread):
    def __init__(self, app):
        HasLifecycle.__init__(self)
        threading.Thread.__init__(self, target=self._run, name=f'LocalFileChangeBatchingThread', daemon=True)
        self._shutdown: bool = False
        self.app = app
        self.local_change_batch_interval_ms: int = ensure_int(self.app.config.get('cache.local_change_batch_interval_ms'))
        self.change_set: Set = set()

    def enqueue(self, file_path: str):
        self.change_set.add(file_path)

    def start(self):
        HasLifecycle.start(self)
        threading.Thread.start(self)

    def shutdown(self):
        logger.debug(f'Shutting down {self.name}')
        HasLifecycle.shutdown(self)
        self._shutdown = True

    def _run(self):
        logger.info(f'Starting {self.name}...')

        while not self._shutdown:
            # hot swap change set:
            change_set = self.change_set
            self.change_set = set()

            count = len(change_set)
            if count > 0:
                logger.debug(f'{self.name}: applying {count} local file updates...')
                for change_path in change_set:
                    try:
                        logger.debug(f'Applying CH file: {change_path}')
                        node: LocalNode = self.app.cacheman.build_local_file_node(change_path)
                        self.app.cacheman.upsert_single_node(node)
                    except FileNotFoundError as err:
                        logger.debug(f'Cannot process external CH event: file not found: "{err.filename}"')

                # TODO: maybe combine with condition variable so we aren't excessively waking
                logger.debug(f'{self.name}: sleeping for {self.local_change_batch_interval_ms} ms')
            time.sleep(self.local_change_batch_interval_ms / 1000.0)


# CLASS LiveMonitor
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼

class LiveMonitor(HasLifecycle):
    def __init__(self, app):
        HasLifecycle.__init__(self)
        self.app = app

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

        self.enable_gdrive_polling_thread: bool = ensure_bool(self.app.config.get('cache.enable_gdrive_polling_thread'))
        self._gdrive_polling_thread: Optional[GDrivePollingThread] = None
        self._count_gdrive_threads: int = 0
        self._local_change_batching_thread: Optional[LocalFileChangeBatchingThread] = None

        self._watchdog_observer = Observer()

    def start(self):
        HasLifecycle.start(self)
        self._watchdog_observer.start()

    def shutdown(self):
        HasLifecycle.shutdown(self)
        self._watchdog_observer.stop()
        self._stop_gdrive_capture()

        if self._local_change_batching_thread:
            self._local_change_batching_thread.shutdown()
            self._local_change_batching_thread = None

    def _start_local_disk_capture(self, full_path: str, tree_id: str):
        logger.debug(f'[{tree_id}] Starting disk capture for path="{full_path}"')

        if not self._local_change_batching_thread or not self._local_change_batching_thread.is_alive():
            self._local_change_batching_thread = LocalFileChangeBatchingThread(self.app)
            self._local_change_batching_thread.start()

        event_handler = LocalChangeEventHandler(self.app, self._local_change_batching_thread)
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
        if not tree_id_set:
            logger.debug(f'Ignoring request to stop local capture; tree_id_set is empty for path "{full_path}"')
            return

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

        self._count_gdrive_threads += 1
        self._gdrive_polling_thread = GDrivePollingThread(self.app, self._count_gdrive_threads)
        self._gdrive_polling_thread.start()

    def _stop_gdrive_capture(self):
        if self._gdrive_polling_thread:
            self._gdrive_polling_thread.shutdown()
            self._gdrive_polling_thread = None

    def start_or_update_capture(self, node_identifier: NodeIdentifier, tree_id: str):
        """Also updates existing capture for the given tree_id"""
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
                    self._stop_local_disk_capture(prev_identifier.get_single_path(), tree_id)

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

                if os.path.exists(node_identifier.get_single_path()):
                    self._start_local_disk_capture(node_identifier.get_single_path(), tree_id)
                else:
                    logger.debug(f'[{tree_id}]] Ignoring request to start local disk capture: path does not exist: '
                                 f'{node_identifier.get_single_path()}')

    def stop_capture(self, tree_id: str):
        """If capture doesn't exist, does nothing"""
        with self._struct_lock:
            prev_identifier: NodeIdentifier = self._active_tree_dict.pop(tree_id, None)
            if prev_identifier:
                logger.debug(f'[{tree_id}] Removing capture tree (was: ({prev_identifier})')

                if prev_identifier.tree_type == TREE_TYPE_GDRIVE:
                    # GDrive
                    assert tree_id in self._active_gdrive_tree_set, f'Expected to find "{tree_id}" in active GDrive tree dict!'
                    self._active_gdrive_tree_set.discard(tree_id)
                    needs_gdrive_stop = len(self._active_gdrive_tree_set) == 0
                    if needs_gdrive_stop:
                        self._stop_gdrive_capture()
                else:
                    # Local
                    assert prev_identifier.tree_type == TREE_TYPE_LOCAL_DISK, f'Expected tree type LOCAL_DISK but is: {prev_identifier}'
                    assert self._local_tree_watcher_dict.get(prev_identifier.get_single_path(), None), \
                        f'Expected to find "{tree_id}" in active local tree dict!'
                    self._stop_local_disk_capture(prev_identifier.get_single_path(), tree_id)

            else:
                logger.debug(f'[{tree_id}] Trying to remove capture which was not found')
                assert tree_id not in self._active_gdrive_tree_set, \
                    f'Expected not to find "{self._active_gdrive_tree_set}" in active GDrive tree set!'

