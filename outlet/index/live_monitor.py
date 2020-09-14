import threading
from typing import Dict, Set

from watchdog.observers import Observer
from watchdog.events import LoggingEventHandler

from constants import TREE_TYPE_GDRIVE, TREE_TYPE_LOCAL_DISK
from local.event_handler import LocalChangeEventHandler
from model.node_identifier import NodeIdentifier
import logging

logger = logging.getLogger(__name__)

# CLASS LiveMonitor
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼

class LiveMonitor:
    def __init__(self, application):
        self.app = application

        self._struct_lock = threading.Lock()
        """Locks the following data structures"""

        self._active_tree_dict: Dict[str, NodeIdentifier] = {}
        """Dict for [tree_id -> NodeIdentifier]. Keeps track of what each tree is looking at"""

        self._active_gdrive_tree_set: Set[str] = set()
        """Keep track of how many displayed trees are currently using GDrive. It is up to the last GDrive user to shut off the lights"""

        self._local_tree_observer_dict: Dict[str, LocalChangeEventHandler] = {}

    def _start_local_disk_capture(self, path: str, tree_id: str) -> LocalChangeEventHandler:
        # TODO!
        event_handler = LocalChangeEventHandler()
        observer = Observer()
        # observer.schedule(LocalChangeEventHandler, path, recursive=True)
        # observer.start()

        # try:
        #     while observer.isAlive():
        #         observer.join(1)
        # except KeyboardInterrupt:
        #     observer.stop()
        # observer.join()

        return event_handler

    def __del__(self):
        self.shutdown()

    def _start_gdrive_capture(self, tree_id: str):
        # TODO
        pass

    def _stop_gdrive_capture(self):
        # TODO
        pass

    def shutdown(self):
        # TODO
        pass

    def start_capture(self, node_identifier: NodeIdentifier, tree_id: str):
        """Also updates existing capture"""
        with self._struct_lock:
            has_prev: bool = False
            prev_identifier: NodeIdentifier = self._active_tree_dict.get(tree_id, None)
            if prev_identifier:
                logger.debug(f'[{tree_id}] Replacing prev capture tree ({node_identifier}) with new tree ({node_identifier})')
                has_prev = True
            else:
                logger.debug(f'[{tree_id}] Starting capture of new tree ({node_identifier})')

            self._active_tree_dict[tree_id] = node_identifier

            if node_identifier.tree_type == TREE_TYPE_GDRIVE:
                needs_gdrive_start = len(self._active_gdrive_tree_set) == 0
                self._active_gdrive_tree_set.add(tree_id)

                if needs_gdrive_start:
                    self._start_gdrive_capture(tree_id)
            else:
                assert node_identifier.tree_type == TREE_TYPE_LOCAL_DISK, f'Expected tree type LOCAL_DISK but is: {node_identifier}'
                if has_prev and not self._local_tree_observer_dict.get(tree_id, None):
                    # internal error
                    raise RuntimeError(f'Expected a value in local tree observer dict but found nothing (tree = {tree_id})!')
                self._local_tree_observer_dict[tree_id] = self._start_local_disk_capture(node_identifier.full_path, tree_id)

    def stop_capture(self, tree_id: str):
        with self._struct_lock:
            prev_identifier: NodeIdentifier = self._active_tree_dict.get(tree_id, None)
            if prev_identifier:
                logger.debug(f'[{tree_id}] Removing capture tree (was: ({prev_identifier})')
                assert tree_id in self._active_gdrive_tree_set or self._local_tree_observer_dict.get(tree_id, None),\
                    f'Expected to find "{tree_id}" in active GDrive or local tree dicts!'
                self._active_gdrive_tree_set.discard(tree_id)
                self._local_tree_observer_dict.pop(tree_id, None)

                needs_gdrive_stop = len(self._active_gdrive_tree_set) == 0
                if needs_gdrive_stop:
                    self._stop_gdrive_capture()
            else:
                logger.error(f'[{tree_id}] Trying to remove capture which was not found!')
                assert tree_id not in self._active_gdrive_tree_set, \
                    f'Expected not to find "{self._active_gdrive_tree_set}" in active GDrive tree set!'

