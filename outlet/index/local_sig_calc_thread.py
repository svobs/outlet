import logging
import threading
import time
from collections import deque
from typing import Deque, Optional

from pydispatch import dispatcher
from pydispatch.errors import DispatcherKeyError

import local.content_hasher
from constants import TREE_TYPE_LOCAL_DISK
from model.node.display_node import DisplayNode
from model.node.local_disk_node import LocalFileNode
from ui import actions

logger = logging.getLogger(__name__)


# CLASS SignatureCalcThread
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
class SignatureCalcThread(threading.Thread):
    """Hasher thread which churns through signature queue and sends updates to cacheman"""
    def __init__(self, app, initial_sleep_sec: float):
        super().__init__(target=self._run_content_scanner_thread, name='SignatureCalcThread', daemon=True)
        self.app = app
        self._initial_sleep_sec = initial_sleep_sec
        self._shutdown: bool = False
        self._node_queue: Deque[LocalFileNode] = deque()
        self._cv_can_get = threading.Condition()
        self._struct_lock = threading.Lock()

    def enqueue(self, node: LocalFileNode):
        logger.debug(f'[{self.name}] Enqueuing node: {node.node_identifier}')
        assert not node.md5 and not node.sha256
        with self._struct_lock:
            self._node_queue.append(node)

        with self._cv_can_get:
            self._cv_can_get.notifyAll()

    def request_shutdown(self):
        logger.debug(f'Requesting shutdown of thread {self.name}')
        self._shutdown = True

        try:
            dispatcher.disconnect(signal=actions.NODE_UPSERTED, receiver=self._on_node_upserted_in_cache)
        except DispatcherKeyError:
            pass

        with self._cv_can_get:
            # unblock thread:
            self._cv_can_get.notifyAll()

    def _process_single_node(self, node: LocalFileNode):
        md5, sha256 = local.content_hasher.calculate_signatures(node.full_path)
        node.md5 = md5
        node.sha256 = sha256

        logger.debug(f'[{self.name}] Node {node.uid} has MD5: {node.md5}')

        # TODO: consider batching writes
        # Send back to ourselves to be re-stored in memory & disk caches:
        self.app.cacheman.upsert_single_node(node)

    def _on_node_upserted_in_cache(self, sender: str, node: DisplayNode):
        if node.get_tree_type() == TREE_TYPE_LOCAL_DISK and node.is_file() and not node.md5 and not node.sha256:
            assert isinstance(node, LocalFileNode)
            self.enqueue(node)

    def _run_content_scanner_thread(self):
        logger.info(f'Starting {self.name}...')

        dispatcher.connect(signal=actions.NODE_UPSERTED, receiver=self._on_node_upserted_in_cache)

        # Wait for CacheMan to finish starting up so as not to deprive it of resources:
        self.app.cacheman.wait_for_startup_done()

        logger.debug(f'[{self.name}] Doing inital sleep {self._initial_sleep_sec} sec to let things settle...')
        time.sleep(self._initial_sleep_sec)  # in seconds

        while not self._shutdown:
            node: Optional[LocalFileNode] = None

            with self._struct_lock:
                if len(self._node_queue) > 0:
                    node = self._node_queue.popleft()

            if node:
                try:
                    if node.md5 or node.sha256:
                        # Other threads, e.g., CommandExecutor, can also fill this in asynchronously
                        logger.debug(f'Node already has signature; skipping; {node}')
                        continue

                    logger.debug(f'[{self.name}] Calculating signature for node: {node.node_identifier}')
                    self._process_single_node(node)
                except Exception:
                    logger.exception(f'Unexpected error while processing node: {node}')
                continue
            else:
                logger.debug(f'[{self.name}] No pending ops; sleeping {self._initial_sleep_sec} sec then waiting till notified...')
                time.sleep(self._initial_sleep_sec)  # in seconds

            with self._cv_can_get:
                self._cv_can_get.wait()
