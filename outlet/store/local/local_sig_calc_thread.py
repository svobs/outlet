import copy
import logging
import threading
import time
import store.local.content_hasher
from collections import deque
from typing import Deque, Optional

from constants import TREE_TYPE_LOCAL_DISK
from model.node.node import Node
from model.node.local_disk_node import LocalFileNode
from ui import actions
from util.has_lifecycle import HasLifecycle

logger = logging.getLogger(__name__)


# CLASS SignatureCalcThread
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
class SignatureCalcThread(HasLifecycle, threading.Thread):
    """Hasher thread which churns through signature queue and sends updates to cacheman"""
    def __init__(self, app, initial_sleep_sec: float):
        HasLifecycle.__init__(self)
        threading.Thread.__init__(self, target=self._run_content_scanner_thread, name='SignatureCalcThread', daemon=True)
        self.app = app
        self._initial_sleep_sec = initial_sleep_sec
        self._shutdown: bool = False
        self._node_queue: Deque[LocalFileNode] = deque()
        self._cv_can_get = threading.Condition()
        self._struct_lock = threading.Lock()

    def start(self):
        HasLifecycle.start(self)
        self.connect_dispatch_listener(signal=actions.NODE_UPSERTED, receiver=self._on_node_upserted_in_cache)

        threading.Thread.start(self)

    def shutdown(self):
        HasLifecycle.shutdown(self)

        if self._shutdown:
            return

        logger.debug(f'Shutting down {self.name}')
        self._shutdown = True

        with self._cv_can_get:
            # unblock thread:
            self._cv_can_get.notifyAll()

    def enqueue(self, node: LocalFileNode):
        logger.debug(f'[{self.name}] Enqueuing node: {node.node_identifier}')
        assert not node.md5 and not node.sha256
        with self._struct_lock:
            self._node_queue.append(node)

        with self._cv_can_get:
            self._cv_can_get.notifyAll()

    def _process_single_node(self, node: LocalFileNode):
        md5, sha256 = store.local.content_hasher.calculate_signatures(full_path=node.get_single_path())
        if not md5 and not sha256:
            logger.debug(f'[{self.name}] Failed to calculate signature for node {node.uid}: assuming it was deleted')
            return

        # Do not modify the original node, or cacheman will not detect that it has changed. Edit and submit a copy instead
        node_with_signature = copy.deepcopy(node)
        node_with_signature.md5 = md5
        node_with_signature.sha256 = sha256

        logger.debug(f'[{self.name}] Node {node_with_signature.uid} has MD5: {node_with_signature.md5}')

        # TODO: consider batching writes
        # Send back to ourselves to be re-stored in memory & disk caches:
        self.app.cacheman.update_single_node(node_with_signature)

    def _on_node_upserted_in_cache(self, sender: str, node: Node):
        if node.get_tree_type() == TREE_TYPE_LOCAL_DISK and node.is_file() and not node.md5 and not node.sha256:
            assert isinstance(node, LocalFileNode)
            self.enqueue(node)

    def _run_content_scanner_thread(self):
        logger.info(f'Starting {self.name}...')

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
