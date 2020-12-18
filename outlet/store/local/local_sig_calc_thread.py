import copy
import logging
import time

import store.local.content_hasher
from constants import TREE_TYPE_LOCAL_DISK
from model.node.local_disk_node import LocalFileNode
from model.node.node import Node
from signal import Signal
from util.qthread import QThread

logger = logging.getLogger(__name__)


class SignatureCalcThread(QThread):
    """
    ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
    CLASS SignatureCalcThread

    Hasher thread which churns through signature queue and sends updates to cacheman
    ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼
    """
    def __init__(self, backend, initial_sleep_sec: float):
        QThread.__init__(self, name='SignatureCalcThread', initial_sleep_sec=initial_sleep_sec)
        self.backend = backend

    def start(self):
        QThread.start(self)
        self.connect_dispatch_listener(signal=Signal.NODE_UPSERTED, receiver=self._on_node_upserted_in_cache)

    def enqueue(self, node: LocalFileNode):
        logger.debug(f'[{self.name}] Enqueuing node: {node.node_identifier}')
        assert not node.md5 and not node.sha256
        super().enqueue(node)

    def _on_node_upserted_in_cache(self, sender: str, node: Node):
        if node.get_tree_type() == TREE_TYPE_LOCAL_DISK and node.is_file() and not node.md5 and not node.sha256:
            assert isinstance(node, LocalFileNode)
            self.enqueue(node)

    def on_thread_start(self):
        # Wait for CacheMan to finish starting up so as not to deprive it of resources:
        self.backend.cacheman.wait_for_startup_done()

        logger.debug(f'[{self.name}] Doing inital sleep {self.initial_sleep_sec} sec to let things settle...')
        time.sleep(self.initial_sleep_sec)  # in seconds

    def process_single_item(self, node: LocalFileNode):
        if node.md5 or node.sha256:
            # Other threads, e.g., CommandExecutor, can also fill this in asynchronously
            logger.debug(f'Node already has signature; skipping; {node}')
            return

        logger.debug(f'[{self.name}] Calculating signature for node: {node.node_identifier}')
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
        self.backend.cacheman.update_single_node(node_with_signature)
