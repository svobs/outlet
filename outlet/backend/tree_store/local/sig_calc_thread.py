import logging
import threading
import time
from collections import deque
from typing import Deque, List, Set
from uuid import UUID

from backend.executor.central import ExecPriority
from backend.tree_store.local import content_hasher
from constants import LARGE_FILE_SIZE_THRESHOLD_BYTES, SUPER_DEBUG_ENABLED, TRACE_ENABLED
from model.node.local_disk_node import LocalFileNode, LocalNode
from model.node_identifier import NodeIdentifier
from model.uid import UID
from signal_constants import Signal
from util.ensure import ensure_int
from util.format import humanfriendlier_size
from util.has_lifecycle import HasLifecycle
from util.task_runner import Task

logger = logging.getLogger(__name__)


class SigCalcBatchingThread(HasLifecycle, threading.Thread):
    """
    ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
    CLASS SigCalcBatchingThread

    Listens for upserted local disk nodes, and enqueues them so that they can have their MD5/SHA256 signatures calculated in batches.
    ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼
    """
    def __init__(self, backend, device_uid: UID):
        HasLifecycle.__init__(self)
        threading.Thread.__init__(self, target=self._run, name=f'SigCalcBatchingThread', daemon=True)
        self.backend = backend
        self.device_uid = device_uid
        self._cv_can_get = threading.Condition()
        self.batch_interval_ms: int = ensure_int(self.backend.get_config('cache.local_disk.signatures.batch_interval_ms'))
        self.bytes_per_batch_high_watermark: int = ensure_int(self.backend.get_config('cache.local_disk.signatures.bytes_per_batch_high_watermark'))
        logger.debug(f'[{self.name}] Bytes per batch high watermark = {self.bytes_per_batch_high_watermark}')
        self._node_queue: Deque[LocalFileNode] = deque()
        self._running_task_set: Set[UUID] = set()

    def _enqueue_node(self, node: LocalFileNode):
        with self._cv_can_get:
            if TRACE_ENABLED:
                logger.debug(f'[{self.name}] Enqueuing node: "{node.node_identifier}"')

            self._node_queue.append(node)
            self._cv_can_get.notifyAll()

    def start(self):
        HasLifecycle.start(self)
        self.connect_dispatch_listener(signal=Signal.NODE_UPSERTED_IN_CACHE, receiver=self._on_node_upserted_in_cache)
        self.connect_dispatch_listener(signal=Signal.SUBTREE_NODES_CHANGED_IN_CACHE, receiver=self._on_subtree_nodes_changed_in_cache)
        threading.Thread.start(self)

    def shutdown(self):
        logger.debug(f'Shutting down {self.name}')
        HasLifecycle.shutdown(self)

        with self._cv_can_get:
            # unblock thread:
            self._cv_can_get.notifyAll()

    def _run(self):
        logger.info(f'Starting {self.name}...')

        while not self.was_shutdown:
            time.sleep(self.batch_interval_ms / 1000.0)  # allow some nodes to collect

            nodes_to_scan = []
            bytes_to_scan = 0

            with self._cv_can_get:
                if not self._node_queue:
                    logger.debug(f'[{self.name}] No nodes in queue. Will wait until notified')
                    self._cv_can_get.wait()
                    continue
                elif len(self._running_task_set) > 0:
                    logger.debug(f'[{self.name}] Prev batch still running. Will wait until notified')
                    self._cv_can_get.wait()
                    continue

                while len(self._node_queue) > 0 and bytes_to_scan <= self.bytes_per_batch_high_watermark:
                    node = self._node_queue.popleft()
                    nodes_to_scan.append(node)
                    size_bytes = node.get_size_bytes()
                    if size_bytes:
                        bytes_to_scan += size_bytes

                logger.debug(f'[{self.name}] Submitting batch task with {len(nodes_to_scan)} nodes and {bytes_to_scan} bytes total '
                             f'({len(self._node_queue)} still in queue)')
                calc_task = Task(ExecPriority.P6_SIGNATURE_CALC, self.calculate_signature_for_batch, nodes_to_scan)
                self._running_task_set.add(calc_task.task_uuid)
            self.backend.executor.submit_async_task(calc_task)

    def calculate_signature_for_batch(self, this_task: Task, nodes_to_scan: List[LocalFileNode]):
        """One task is created for each execution of this method."""
        assert this_task.priority == ExecPriority.P6_SIGNATURE_CALC

        if len(nodes_to_scan) == 0:
            # indicates a bug in this file
            logger.warning(f'[{self.name}] Task launched with empty batch of zero nodes!')
            return

        logger.debug(f'[{self.name}] Calculating signatures for batch of {len(nodes_to_scan)} nodes')
        for node in nodes_to_scan:
            self.calculate_signature_for_local_node(node)

        with self._cv_can_get:
            self._running_task_set.remove(this_task.task_uuid)
            self._cv_can_get.notifyAll()

    def _on_node_upserted_in_cache(self, sender: str, node: LocalNode):
        if node.device_uid == self.device_uid and node.is_file() and not node.md5 and not node.sha256:
            assert isinstance(node, LocalFileNode)
            if SUPER_DEBUG_ENABLED:
                logger.debug(f'[{self.name}] Enqueuing node: {node.node_identifier}')
            self._enqueue_node(node)

    def _on_subtree_nodes_changed_in_cache(self, sender: str, subtree_root: NodeIdentifier,
                                           upserted_node_list: List[LocalNode], removed_node_list: List[LocalNode]):
        if subtree_root.device_uid != self.device_uid:
            return

        for node in upserted_node_list:
            if node.is_file() and not node.md5 and not node.sha256:
                assert isinstance(node, LocalFileNode)
                if SUPER_DEBUG_ENABLED:
                    logger.debug(f'[{self.name}] Enqueuing node (from batch): {node.node_identifier}')
                self._enqueue_node(node)

    # Signature calculation
    # ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼

    def calculate_signature_for_local_node(self, node: LocalFileNode):
        # Get up-to-date copy:
        node = self.backend.cacheman.get_node_for_uid(node.uid, node.device_uid)

        if node.md5 or node.sha256:
            # Other threads, e.g., CommandExecutor, can also fill this in asynchronously
            logger.debug(f'[{self.name}] Node already has signature; skipping; {node}')
            return

        size_bytes = node.get_size_bytes()
        if size_bytes and size_bytes > LARGE_FILE_SIZE_THRESHOLD_BYTES:
            logger.info(f'[{self.name}] Calculating signature for node (note: this file is very large ({humanfriendlier_size(size_bytes)}) '
                        f'and may take a while: {node.node_identifier}')
        elif SUPER_DEBUG_ENABLED:
            logger.debug(f'[{self.name}] Calculating signature for node: {node.node_identifier}')

        node_with_signature = content_hasher.try_calculating_signatures(node)
        if not node_with_signature:
            logger.debug(f'[{self.name}] Failed to calculate signature for node {node.uid}: assuming it was deleted from disk')
            return

        if SUPER_DEBUG_ENABLED:
            logger.debug(f'[{self.name}] Node {node_with_signature.node_identifier.guid} has MD5: {node_with_signature.md5}')

        # TODO: consider batching writes
        # Send back to ourselves to be re-stored in memory & disk caches:
        self.backend.cacheman.update_single_node(node_with_signature)
