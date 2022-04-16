import copy
import logging
import threading
import time
from collections import deque
from typing import Deque, List, Set
from uuid import UUID

from be.exec.central import ExecPriority
from logging_constants import TRACE_ENABLED
from model.node.local_disk_node import LocalFileNode, LocalNode
from model.node_identifier import NodeIdentifier
from model.uid import UID
from signal_constants import Signal
from util.ensure import ensure_int
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
        self.device_uid = device_uid  # device_uid of local disk
        self._cv_can_get = threading.Condition()
        self.batch_interval_ms: int = ensure_int(self.backend.get_config('cache.local_disk.signatures.batch_interval_ms'))
        self.bytes_per_batch_high_watermark: int = ensure_int(self.backend.get_config('cache.local_disk.signatures.bytes_per_batch_high_watermark'))
        logger.debug(f'[{self.name}] Bytes per batch high watermark = {self.bytes_per_batch_high_watermark}')
        self._node_queue: Deque[LocalFileNode] = deque()
        self._running_task_set: Set[UUID] = set()

    def _enqueue_node(self, node: LocalFileNode):
        with self._cv_can_get:
            if TRACE_ENABLED:
                logger.debug(f'[{self.name}] Enqueuing node: {node.node_identifier}')

            self._node_queue.append(node)
            self._cv_can_get.notifyAll()

    def start(self):
        logger.debug(f'[{self.name}] Startup started')
        HasLifecycle.start(self)
        self.connect_dispatch_listener(signal=Signal.NODE_NEEDS_SIG_CALC, receiver=self._on_node_upserted_in_cache)
        self.connect_dispatch_listener(signal=Signal.NODE_UPSERTED_IN_CACHE, receiver=self._on_node_upserted_in_cache)
        self.connect_dispatch_listener(signal=Signal.SUBTREE_NODES_CHANGED_IN_CACHE, receiver=self._on_subtree_nodes_changed_in_cache)
        threading.Thread.start(self)
        logger.debug(f'[{self.name}] Startup done')

    def shutdown(self):
        logger.debug(f'[{self.name}] Shutdown started')
        HasLifecycle.shutdown(self)

        with self._cv_can_get:
            # unblock thread:
            self._cv_can_get.notifyAll()
        logger.debug(f'[{self.name}] Shutdown done')

    def _run(self):
        logger.info(f'[{self.name}] Starting thread...')
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
                    logger.debug(f'[{self.name}] Prev batch has not yet returned. Will wait until notified')
                    self._cv_can_get.wait()
                    continue

                while len(self._node_queue) > 0 and bytes_to_scan <= self.bytes_per_batch_high_watermark:
                    node = self._node_queue.popleft()
                    nodes_to_scan.append(node)
                    size_bytes = node.get_size_bytes()
                    if size_bytes:
                        bytes_to_scan += size_bytes

                logger.info(f'[{self.name}] Submitting batch calc task with {len(nodes_to_scan)} nodes and {bytes_to_scan:d} bytes total '
                            f'({len(self._node_queue)} nodes still enqueued)')
                calc_task = Task(ExecPriority.P7_SIGNATURE_CALC, self.batch_calculate_signatures, nodes_to_scan)
                self._running_task_set.add(calc_task.task_uuid)
            self.backend.executor.submit_async_task(calc_task)

    def batch_calculate_signatures(self, this_task: Task, nodes_to_scan: List[LocalFileNode]):
        """One task is created for each execution of this method."""
        assert this_task.priority == ExecPriority.P7_SIGNATURE_CALC

        if len(nodes_to_scan) == 0:
            # indicates a bug in this file
            logger.warning(f'[{self.name}] Task launched with empty batch of zero nodes!')
            return

        logger.debug(f'[{self.name}] Starting a batch of {len(nodes_to_scan)} nodes')
        for node in nodes_to_scan:
            self._calculate_signature_for_local_node(node)

        with self._cv_can_get:
            self._running_task_set.remove(this_task.task_uuid)
            self._cv_can_get.notifyAll()

    def _on_node_upserted_in_cache(self, sender: str, node: LocalNode):
        if node.device_uid == self.device_uid and node.is_file() and not node.has_signature():
            assert isinstance(node, LocalFileNode)
            self._enqueue_node(node)

    def _on_subtree_nodes_changed_in_cache(self, sender: str, subtree_root: NodeIdentifier,
                                           upserted_node_list: List[LocalNode], removed_node_list: List[LocalNode]):
        if subtree_root.device_uid != self.device_uid:
            return

        for node in upserted_node_list:
            if node.is_file() and not node.md5 and not node.sha256:
                assert isinstance(node, LocalFileNode)
                self._enqueue_node(node)

    # Signature calculation
    # ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼

    def _calculate_signature_for_local_node(self, node: LocalFileNode):
        if self.was_shutdown:
            return

        # Get up-to-date copy:
        node = self.backend.cacheman.get_node_for_uid(node.uid, node.device_uid)
        if not node:
            logger.warning(f'[{self.name}] Skipping signature calculation: node is no longer present in the cache: {node}')
            return

        if node.has_signature():
            # Other threads, e.g., CommandExecutor, can also fill this in asynchronously
            logger.debug(f'[{self.name}] TNode already has signature; skipping; {node}')
            return

        content_meta = self.backend.cacheman.calculate_signature_for_local_file(device_uid=node.device_uid, full_path=node.get_single_path())
        if not content_meta:
            # exceptional case should already have been logged; just return
            return

        updated_node: LocalFileNode = copy.deepcopy(node)
        updated_node.content_meta = content_meta

        # TODO: consider batching writes
        # Send back to ourselves to be re-stored in memory & disk caches:
        self.backend.cacheman.update_single_node(updated_node)
