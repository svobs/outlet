import logging
import os
import threading
import time
from abc import ABC, abstractmethod
from typing import Dict, List, Set

from be.exec.central import ExecPriority
from logging_constants import SUPER_DEBUG_ENABLED
from model.node.locald_node import LocalDirNode, LocalNode
from util.ensure import ensure_int
from util.has_lifecycle import HasLifecycle
from util.task_runner import Task

logger = logging.getLogger(__name__)


class PathOp(ABC):
    @abstractmethod
    def execute(self, this_task: Task, caller):
        pass


class LocalFileChangeBatchingThread(HasLifecycle, threading.Thread):
    """
    ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
    CLASS LocalFileChangeBatchingThread

    Local file update notifications tend to be messy, and often we get lots of duplicate notifictations, even during a short period.
    This thread collects all the updated file paths in a Set, then processes the whole set after a fixed period.
    This thread has also been retrofitted to execute all changes in a batch, so that work can be broken into smaller chunks and run via the
    Central Executor.
    ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼
    """
    def __init__(self, backend):
        HasLifecycle.__init__(self)
        threading.Thread.__init__(self, target=self._run, name=f'LocalFileChangeBatchingThread', daemon=True)
        self.backend = backend
        self._cv_can_get = threading.Condition()
        self.local_change_batch_interval_ms: int = ensure_int(self.backend.get_config('cache.monitoring.local_change_batch_interval_ms'))
        self.modified_file_set: Set[str] = set()
        self.other_op_list: List[PathOp] = []

        self.expected_node_moves: Dict[str, str] = {}
        """When the FileSystemEventHandler gives us MOVE notifications for a tree, it gives us a separate notification for each
        and every node. Since we want our tree move to be an atomic operation, we do it all at once, but then keep track of the
        nodes we've moved so that we know exactly which notifications to ignore after that.
        Dict is key-value pair of [old_file_path -> new_file_path]"""

    def enqueue_modify(self, file_path: str):
        with self._cv_can_get:
            if SUPER_DEBUG_ENABLED:
                logger.debug(f'[{self.name}] Enqueuing modified path: "{file_path}"')
            self.modified_file_set.add(file_path)
            self._cv_can_get.notifyAll()

    def enqueue_move(self, src_path: str, dst_path: str):
        with self._cv_can_get:
            if SUPER_DEBUG_ENABLED:
                logger.debug(f'[{self.name}] Enqueuing MV: "{src_path}" -> "{dst_path}"')
            self.other_op_list.append(MvPath(src_path, dst_path))
            self._cv_can_get.notifyAll()

    def enqueue_delete(self, path: str):
        with self._cv_can_get:
            if SUPER_DEBUG_ENABLED:
                logger.debug(f'[{self.name}] Enqueuing RM: "{path}"')
            self.other_op_list.append(RmPath(path))
            self._cv_can_get.notifyAll()

    def enqueue_create(self, path: str):
        with self._cv_can_get:
            if SUPER_DEBUG_ENABLED:
                logger.debug(f'[{self.name}] Enqueuing MK: "{path}"')
            self.other_op_list.append(MkPath(path))
            self._cv_can_get.notifyAll()

    def start(self):
        HasLifecycle.start(self)
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

            with self._cv_can_get:
                if not self.modified_file_set and not self.other_op_list:
                    logger.debug(f'[{self.name}] No pending operations. Will wait until notified')
                    self._cv_can_get.wait()

                other_op_list: List[PathOp] = self.other_op_list
                self.other_op_list = []

                modified_file_set = self.modified_file_set
                self.modified_file_set = set()

            if SUPER_DEBUG_ENABLED:
                logger.debug(f'[{self.name}] Submitting local filesystem update task to Central Exec')
            self.backend.executor.submit_async_task(Task(ExecPriority.P3_LIVE_UPDATE, self._apply_fs_update_batch, modified_file_set, other_op_list))

            logger.debug(f'[{self.name}] Sleeping for {self.local_change_batch_interval_ms} ms')
            time.sleep(self.local_change_batch_interval_ms / 1000.0)

    def _apply_fs_update_batch(self, this_task: Task, modified_file_set: Set[str], other_op_list: List[PathOp]):
        """One task is created for each execution of this method."""
        assert this_task.priority == ExecPriority.P3_LIVE_UPDATE

        if len(other_op_list) > 0:
            logger.debug(f'[{self.name}] Executing batch of {len(other_op_list)} operations')
            for op in other_op_list:
                op.execute(this_task, self)

        if len(modified_file_set) > 0:
            logger.debug(f'[{self.name}] Applying batch of {len(other_op_list)} modifications')
            self._apply_modified_file_set(this_task, modified_file_set)

    def _apply_modified_file_set(self, this_task: Task, modified_file_set: Set[str]):
        logger.debug(f'[{self.name}] applying {len(modified_file_set)} local file updates...')
        for change_path in modified_file_set:
            try:
                logger.debug(f'[{self.name}] Applying CH file: {change_path}')
                node: LocalNode = self.backend.cacheman.build_local_file_node(change_path)
                if node:
                    self.backend.cacheman.upsert_single_node(node)
                else:
                    logger.debug(f'[{self.name}] CH: failed to build LocalNode; skipping "{change_path}"')
            except FileNotFoundError as err:
                logger.debug(f'[{self.name}] Cannot process external CH event: file not found: "{err.filename}"')

# File Operations
# ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼


class MkPath(PathOp):
    def __init__(self, path: str):
        self.path = path

    def execute(self, this_task: Task, caller):
        if os.path.isdir(self.path):
            node: LocalDirNode = caller.backend.cacheman.build_local_dir_node(self.path, is_live=True, all_children_fetched=True)
        else:
            node: LocalNode = caller.backend.cacheman.build_local_file_node(self.path)

        if node:
            caller.backend.cacheman.upsert_single_node(node)
        else:
            logger.debug(f'MkPath: failed to build LocalNode; skipping "{self.path}"')


class RmPath(PathOp):
    def __init__(self, path: str):
        self.path = path

    def execute(self, this_task: Task, caller):
        node: LocalNode = caller.backend.cacheman.get_node_for_local_path(self.path)
        if node:
            if node.is_dir():
                caller.backend.cacheman.remove_subtree(node, to_trash=False)
            else:
                caller.backend.cacheman.remove_node(node, to_trash=False)
        else:
            logger.debug(f'Cannot remove from cache: node not found in cache for path: {self.path}')


class MvPath(PathOp):
    def __init__(self, src_path: str, dst_path: str):
        self.src_path = src_path
        self.dst_path = dst_path

    def execute(self, this_task: Task, caller):
        # See if we safely ignore this:
        expected_move_dst = caller.expected_node_moves.pop(self.src_path, None)
        if expected_move_dst:
            if expected_move_dst == self.dst_path:
                logger.debug(f'Ignoring MV ("{self.src_path}" -> "{self.dst_path}") because it was already done')
                return
            else:
                logger.error(f'MV ("{self.src_path}" -> "{self.dst_path}"): was expecting dst = "{expected_move_dst}"!')

        node_list_tuple = caller.backend.cacheman.move_local_subtree(this_task, self.src_path, self.dst_path)

        if node_list_tuple:
            self._add_to_expected_node_moves(caller.expected_node_moves, node_list_tuple[0], node_list_tuple[1])

    @staticmethod
    def _add_to_expected_node_moves(expected_node_moves, src_node_list: List[LocalNode], dst_node_list: List[LocalNode]):
        first = True
        # Let's collate these two operations so that in case of failure, we have less inconsistent state
        for src_node, dst_node in zip(src_node_list, dst_node_list):
            logger.debug(f'Migrating copy of node {src_node.node_identifier} to {dst_node.node_identifier}')
            if first:
                # ignore subroot
                first = False
            else:
                expected_node_moves[src_node.get_single_path()] = dst_node.get_single_path()
