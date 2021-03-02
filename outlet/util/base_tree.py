from abc import ABC, abstractmethod
import logging
from collections import deque
from typing import Any, Callable, Deque, Dict, List, Optional

from model.has_get_children import HasGetChildren
from util.stopwatch_sec import Stopwatch
from model.node.node import Node
from model.uid import UID
from model.node.directory_stats import DirectoryStats
from constants import SUPER_DEBUG, TrashStatus

logger = logging.getLogger(__name__)


class BaseTree(HasGetChildren, ABC):
    """
    ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
    CLASS BaseTree

    Parent of ALL trees, from SimpleTree to GDriveWholeTree
    ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼
    """

    @abstractmethod
    def get_root_node(self) -> Optional:
        pass

    @abstractmethod
    def get_node_for_uid(self, uid: UID):
        pass

    def get_child_list_for_root(self) -> List:
        return self.get_child_list(self.get_root_node())

    def generate_dir_stats(self, tree_id: str, subtree_root_node: Optional[Node] = None) -> Dict[UID, DirectoryStats]:
        logger.debug(f'[{tree_id}] Generating stats for tree with root: {subtree_root_node.node_identifier}')
        stats_sw = Stopwatch()

        dir_stats_dict: Dict[UID, DirectoryStats] = {}

        if not subtree_root_node:
            subtree_root_node = self.get_root_node()

        second_pass_stack: Deque[Node] = deque()
        second_pass_stack.append(subtree_root_node)

        def add_dirs_to_stack(n):
            if n.is_dir():
                second_pass_stack.append(n)

        # go down tree, zeroing out existing stats and adding children to stack
        self.for_each_node_breadth_first(action_func=add_dirs_to_stack, subtree_root_uid=subtree_root_node.uid)

        # now go back up the tree by popping the stack and building stats as we go:
        while len(second_pass_stack) > 0:
            node = second_pass_stack.pop()
            assert node.is_dir()

            dir_stats = DirectoryStats()
            dir_stats_dict[node.uid] = dir_stats

            child_list = self.get_child_list(node)
            if child_list:
                for child in child_list:
                    if child.is_dir():
                        child_stats = dir_stats_dict[child.uid]
                        dir_stats.add_dir_stats(child_stats, child.get_trashed_status() == TrashStatus.NOT_TRASHED)
                    else:
                        dir_stats.add_file_node(child)

            if SUPER_DEBUG:
                logger.debug(f'Dir node {node.uid} ("{node.name}") has size={dir_stats.get_size_bytes()}, etc="{dir_stats.get_etc()}"')

        logger.debug(f'[{tree_id}] {stats_sw} Generated stats for local tree ("{subtree_root_node.node_identifier}")')
        return dir_stats_dict

    def for_each_node_breadth_first(self, action_func: Callable[[Any], None], subtree_root_uid: Optional[UID] = None):
        dir_queue: Deque = deque()
        if subtree_root_uid:
            # Only part of the tree
            subtree_root = self.get_node_for_uid(subtree_root_uid)
        else:
            subtree_root = self.get_root_node()

        if not subtree_root:
            return

        action_func(subtree_root)

        if subtree_root.is_dir():
            dir_queue.append(subtree_root)

        while len(dir_queue) > 0:
            node = dir_queue.popleft()

            children = self.get_child_list(node)
            if children:
                for child in children:
                    if child.is_dir():
                        dir_queue.append(child)
                    action_func(child)

    def get_subtree_bfs(self, subtree_root_uid: UID = None) -> List:
        """Returns an iterator which will do a breadth-first traversal of the tree. If subtree_root is provided, do a breadth-first traversal
        of the subtree whose root is subtree_root (returning None if this tree does not contain subtree_root).
        """
        if not subtree_root_uid:
            root_node = self.get_root_node()
            if not root_node:
                return []
            subtree_root_uid = root_node.identifier

        node = self.get_node_for_uid(uid=subtree_root_uid)
        if not node:
            return []

        bfs_list: List = []

        node_queue: Deque = deque()
        node_queue.append(node)

        while len(node_queue) > 0:
            node = node_queue.popleft()
            bfs_list.append(node)
            for child in self.get_child_list(node):
                node_queue.append(child)

        return bfs_list
