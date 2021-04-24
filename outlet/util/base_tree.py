from abc import ABC, abstractmethod
import logging
from collections import deque
from typing import Any, Callable, Deque, Dict, Generic, List, Optional, TypeVar

from util.stopwatch_sec import Stopwatch
from model.node.directory_stats import DirectoryStats
from constants import TrashStatus, TreeID

logger = logging.getLogger(__name__)

IdentifierT = TypeVar('IdentifierT')
NodeT = TypeVar('NodeT')


class BaseTree(Generic[IdentifierT, NodeT], ABC):
    """
    ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
    CLASS BaseTree

    Parent of ALL trees, from SimpleTree to GDriveWholeTree
    ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼
    """

    def __init__(self, extract_identifier_func: Callable[[NodeT], IdentifierT] = None, extract_node_func: Callable = None):
        self.extract_id: Callable[[NodeT], IdentifierT] = self._default_identifier_func
        if extract_identifier_func:
            self.extract_id = extract_identifier_func
        self.extract_node_func: Callable = self._default_get_node
        if extract_node_func:
            self.extract_node_func = extract_node_func

    @staticmethod
    def _default_identifier_func(node: NodeT):
        return node.identifier

    @abstractmethod
    def get_root_node(self) -> Optional[NodeT]:
        pass

    @abstractmethod
    def get_child_list_for_node(self, node: NodeT) -> List[NodeT]:
        pass

    @abstractmethod
    def get_node_for_identifier(self, identifier: IdentifierT) -> Optional[NodeT]:
        pass

    def get_child_list_for_root(self) -> List[NodeT]:
        return self.get_child_list_for_node(self.get_root_node())

    @staticmethod
    def _default_get_node(n):
        return n

    def generate_dir_stats(self, tree_id: TreeID, subtree_root_node: Optional[NodeT] = None) \
            -> Dict[IdentifierT, DirectoryStats]:
        logger.debug(f'[{tree_id}] Generating unfiltered stats for tree, subtree_root_node={subtree_root_node}')
        stats_sw = Stopwatch()

        dir_stats_dict: Dict[IdentifierT, DirectoryStats] = {}

        if not subtree_root_node:
            subtree_root_node = self.get_root_node()

        second_pass_stack: Deque[NodeT] = deque()
        second_pass_stack.append(subtree_root_node)

        def add_dirs_to_stack(n):
            n_node = self.extract_node_func(n)
            if n_node.is_dir():
                second_pass_stack.append(n)

        # go down tree, zeroing out existing stats and adding children to stack
        self.for_each_node_breadth_first(action_func=add_dirs_to_stack, subtree_root_identifier=self.extract_id(subtree_root_node))

        # now go back up the tree by popping the stack and building stats as we go:
        while len(second_pass_stack) > 0:
            node = second_pass_stack.pop()
            assert self.extract_node_func(node).is_dir()

            dir_stats = DirectoryStats()
            node_identifier = self.extract_id(node)
            dir_stats_dict[node_identifier] = dir_stats

            child_list = self.get_child_list_for_node(node)
            if child_list:
                for child in child_list:
                    child_node = self.extract_node_func(child)
                    if child_node.is_dir():
                        child_stats = dir_stats_dict[self.extract_id(child)]
                        dir_stats.add_dir_stats(child_stats, child_node.get_trashed_status() == TrashStatus.NOT_TRASHED)
                    else:
                        dir_stats.add_file_node(child_node)

            # if SUPER_DEBUG:
            #     logger.debug(f'DirNode {self.extract_id(child)} ("{node.name}") has size={dir_stats.get_size_bytes()}, etc="{dir_stats.get_etc()}"')

        logger.debug(f'[{tree_id}] {stats_sw} Generated stats for tree ("{subtree_root_node}")')
        return dir_stats_dict

    def for_each_node_breadth_first(self, action_func: Callable[[NodeT], None], subtree_root_identifier: Optional[IdentifierT] = None):
        dir_queue: Deque = deque()
        if subtree_root_identifier:
            # Only part of the tree
            subtree_root = self.get_node_for_identifier(subtree_root_identifier)
        else:
            subtree_root = self.get_root_node()

        if not subtree_root:
            return

        action_func(subtree_root)

        subtree_root_node = self.extract_node_func(subtree_root)
        if subtree_root_node.is_dir():
            dir_queue.append(subtree_root)

        while len(dir_queue) > 0:
            node = dir_queue.popleft()

            children = self.get_child_list_for_node(node)
            if children:
                for child in children:
                    child_node = self.extract_node_func(child)
                    if child_node.is_dir():
                        dir_queue.append(child)
                    action_func(child)

    def get_subtree_bfs(self, subtree_root_identifier: IdentifierT = None) -> List:
        """Returns an iterator which will do a breadth-first traversal of the tree. If subtree_root is provided, do a breadth-first traversal
        of the subtree whose root is subtree_root (returning None if this tree does not contain subtree_root).
        """
        if not subtree_root_identifier:
            root_node = self.get_root_node()
            if not root_node:
                return []
            subtree_root_identifier = self.extract_id(root_node)

        subtree_root_node = self.get_node_for_identifier(subtree_root_identifier)
        if not subtree_root_node:
            return []

        bfs_list: List = []

        node_queue: Deque = deque()
        node_queue.append(subtree_root_node)

        while len(node_queue) > 0:
            node = node_queue.popleft()
            bfs_list.append(node)
            for child in self.get_child_list_for_node(node):
                node_queue.append(child)

        return bfs_list
