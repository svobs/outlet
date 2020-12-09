import os
from collections import deque
from typing import Callable, Deque, List, Optional, Tuple
import logging

from constants import SUPER_DEBUG, TrashStatus
from model.uid import UID
from util import file_util
from model.node.node import HasChildStats, Node
from model.node.local_disk_node import LocalDirNode, LocalFileNode, LocalNode
from model.node_identifier import LocalNodeIdentifier, NodeIdentifier
from util.simple_tree import BaseNode, SimpleTree
from util.stopwatch_sec import Stopwatch

logger = logging.getLogger(__name__)


class LocalDiskTree(SimpleTree):
    """
    ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
    CLASS LocalDiskTree

    Tree data structure, representing a subtree on a local disk, backed by a SimpleTree data structure.
    ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼
    """
    def __init__(self, backend):
        super().__init__()
        self.backend = backend

    def can_add_without_mkdir(self, node: LocalNode) -> bool:
        parent_path: str = node.derive_parent_path()
        uid = self.backend.cacheman.get_uid_for_local_path(parent_path)
        return self.get_node(uid) is not None

    def add_to_tree(self, node: LocalNode):
        root_node: LocalNode = self.get_root_node()
        root_node_identifier: NodeIdentifier = root_node.node_identifier
        path_so_far: str = root_node_identifier.get_single_path()
        parent: LocalNode = self.get_node(root_node_identifier.uid)

        # A trailing '/' will really screw us up:
        assert file_util.is_normalized(root_node_identifier.get_single_path()), f'Path: {root_node_identifier.get_single_path()}'
        node_rel_path = file_util.strip_root(node.get_single_path(), root_node_identifier.get_single_path())
        path_segments = file_util.split_path(node_rel_path)
        if path_segments:
            # strip off last node (i.e. the target node)
            path_segments.pop()

        if path_segments:
            for dir_name in path_segments:
                path_so_far: str = os.path.join(path_so_far, dir_name)
                # TODO: Should not be using override_load_check=True here
                uid = self.backend.cacheman.get_uid_for_local_path(path_so_far, override_load_check=True)
                child: LocalNode = self.get_node(nid=uid)
                if not child:
                    # logger.debug(f'Creating dir node: nid={uid}')
                    child = LocalDirNode(node_identifier=LocalNodeIdentifier(path_list=path_so_far, uid=uid),
                                         parent_uid=parent.uid, trashed=TrashStatus.NOT_TRASHED, is_live=True)
                    try:
                        self.add_node(node=child, parent=parent)
                    except Exception:
                        logger.error(f'Error occurred while adding node: {child} to parent: {parent}')
                        raise
                parent = child

        # Finally, add the node itself:
        child: Node = self.get_node(nid=node.uid)
        if child:
            if child.is_dir() and node.is_dir():
                # Just update
                assert isinstance(child, LocalNode)
                child.set_is_live(node.is_live())
            else:
                assert False, f'For old={child}, new={node}, path_segments={path_segments}'
        else:
            if not parent:
                logger.error(f'Parent is None for node: {node}')
            self.add_node(node=node, parent=parent)

    def for_each_node_breadth_first(self, action_func: Callable, subtree_root_node: Optional[LocalNode] = None):
        dir_queue: Deque[LocalDirNode] = deque()
        if not subtree_root_node:
            subtree_root_node = self.get_root_node()
            if not subtree_root_node:
                return

        action_func(subtree_root_node)

        if subtree_root_node.is_dir():
            assert isinstance(subtree_root_node, LocalDirNode)
            dir_queue.append(subtree_root_node)

        while len(dir_queue) > 0:
            node: LocalDirNode = dir_queue.popleft()

            children = self.get_children(node)
            if children:
                for child in children:
                    action_func(child)

                    if child.is_dir():
                        assert isinstance(child, LocalDirNode)
                        dir_queue.append(child)

    def replace_subtree(self, sub_tree: SimpleTree):
        sub_tree_root_node: LocalNode = sub_tree.get_root_node()
        if not self.contains(sub_tree_root_node.uid):
            # quick and dirty way to add any missing parents:
            logger.debug(f'This tree (root: {self.get_root_node().node_identifier}) does not contain sub-tree '
                         f'(root: {sub_tree_root_node.node_identifier}): it and its ancestors will be added')
            assert isinstance(sub_tree_root_node, LocalNode)
            self.add_to_tree(sub_tree_root_node)

        parent_of_subtree: LocalNode = self.parent(sub_tree_root_node.identifier)
        count_removed = self.remove_node(sub_tree_root_node.identifier)
        logger.debug(f'Removed {count_removed} nodes from this tree, to be replaced with {len(sub_tree)} subtree nodes')
        self.paste(parent_nid=parent_of_subtree.uid, new_tree=sub_tree)

    def get_all_files_and_dirs_for_subtree(self, subtree_root: LocalNodeIdentifier) -> Tuple[List[LocalFileNode], List[LocalDirNode]]:
        file_list: List[LocalFileNode] = []
        dir_list: List[LocalDirNode] = []

        def add_to_lists(node):
            if node.is_dir():
                assert isinstance(node, LocalDirNode)
                dir_list.append(node)
            else:
                assert isinstance(node, LocalFileNode)
                file_list.append(node)

        if subtree_root:
            subtree_root_node = self.get_node(subtree_root.uid)
        else:
            subtree_root_node = None
        self.for_each_node_breadth_first(action_func=add_to_lists, subtree_root_node=subtree_root_node)

        logger.debug(f'Returning {len(file_list)} files and {len(dir_list)} dirs')
        return file_list, dir_list

    def get_subtree_bfs(self, subtree_root_uid: UID = None) -> List[LocalNode]:
        """Returns an iterator which will do a breadth-first traversal of the tree. If subtree_root is provided, do a breadth-first traversal
        of the subtree whose root is subtree_root (returning None if this tree does not contain subtree_root).
        """
        if not subtree_root_uid:
            root_node = self.get_root_node()
            if not root_node:
                return []
            subtree_root_uid = root_node.uid

        if not self.contains(subtree_root_uid):
            return []

        node: LocalNode = self.get_node(nid=subtree_root_uid)
        assert isinstance(node, LocalNode)

        bfs_list: List[LocalNode] = []

        dir_queue: Deque[LocalNode] = deque()
        dir_queue.append(node)

        while len(dir_queue) > 0:
            node = dir_queue.popleft()
            assert isinstance(node, LocalNode)
            bfs_list.append(node)
            if node.is_dir():
                for child in self.children(node.uid):
                    assert isinstance(child, LocalNode)
                    dir_queue.append(child)

        return bfs_list

    def get_children(self, node: LocalNode) -> List[LocalNode]:
        return self.get_child_list(node.identifier)

    def refresh_stats(self, subtree_root_node: LocalNode, tree_id: str):
        logger.debug(f'[{tree_id}] Refreshing stats for local disk tree with root: {subtree_root_node.node_identifier}')
        stats_sw = Stopwatch()
        # dir_queue: Deque[LocalNode] = deque()
        second_pass_stack: Deque[LocalNode] = deque()

        # dir_queue.append(root_node)
        second_pass_stack.append(subtree_root_node)

        def zero_out_stats_and_add_dirs_to_stack(n):
            if n.is_dir():
                if SUPER_DEBUG:
                    logger.debug(f'[{tree_id}] Zeroing out stats for node: {n}')
                assert isinstance(n, HasChildStats) and isinstance(n, LocalDirNode)
                n.zero_out_stats()

                second_pass_stack.append(n)

        # go down tree, zeroing out existing stats and adding children to stack
        self.for_each_node_breadth_first(action_func=zero_out_stats_and_add_dirs_to_stack, subtree_root_node=subtree_root_node)

        # now go back up the tree by popping the stack and building stats as we go:
        while len(second_pass_stack) > 0:
            node = second_pass_stack.pop()
            assert node.is_dir() and isinstance(node, HasChildStats) and isinstance(node, LocalNode)
            node.set_stats_for_no_children()

            children = self.get_children(node)
            if children:
                for child in children:
                    node.add_meta_metrics(child)

            if SUPER_DEBUG:
                logger.debug(f'[{tree_id}] Dir node {node.uid} ("{node.name}") has size={node.get_size_bytes()}, etc={node.get_etc()}')

        logger.debug(f'[{tree_id}] {stats_sw} Refreshed stats for local tree ("{subtree_root_node.node_identifier}")')
