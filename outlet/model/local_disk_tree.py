import os
from collections import deque
from typing import Deque, Iterator, List, Tuple
import logging

import treelib
from treelib.exceptions import NodeIDAbsentError

from index.uid.uid import UID
from util import file_util
from model.node.display_node import HasChildList
from model.node.local_disk_node import LocalDirNode, LocalFileNode, LocalNode
from model.node_identifier import LocalFsIdentifier, NodeIdentifier
from util.stopwatch_sec import Stopwatch

logger = logging.getLogger(__name__)


# CLASS LocalDiskTree
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼

class LocalDiskTree(treelib.Tree):
    """Tree data structure, representing a subtree on a local disk, backed by a treelib.Tree data structure."""
    def __init__(self, app):
        super().__init__()
        self.app = app

    def get_root_node(self) -> LocalNode:
        return self.get_node(self.root)

    def add_to_tree(self, node: LocalNode):
        root_node: LocalNode = self.get_root_node()
        root_node_identifier: NodeIdentifier = root_node.node_identifier
        path_so_far: str = root_node_identifier.full_path
        parent: LocalNode = self.get_node(root_node_identifier.uid)

        # A trailing '/' will really screw us up:
        assert file_util.is_normalized(root_node_identifier.full_path), f'Path: {root_node_identifier.full_path}'
        node_rel_path = file_util.strip_root(node.full_path, root_node_identifier.full_path)
        path_segments = file_util.split_path(node_rel_path)
        if path_segments:
            # strip off last node (i.e. the target node)
            path_segments.pop()

        if path_segments:
            for dir_name in path_segments:
                path_so_far: str = os.path.join(path_so_far, dir_name)
                uid = self.app.cacheman.get_uid_for_path(path_so_far)
                child: LocalNode = self.get_node(nid=uid)
                if not child:
                    # logger.debug(f'Creating dir node: nid={uid}')
                    child = LocalDirNode(node_identifier=LocalFsIdentifier(full_path=path_so_far, uid=uid), exists=True)
                    try:
                        self.add_node(node=child, parent=parent)
                    except Exception:
                        logger.error(f'Error occurred while adding node: {child} to parent: {parent}')
                        raise
                parent = child

        # Finally, add the node itself:
        child: LocalNode = self.get_node(nid=node.uid)
        assert not child, f'For old={child}, new={node}, path_segments={path_segments}'
        if not child:
            if not parent:
                logger.error(f'Parent is None for node: {node}')
            self.add_node(node=node, parent=parent)

    def get_subtree_bfs(self, subtree_root_uid: UID = None) -> List[LocalNode]:
        """Returns an iterator which will do a breadth-first traversal of the tree. If subtree_root is provided, do a breadth-first traversal
        of the subtree whose root is subtree_root (returning None if this tree does not contain subtree_root).
        """
        if not subtree_root_uid:
            subtree_root_uid = self.root

        if not self.contains(subtree_root_uid):
            return []

        node: LocalNode = self.get_node(nid=subtree_root_uid)

        queue: Deque[LocalNode] = deque()
        bfs_list: List[LocalNode] = []

        queue.append(node)

        while len(queue) > 0:
            node = queue.popleft()
            bfs_list.append(node)
            if node.is_dir():
                for child in self.children(node.uid):
                    queue.append(child)

        return bfs_list

    def replace_subtree(self, sub_tree: treelib.Tree):
        if not self.contains(sub_tree.root):
            # quick and dirty way to add any missing parents:
            sub_tree_root_node: LocalNode = sub_tree.get_node(sub_tree.root)
            logger.debug(f'This tree (root: {self.get_root_node().node_identifier}) does not contain sub-tree '
                         f'(root: {sub_tree_root_node.node_identifier}): it and its ancestors will be added')
            self.add_to_tree(sub_tree_root_node)

        parent_of_subtree: LocalNode = self.parent(sub_tree.root)
        count_removed = self.remove_node(sub_tree.root)
        logger.debug(f'Removed {count_removed} nodes from this tree, to be replaced with {len(sub_tree)} subtree nodes')
        self.paste(nid=parent_of_subtree.uid, new_tree=sub_tree)

    def get_all_files_and_dirs_for_subtree(self, subtree_root: LocalFsIdentifier) -> Tuple[List[LocalFileNode], List[LocalDirNode]]:
        file_list: List[LocalFileNode] = []
        dir_list: List[LocalDirNode] = []
        queue: Deque[LocalNode] = deque()
        node = self.get_node(nid=subtree_root.uid)
        queue.append(node)
        while len(queue) > 0:
            node = queue.popleft()
            if node.is_dir():
                assert isinstance(node, LocalDirNode)
                dir_list.append(node)
                for child in self.children(node.uid):
                    queue.append(child)
            else:
                assert isinstance(node, LocalFileNode)
                file_list.append(node)

        logger.debug(f'Returning {len(file_list)} files and {len(dir_list)} dirs')
        return file_list, dir_list
    
    def get_children(self, node: LocalNode) -> List[LocalNode]:
        try:
            return self.children(node.uid)
        except NodeIDAbsentError:
            raise RuntimeError(f'Node is not in the tree: {node} (uid={node.uid})')

    def refresh_stats(self, tree_id: str, subtree_root_node: LocalNode):
        logger.debug(f'[{tree_id}] Refreshing stats for local disk tree with root: {subtree_root_node.node_identifier}')
        stats_sw = Stopwatch()
        queue: Deque[LocalNode] = deque()
        stack: Deque[LocalNode] = deque()

        if subtree_root_node:
            root_node = subtree_root_node
        else:
            root_node = self.root_node

        queue.append(root_node)
        stack.append(root_node)

        # go down tree, zeroing out existing stats and adding children to stack
        while len(queue) > 0:
            node: LocalNode = queue.popleft()
            # logger.debug(f'[{tree_id}] Zeroing out stats for node: {node}')
            assert isinstance(node, HasChildList) and isinstance(node, LocalNode) and node.is_dir()
            node.zero_out_stats()

            children = self.get_children(node)
            if children:
                for child in children:
                    # logger.debug(f'[{tree_id}] Appending child to stats queue: {child}')
                    if child.is_dir():
                        assert isinstance(child, HasChildList) and isinstance(child, LocalNode)
                        queue.append(child)
                        stack.append(child)

        # now go back up the tree by popping the stack and building stats as we go:
        while len(stack) > 0:
            node = stack.pop()
            assert node.is_dir() and isinstance(node, HasChildList) and isinstance(node, LocalNode)
            node.set_stats_for_no_children()

            children = self.get_children(node)
            if children:
                for child in children:
                    node.add_meta_metrics(child)

            # logger.debug(f'[{tree_id}] Node {node.uid} ("{node.name}") has size={node.get_size_bytes()}, etc={node.get_etc()}')

        logger.debug(f'[{tree_id}] {stats_sw} Refreshed stats for local tree ("{subtree_root_node.node_identifier}")')
