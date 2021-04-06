import logging
import os
from typing import List, Tuple

from constants import TrashStatus
from model.node.local_disk_node import LocalDirNode, LocalFileNode, LocalNode
from model.node.node import Node
from model.node_identifier import LocalNodeIdentifier, NodeIdentifier
from util import file_util
from util.simple_tree import SimpleTree

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
        return self.get_node_for_uid(uid) is not None

    def add_to_tree(self, node: LocalNode):
        root_node: LocalNode = self.get_root_node()
        root_node_identifier: NodeIdentifier = root_node.node_identifier
        path_so_far: str = root_node_identifier.get_single_path()
        parent: LocalNode = self.get_node_for_uid(root_node_identifier.node_uid)

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
                uid = self.backend.cacheman.get_uid_for_local_path(path_so_far)
                child: LocalNode = self.get_node_for_uid(uid)
                if not child:
                    # logger.debug(f'Creating dir node: nid={uid}')
                    node_identifier = LocalNodeIdentifier(path_list=path_so_far, uid=uid, device_uid=root_node_identifier.device_uid)
                    child = LocalDirNode(node_identifier=node_identifier,
                                         parent_uid=parent.uid, trashed=TrashStatus.NOT_TRASHED, is_live=True)
                    try:
                        self.add_node(node=child, parent=parent)
                    except Exception:
                        logger.error(f'Error occurred while adding node: {child} to parent: {parent}')
                        raise
                parent = child

        # Finally, add the node itself:
        child: Node = self.get_node_for_uid(node.uid)
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

    def replace_subtree(self, sub_tree: SimpleTree):
        sub_tree_root_node: LocalNode = sub_tree.get_root_node()
        if not self.contains(sub_tree_root_node.uid):
            # quick and dirty way to add any missing parents:
            logger.debug(f'This tree (root: {self.get_root_node().node_identifier}) does not contain sub-tree '
                         f'(root: {sub_tree_root_node.node_identifier}): it and its ancestors will be added')
            assert isinstance(sub_tree_root_node, LocalNode)
            self.add_to_tree(sub_tree_root_node)
            # if SUPER_DEBUG:
            #     logger.debug(f'Tree contents (after adding subtree root): \n{self.show(show_identifier=True)}')

        assert sub_tree_root_node.get_single_parent(), f'Node is missing parent: {sub_tree_root_node}'
        if sub_tree_root_node.get_single_parent() != self.get_parent(sub_tree_root_node.uid).uid:
            # TODO: submit to adjudicator (eventually)
            logger.warning(f'Parent referenced by node "{sub_tree_root_node.uid}" ({sub_tree_root_node.get_single_parent()}) '
                           f'does not match actual parent ({self.get_parent(sub_tree_root_node.uid).uid})!')
        sub_tree_root_node_identifier = self.extract_identifier(sub_tree_root_node)
        parent_of_subtree: LocalNode = self.get_parent(sub_tree_root_node_identifier)
        # assert parent_of_subtree, f'Could not find node in tree with parent: {sub_tree_root_node.get_single_parent()}'
        count_removed = self.remove_node(sub_tree_root_node_identifier)
        logger.debug(f'Removed {count_removed} nodes from this tree, to be replaced with {len(sub_tree)} subtree nodes')
        self.paste(parent_uid=parent_of_subtree.uid, new_tree=sub_tree)

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
            subtree_root_uid = subtree_root.node_uid
        else:
            subtree_root_uid = None
        self.for_each_node_breadth_first(action_func=add_to_lists, subtree_root_uid=subtree_root_uid)

        logger.debug(f'Returning {len(file_list)} files and {len(dir_list)} dirs')
        return file_list, dir_list
