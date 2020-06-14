import os
from collections import deque
from typing import Deque, List
import logging

import treelib

import file_util
from model.display_node import DirNode, DisplayNode
from model.fmeta import FMeta
from model.node_identifier import LocalFsIdentifier, NodeIdentifier

logger = logging.getLogger(__name__)


class LocalDiskTree(treelib.Tree):
    def __init__(self, application):
        super().__init__()
        self.application = application

    def add_to_tree(self, item: DisplayNode):
        root_node: DisplayNode = self.get_node(self.root)
        root_node_identifier: NodeIdentifier = root_node.node_identifier
        path_so_far: str = root_node_identifier.full_path
        parent: DisplayNode = self.get_node(root_node_identifier.uid)

        # A trailing '/' will really screw us up:
        assert file_util.is_normalized(root_node_identifier.full_path), f'Path: {root_node_identifier.full_path}'
        item_rel_path = file_util.strip_root(item.full_path, root_node_identifier.full_path)
        path_segments = file_util.split_path(item_rel_path)
        if path_segments:
            # strip off last item (i.e. the target item)
            path_segments.pop()

        if path_segments:
            for dir_name in path_segments:
                path_so_far: str = os.path.join(path_so_far, dir_name)
                uid = self.application.cache_manager.get_uid_for_path(path_so_far)
                child: DisplayNode = self.get_node(nid=uid)
                if not child:
                    # logger.debug(f'Creating dir node: nid={uid}')
                    child = DirNode(node_identifier=LocalFsIdentifier(full_path=path_so_far, uid=uid))
                    try:
                        self.add_node(node=child, parent=parent)
                    except Exception:
                        logger.error(f'Error occurred while adding node: {child} to parent: {parent}')
                        raise
                parent = child

        # Finally, add the node itself:
        child: DisplayNode = self.get_node(nid=item.uid)
        assert not child, f'For old={child}, new={item}, path_segments={path_segments}'
        if not child:
            self.add_node(node=item, parent=parent)

    def replace_subtree(self, sub_tree: treelib.Tree):
        if not self.contains(sub_tree.root):
            # quick and dirty way to add any missing parents:
            sub_tree_root_node: DisplayNode = sub_tree.get_node(sub_tree.root)
            logger.debug(f'Super-tree does not contain sub-tree root ({sub_tree_root_node.node_identifier}): it and its ancestors will be added')
            self.add_to_tree(sub_tree_root_node)

        parent_of_subtree: DisplayNode = self.parent(sub_tree.root)
        count_removed = self.remove_node(sub_tree.root)
        logger.debug(f'Removed {count_removed} nodes from super-tree, to be replaced with {len(sub_tree)} nodes')
        self.paste(nid=parent_of_subtree.uid, new_tree=sub_tree)

    def get_all_files_for_subtree(self, subtree_root: LocalFsIdentifier) -> List[FMeta]:
        fmeta_list: List[FMeta] = []
        queue: Deque[DisplayNode] = deque()
        node = self.get_node(nid=subtree_root.uid)
        queue.append(node)
        while len(queue) > 0:
            node = queue.popleft()
            if node.is_dir():
                for child in self.children(node.uid):
                    queue.append(child)
            else:
                assert isinstance(node, FMeta)
                fmeta_list.append(node)

        return fmeta_list
