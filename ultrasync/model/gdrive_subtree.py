import collections
import logging
from typing import Deque, List, Optional, Union, ValuesView

import file_util
import format_util
from index.two_level_dict import Md5BeforeUidDict
from model.display_node import DisplayNode
from model.gdrive_whole_tree import GDriveItemNotFoundError, GDriveWholeTree
from model.goog_node import GoogFolder, GoogNode
from model.node_identifier import GDriveIdentifier, NodeIdentifier
from model.subtree_snapshot import SubtreeSnapshot
from stopwatch_sec import Stopwatch

logger = logging.getLogger(__name__)

SUPER_DEBUG = False

"""
◤━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━◥
    CLASS GDriveSubtree
    Represents a branch of the whole tree.
◣━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━◢
"""


class GDriveSubtree(SubtreeSnapshot):
    def __init__(self, whole_tree: GDriveWholeTree, root_node: GoogFolder):
        SubtreeSnapshot.__init__(self, root_node=root_node)

        self._whole_tree = whole_tree
        self._root_node: GoogFolder = root_node
        self._ignored_items: List[GoogNode] = []

        # See refresh_stats() for the following
        self._stats_loaded = False

    @classmethod
    def create_identifier(cls, full_path, uid, category) -> NodeIdentifier:
        return GDriveIdentifier(uid=uid, full_path=full_path, category=category)

    def get_ignored_items(self):
        return self._ignored_items

    def get_md5_dict(self):
        md5_set_stopwatch = Stopwatch()

        md5_dict: Md5BeforeUidDict = Md5BeforeUidDict()

        queue: Deque[GoogNode] = collections.deque()
        assert isinstance(self.root_node, GoogFolder)
        queue.append(self.root_node)

        while len(queue) > 0:
            item: GoogNode = queue.popleft()
            if item.is_dir():
                child_list = self._whole_tree.get_children(item)
                if child_list:
                    for child in child_list:
                        queue.append(child)
            elif item.md5:
                md5_dict.put(item)

        logger.info(f'{md5_set_stopwatch} Found {md5_dict.total_entries} MD5s')
        return md5_dict

    def get_children_for_root(self) -> List[GoogNode]:
        assert isinstance(self.root_node, GoogFolder)
        return self.get_children(self.root_node)

    def get_children(self, node: GoogNode) -> List[GoogNode]:
        return self._whole_tree.get_children(node=node)

    def get_full_path_for_item(self, item: GoogNode) -> List[str]:
        return self._whole_tree.get_full_path_for_item(item)

    def get_for_path(self, path: str, include_ignored=False) -> List[GoogNode]:
        if not self.in_this_subtree(path):
            raise RuntimeError(f'Not in this tree: "{path}" (tree root: {self.root_path}')
        try:
            identifiers = self._whole_tree.get_all_identifiers_for_path(path)
        except GDriveItemNotFoundError:
            return []

        if len(identifiers) == 1:
            return [self._whole_tree.get_item_for_uid(identifiers[0].uid)]

        # In Google Drive it is legal to have two different files with the same path
        logger.warning(f'Found {len(identifiers)} identifiers for path: "{path}"). Returning the whole list')
        return list(map(lambda x: self._whole_tree.get_item_for_uid(x.uid), identifiers))

    def get_parent_for_item(self, item: DisplayNode) -> Optional[GoogNode]:
        return self._whole_tree.get_parent_for_item(item, self.root_path)

    def get_relative_path_for_item(self, goog_node: GoogNode):
        """Get the path for the given ID, relative to the root of this subtree"""
        if not goog_node.full_path:
            node_full_path = self._whole_tree.get_all_paths_for_id(goog_node.uid)
        else:
            node_full_path = goog_node.full_path
        if isinstance(node_full_path, list):
            # Use the first path we find which is under this subtree:
            for full_path in node_full_path:
                if self.in_this_subtree(full_path):
                    return file_util.strip_root(full_path, self.root_path)
            raise RuntimeError(f'Could not get relative path for {node_full_path} in "{self.root_path}"')
        return file_util.strip_root(node_full_path, self.root_path)

    def __repr__(self):
        if self._stats_loaded:
            id_count_str = f' id_count={self._root_node.file_count + self._root_node.dir_count}'
        else:
            id_count_str = ''
        return f'GDriveSubtree(root_uid={self.root_uid} root_path="{self.root_path}"{id_count_str})'

    def get_summary(self):
        if self._stats_loaded:
            size_hf = format_util.humanfriendlier_size(self._root_node.size_bytes)
            trashed_size_hf = format_util.humanfriendlier_size(self._root_node.trashed_bytes)
            return f'{size_hf} total in {self._root_node.file_count:n} items (including {trashed_size_hf} in ' \
                   f'{self._root_node.trashed_file_count:n} trashed)'
        else:
            return 'Loading stats...'
