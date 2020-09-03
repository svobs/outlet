import collections
import logging
from typing import Deque, List, Optional

from pydispatch import dispatcher

from ui import actions
from util import file_util, format
from index.two_level_dict import Md5BeforeUidDict
from model.node.display_node import DisplayNode
from model.gdrive_whole_tree import GDriveItemNotFoundError, GDriveWholeTree
from model.node.gdrive_node import GDriveFolder, GDriveNode
from model.display_tree.display_tree import DisplayTree
from util.stopwatch_sec import Stopwatch

logger = logging.getLogger(__name__)

SUPER_DEBUG = False

"""
◤━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━◥
    CLASS GDriveDisplayTree
    Represents a branch of the whole tree.
◣━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━◢
"""


class GDriveDisplayTree(DisplayTree):
    def __init__(self, cache_manager,  whole_tree: GDriveWholeTree, root_node: GDriveFolder):
        DisplayTree.__init__(self, root_node=root_node)

        self.cache_manager = cache_manager
        self._whole_tree = whole_tree
        self._root_node: GDriveFolder = root_node

        # See refresh_stats() for the following
        self._stats_loaded = False

    def get_md5_dict(self):
        md5_set_stopwatch = Stopwatch()

        md5_dict: Md5BeforeUidDict = Md5BeforeUidDict()

        queue: Deque[GDriveNode] = collections.deque()
        assert isinstance(self.root_node, GDriveFolder)
        queue.append(self.root_node)

        while len(queue) > 0:
            node: GDriveNode = queue.popleft()
            if node.exists():
                if node.is_dir():
                    child_list = self._whole_tree.get_children(node)
                    if child_list:
                        for child in child_list:
                            queue.append(child)
                elif node.md5:
                    md5_dict.put(node)

        logger.info(f'{md5_set_stopwatch} Found {md5_dict.total_entries} MD5s')
        return md5_dict

    def get_children_for_root(self) -> List[GDriveNode]:
        assert isinstance(self.root_node, GDriveFolder), f'Expected root node to be type GDriveFolder but found instead: {self.root_node}'
        return self.get_children(self.root_node)

    def get_children(self, parent: GDriveNode) -> List[GDriveNode]:
        return self._whole_tree.get_children(node=parent)

    def get_full_path_for_item(self, item: GDriveNode) -> List[str]:
        return self._whole_tree.get_full_path_for_item(item)

    def get_for_path(self, path: str, include_ignored=False) -> List[GDriveNode]:
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

    def get_parent_for_item(self, item: GDriveNode) -> Optional[GDriveNode]:
        return self._whole_tree.get_parent_for_item(item, self.root_path)

    def get_relative_path_for_item(self, goog_node: GDriveNode):
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
        return f'GDriveDisplayTree(root_uid={self.root_uid} root_path="{self.root_path}"{id_count_str})'

    def get_summary(self):
        if self._stats_loaded:
            size_hf = format.humanfriendlier_size(self._root_node.get_size_bytes())
            trashed_size_hf = format.humanfriendlier_size(self._root_node.trashed_bytes)
            return f'{size_hf} total in {self._root_node.file_count:n} items (including {trashed_size_hf} in ' \
                   f'{self._root_node.trashed_file_count:n} trashed)'
        else:
            return 'Loading stats...'

    def refresh_stats(self, tree_id: str):
        self.cache_manager.refresh_stats(tree_id, self.root_node)
        self._stats_loaded = True
        dispatcher.send(signal=actions.REFRESH_SUBTREE_STATS_DONE, sender=tree_id)
        dispatcher.send(signal=actions.SET_STATUS, sender=tree_id, status_msg=self.get_summary())
