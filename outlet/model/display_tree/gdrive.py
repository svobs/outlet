import collections
import logging
from typing import Deque, List, Optional, Union

from pydispatch import dispatcher

from constants import TREE_TYPE_GDRIVE
from model.node_identifier import ensure_list, NodeIdentifier
from ui import actions
from util import file_util, format
from util.two_level_dict import Md5BeforeUidDict
from model.gdrive_whole_tree import GDriveItemNotFoundError, GDriveWholeTree
from model.node.gdrive_node import GDriveFolder, GDriveNode
from model.display_tree.display_tree import DisplayTree
from util.stopwatch_sec import Stopwatch

logger = logging.getLogger(__name__)


"""
◤━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━◥
    CLASS GDriveDisplayTree
    Represents a branch of the whole tree.
◣━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━◢
"""


class GDriveDisplayTree(DisplayTree):
    def __init__(self,  whole_tree: GDriveWholeTree, root_node: GDriveFolder, tree_id: str):
        DisplayTree.__init__(self, root_node=root_node)

        self._whole_tree = whole_tree
        self._root_node: GDriveFolder = root_node
        self.tree_id = tree_id

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

    def get_node_list_for_path_list(self, path_list: List[str]) -> List[GDriveNode]:
        path_list = ensure_list(path_list)
        if not self.in_this_subtree(path_list):
            raise RuntimeError(f'Not in this tree: "{path_list}" (tree root: {self.root_path}')

        identifiers_found: List[NodeIdentifier] = []

        for single_path in path_list:
            try:
                identifiers = self._whole_tree.get_identifier_list_for_single_path(single_path)
                if identifiers:
                    identifiers_found += identifiers
            except GDriveItemNotFoundError:
                return []

        if len(identifiers_found) == 1:
            return [self._whole_tree.get_node_for_uid(identifiers_found[0].uid)]

        # In Google Drive it is legal to have two different files with the same path
        logger.debug(f'Found {len(identifiers_found)} nodes for path list: "{path_list}"). Returning the whole list')
        return list(map(lambda x: self._whole_tree.get_node_for_uid(x.uid), identifiers_found))

    def get_parent_for_node(self, node: GDriveNode) -> Optional[GDriveNode]:
        if node.get_tree_type() != TREE_TYPE_GDRIVE:
            return None

        return self._whole_tree.get_parent_for_node(node, self.root_path)

    def __repr__(self):
        if self._stats_loaded:
            id_count_str = f' id_count={self._root_node.file_count + self._root_node.dir_count}'
        else:
            id_count_str = ''
        return f'GDriveDisplayTree(tree_id={self.tree_id} root_uid={self.root_uid} root_path="{self.root_path}"{id_count_str})'

    def get_summary(self):
        if self._stats_loaded:
            size_hf = format.humanfriendlier_size(self._root_node.get_size_bytes())
            trashed_size_hf = format.humanfriendlier_size(self._root_node.trashed_bytes)
            return f'{size_hf} total in {self._root_node.file_count:n} nodes (including {trashed_size_hf} in ' \
                   f'{self._root_node.trashed_file_count:n} trashed)'
        else:
            return 'Loading stats...'

    def print_tree_contents_debug(self):
        # TODO
        logger.debug(f'[{self.tree_id}] GDriveDisplayTree for "{self.node_identifier}": NOT IMPLEMENTED')

    def refresh_stats(self, tree_id: str):
        logger.debug(f'[{tree_id}] Refreshing stats...')
        assert isinstance(self.root_node, GDriveFolder)
        self._whole_tree.refresh_stats(self.root_node, tree_id)
        self._stats_loaded = True
        logger.debug(f'[{tree_id}] Sending signal "{actions.REFRESH_SUBTREE_STATS_DONE}"')
        dispatcher.send(signal=actions.REFRESH_SUBTREE_STATS_DONE, sender=tree_id)
        dispatcher.send(signal=actions.SET_STATUS, sender=tree_id, status_msg=self.get_summary())
