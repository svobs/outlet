import logging
from typing import List, Optional

from pydispatch import dispatcher

from constants import TREE_TYPE_GDRIVE
from model.display_tree.display_tree import DisplayTree
from model.gdrive_whole_tree import GDriveWholeTree
from model.node.gdrive_node import GDriveFolder, GDriveNode
from model.node_identifier import ensure_list, SinglePathNodeIdentifier
from ui import actions
from util import format

logger = logging.getLogger(__name__)


"""
◤━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━◥
    CLASS GDriveDisplayTree
    Represents a branch of the whole tree.
◣━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━◢
"""


class GDriveDisplayTree(DisplayTree):
    def __init__(self, app, tree_id: str, root_identifier: SinglePathNodeIdentifier, whole_tree: GDriveWholeTree):
        DisplayTree.__init__(self, app, tree_id, root_identifier)

        self._whole_tree: GDriveWholeTree = whole_tree

    def get_root_node(self):
        return self._whole_tree.get_node_for_uid(self.root_identifier.uid)

    def get_children_for_root(self) -> List[GDriveNode]:
        root_node = self.get_root_node()
        assert isinstance(root_node, GDriveFolder), f'Expected root node to be type GDriveFolder but found instead: {root_node}'
        return self.get_children(root_node)

    def get_children(self, parent: GDriveNode) -> List[GDriveNode]:
        return self._whole_tree.get_children(node=parent)

    def get_node_list_for_path_list(self, path_list: List[str]) -> List[GDriveNode]:
        path_list = ensure_list(path_list)
        if not self.is_path_in_subtree(path_list):
            raise RuntimeError(f'Not in this tree: "{path_list}" (tree root: {self.root_path}')

        return self._whole_tree.get_node_list_for_path_list(path_list)

    def get_single_parent_for_node(self, node: GDriveNode) -> Optional[GDriveNode]:
        if node.get_tree_type() != TREE_TYPE_GDRIVE:
            return None

        return self._whole_tree.get_single_parent_for_node(node, self.root_path)

    def __repr__(self):
        if self._stats_loaded:
            root_node = self.get_root_node()
            assert isinstance(root_node, GDriveFolder)
            id_count_str = f' id_count={root_node.file_count + root_node.dir_count}'
        else:
            id_count_str = ''
        return f'GDriveDisplayTree(tree_id={self.tree_id} root_identifier={self.root_identifier}{id_count_str})'

    def get_summary(self):
        if self._stats_loaded:
            root_node = self.get_root_node()
            assert isinstance(root_node, GDriveFolder)
            size_hf = format.humanfriendlier_size(root_node.get_size_bytes())
            trashed_size_hf = format.humanfriendlier_size(root_node.trashed_bytes)
            return f'{size_hf} total in {root_node.file_count:n} nodes (including {trashed_size_hf} in ' \
                   f'{root_node.trashed_file_count:n} trashed)'
        else:
            return 'Loading stats...'

    def print_tree_contents_debug(self):
        # TODO
        logger.debug(f'[{self.tree_id}] GDriveDisplayTree for "{self.root_identifier}": NOT IMPLEMENTED')

    def refresh_stats(self, tree_id: str):
        logger.debug(f'[{tree_id}] Refreshing stats...')
        root_node = self.get_root_node()
        assert isinstance(root_node, GDriveFolder)
        self._whole_tree.refresh_stats(root_node, tree_id)
        self._stats_loaded = True
        logger.debug(f'[{tree_id}] Sending signal "{actions.REFRESH_SUBTREE_STATS_DONE}"')
        dispatcher.send(signal=actions.REFRESH_SUBTREE_STATS_DONE, sender=tree_id)
        dispatcher.send(signal=actions.SET_STATUS, sender=tree_id, status_msg=self.get_summary())
