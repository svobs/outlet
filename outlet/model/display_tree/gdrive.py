import logging
from typing import List

from model.display_tree.display_tree import DisplayTree
from model.node.gdrive_node import GDriveFolder, GDriveNode
from model.node_identifier import SinglePathNodeIdentifier
from ui.tree.filter_criteria import FilterCriteria

logger = logging.getLogger(__name__)


"""
◤━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━◥
    CLASS GDriveDisplayTree
    Represents a branch of the whole tree.
◣━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━◢
"""


class GDriveDisplayTree(DisplayTree):
    def __init__(self, app, tree_id: str, root_identifier: SinglePathNodeIdentifier):
        DisplayTree.__init__(self, app, tree_id, root_identifier)

    def get_root_node(self):
        return self.app.cacheman.get_node_for_uid(self.root_identifier.uid)

    def get_children_for_root(self, filter_criteria: FilterCriteria = None) -> List[GDriveNode]:
        root_node = self.get_root_node()
        assert isinstance(root_node, GDriveFolder), f'Expected root node to be type GDriveFolder but found instead: {root_node}'
        return self.get_children(root_node, filter_criteria)

    def get_children(self, parent: GDriveNode, filter_criteria: FilterCriteria = None) -> List[GDriveNode]:
        return self.app.cacheman.get_children(node=parent, filter_criteria=filter_criteria)

    def __repr__(self):
        root_node = self.get_root_node()
        assert isinstance(root_node, GDriveFolder)
        if root_node.is_stats_loaded():
            id_count_str = f' id_count={root_node.file_count + root_node.dir_count}'
        else:
            id_count_str = ''
        return f'GDriveDisplayTree(tree_id={self.tree_id} root_identifier={self.root_identifier}{id_count_str})'

    def print_tree_contents_debug(self):
        logger.debug(f'[{self.tree_id}] GDriveDisplayTree for "{self.root_identifier}": {self.app.cacheman.show_tree(self.root_identifier)}')
