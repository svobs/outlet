import logging
from typing import Iterable

import constants
from model.display_tree.display_tree import DisplayTree
from model.node.local_disk_node import LocalFileNode
from model.node.node import Node
from model.node_identifier import SinglePathNodeIdentifier
from ui.tree.filter_criteria import FilterCriteria

logger = logging.getLogger(__name__)

"""
━━━━━━━━━━━━━━━━━┛ ✠ ┗━━━━━━━━━━━━━━━━━
           LocalDiskDisplayTree
━━━━━━━━━━━━━━━━━┓ ✠ ┏━━━━━━━━━━━━━━━━━
"""


class LocalDiskDisplayTree(DisplayTree):
    def __init__(self, app, tree_id: str, root_identifier: SinglePathNodeIdentifier):
        super().__init__(app, tree_id, root_identifier)

    def get_children_for_root(self, filter_criteria: FilterCriteria = None) -> Iterable[Node]:
        root_node = self.get_root_node()
        return self.app.cacheman.get_children(root_node, filter_criteria)

    def get_children(self, parent: Node, filter_criteria: FilterCriteria = None) -> Iterable[Node]:
        assert parent.node_identifier.tree_type == constants.TREE_TYPE_LOCAL_DISK, f'For: {parent.node_identifier}'
        return self.app.cacheman.get_children(parent, filter_criteria)

    def remove(self, node: LocalFileNode):
        raise RuntimeError('Can no longer do this in LocalDiskDisplayTree!')

    def print_tree_contents_debug(self):
        logger.debug(f'[{self.tree_id}] Contents of LocalDiskDisplayTree for "{self.root_identifier}": \n' +
                     self.app.cacheman.show_tree(self.root_identifier))

    def __repr__(self):
        return f'LocalDiskDisplayTree(root="{self.root_identifier}"])'
