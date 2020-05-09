import copy
import logging
import os
from typing import Dict, List, Optional

import treelib

import file_util
from model.category import Category
from model.display_id import Identifier, LogicalNodeIdentifier
from model.display_node import CategoryNode, DirNode, DisplayNode
from model.subtree_snapshot import SubtreeSnapshot
from stopwatch_sec import Stopwatch
from ui.tree.display_tree_builder import DisplayTreeBuilder

logger = logging.getLogger(__name__)


class CategoryTreeBuilder(DisplayTreeBuilder):
    def __init__(self, controller, root: Identifier = None, tree: SubtreeSnapshot = None):
        super().__init__(controller=controller, root=root, tree=tree)

        self._category_trees: Dict[Category, treelib.Tree] = {}
        """Each entry is lazy-loaded"""

    def get_category_trees_static(self):
        change_trees = []
        category_nodes = self.get_children_for_root()
        for cat_root in category_nodes:
            change_tree = self.tree.get_category_tree(cat_root.category)
            change_trees.append(change_tree)
        return change_trees

    def get_children_for_root(self) -> Optional[List[DisplayNode]]:
        return self.tree.get_children_for_root()

    def get_children(self, parent_identifier: Identifier) -> Optional[List[DisplayNode]]:
        return self.tree.get_children(parent_identifier)


def _make_category_node(tree_root_identifier: Identifier, category):
    # uid must be unique within the tree
    cat_id: LogicalNodeIdentifier = LogicalNodeIdentifier(uid=f'Category:{category.name}',
                                                          full_path=tree_root_identifier.full_path, category=category)
    return CategoryNode(cat_id)


def _build_category_tree(source_tree: SubtreeSnapshot, root_node: CategoryNode) -> treelib.Tree:
    """
    Builds a tree out of the flat file set.
    Args:
        source_tree: source tree
        root_node: a display node representing the category

    Returns:
        change tree
    """
    # The change set in tree form
    change_tree = treelib.Tree()

    category: Category = root_node.category
    cat_item_list: List[DisplayNode] = source_tree.get_for_cat(category)
    set_len = len(cat_item_list)

    logger.debug(f'Building change trees for category {category.name} with {set_len} files...')

    root: treelib.Node = change_tree.create_node(identifier=root_node.uid, data=root_node)
    for item in cat_item_list:
        if item.is_dir():
            # Skip any actual directories we encounter. We won't use them for our display, because:
            # (1) each category has a logically different dir with the same ID, and let's not get confused, and
            # (2) there's nothing for us in these objects from a display perspective. The name can be inferred
            # from each file's path, and we don't want to display empty dirs when there's no file of that category
            continue
        ancestor_identifiers = source_tree.get_ancestor_identifiers_as_list(item)
        # nid == Node ID == directory name
        parent = root
        parent.data.add_meta_emtrics(item)

        if ancestor_identifiers:
            # Create a node for each ancestor dir (path segment)
            for identifier in ancestor_identifiers:
                nid = identifier.uid
                child: treelib.Node = change_tree.get_node(nid=nid)
                if child is None:
                    # logger.debug(f'Creating dir node: nid={nid}')
                    id_copy = copy.copy(identifier)
                    id_copy.category = category
                    dir_node = DirNode(identifier=id_copy)
                    child = change_tree.create_node(identifier=nid, parent=parent, data=dir_node)
                parent = child
                assert isinstance(parent.data, DirNode)
                parent.data.add_meta_emtrics(item)

        # Each node's ID will be either
        # logger.debug(f'Creating file node: nid={nid}')
        change_tree.create_node(identifier=item.uid, parent=parent, data=item)

    return change_tree
