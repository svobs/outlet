import logging
from typing import List, Optional

import treelib

from model.display_id import GDriveIdentifier, Identifier
from model.display_node import DirNode, DisplayNode
from model.gdrive_tree import GDriveSubtree, GDriveTree, GDriveWholeTree
from model.subtree_snapshot import SubtreeSnapshot
from ui.tree.display_tree_builder import DisplayTreeBuilder

logger = logging.getLogger(__name__)


# CLASS AllItemsGDriveTreeBuilder
# ⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟

class AllItemsGDriveTreeBuilder(DisplayTreeBuilder):
    def __init__(self, controller, root: Identifier = None, tree: SubtreeSnapshot = None):
        super().__init__(controller=controller, root=root, tree=tree)

    def get_children_for_root(self) -> Optional[List[DisplayNode]]:
        if isinstance(self.tree, GDriveWholeTree):
            # Whole tree? -> return the root nodes
            return self.tree.roots
        else:
            # Subtree? -> return the subtree root
            assert isinstance(self.tree, GDriveSubtree)
            parent_id = self.tree.root_id
            return self.tree.get_children(parent_id)

    def get_children(self, parent_identifier: Identifier) -> Optional[List[DisplayNode]]:
        assert isinstance(self.tree, GDriveTree)
        return self.tree.get_children(parent_id=parent_identifier)


# CLASS AllItemsLocalFsTreeBuilder
# ⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟

class AllItemsLocalFsTreeBuilder(DisplayTreeBuilder):
    def __init__(self, controller, root: Identifier = None, tree: SubtreeSnapshot = None):
        super().__init__(controller=controller, root=root, tree=tree)
        self.display_tree = None

    def get_children_for_root(self) -> Optional[List[DisplayNode]]:
        if not self.display_tree:
            root_node = DirNode(self.tree.identifier)
            self.display_tree = _build_display_tree(self.tree, root_node)
        return self.get_children(parent_identifier=self.tree.identifier)

    def get_children(self, parent_identifier: Identifier) -> Optional[List[DisplayNode]]:
        children = []
        try:
            for child in self.display_tree.children(parent_identifier.uid):
                children.append(child.data)
        except Exception:
            logger.debug(f'CategoryTree for "{self.tree.identifier}": ' + self.display_tree.show(stdout=False))
            raise

        return children


def _build_display_tree(source_tree: SubtreeSnapshot, root_node) -> treelib.Tree:
    """
    Builds a tree out of the flat file set.
    Args:
        source_tree: source tree
        root_node: a display node representing the category

    Returns:
        change tree
    """
    # The change set in tree form
    display_tree = treelib.Tree()

    item_list: List[DisplayNode] = source_tree.get_all()
    set_len = len(item_list)

    logger.debug(f'Building display tree for {set_len} files...')

    root: treelib.Node = display_tree.create_node(identifier=root_node.uid, data=root_node)  # root
    for item in item_list:
        if item.is_dir():
            # Skip any actual directories we encounter. We won't use them for our display, because:
            # (1) each category has a logically different dir with the same ID, and let's not get confused, and
            # (2) there's nothing for us in these objects from a display perspective. The name can be inferred
            # from each file's path, and we don't want to display empty dirs when there's no file of that category
            continue
        ancestor_identifiers = source_tree.get_ancestor_chain(item)
        # nid == Node ID == directory name
        parent = root
        parent.data.add_meta_metrics(item)

        if ancestor_identifiers:
            # Create a node for each ancestor dir (path segment)
            for identifier in ancestor_identifiers:
                nid = identifier.uid
                child: treelib.Node = display_tree.get_node(nid=nid)
                if child is None:
                    # logger.debug(f'Creating dir node: nid={nid}')
                    dir_node = DirNode(identifier=identifier)
                    child = display_tree.create_node(identifier=nid, parent=parent, data=dir_node)
                parent = child
                assert isinstance(parent.data, DirNode)
                parent.data.add_meta_metrics(item)

        # Each node's ID will be either
        # logger.debug(f'Creating file node: nid={nid}')
        display_tree.create_node(identifier=item.uid, parent=parent, data=item)

    return display_tree
