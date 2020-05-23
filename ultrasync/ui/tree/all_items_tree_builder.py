import logging
from collections import deque
from typing import Deque, Iterable, List, Optional, Tuple

import treelib

from model.node_identifier import NodeIdentifier
from model.display_node import DirNode, DisplayNode
from model.gdrive_subtree import GDriveSubtree
from model.gdrive_whole_tree import GDriveWholeTree
from model.subtree_snapshot import SubtreeSnapshot
from stopwatch_sec import Stopwatch
from ui.tree.display_tree_builder import DisplayTreeBuilder

logger = logging.getLogger(__name__)


# CLASS AllItemsGDriveTreeBuilder
# ⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟

class AllItemsGDriveTreeBuilder(DisplayTreeBuilder):
    """Works with either a GDriveWholeTree or a GDriveSubtree"""
    def __init__(self, controller, root: NodeIdentifier = None, tree: SubtreeSnapshot = None):
        super().__init__(controller=controller, root=root, tree=tree)

    def get_children_for_root(self) -> Optional[List[DisplayNode]]:
        if isinstance(self.tree, GDriveWholeTree):
            # Whole tree? -> return the root nodes
            return self.tree.roots
        else:
            # Subtree? -> return the subtree root
            assert isinstance(self.tree, GDriveSubtree)
            parent_uid = self.tree.root_id
            return self.tree.get_children(parent_uid)

    def get_children(self, parent_identifier: NodeIdentifier) -> Optional[List[DisplayNode]]:
        return self.tree.get_children(parent_uid=parent_identifier)


# CLASS AllItemsLocalFsTreeBuilder
# ⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟

class AllItemsLocalFsTreeBuilder(DisplayTreeBuilder):
    def __init__(self, controller, root: NodeIdentifier = None, tree: SubtreeSnapshot = None):
        super().__init__(controller=controller, root=root, tree=tree)
        self.display_tree = None

    def get_children_for_root(self) -> Optional[List[DisplayNode]]:
        if not self.display_tree:
            self.display_tree = self._build_display_tree()
        return self.get_children(parent_identifier=self.tree.node_identifier)

    def get_children(self, parent_identifier: NodeIdentifier) -> Optional[List[treelib.Node]]:
        try:
            return self.display_tree.children(parent_identifier.uid)
        except Exception:
            logger.debug(f'CategoryTree for "{self.tree.node_identifier}": ' + self.display_tree.show(stdout=False))
            raise

    def _get_ancestors(self, item: DisplayNode) -> Deque[DisplayNode]:
        ancestors: Deque[DisplayNode] = deque()

        # Walk up the source tree, adding ancestors as we go, until we reach either a node which has already
        # been added to this tree, or the root of the source tree
        ancestor = item
        while ancestor:
            ancestor = self.tree.get_parent_for_item(ancestor)
            if ancestor:
                if ancestor.uid == self.tree.uid:
                    # do not include source tree's root node; that is already covered by the CategoryNode
                    # (in pre-ancestors)
                    return ancestors
                ancestors.appendleft(ancestor)

        return ancestors

    def _build_display_tree(self) -> treelib.Tree:
        """
        Builds a tree out of the flat file set.
        """
        sw = Stopwatch()
        root_node = DirNode(self.tree.node_identifier)
        # The change set in tree form
        display_tree = treelib.Tree()

        item_list: List[DisplayNode] = self.tree.get_all()
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
            ancestors: Iterable[DisplayNode] = self._get_ancestors(item)
            # nid == Node ID == directory name
            parent = root
            parent.data.add_meta_metrics(item)

            if ancestors:
                # Create a node for each ancestor dir (path segment)
                for ancestor in ancestors:
                    nid = ancestor.uid
                    child: treelib.Node = display_tree.get_node(nid=nid)
                    if child is None:
                        # logger.debug(f'Creating dir node: nid={nid}')
                        child = display_tree.create_node(identifier=nid, parent=parent, data=ancestor)
                    parent = child
                    assert isinstance(parent.data, DirNode)
                    parent.data.add_meta_metrics(item)

            # Each node's ID will be either
            # logger.debug(f'Creating file node: nid={nid}')
            display_tree.create_node(identifier=item.uid, parent=parent, data=item)

        logger.debug(f'{sw} Constructed display tree for {set_len} items')
        return display_tree
