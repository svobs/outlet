import copy
from typing import Dict, Iterable, List, Optional, Union
import logging

import treelib

from model.category import Category
from model.display_id import Identifier, LogicalNodeIdentifier
from model.display_node import CategoryNode, DirNode, DisplayNode
from model.subtree_snapshot import SubtreeSnapshot

logger = logging.getLogger(__name__)

CATEGORIES = [Category.Added, Category.Deleted, Category.Moved, Category.Updated, Category.Ignored]

# CLASS CategoryDisplayTree
# ⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟


class CategoryDisplayTree:
    def __init__(self, root: Union[DisplayNode, Identifier]):
        self._category_trees: Dict[Category, treelib.Tree] = {}
        """One tree per category"""

        if isinstance(root, DisplayNode):
            self.identifier: Identifier = root.identifier
        else:
            self.identifier: Identifier = root
        self._roots: Dict[Category, treelib.Node] = {}
        """Don't know of an easy way to get the roots of each tree...just hold the refs here"""

        for category in CATEGORIES:
            # Make CategoryNode:
            category_node = self._make_category_node(category)

            # Make treelib.Tree
            category_tree = treelib.Tree()
            self._category_trees[category] = category_tree

            # Make treelib root:
            root = category_tree.create_node(identifier=category_node.uid, data=category_node)

            self._roots[category] = root

    @property
    def tree_type(self) -> int:
        return self.identifier.tree_type

    @property
    def root_path(self):
        return self.identifier.full_path

    @property
    def uid(self):
        return self.identifier.uid

    @classmethod
    def create_empty_subtree(cls, subtree_root: Union[str, Identifier, DisplayNode]):
        if type(subtree_root) == str:
            return CategoryDisplayTree(cls.create_identifier(subtree_root, Category.NA))
        elif isinstance(subtree_root, Identifier):
            return CategoryDisplayTree(subtree_root)
        else:
            assert isinstance(subtree_root, Identifier) or isinstance(subtree_root, DisplayNode)
            return CategoryDisplayTree(subtree_root)

    @classmethod
    def create_identifier(cls, full_path: str, category: Category) -> Identifier:
        """Create a new identifier of the type matching this tree"""
        return LogicalNodeIdentifier(uid=full_path, full_path=full_path, category=category)

    def get_children_for_root(self) -> Iterable[DisplayNode]:
        return list(map(lambda x: x.data, self._roots.values()))

    def get_category_tree(self, category: Category):
        return self._category_trees.get(category, None)

    def get_children(self, parent_identifier: Identifier) -> Optional[List[DisplayNode]]:
        category_tree: treelib.Tree = self._category_trees.get(parent_identifier.category, None)
        children = []
        if category_tree:
            try:
                for child in category_tree.children(parent_identifier.uid):
                    children.append(child.data)
            except Exception:
                logger.debug(f'CategoryTree for "{self.identifier}": ' + category_tree.show(stdout=False))
                raise

        return children

    def add_item(self, item: DisplayNode, category: Category, source_tree: SubtreeSnapshot):
        category_tree: treelib.Tree = self._category_trees[category]
        root: treelib.Node = self._roots[category]

        if item.is_dir():
            # Skip any actual directories we encounter. We won't use them for our display, because:
            # (1) each category has a logically different dir with the same ID, and let's not get confused, and
            # (2) there's nothing for us in these objects from a display perspective. The name can be inferred
            # from each file's path, and we don't want to display empty dirs when there's no file of that category
            logger.warning(f'Skipping dir node: {item}')
            return
        ancestor_identifiers = source_tree.get_ancestor_chain(item)
        # nid == Node ID == directory name
        parent = root
        parent.data.add_meta_emtrics(item)

        if ancestor_identifiers:
            # Create a node for each ancestor dir (path segment)
            for identifier in ancestor_identifiers:
                nid = identifier.uid
                child: treelib.Node = category_tree.get_node(nid=nid)
                if child is None:
                    # logger.debug(f'Creating dir node: nid={nid}')
                    # id_copy = copy.copy(identifier)
                    # id_copy.category = category
                    dir_node = DirNode(identifier=identifier)
                    child = category_tree.create_node(identifier=nid, parent=parent, data=dir_node)
                parent = child
                assert isinstance(parent.data, DirNode)
                parent.data.add_meta_emtrics(item)

        # Each node's ID will be either
        # logger.debug(f'Creating file node: nid={nid}')
        category_tree.create_node(identifier=item.uid, parent=parent, data=item)

    def get_full_path_for_item(self, item: DisplayNode) -> str:
        """Gets the absolute path for the item"""
        assert item.full_path
        return item.full_path

    def get_summary(self) -> str:
        summary = []
        for category, tree in self._category_trees.items():
            length = len(tree)
            root = self._roots[category]
            cat_node: CategoryNode = root.data
            summary.append(f'{category.name}: {cat_node.get_summary()}')
        return ', '.join(summary)

    def _make_category_node(self, category: Category):
        # uid must be unique within the tree
        cat_id: LogicalNodeIdentifier = LogicalNodeIdentifier(uid=f'Category:{category.name}',
                                                              full_path=self.identifier.full_path, category=category)
        return CategoryNode(cat_id)
