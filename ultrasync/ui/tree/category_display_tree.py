import copy
from typing import Dict, Iterable, List, Optional, Union
import logging

import treelib

from constants import OBJ_TYPE_GDRIVE, OBJ_TYPE_LOCAL_DISK
from model import display_id
from model.category import Category
from model.display_id import Identifier, LogicalNodeIdentifier
from model.display_node import CategoryNode, DirNode, DisplayNode, RootTypeNode
from model.subtree_snapshot import SubtreeSnapshot

logger = logging.getLogger(__name__)

CATEGORIES = [Category.Added, Category.Deleted, Category.Moved, Category.Updated, Category.Ignored]

# CLASS CategoryDisplayTree
# ⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟


class CategoryDisplayTree:
    def __init__(self, root: Union[DisplayNode, Identifier], extra_node_for_type=False):
        self._category_trees: Dict[Category, treelib.Tree] = {}
        self.extra_node_for_type = extra_node_for_type
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
    def create_empty_subtree(cls, subtree_root: Union[Identifier, DisplayNode]):
        if isinstance(subtree_root, Identifier):
            return CategoryDisplayTree(subtree_root)
        else:
            assert isinstance(subtree_root, Identifier) or isinstance(subtree_root, DisplayNode)
            return CategoryDisplayTree(subtree_root)

    def get_children_for_root(self) -> Iterable[DisplayNode]:
        return list(map(lambda x: x.data, self._roots.values()))

    def get_category_tree(self, category: Category) -> Optional[treelib.Tree]:
        return self._category_trees.get(category, None)

    def get_children(self, parent_identifier: Identifier) -> List[treelib.Node]:
        assert parent_identifier.category != Category.NA, f'For item: {parent_identifier}'

        category_tree: treelib.Tree = self._category_trees[parent_identifier.category]

        if category_tree:
            try:
                return category_tree.children(parent_identifier.uid)
            except Exception:
                logger.debug(f'CategoryTree for "{self.identifier}": ' + category_tree.show(stdout=False))
                raise

    def add_item(self, item: DisplayNode, category: Category, source_tree: SubtreeSnapshot):
        assert category != Category.NA, f'For item: {item}'
        category_tree: treelib.Tree = self._category_trees[category]
        root: treelib.Node = self._roots[category]

        # FIXME: find an elegant way to display all the nodes going back to each root

        # TODO: really, really need to get rid of one-tree-per-category

        if item.is_dir():
            # Skip any actual directories we encounter. We won't use them for our display, because:
            # (1) each category has a logically different dir with the same ID, and let's not get confused, and
            # (2) there's nothing for us in these objects from a display perspective. The name can be inferred
            # from each file's path, and we don't want to display empty dirs when there's no file of that category
            logger.warning(f'Skipping dir node: {item}')
            return
        ancestor_identifiers: List[Identifier] = source_tree.get_ancestor_chain(item)

        if self.extra_node_for_type:
            if item.identifier.tree_type == OBJ_TYPE_GDRIVE:
                subroot = root.identifier + '/Google Drive'
            elif item.identifier.tree_type == OBJ_TYPE_LOCAL_DISK:
                subroot = root.identifier + '/Local Disk'
            else:
                raise RuntimeError(f'bad: {item.identifier.tree_type}')

            subroot_node: treelib.Node = category_tree.get_node(nid=subroot)
            if not subroot_node:
                identifier = display_id.for_values(tree_type=item.identifier.tree_type, full_path=subroot, uid=subroot, category=category)
                subroot_node_data = RootTypeNode(identifier=identifier)
                subroot_node = category_tree.create_node(identifier=subroot, parent=root, data=subroot_node_data)

            root.data.add_meta_metrics(item)
            parent = subroot_node
        else:
            parent = root

        parent.data.add_meta_metrics(item)

        if ancestor_identifiers:
            # Create a node for each ancestor dir (path segment)
            for identifier in ancestor_identifiers:
                nid = identifier.uid
                child: treelib.Node = category_tree.get_node(nid=nid)
                if child is None:
                    # Need to copy the identifier so that we can ensure the category is correct for where it is
                    # going in our tree. When get_children() is called, it uses the identifier's category to
                    # determine which tree to look up. Need to brainstorm a more elegant solution!
                    id_copy = copy.copy(identifier)
                    id_copy.category = category
                    dir_node = DirNode(identifier=id_copy)
                    child = category_tree.create_node(identifier=nid, parent=parent, data=dir_node)
                parent = child
                assert isinstance(parent.data, DirNode)
                parent.data.add_meta_metrics(item)

        # logger.debug(f'Creating file node: nid={nid}')
        categorized_item = copy.copy(item)
        categorized_item.identifier.category = category
        category_tree.create_node(identifier=item.uid, parent=parent, data=categorized_item)

    def get_ancestor_chain(self, item: DisplayNode):
        assert item.category != Category.NA
        identifiers: List[Identifier] = []

        category_tree: treelib.Tree = self._category_trees[item.category]
        category_tree_root_uid: str = self._roots[item.category].data.uid
        current_uid: str = item.uid

        while True:
            ancestor: treelib.Node = category_tree.parent(current_uid)
            if ancestor:
                if ancestor.data.uid == category_tree_root_uid:
                    # Do not include root
                    break
                identifiers.append(ancestor.data.identifier)
                current_uid = ancestor.data.uid
            else:
                break

        identifiers.reverse()
        return identifiers

    def get_full_path_for_item(self, item: DisplayNode) -> str:
        """Gets the absolute path for the item"""
        assert item.full_path
        return item.full_path

    def __repr__(self):
        return f'CategoryDisplayTree({self.get_summary()})'

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
