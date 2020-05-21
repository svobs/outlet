import copy
from collections import deque
from typing import Deque, Dict, Iterable, List, Optional, Union
import logging

import treelib
from treelib.exceptions import DuplicatedNodeIdError

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
    def __init__(self, source_tree: SubtreeSnapshot, root: Union[DisplayNode, Identifier] = None, extra_node_for_type=False):
        self._category_trees: Dict[Category, treelib.Tree] = {}
        self.extra_node_for_type = extra_node_for_type
        """One tree per category"""

        self.source_tree = source_tree

        if not root:
            # If root is not set, set it to the root of the source tree:
            self.identifier = self.source_tree.identifier
        elif isinstance(root, DisplayNode):
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
                logger.debug(f'While retrieving children for: {parent_identifier}')
                raise

    def get_item_for_identifier(self, identifer: Identifier) -> Optional[DisplayNode]:
        # FIXME
        if isinstance(identifer, int):
            # TODO: search each tree...? Eww
            pass
        elif identifer.uid:
            pass
            # item = self._whole_tree.get_item_for_id(identifer.uid)
            # if item and self.in_this_subtree(item.full_path):
            #     return item
        elif identifer.full_path:
            # TODO: not sure if we wanna pursue this
            pass
            # item_list = self._whole_tree.get_all_ids_for_path(identifer.full_path)
        return None

    def get_all(self):
        all_nodes = []
        queue = deque()
        nodes = self.get_children_for_root()
        for node in nodes:
            if node.is_dir():
                queue.append(node)
            elif node.is_file():
                all_nodes.append(node)

        while len(queue) > 0:
            node: DisplayNode = queue.popleft()
            for node in self.get_children(node.identifier):
                if node.is_dir():
                    queue.append(node)
                elif node.is_file():
                    all_nodes.append(node)

    def add_item(self, item: DisplayNode, category: Category):
        assert category != Category.NA, f'For item: {item}'
        category_tree: treelib.Tree = self._category_trees[category]
        root: treelib.Node = self._roots[category]

        # FIXME: find an elegant way to display all the nodes going back to each root

        # TODO: really, really need to get rid of one-tree-per-category

        if self.extra_node_for_type:
            if item.identifier.tree_type == OBJ_TYPE_GDRIVE:
                subroot_nid = root.identifier + '/Google Drive'
            elif item.identifier.tree_type == OBJ_TYPE_LOCAL_DISK:
                subroot_nid = root.identifier + '/Local Disk'
            else:
                raise RuntimeError(f'bad: {item.identifier.tree_type}')

            subroot_node: treelib.Node = category_tree.get_node(nid=subroot_nid)
            if not subroot_node:
                identifier = display_id.for_values(tree_type=item.identifier.tree_type, full_path=subroot_nid,
                                                   uid=subroot_nid, category=category)
                subroot_node_data = RootTypeNode(identifier=identifier)
                subroot_node = category_tree.create_node(identifier=subroot_nid, parent=root, data=subroot_node_data)

            root.data.add_meta_metrics(item)
            parent = subroot_node
        else:
            parent = root

        parent.data.add_meta_metrics(item)

        ancestor_identifiers: Deque[Identifier] = deque()
        if item.parent_ids:
            ancestor = item
            while ancestor and ancestor.parent_ids:
                # In this tree already? Saves us work, and allow us to use nodes not in the parent tree (e.g. FolderToAdds)
                node: treelib.Node = category_tree.get_node(nid=ancestor.parent_ids[0])
                if node:
                    parent = node
                    break
                ancestor = self.source_tree.get_item_for_identifier(ancestor.parent_ids[0])
                if ancestor:
                    if ancestor.uid == self.source_tree.uid:
                        # do not include root node
                        break
                    ancestor_identifiers.appendleft(ancestor.identifier)
        else:
            # TODO: get rid of this
            ancestor_identifiers = self.source_tree.get_ancestor_chain(item)

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
                if type(id_copy.full_path) == list:
                    # try to filter by whether the path is in the subtree. this won't always work
                    filtered_list = [x for x in id_copy.full_path if self.source_tree.in_this_subtree(x)]
                    if len(filtered_list) == 1:
                        id_copy.full_path = filtered_list[0]
                    else:
                        assert len(filtered_list) > 0
                        id_copy.full_path = filtered_list
                # TODO: subclass this from treelib.Node! Then we don't have to allocate twice
                # TODO: need to rename identifier to something else
                dir_node = DirNode(identifier=id_copy)
                child = category_tree.create_node(identifier=nid, parent=parent, data=dir_node)
            parent = child
            assert isinstance(parent.data, DirNode)
            parent.data.add_meta_metrics(item)

        # logger.debug(f'Creating file node for item {item}')
        categorized_item = copy.copy(item)
        categorized_item.identifier.category = category
        try:
            category_tree.create_node(identifier=item.uid, parent=parent, data=categorized_item)
        except DuplicatedNodeIdError:
            logger.error(f'Duplicate path for node {item}')
            raise

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

    def get_new_uid(self):
        return self.source_tree.get_new_uid()

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
