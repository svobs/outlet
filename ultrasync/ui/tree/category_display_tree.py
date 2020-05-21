import copy
from collections import deque
from typing import Deque, Dict, Iterable, List, Optional, Tuple, Union
import logging

import treelib
from treelib.exceptions import DuplicatedNodeIdError

import file_util
from constants import OBJ_TYPE_GDRIVE, OBJ_TYPE_LOCAL_DISK
from index.uid_generator import UID
from model.category import Category
from model.node_identifier import NodeIdentifier, LogicalNodeIdentifier, NodeIdentifierFactory
from model.display_node import CategoryNode, DirNode, DisplayNode, RootTypeNode
from model.subtree_snapshot import SubtreeSnapshot

logger = logging.getLogger(__name__)

CATEGORIES = [Category.Added, Category.Deleted, Category.Moved, Category.Updated, Category.Ignored]

# CLASS CategoryDisplayTree
# ⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟


class CategoryDisplayTree:
    def __init__(self, source_tree: SubtreeSnapshot, root: Union[DisplayNode, NodeIdentifier] = None, extra_node_for_type=False):
        self._category_tree: treelib.Tree = treelib.Tree()
        self.root = self._category_tree.create_node(identifier='//', parent=None, data=None)
        self.extra_node_for_type: bool = extra_node_for_type
        self.source_tree: SubtreeSnapshot = source_tree

        if not root:
            # If root is not set, set it to the root of the source tree:
            self.node_identifier = self.source_tree.node_identifier
        elif isinstance(root, DisplayNode):
            self.node_identifier: NodeIdentifier = root.node_identifier
        else:
            self.node_identifier: NodeIdentifier = root
        """Don't know of an easy way to get the roots of each tree...just hold the refs here"""

    @property
    def tree_type(self) -> int:
        return self.node_identifier.tree_type

    @property
    def root_path(self):
        return self.node_identifier.full_path

    @property
    def uid(self):
        return self.node_identifier.uid

    def get_children_for_root(self) -> Iterable[treelib.Node]:
        return self._category_tree.children(self.root.identifier)

    def get_children(self, parent_identifier: NodeIdentifier) -> List[treelib.Node]:
        assert parent_identifier.category != Category.NA, f'For item: {parent_identifier}'

        try:
            # Bit of a kludge here to fit in the fact that we're modifying the NIDs of some nodes and not others...
            # why is this being so difficult??
            if str(parent_identifier.uid).startswith(self.root.identifier):
                nid = parent_identifier.uid
            else:
                subroot_nid, cat_nid = self._get_useful_nids(parent_identifier, parent_identifier.category)
                nid = f'{cat_nid}/{parent_identifier.uid}'

            return self._category_tree.children(nid)
        except Exception:
            logger.debug(f'CategoryTree for "{self.node_identifier}": ' + self._category_tree.show(stdout=False))
            logger.debug(f'While retrieving children for: {parent_identifier}')
            raise

    # TODO: refactor signature: this is UID, not NodeIdentifer
    def get_item_for_identifier(self, identifer: NodeIdentifier) -> Optional[DisplayNode]:
        node = self._category_tree.get_node(identifer)
        if node:
            return node.data
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
            for node in self.get_children(node.node_identifier):
                if node.is_dir():
                    queue.append(node)
                elif node.is_file():
                    all_nodes.append(node)

    def _get_subtroot_nid(self, node_identifier: NodeIdentifier) -> Optional[str]:
        if self.extra_node_for_type:
            if node_identifier.tree_type == OBJ_TYPE_GDRIVE:
                return f'{self.root.identifier}GD'
            elif node_identifier.tree_type == OBJ_TYPE_LOCAL_DISK:
                return f'{self.root.identifier}LO'
            else:
                raise RuntimeError(f'bad: {node_identifier.tree_type}')
        return None

    def _get_useful_nids(self, node_identifier: NodeIdentifier, category: Category):
        if self.extra_node_for_type:
            subroot_nid = self._get_subtroot_nid(node_identifier)
            cat_nid = f'{subroot_nid}/{category.name}'
            return subroot_nid, cat_nid
        else:
            cat_nid = f'{self.root.identifier}{category.name}'
            return None, cat_nid

    def _get_or_create_pre_ancestors(self, item: DisplayNode, category: Category) -> Tuple[str, treelib.Node]:
        subroot_nid, cat_nid = self._get_useful_nids(item.node_identifier, category)

        last_pre_ancestor_nid = f'{cat_nid}/{self.node_identifier.uid}'

        # FIXME: we have an extraneous root node
        last_pre_ancestor: treelib.Node = self._category_tree.get_node(last_pre_ancestor_nid)
        if last_pre_ancestor:
            return cat_nid, last_pre_ancestor

        # else we need to create pre-ancestors...

        if self.extra_node_for_type:
            # Create sub-root (i.e. 'GDrive' or 'Local Disk')
            subroot_node: treelib.Node = self._category_tree.get_node(subroot_nid)
            if not subroot_node:
                node_identifier = NodeIdentifierFactory.for_values(tree_type=item.node_identifier.tree_type,
                                                                   uid=subroot_nid, category=category)
                subroot_node_data = RootTypeNode(node_identifier=node_identifier)
                subroot_node = self._category_tree.create_node(identifier=subroot_nid, parent=self.root, data=subroot_node_data)
            parent_node = subroot_node
        else:
            # no sub-root used
            parent_node = self.root

        tree_type: int = self.node_identifier.tree_type

        cat_node: treelib.Node = self._category_tree.get_node(cat_nid)
        if not cat_node:
            # Create category display node

            node_identifier = NodeIdentifierFactory.for_values(tree_type=tree_type, full_path=self.node_identifier.full_path,
                                                               uid=self.node_identifier.uid, category=category)
            cat_node_data = CategoryNode(node_identifier=node_identifier)
            cat_node = self._category_tree.create_node(identifier=last_pre_ancestor_nid, parent=parent_node, data=cat_node_data)
        parent_node = cat_node

        if self.extra_node_for_type:
            # Create remaining pre-ancestors:
            path_segments: List[str] = file_util.split_path(self.node_identifier.full_path)
            path_so_far = ''
            for path in path_segments[:-1]:
                path_so_far += path
                # being stingy with node identifiers - just using path instead here. we never access this stuff anyway
                dir_uid = f'{cat_nid}/{path_so_far}'
                node_identifier = NodeIdentifierFactory.for_values(tree_type=tree_type, full_path=path_so_far, uid=dir_uid, category=category)
                dir_node_data = DirNode(node_identifier=node_identifier)
                parent_node = self._category_tree.create_node(identifier=dir_node_data.uid, parent=parent_node, data=dir_node_data)
            return cat_nid, parent_node
        else:
            # last pre-ancestor:
            return cat_nid, cat_node

    def add_item(self, item: DisplayNode, category: Category):
        assert category != Category.NA, f'For item: {item}'

        cat_nid, parent = self._get_or_create_pre_ancestors(item, category)

        parent.data.add_meta_metrics(item)

        ancestor_identifiers: Deque[NodeIdentifier] = deque()
        if item.parent_ids:
            ancestor = item
            while ancestor and ancestor.parent_ids:
                # In this tree already? Saves us work, and allow us to use nodes not in the parent tree (e.g. FolderToAdds)
                nid = f'{cat_nid}/{ancestor.parent_ids[0]}'
                node: treelib.Node = self._category_tree.get_node(nid=nid)
                if node:
                    parent = node
                    break
                ancestor = self.source_tree.get_item_for_identifier(ancestor.parent_ids[0])
                if ancestor:
                    if ancestor.uid == self.source_tree.uid:
                        # do not include root node
                        break
                    ancestor_identifiers.appendleft(ancestor.node_identifier)
        else:
            # TODO: get rid of this
            ancestor_identifiers = self.source_tree.get_ancestor_chain(item)

        # Create a node for each ancestor dir (path segment)
        for node_identifier in ancestor_identifiers:
            nid = f'{cat_nid}/{node_identifier.uid}'
            child: treelib.Node = self._category_tree.get_node(nid=nid)
            if child is None:
                # Need to copy the node_identifier so that we can ensure the category is correct for where it is
                # going in our tree. When get_children() is called, it uses the node_identifier's category to
                # determine which tree to look up. Need to brainstorm a more elegant solution!
                new_identifier = NodeIdentifierFactory.for_values(tree_type=node_identifier.tree_type, full_path=node_identifier.full_path,
                                                                  uid=node_identifier.uid, category=category)
                # TODO: subclass this from treelib.Node! Then we don't have to allocate twice
                dir_node = DirNode(node_identifier=new_identifier)
                child = self._category_tree.create_node(identifier=nid, parent=parent, data=dir_node)
            parent = child
            assert isinstance(parent.data, DirNode)
            parent.data.add_meta_metrics(item)

        # logger.debug(f'Creating file node for item {item}')
        categorized_item = copy.copy(item)
        categorized_item.node_identifier.category = category
        try:
            nid = f'{cat_nid}/{item.uid}'
            self._category_tree.create_node(identifier=nid, parent=parent, data=categorized_item)
        except DuplicatedNodeIdError:
            logger.error(f'Duplicate path for node {item}')
            raise

    def get_ancestor_chain(self, item: DisplayNode):
        assert item.category != Category.NA
        identifiers: List[NodeIdentifier] = []

        subroot_nid, cat_nid = self._get_useful_nids(item.node_identifier, item.category)
        current_uid: UID = item.uid

        while True:
            ancestor: treelib.Node = self._category_tree.parent(current_uid)
            if ancestor:
                if ancestor.data.uid == cat_nid:
                    # Do not include root
                    break
                identifiers.append(ancestor.data.node_identifier)
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
        for child in self._category_tree.children(self.root.identifier):
            if isinstance(child.data, CategoryNode):
                cat_node: CategoryNode = child.data
                summary.append(f'{cat_node.name}: {cat_node.get_summary()}')
        return ', '.join(summary)
