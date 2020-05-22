import copy
from collections import deque
from typing import Deque, Dict, Iterable, List, Optional, Tuple, Union
import logging

import treelib
from treelib.exceptions import DuplicatedNodeIdError

import file_util
from constants import OBJ_TYPE_GDRIVE, OBJ_TYPE_LOCAL_DISK, OBJ_TYPE_MIXED
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
    def __init__(self, source_tree: SubtreeSnapshot, uid_generator,
                 root: Union[DisplayNode, NodeIdentifier] = None, show_whole_forest=False):
        self._category_tree: treelib.Tree = treelib.Tree()
        self.uid_generator = uid_generator
        self.root = self._category_tree.create_node(identifier='//', parent=None, data=None)
        self.show_whole_forest: bool = show_whole_forest
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
                cat_nid_prefix = self._get_cat_nid_prefix(parent_identifier, parent_identifier.category)
                nid = f'{cat_nid_prefix}/{parent_identifier.uid}'

            return self._category_tree.children(nid)
        except Exception:
            logger.debug(f'CategoryTree for "{self.node_identifier}": ' + self._category_tree.show(stdout=False))
            logger.debug(f'While retrieving children for: {parent_identifier}')
            raise

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
        if self.show_whole_forest:
            if node_identifier.tree_type == OBJ_TYPE_GDRIVE:
                return f'{self.root.identifier}GD'
            elif node_identifier.tree_type == OBJ_TYPE_LOCAL_DISK:
                return f'{self.root.identifier}LO'
            elif node_identifier.tree_type == OBJ_TYPE_MIXED:
                return f'{self.root.identifier}MI'
            else:
                raise RuntimeError(f'bad: {node_identifier.tree_type}, for {node_identifier}')
        return None

    def _get_cat_nid_prefix(self, node_identifier: NodeIdentifier, category: Category):
        """We need a unique identifier for each node in the tree, but we also need a reliable way to look up a given
        data node. This creates a problem because our display tree splits a real directory node into several "display"
        directories (one for each category, multiplied by the number of tree types).
        We solve this problem by prefixing each node in each RootType+Category subtree by a simple string which includes
        the tree type and category information (returned by this method)"""
        if self.show_whole_forest:
            subroot_nid = self._get_subtroot_nid(node_identifier)
            cat_nid_prefix = f'{subroot_nid}/{category.name}'
        else:
            cat_nid_prefix = f'{self.root.identifier}{category.name}'
        return cat_nid_prefix

    def _get_or_create_pre_ancestors(self, item: DisplayNode, category: Category) -> Tuple[str, treelib.Node]:
        """Last pre-ancestor is easily derived and its prescence indicates whether its ancestors were already created"""
        cat_nid_prefix = self._get_cat_nid_prefix(item.node_identifier, category)

        last_pre_ancestor_uid = self.source_tree.node_identifier.uid
        last_pre_ancestor_nid = f'{cat_nid_prefix}/{last_pre_ancestor_uid}'

        tree_type: int = item.node_identifier.tree_type
        assert tree_type != OBJ_TYPE_MIXED, f'For {item.node_identifier}'

        last_pre_ancestor: treelib.Node = self._category_tree.get_node(last_pre_ancestor_nid)
        if last_pre_ancestor:
            return cat_nid_prefix, last_pre_ancestor

        # else we need to create pre-ancestors...

        if self.show_whole_forest:
            # Create sub-root (i.e. 'GDrive' or 'Local Disk')
            subroot_nid = self._get_subtroot_nid(item.node_identifier)
            subroot_node: treelib.Node = self._category_tree.get_node(subroot_nid)
            if not subroot_node:
                node_identifier = NodeIdentifierFactory.for_values(tree_type=item.node_identifier.tree_type,
                                                                   uid=subroot_nid, category=category)
                subroot_node_data = RootTypeNode(node_identifier=node_identifier)
                logger.debug(f'Creating pre-ancestor RootType node: {subroot_nid}')
                subroot_node = self._category_tree.create_node(identifier=subroot_nid, parent=self.root, data=subroot_node_data)
            parent_node = subroot_node
        else:
            # no sub-root used
            parent_node = self.root

        cat_node: treelib.Node = self._category_tree.get_node(cat_nid_prefix)
        if not cat_node:
            # Create category display node. This may be the "last pre-ancestor"

            if self.show_whole_forest:
                uid = self.uid_generator.get_new_uid()
                nid = f'{cat_nid_prefix}/{uid}'
            else:
                uid = last_pre_ancestor_uid
                nid = last_pre_ancestor_nid
            node_identifier = NodeIdentifierFactory.for_values(tree_type=tree_type, full_path=self.node_identifier.full_path,
                                                               uid=uid, category=category)
            cat_node_data = CategoryNode(node_identifier=node_identifier)
            logger.debug(f'Creating pre-ancestor CAT node: {nid}')
            cat_node = self._category_tree.create_node(identifier=nid, parent=parent_node, data=cat_node_data)
        parent_node = cat_node

        if self.show_whole_forest:
            # Create remaining pre-ancestors:
            full_path = self.source_tree.node_identifier.full_path
            path_segments: List[str] = file_util.split_path(full_path)
            path_so_far = ''
            # Skip first (already covered by CategoryNode) and last (covered by the last_pre_ancestor_nid)
            for path in path_segments[1:-1]:
                path_so_far += '/' + path

                uid = self.uid_generator.get_new_uid()
                nid = f'{cat_nid_prefix}/{uid}'
                node_identifier = NodeIdentifierFactory.for_values(tree_type=tree_type, full_path=path_so_far, uid=uid, category=category)
                dir_node_data = DirNode(node_identifier=node_identifier)
                logger.debug(f'Creating pre-ancestor DIR node: {nid}')
                parent_node = self._category_tree.create_node(identifier=nid, parent=parent_node, data=dir_node_data)

            # Last air-bender (er, pre-ancestor)
            node_identifier = NodeIdentifierFactory.for_values(tree_type=tree_type, full_path=full_path,
                                                               uid=last_pre_ancestor_uid, category=category)
            dir_node_data = DirNode(node_identifier=node_identifier)
            logger.debug(f'Creating last pre-ancestor node: {last_pre_ancestor_nid}')
            last_pre_ancestor = self._category_tree.create_node(identifier=last_pre_ancestor_nid, parent=parent_node, data=dir_node_data)

            return cat_nid_prefix, last_pre_ancestor
        else:
            # cat node is last pre-ancestor:
            return cat_nid_prefix, cat_node

    def _get_ancestor_identifiers(self, item: DisplayNode, cat_nid) -> Tuple[Optional[treelib.Node], Deque[NodeIdentifier]]:
        ancestor_identifiers: Deque[NodeIdentifier] = deque()

        # Walk up the source tree, adding ancestors as we go, until we reach either a node which has already
        # been added to this tree, or the root of the source tree
        ancestor = item
        while ancestor:
            if ancestor.parent_ids:
                assert item.node_identifier.tree_type == OBJ_TYPE_GDRIVE
                # In this tree already? Saves us work, and more importantly,
                # allow us to use nodes not in the parent tree (e.g. FolderToAdds)
                for parent_uid in ancestor.parent_ids:
                    nid = f'{cat_nid}/{parent_uid}'
                    node: treelib.Node = self._category_tree.get_node(nid=nid)
                    if node:
                        parent = node
                        # Found: existing node in this tree. This should happen on the first iteration or not at all
                        return parent, ancestor_identifiers
            ancestor = self.source_tree.get_parent_for_item(ancestor)
            if ancestor:
                if ancestor.uid == self.source_tree.uid:
                    # do not include source tree's root node; that is already covered by the CategoryNode
                    # (in pre-ancestors)
                    return None, ancestor_identifiers
                ancestor_identifiers.appendleft(ancestor.node_identifier)

        return None, ancestor_identifiers

    def add_item(self, item: DisplayNode, category: Category):
        """When we add the item, we add any necessary ancestors for it as well.
        1. Create and add "pre-ancestors": fake nodes which need to be displayed at the top of the tree but aren't
        backed by any actual data nodes. This includes possibly tree-type nodes, category nodes, and ancestors
        which aren't in the source tree.
        2. Create and add "ancestors": dir nodes from the source tree for display, and possibly any FolderToAdd nodes
        3. Add a node for the item iteself
        """
        assert category != Category.NA, f'For item: {item}'

        cat_nid_prefix, parent = self._get_or_create_pre_ancestors(item, category)

        parent.data.add_meta_metrics(item)

        # Walk up the source tree and compose a list of ancestors:
        new_parent, ancestor_identifiers = self._get_ancestor_identifiers(item, cat_nid_prefix)
        if new_parent:
            parent = new_parent

        # Walk down the ancestor list and create a node for each ancestor dir:
        for node_identifier in ancestor_identifiers:
            nid = f'{cat_nid_prefix}/{node_identifier.uid}'
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
            nid = f'{cat_nid_prefix}/{item.uid}'
            self._category_tree.create_node(identifier=nid, parent=parent, data=categorized_item)
        except DuplicatedNodeIdError:
            logger.error(f'Duplicate path for node {item}')
            raise

    def get_parent_for_item(self, item: DisplayNode) -> Optional[DisplayNode]:
        cat_nid_prefix = self._get_cat_nid_prefix(item.node_identifier, item.category)
        nid = f'{cat_nid_prefix}/{item.uid}'

        if not self._category_tree.get_node(nid):
            return None

        ancestor: treelib.Node = self._category_tree.parent(nid)
        if ancestor:
            # Do not include root
            if ancestor.data.uid != cat_nid_prefix:
                return ancestor.data
        return None

    def get_full_path_for_item(self, item: DisplayNode) -> str:
        """Gets the absolute path for the item"""
        assert item.full_path
        return item.full_path

    def __repr__(self):
        return f'CategoryDisplayTree({self.get_summary()})'

    def get_summary(self) -> str:
        total_summary = []
        for child in self._category_tree.children(self.root.identifier):
            if self.show_whole_forest:
                summary = []
                for grandchild in self._category_tree.children(child.identifier):
                    assert isinstance(grandchild.data, CategoryNode), f'For {grandchild.data}'
                    cat_node: CategoryNode = grandchild.data
                    summary.append(f'{cat_node.name}: {cat_node.get_summary()}')
                assert isinstance(child.data, RootTypeNode), f'For {child.data}'
                total_summary.append(f'{child.data.name}: {", ".join(summary)}')
            else:
                assert isinstance(child.data, CategoryNode), f'For {child.data}'
                cat_node: CategoryNode = child.data
                total_summary.append(f'{cat_node.name}: {cat_node.get_summary()}')

        if self.show_whole_forest:
            return '; '.join(total_summary)
        else:
            return ', '.join(total_summary)
