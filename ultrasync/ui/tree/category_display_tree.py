import copy
from collections import deque
from typing import Deque, Dict, Iterable, List, Optional, Tuple, Union
import logging

import treelib
from treelib.exceptions import DuplicatedNodeIdError

import file_util
from constants import TREE_TYPE_GDRIVE, TREE_TYPE_LOCAL_DISK, TREE_TYPE_MIXED
from index.two_level_dict import TwoLevelDict
from model.category import Category
from model.node_identifier import NodeIdentifier, NodeIdentifierFactory
from model.display_node import CategoryNode, DirNode, DisplayNode, RootTypeNode
from model.subtree_snapshot import SubtreeSnapshot

logger = logging.getLogger(__name__)

CATEGORIES = [Category.Added, Category.Deleted, Category.Moved, Category.Updated, Category.Ignored]


# CLASS TreeTypeBeforeCategoryDict
# ⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟

class TreeTypeBeforeCategoryDict(TwoLevelDict):
    def __init__(self):
        super().__init__(lambda x: x.node_identifier.tree_type, lambda x: x.category, None)


# CLASS CategoryDisplayTree
# ⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟


class CategoryDisplayTree:
    def __init__(self, application, root_node_identifier: NodeIdentifier, show_whole_forest=False):
        self._category_tree: treelib.Tree = treelib.Tree()
        self.uid_generator = application.uid_generator
        self.node_identifier_factory = application.node_identifier_factory
        # Root node will never be displayed in the UI:
        self.root = self._category_tree.create_node(identifier=self.uid_generator.get_new_uid(), parent=None, data=None)
        self.show_whole_forest: bool = show_whole_forest
        # saved in a nice dict for easy reference:
        self._pre_ancestor_dict: TreeTypeBeforeCategoryDict = TreeTypeBeforeCategoryDict()

        self.node_identifier = root_node_identifier

    @property
    def tree_type(self) -> int:
        return self.node_identifier.tree_type

    @property
    def root_path(self):
        return self.node_identifier.full_path

    @property
    def uid(self):
        return self.node_identifier.uid

    def get_children_for_root(self) -> Iterable[DisplayNode]:
        return self._category_tree.children(self.root.identifier)

    def get_children(self, parent_identifier: NodeIdentifier) -> Iterable[DisplayNode]:
        if isinstance(parent_identifier, NodeIdentifier):
            parent_identifier = parent_identifier.uid

        try:
            return self._category_tree.children(parent_identifier)
        except Exception:
            if logger.isEnabledFor(logging.DEBUG):
                logger.debug(f'CategoryTree for "{self.node_identifier}": ' + self._category_tree.show(stdout=False))
            logger.error(f'While retrieving children for: {parent_identifier}')
            raise

    def _get_subroot_node(self, node_identifier: NodeIdentifier) -> Optional[DirNode]:
        for child in self._category_tree.children(self.root.identifier):
            if child.node_identifier.tree_type == node_identifier.tree_type:
                return child
        return None

    def _get_or_create_pre_ancestors(self, item: DisplayNode, source_tree: SubtreeSnapshot) -> DirNode:
        """Pre-ancestors are those nodes (either logical or pointing to real data) which are higher up than the source tree.
        Last pre-ancestor is easily derived and its prescence indicates whether its ancestors were already created"""

        tree_type: int = item.node_identifier.tree_type
        assert tree_type != TREE_TYPE_MIXED, f'For {item.node_identifier}'
        assert item.category != Category.NA, f'For {item.node_identifier}'

        last_pre_ancestor = self._pre_ancestor_dict.get_single(tree_type, item.category)
        if last_pre_ancestor:
            return last_pre_ancestor

        # else we need to create pre-ancestors...

        if self.show_whole_forest:
            # Create sub-root (i.e. 'GDrive' or 'Local Disk')
            subroot_node = self._get_subroot_node(item.node_identifier)
            if not subroot_node:
                uid = self.uid_generator.get_new_uid()
                node_identifier = self.node_identifier_factory.for_values(tree_type=item.node_identifier.tree_type, uid=uid, category=item.category)
                subroot_node = RootTypeNode(node_identifier=node_identifier)
                logger.debug(f'Creating pre-ancestor RootType node: uid={uid}')
                self._category_tree.add_node(node=subroot_node, parent=self.root)
            parent_node = subroot_node
        else:
            # no sub-root used
            parent_node = self.root

        cat_node = None
        for child in self._category_tree.children(parent_node.identifier):
            if child.category == item.category:
                cat_node = child
                break

        if not cat_node:
            # Create category display node. This may be the "last pre-ancestor"
            uid = self.uid_generator.get_new_uid()

            node_identifier = self.node_identifier_factory.for_values(tree_type=tree_type, full_path=self.node_identifier.full_path,
                                                               uid=uid, category=item.category)
            cat_node = CategoryNode(node_identifier=node_identifier)
            logger.debug(f'Creating pre-ancestor CAT node: uid={uid}')
            self._category_tree.add_node(node=cat_node, parent=parent_node)
        parent_node = cat_node

        if self.show_whole_forest:
            # Create remaining pre-ancestors:
            full_path = source_tree.node_identifier.full_path
            path_segments: List[str] = file_util.split_path(full_path)
            path_so_far = ''
            # Skip first (already covered by CategoryNode):
            for path in path_segments[1:]:
                path_so_far += '/' + path

                uid = self.uid_generator.get_new_uid()
                node_identifier = self.node_identifier_factory.for_values(tree_type=tree_type, full_path=path_so_far, uid=uid, category=item.category)
                dir_node = DirNode(node_identifier=node_identifier)
                logger.debug(f'Creating pre-ancestor DIR node: {uid}')
                self._category_tree.add_node(node=dir_node, parent=parent_node)
                parent_node = dir_node

        # this is the last pre-ancestor:
        self._pre_ancestor_dict.put(parent_node)
        return parent_node

    def add_item(self, item: DisplayNode, category: Category, source_tree: SubtreeSnapshot):
        """When we add the item, we add any necessary ancestors for it as well.
        1. Create and add "pre-ancestors": fake nodes which need to be displayed at the top of the tree but aren't
        backed by any actual data nodes. This includes possibly tree-type nodes, category nodes, and ancestors
        which aren't in the source tree.
        2. Create and add "ancestors": dir nodes from the source tree for display, and possibly any FolderToAdd nodes
        3. Add a node for the item iteself
        """
        assert category != Category.NA, f'For item: {item}'
        # Clone the item so as not to mutate the source tree
        item = copy.copy(item)
        item.node_identifier.category = category

        parent: DisplayNode = self._get_or_create_pre_ancestors(item, source_tree)

        if isinstance(parent, DirNode):
            parent.add_meta_metrics(item)

        def stop_before(_ancestor) -> bool:
            if _ancestor.parent_uids:
                assert item.node_identifier.tree_type == TREE_TYPE_GDRIVE
                # In this tree already? Saves us work, and more importantly,
                # allow us to use nodes not in the parent tree (e.g. FolderToAdds)
                for parent_uid in _ancestor.parent_uids:
                    node: DirNode = self._category_tree.get_node(nid=parent_uid)
                    if node:
                        stop_before.parent = node
                        # Found: existing node in this tree. This should happen on the first iteration or not at all
                        return True

        stop_before.parent = None

        # Walk up the source tree and compose a list of ancestors:
        ancestors: Iterable[DisplayNode] = source_tree.get_ancestors(item, stop_before)
        if stop_before.parent:
            parent = stop_before.parent

        # Walk down the ancestor list and create a node for each ancestor dir:
        for ancestor in ancestors:
            existing_node: DisplayNode = self._category_tree.get_node(nid=ancestor.uid)
            if existing_node is None:
                # TODO: review whether we still need to set category so rabidly, or whether we can avoid cloning
                new_ancestor = copy.copy(ancestor)
                new_ancestor.node_identifier.category = category
                self._category_tree.add_node(node=new_ancestor, parent=parent)
                existing_node = new_ancestor
            parent = existing_node
            # This will most often be a DirNode, but may occasionally be a FolderToAdd.
            if isinstance(parent, DirNode):
                parent.add_meta_metrics(item)

        try:
            # Finally add the item itself:
            self._category_tree.add_node(node=item, parent=parent)
        except DuplicatedNodeIdError:
            logger.error(f'Duplicate path for node {item}')
            raise

    def get_parent_for_item(self, item: DisplayNode) -> Optional[DisplayNode]:
        if not self._category_tree.get_node(item.uid):
            return None

        ancestor: DirNode = self._category_tree.parent(item.uid)
        if ancestor:
            # Do not include CategoryNode and above
            if not isinstance(ancestor, CategoryNode):
                return ancestor.data
        return None

    def get_full_path_for_item(self, item: DisplayNode) -> str:
        """Gets the absolute path for the item"""
        assert item.full_path
        return item.full_path

    def __repr__(self):
        return f'CategoryDisplayTree({self.get_summary()})'

    def get_summary(self) -> str:
        def make_cat_map():
            cm = {}
            for c in Category.Added, Category.Updated, Category.Moved, Category.Deleted, Category.Ignored:
                cm[c] = f'{CategoryNode.display_names[c]}: 0'
            return cm

        cat_count = 0
        if self.show_whole_forest:
            # need to preserve ordering...
            type_summaries = []
            type_map = {}
            for child in self._category_tree.children(self.root.identifier):
                assert isinstance(child, RootTypeNode), f'For {child}'
                cat_map = make_cat_map()
                for grandchild in self._category_tree.children(child.identifier):
                    assert isinstance(grandchild, CategoryNode), f'For {grandchild}'
                    cat_count += 1
                    cat_map[grandchild.category] = f'{grandchild.name}: {grandchild.get_summary()}'
                type_map[child.node_identifier.tree_type] = cat_map
            if cat_count == 0:
                return 'Contents are identical'
            for tree_type, tree_type_name in (TREE_TYPE_LOCAL_DISK, 'Local Disk'), (TREE_TYPE_GDRIVE, 'Google Drive'):
                cat_map = type_map.get(tree_type, None)
                if cat_map:
                    cat_summaries = []
                    for cat in Category.Added, Category.Updated, Category.Moved, Category.Deleted, Category.Ignored:
                        cat_summaries.append(cat_map[cat])
                    type_summaries.append(f'{tree_type_name}: {",".join(cat_summaries)}')
            return ';'.join(type_summaries)
        else:
            cat_map = make_cat_map()
            for child in self._category_tree.children(self.root.identifier):
                assert isinstance(child, CategoryNode), f'For {child}'
                cat_count += 1
                cat_map[child.category] = f'{child.name}: {child.get_summary()}'
            if cat_count == 0:
                return 'Contents are identical'
            cat_summaries = []
            for cat in Category.Added, Category.Updated, Category.Moved, Category.Deleted, Category.Ignored:
                cat_summaries.append(cat_map[cat])
            return ','.join(cat_summaries)

