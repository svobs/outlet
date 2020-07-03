import copy
import logging
import pathlib
from collections import deque
from typing import Callable, Deque, Dict, Iterable, List, Optional

import treelib
from treelib.exceptions import DuplicatedNodeIdError

import file_util
from model.change_action import ChangeAction, ChangeType
from constants import TREE_TYPE_GDRIVE, TREE_TYPE_LOCAL_DISK, TREE_TYPE_MIXED
from index.uid.uid import UID
from model.node.container_node import CategoryNode, ContainerNode, RootTypeNode
from model.node.display_node import DisplayNode
from model.node_identifier import LogicalNodeIdentifier, NodeIdentifier
from model.node_identifier_factory import NodeIdentifierFactory
from model.display_tree.display_tree import DisplayTree

logger = logging.getLogger(__name__)

CHANGE_TYPES = [ChangeType.CP, ChangeType.RM, ChangeType.UP, ChangeType.MV]

SUPER_DEBUG = False


# CLASS TreeTypeBeforeCategoryDict
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼

class PreAncestorDict:
    def __init__(self):
        self._dict: Dict[str, ContainerNode] = {}

    def get_for(self, source_tree: DisplayTree, change_type: ChangeType) -> Optional[ContainerNode]:
        key = NodeIdentifierFactory.nid(tree_type=source_tree.tree_type, uid=source_tree.uid, change_type=change_type)
        return self._dict.get(key, None)

    def put_for(self, source_tree: DisplayTree, change_type: ChangeType, item: ContainerNode):
        key = NodeIdentifierFactory.nid(tree_type=source_tree.tree_type, uid=source_tree.uid, change_type=change_type)
        self._dict[key] = item


# CLASS CategoryDisplayTree
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼

class CategoryDisplayTree(DisplayTree):
    """Note: this doesn't completely map to DisplayTree, but it's close enough for it to be useful to
    inherit its functionality"""
    def __init__(self, application, root_node_identifier: NodeIdentifier, tree_id: str, show_whole_forest=False):
        # Root node will never be displayed in the UI, but treelib requires a root node, as does parent class
        super().__init__(ContainerNode(root_node_identifier))

        self.tree_id = tree_id
        self.cache_manager = application.cache_manager
        self.uid_generator = application.uid_generator
        self.node_identifier_factory = application.node_identifier_factory

        self._category_tree: treelib.Tree = treelib.Tree()
        self._category_tree.add_node(self.root_node, parent=None)

        self.show_whole_forest: bool = show_whole_forest
        # saved in a nice dict for easy reference:
        self._pre_ancestor_dict: PreAncestorDict = PreAncestorDict()

        self.change_action_dict: Dict[UID, ChangeAction] = {}

        self.count_conflict_warnings = 0
        self.count_conflict_errors = 0

    def get_children_for_root(self) -> Iterable[DisplayNode]:
        return self.get_children(self.root_node)

    def get_children(self, parent: DisplayNode) -> Iterable[DisplayNode]:
        try:
            return self._category_tree.children(parent.identifier)
        except Exception:
            if logger.isEnabledFor(logging.DEBUG):
                logger.debug(f'[{self.tree_id}] CategoryTree for "{self.node_identifier}": ' + self._category_tree.show(stdout=False))
            logger.error(f'[{self.tree_id}] While retrieving children for: {parent.identifier}')
            raise

    def get_ancestors(self, item: DisplayNode, stop_before_func: Callable[[DisplayNode], bool] = None) -> Deque[DisplayNode]:
        ancestors: Deque[DisplayNode] = deque()

        # Walk up the source tree, adding ancestors as we go, until we reach either a node which has already
        # been added to this tree, or the root of the source tree
        ancestor = item
        while ancestor:
            if stop_before_func is not None and stop_before_func(ancestor):
                return ancestors
            ancestor = self.get_parent_for_item(ancestor)
            if ancestor:
                if ancestor.uid == self.uid:
                    # do not include source tree's root node:
                    return ancestors
                ancestors.appendleft(ancestor)

        return ancestors

    def _get_subroot_node(self, node_identifier: NodeIdentifier) -> Optional[ContainerNode]:
        for child in self._category_tree.children(self.root_node.identifier):
            if child.node_identifier.tree_type == node_identifier.tree_type:
                return child
        return None

    def _get_or_create_pre_ancestors(self, item: DisplayNode, change_type: ChangeType, source_tree: DisplayTree) -> ContainerNode:
        """Pre-ancestors are those nodes (either logical or pointing to real data) which are higher up than the source tree.
        Last pre-ancestor is easily derived and its prescence indicates whether its ancestors were already created"""

        tree_type: int = item.node_identifier.tree_type
        assert tree_type != TREE_TYPE_MIXED, f'For {item.node_identifier}'

        last_pre_ancestor = self._pre_ancestor_dict.get_for(source_tree, change_type)
        if last_pre_ancestor:
            return last_pre_ancestor

        # else we need to create pre-ancestors...

        if self.show_whole_forest:
            # Create sub-root (i.e. 'GDrive' or 'Local Disk')
            subroot_node = self._get_subroot_node(item.node_identifier)
            if not subroot_node:
                uid = self.uid_generator.next_uid()
                node_identifier = self.node_identifier_factory.for_values(tree_type=item.node_identifier.tree_type, uid=uid)
                subroot_node = RootTypeNode(node_identifier=node_identifier)
                logger.debug(f'[{self.tree_id}] Creating pre-ancestor RootType node: {node_identifier}')
                self._category_tree.add_node(node=subroot_node, parent=self.root_node)
            parent_node = subroot_node
        else:
            # no sub-root used
            parent_node = self.root_node

        cat_node = None
        for child in self._category_tree.children(parent_node.identifier):
            if child.change_type == change_type:
                cat_node = child
                break

        if not cat_node:
            # Create category display node. This may be the "last pre-ancestor".
            # Note that we can use this for GDrive paths because we are combining it with tree_type and ChangeType (below) into a new identifier:
            uid = self.cache_manager.get_uid_for_path(self.root_path)
            nid = NodeIdentifierFactory.nid(uid, item.node_identifier.tree_type, change_type)

            node_identifier = self.node_identifier_factory.for_values(tree_type=tree_type, full_path=self.root_path, uid=uid)
            cat_node = CategoryNode(node_identifier=node_identifier, change_type=change_type)
            cat_node.identifier = nid
            logger.debug(f'Creating pre-ancestor CAT node: {node_identifier}')
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

                child_node = None
                for child in self._category_tree.children(parent_node.identifier):
                    if child.full_path == path_so_far:
                        child_node = child
                        break

                if not child_node:
                    uid = self.cache_manager.get_uid_for_path(path_so_far)
                    nid = NodeIdentifierFactory.nid(uid, item.node_identifier.tree_type, change_type)
                    node_identifier = self.node_identifier_factory.for_values(tree_type=tree_type, full_path=path_so_far, uid=uid)
                    child_node = ContainerNode(node_identifier=node_identifier)
                    child_node.identifier = nid
                    logger.debug(f'[{self.tree_id}] Creating pre-ancestor DIR node: {node_identifier}')
                    self._category_tree.add_node(node=child_node, parent=parent_node)
                parent_node = child_node

        # this is the last pre-ancestor. Cache it:
        self._pre_ancestor_dict.put_for(source_tree, change_type, parent_node)
        return parent_node

    def get_relative_path_for_full_path(self, full_path: str):
        assert full_path.startswith(self.root_path), f'Full path ({full_path}) does not contain root ({self.root_path})'
        return file_util.strip_root(full_path, self.root_path)

    def get_change_actions(self) -> Iterable[ChangeAction]:
        return self.change_action_dict.values()

    def get_change_action_for_node(self, node: DisplayNode) -> Optional[ChangeAction]:
        return self.change_action_dict.get(node.uid, None)

    def _append_change_action(self, change_action: ChangeAction):
        if change_action.dst_node:
            assert not self.change_action_dict.get(change_action.dst_node.uid, None), f'Duplicate ChangeAction: {change_action}'
            self.change_action_dict[change_action.dst_node.uid] = change_action
        else:
            assert not self.change_action_dict.get(change_action.src_node.uid, None), f'Duplicate ChangeAction: {change_action}'
            self.change_action_dict[change_action.src_node.uid] = change_action

    def add_item(self, item: DisplayNode, change_action: ChangeAction, source_tree: DisplayTree):
        """When we add the item, we add any necessary ancestors for it as well.
        1. Create and add "pre-ancestors": fake nodes which need to be displayed at the top of the tree but aren't
        backed by any actual data nodes. This includes possibly tree-type nodes, category nodes, and ancestors
        which aren't in the source tree.
        2. Create and add "ancestors": dir nodes from the source tree for display, and possibly any FolderToAdd nodes.
        The "ancestors" are duplicated for each ChangeType, so we need to generate a separate unique identifier which includes the ChangeType.
        For this, we take advantage of the fact that each node has a separate "identifier" field which is nominally identical to its UID,
        but in this case it will be a string which includes the ChangeType name.
        3. Add a node for the item itself
        """
        assert change_action is not None, f'For item: {item}'
        self._append_change_action(change_action)

        change_type_for_display = change_action.change_type
        if change_type_for_display == ChangeType.MKDIR:
            # Group "mkdir" with "copy" for display purposes:
            change_type_for_display = ChangeType.CP

        # Clone the item so as not to mutate the source tree. The item category is needed to determine which bra
        item_clone = copy.copy(item)
        item_clone.node_identifier = copy.copy(item.node_identifier)
        item_clone.identifier = NodeIdentifierFactory.nid(item.uid, item.node_identifier.tree_type, change_type_for_display)
        item = item_clone

        parent: DisplayNode = self._get_or_create_pre_ancestors(item, change_type_for_display, source_tree)

        if isinstance(parent, ContainerNode):
            parent.add_meta_metrics(item)

        stack: Deque = deque()
        full_path = item.full_path
        # Walk up the source tree and compose a list of ancestors:
        while True:
            assert full_path.startswith(self.root_path), f'ItemPath="{full_path}", TreeRootPath="{self.root_path}"'
            # Go up one dir:
            full_path: str = str(pathlib.Path(full_path).parent)
            # Get standard UID for path (note: this is a kludge for non-local trees, but should be OK because we just need a UID which
            # is unique for this tree)
            uid = self.cache_manager.get_uid_for_path(full_path)
            nid = NodeIdentifierFactory.nid(uid, item.node_identifier.tree_type, change_type_for_display)
            parent = self._category_tree.get_node(nid=nid)
            if parent:
                break
            else:
                node_identifier = LogicalNodeIdentifier(uid, full_path, item.node_identifier.tree_type)
                dir_node = ContainerNode(node_identifier)
                dir_node.identifier = nid
                stack.append(dir_node)

        # Walk down the ancestor list and create a node for each ancestor dir:
        assert parent
        while len(stack) > 0:
            ancestor = stack.pop()
            if SUPER_DEBUG:
                logger.info(f'[{self.tree_id}] Adding dir node: {ancestor.node_identifier} ({ancestor.identifier}) '
                            f'to parent: {parent.node_identifier} ({parent.identifier})')
            self._category_tree.add_node(node=ancestor, parent=parent)
            parent = ancestor

        try:
            # Finally add the item itself:

            if SUPER_DEBUG:
                logger.info(f'[{self.tree_id}] Adding node: {item.node_identifier} ({item.identifier}) '
                            f'to parent: {parent.node_identifier} ({parent.identifier})')
            self._category_tree.add_node(node=item, parent=parent)
        except DuplicatedNodeIdError:
            # TODO: configurable handling of conflicts. Google Drive allows items with the same path and name, which is not allowed on local FS
            conflict_node = self._category_tree.get_node(item.identifier)
            if conflict_node.md5 == item.md5:
                self.count_conflict_warnings += 1
                if SUPER_DEBUG:
                    logger.warning(f'[{self.tree_id}] Duplicate nodes for the same path! However, items have same MD5, so we will just ignore the new'
                                   f' item: existing={conflict_node} new={item}')
            else:
                self.count_conflict_errors += 1
                if SUPER_DEBUG:
                    logger.error(f'[{self.tree_id}] Duplicate nodes for the same path and different content: existing={conflict_node} new={item}')
                # raise

        if SUPER_DEBUG:
            logger.debug(f'[{self.tree_id}] CategoryTree for "{self.node_identifier}": ' + self._category_tree.show(stdout=False))

    def get_parent_for_item(self, item: DisplayNode) -> Optional[DisplayNode]:
        if not self._category_tree.get_node(item.identifier):
            return None

        ancestor: ContainerNode = self._category_tree.parent(item.identifier)
        if ancestor:
            # Do not include CategoryNode and above
            if not isinstance(ancestor, CategoryNode):
                return ancestor
        return None

    def get_full_path_for_item(self, item: DisplayNode) -> str:
        """Gets the absolute path for the item"""
        assert item.full_path
        return item.full_path

    def __repr__(self):
        return f'CategoryDisplayTree(tree_id=[{self.tree_id}], {self.get_summary()})'

    def get_relative_path_for_item(self, item):
        raise NotImplementedError

    def get_for_path(self, path: str, include_ignored=False) -> List[DisplayNode]:
        raise NotImplementedError

    def get_md5_dict(self):
        raise NotImplementedError

    def get_summary(self) -> str:
        def make_cat_map():
            cm = {}
            for c in CHANGE_TYPES:
                cm[c] = f'{CategoryNode.display_names[c]}: 0'
            return cm

        cat_count = 0
        if self.show_whole_forest:
            # need to preserve ordering...
            type_summaries = []
            type_map = {}
            for child in self._category_tree.children(self.root_node.identifier):
                assert isinstance(child, RootTypeNode), f'For {child}'
                cat_map = make_cat_map()
                for grandchild in self._category_tree.children(child.identifier):
                    assert isinstance(grandchild, CategoryNode), f'For {grandchild}'
                    cat_count += 1
                    cat_map[grandchild.change_type] = f'{grandchild.name}: {grandchild.get_summary()}'
                type_map[child.node_identifier.tree_type] = cat_map
            if cat_count == 0:
                return 'Contents are identical'
            for tree_type, tree_type_name in (TREE_TYPE_LOCAL_DISK, 'Local Disk'), (TREE_TYPE_GDRIVE, 'Google Drive'):
                cat_map = type_map.get(tree_type, None)
                if cat_map:
                    cat_summaries = []
                    for cat in CHANGE_TYPES:
                        cat_summaries.append(cat_map[cat])
                    type_summaries.append(f'{tree_type_name}: {",".join(cat_summaries)}')
            return '; '.join(type_summaries)
        else:
            cat_map = make_cat_map()
            for child in self._category_tree.children(self.root_node.identifier):
                assert isinstance(child, CategoryNode), f'For {child}'
                cat_count += 1
                cat_map[child.change_type] = f'{child.name}: {child.get_summary()}'
            if cat_count == 0:
                return 'Contents are identical'
            cat_summaries = []
            for cat in CHANGE_TYPES:
                cat_summaries.append(cat_map[cat])
            return ', '.join(cat_summaries)
