import copy
import logging
import os
import pathlib
from collections import deque
from typing import Callable, Deque, Dict, Iterable, List, Optional

import treelib
from pydispatch import dispatcher
from treelib.exceptions import DuplicatedNodeIdError

from model.node.gdrive_node import GDriveFolder
from model.node.local_disk_node import LocalDirNode
from ui import actions
from util import file_util
from model.op import Op, OpType
from constants import TREE_TYPE_GDRIVE, TREE_TYPE_LOCAL_DISK, TREE_TYPE_MIXED
from index.uid.uid import UID
from model.node.container_node import CategoryNode, ContainerNode, RootTypeNode
from model.node.display_node import DisplayNode, HasChildList
from model.node_identifier import LogicalNodeIdentifier, NodeIdentifier
from model.node_identifier_factory import NodeIdentifierFactory
from model.display_tree.display_tree import DisplayTree
from util.stopwatch_sec import Stopwatch

logger = logging.getLogger(__name__)

CHANGE_TYPES = [OpType.CP, OpType.RM, OpType.UP, OpType.MV]

SUPER_DEBUG = False


# CLASS TreeTypeBeforeCategoryDict
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼

class PreAncestorDict:
    def __init__(self):
        self._dict: Dict[str, ContainerNode] = {}

    def get_for(self, source_tree: DisplayTree, op_type: OpType) -> Optional[ContainerNode]:
        key = NodeIdentifierFactory.nid(tree_type=source_tree.tree_type, uid=source_tree.uid, op_type=op_type)
        return self._dict.get(key, None)

    def put_for(self, source_tree: DisplayTree, op_type: OpType, node: ContainerNode):
        key = NodeIdentifierFactory.nid(tree_type=source_tree.tree_type, uid=source_tree.uid, op_type=op_type)
        self._dict[key] = node


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

        self.op_dict: Dict[UID, Op] = {}
        """Lookup for target node UID -> Op. The target node will be the dst node if the Op has one; otherwise
        it will be the source node."""
        self._op_list: List[Op] = []
        """We want to keep track of change action creation order."""

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

    def get_ancestors(self, node: DisplayNode, stop_before_func: Callable[[DisplayNode], bool] = None) -> Deque[DisplayNode]:
        ancestors: Deque[DisplayNode] = deque()

        # Walk up the source tree, adding ancestors as we go, until we reach either a node which has already
        # been added to this tree, or the root of the source tree
        ancestor = node
        while ancestor:
            if stop_before_func is not None and stop_before_func(ancestor):
                return ancestors
            ancestor = self.get_parent_for_node(ancestor)
            if ancestor:
                if ancestor.uid == self.uid:
                    # do not include source tree's root node:
                    return ancestors
                ancestors.appendleft(ancestor)

        return ancestors

    def _get_subroot_node(self, node_identifier: NodeIdentifier) -> Optional[DisplayNode]:
        for child in self._category_tree.children(self.root_node.identifier):
            if child.node_identifier.tree_type == node_identifier.tree_type:
                return child
        return None

    def _get_or_create_pre_ancestors(self, node: DisplayNode, op_type: OpType, source_tree: DisplayTree) -> ContainerNode:
        """Pre-ancestors are those nodes (either logical or pointing to real data) which are higher up than the source tree.
        Last pre-ancestor is easily derived and its prescence indicates whether its ancestors were already created"""

        tree_type: int = node.node_identifier.tree_type
        assert tree_type != TREE_TYPE_MIXED, f'For {node.node_identifier}'

        last_pre_ancestor = self._pre_ancestor_dict.get_for(source_tree, op_type)
        if last_pre_ancestor:
            return last_pre_ancestor

        # else we need to create pre-ancestors...

        if self.show_whole_forest:
            # Create sub-root (i.e. 'GDrive' or 'Local Disk')
            subroot_node = self._get_subroot_node(node.node_identifier)
            if not subroot_node:
                uid = self.uid_generator.next_uid()
                node_identifier = self.node_identifier_factory.for_values(tree_type=node.node_identifier.tree_type, full_path=self.root_path, uid=uid)
                subroot_node = RootTypeNode(node_identifier=node_identifier)
                logger.debug(f'[{self.tree_id}] Creating pre-ancestor RootType node: {node_identifier}')
                self._category_tree.add_node(node=subroot_node, parent=self.root_node)
            parent_node = subroot_node
        else:
            # no sub-root used
            parent_node = self.root_node

        cat_node = None
        for child in self._category_tree.children(parent_node.identifier):
            if child.op_type == op_type:
                cat_node = child
                break

        if not cat_node:
            # Create category display node. This may be the "last pre-ancestor".
            # Note that we can use this for GDrive paths because we are combining it with tree_type and OpType (below) into a new identifier:
            uid = self.cache_manager.get_uid_for_path(self.root_path)
            nid = NodeIdentifierFactory.nid(uid, node.node_identifier.tree_type, op_type)

            node_identifier = self.node_identifier_factory.for_values(tree_type=tree_type, full_path=self.root_path, uid=uid)
            cat_node = CategoryNode(node_identifier=node_identifier, op_type=op_type)
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
                    nid = NodeIdentifierFactory.nid(uid, node.node_identifier.tree_type, op_type)
                    node_identifier = self.node_identifier_factory.for_values(tree_type=tree_type, full_path=path_so_far, uid=uid)
                    child_node = ContainerNode(node_identifier=node_identifier)
                    child_node.identifier = nid
                    logger.debug(f'[{self.tree_id}] Creating pre-ancestor DIR node: {node_identifier}')
                    self._category_tree.add_node(node=child_node, parent=parent_node)
                parent_node = child_node

        # this is the last pre-ancestor. Cache it:
        self._pre_ancestor_dict.put_for(source_tree, op_type, parent_node)
        return parent_node

    def get_relative_path_for_full_path(self, full_path: str):
        assert full_path.startswith(self.root_path), f'Full path ({full_path}) does not contain root ({self.root_path})'
        return file_util.strip_root(full_path, self.root_path)

    def get_ops(self) -> Iterable[Op]:
        return self._op_list

    def get_op_for_node(self, node: DisplayNode) -> Optional[Op]:
        return self.op_dict.get(node.uid, None)

    def _append_op(self, op: Op):
        logger.debug(f'Appending op: {op}')
        if op.dst_node:
            if self.op_dict.get(op.dst_node.uid, None):
                raise RuntimeError(f'Duplicate Op: 1st={op}; 2nd={self.op_dict.get(op.dst_node.uid)}')
            self.op_dict[op.dst_node.uid] = op
        else:
            if self.op_dict.get(op.src_node.uid, None):
                raise RuntimeError(f'Duplicate Op: 1st={op}; 2nd={self.op_dict.get(op.src_node.uid)}')
            self.op_dict[op.src_node.uid] = op
        self._op_list.append(op)

    def add_node(self, node: DisplayNode, op: Op, source_tree: DisplayTree):
        """When we add the node, we add any necessary ancestors for it as well.
        1. Create and add "pre-ancestors": fake nodes which need to be displayed at the top of the tree but aren't
        backed by any actual data nodes. This includes possibly tree-type nodes, category nodes, and ancestors
        which aren't in the source tree.
        2. Create and add "ancestors": dir nodes from the source tree for display, and possibly any FolderToAdd nodes.
        The "ancestors" are duplicated for each OpType, so we need to generate a separate unique identifier which includes the OpType.
        For this, we take advantage of the fact that each node has a separate "identifier" field which is nominally identical to its UID,
        but in this case it will be a string which includes the OpType name.
        3. Add a node for the node itself
        """
        assert op is not None, f'For node: {node}'
        self._append_op(op)

        op_type_for_display = op.op_type
        if op_type_for_display == OpType.MKDIR:
            # Group "mkdir" with "copy" for display purposes:
            op_type_for_display = OpType.CP

        # Clone the node so as not to mutate the source tree. The node category is needed to determine which bra
        node_clone = copy.copy(node)
        node_clone.node_identifier = copy.copy(node.node_identifier)
        node_clone.identifier = NodeIdentifierFactory.nid(node.uid, node.node_identifier.tree_type, op_type_for_display)
        node = node_clone

        parent: DisplayNode = self._get_or_create_pre_ancestors(node, op_type_for_display, source_tree)

        if isinstance(parent, HasChildList):
            parent.add_meta_metrics(node)

        stack: Deque = deque()
        full_path = node.full_path
        # Walk up the source tree and compose a list of ancestors:
        while True:
            assert full_path, f'Item does not have a path: {node}'
            assert full_path.startswith(self.root_path), f'ItemPath="{full_path}", TreeRootPath="{self.root_path}"'
            # Go up one dir:
            full_path: str = str(pathlib.Path(full_path).parent)
            # Get standard UID for path (note: this is a kludge for non-local trees, but should be OK because we just need a UID which
            # is unique for this tree)
            uid = self.cache_manager.get_uid_for_path(full_path)
            nid = NodeIdentifierFactory.nid(uid, node.node_identifier.tree_type, op_type_for_display)
            parent = self._category_tree.get_node(nid=nid)
            if parent:
                break
            else:
                node_identifier = LogicalNodeIdentifier(uid, full_path, node.node_identifier.tree_type)
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
            # Finally add the node itself:

            if SUPER_DEBUG:
                logger.info(f'[{self.tree_id}] Adding node: {node.node_identifier} ({node.identifier}) '
                            f'to parent: {parent.node_identifier} ({parent.identifier})')
            self._category_tree.add_node(node=node, parent=parent)
        except DuplicatedNodeIdError:
            # TODO: configurable handling of conflicts. Google Drive allows nodes with the same path and name, which is not allowed on local FS
            conflict_node = self._category_tree.get_node(node.identifier)
            if conflict_node.md5 == node.md5:
                self.count_conflict_warnings += 1
                if SUPER_DEBUG:
                    logger.warning(f'[{self.tree_id}] Duplicate nodes for the same path! However, nodes have same MD5, so we will just ignore the new'
                                   f' node: existing={conflict_node} new={node}')
            else:
                self.count_conflict_errors += 1
                if SUPER_DEBUG:
                    logger.error(f'[{self.tree_id}] Duplicate nodes for the same path and different content: existing={conflict_node} new={node}')
                # raise

        if SUPER_DEBUG:
            logger.debug(f'[{self.tree_id}] CategoryTree for "{self.node_identifier}": ' + self._category_tree.show(stdout=False))

    def get_parent_for_node(self, node: DisplayNode) -> Optional[DisplayNode]:
        if not self._category_tree.get_node(node.identifier):
            return None

        ancestor: ContainerNode = self._category_tree.parent(node.identifier)
        if ancestor:
            # Do not include CategoryNode and above
            if not isinstance(ancestor, CategoryNode):
                return ancestor
        return None

    def get_full_path_for_node(self, node: DisplayNode) -> str:
        """Gets the absolute path for the node"""
        assert node.full_path
        return node.full_path

    def __repr__(self):
        return f'CategoryDisplayTree(tree_id=[{self.tree_id}], {self.get_summary()})'

    def get_relative_path_for_node(self, node):
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
                    cat_map[grandchild.op_type] = f'{grandchild.name}: {grandchild.get_summary()}'
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
                cat_map[child.op_type] = f'{child.name}: {child.get_summary()}'
            if cat_count == 0:
                return 'Contents are identical'
            cat_summaries = []
            for cat in CHANGE_TYPES:
                cat_summaries.append(cat_map[cat])
            return ', '.join(cat_summaries)

    def refresh_stats(self, tree_id: str):
        logger.debug(f'[{tree_id}] Refreshing stats for display tree')
        stats_sw = Stopwatch()
        queue: Deque[DisplayNode] = deque()
        stack: Deque[DisplayNode] = deque()
        queue.append(self.root_node)
        stack.append(self.root_node)

        # go down tree, zeroing out existing stats and adding children to stack
        while len(queue) > 0:
            node: DisplayNode = queue.popleft()
            assert isinstance(node, HasChildList) and isinstance(node, DisplayNode)
            node.zero_out_stats()

            children = self.get_children(node)
            if children:
                for child in children:
                    if child.is_dir():
                        assert isinstance(child, DisplayNode)
                        queue.append(child)
                        stack.append(child)

        # now go back up the tree by popping the stack and building stats as we go:
        while len(stack) > 0:
            node = stack.pop()
            assert node.is_dir() and isinstance(node, HasChildList) and isinstance(node, DisplayNode)

            children = self.get_children(node)
            if children:
                for child in children:
                    node.add_meta_metrics(child)

        self._stats_loaded = True
        dispatcher.send(signal=actions.REFRESH_SUBTREE_STATS_DONE, sender=tree_id)
        logger.debug(f'[{tree_id}] {stats_sw} Refreshed stats for tree')
