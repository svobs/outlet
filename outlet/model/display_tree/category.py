import logging
import pathlib
from collections import deque
from typing import Deque, Dict, Iterable, List, Optional, Union

import treelib
from pydispatch import dispatcher
from treelib.exceptions import DuplicatedNodeIdError

from constants import NULL_UID, ROOT_PATH, SUPER_DEBUG, TREE_TYPE_GDRIVE, TREE_TYPE_LOCAL_DISK, TREE_TYPE_MIXED
from error import InvalidOperationError
from model.display_tree.display_tree import DisplayTree
from model.node.container_node import CategoryNode, ContainerNode, RootTypeNode
from model.node.node import HasChildList, Node, SPIDNodePair
from model.node_identifier import SinglePathNodeIdentifier
from model.op import Op, OP_TYPES, OpType
from model.uid import UID
from ui import actions
from util import file_util
from util.stopwatch_sec import Stopwatch

logger = logging.getLogger(__name__)


# CLASS CategoryDisplayTree
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼

class CategoryDisplayTree(DisplayTree):
    """Note: this doesn't completely map to DisplayTree, but it's close enough for it to be useful to
    inherit its functionality"""
    def __init__(self, app, tree_id: str, root_node_identifier: SinglePathNodeIdentifier, show_whole_forest=False):
        # Root node will never be displayed in the UI, but treelib requires a root node, as does parent class
        super().__init__(app, tree_id, root_node_identifier)
        self.root_node = ContainerNode(root_node_identifier)

        self.uid_generator = app.uid_generator
        self.node_identifier_factory = app.node_identifier_factory

        logger.debug(f'CategoryDisplayTree: adding root node: {self.root_node.node_identifier}')
        self._category_tree: treelib.Tree = treelib.Tree()
        self._category_tree.add_node(self.root_node, parent=None)

        self.show_whole_forest: bool = show_whole_forest

        self.op_dict: Dict[UID, Op] = {}
        """Lookup for target node UID -> Op. The target node will be the dst node if the Op has one; otherwise
        it will be the source node."""
        self._op_list: List[Op] = []
        """We want to keep track of change action creation order."""

        self.count_conflict_warnings = 0
        self.count_conflict_errors = 0

    # TODO: simplify
    def _build_tree_nid(self, tree_type: int, single_path: str, op: OpType) -> str:
        # note: this is kind of a kludge because we're using the local path UID mapper for GDrive paths...but who cares
        # path_uid: UID = self.app.cacheman.get_uid_for_path(spid.get_single_path())
        return f'{tree_type}-{op.name}-{single_path}'

    def get_root_node(self):
        return self.root_node

    def get_children_for_root(self) -> Iterable[Node]:
        return self.get_children(self.root_node)

    def get_children(self, parent: Node) -> Iterable[Node]:
        try:
            return self._category_tree.children(parent.identifier)
        except Exception:
            if logger.isEnabledFor(logging.DEBUG):
                self.print_tree_contents_debug()
            logger.error(f'[{self.tree_id}] While retrieving children for: {parent.identifier}')
            raise

    def print_tree_contents_debug(self):
        logger.debug(f'[{self.tree_id}] CategoryTree for "{self.root_identifier}": ' + self._category_tree.show(stdout=False))

    def get_ancestor_list(self, spid: SinglePathNodeIdentifier) -> Deque[Node]:
        raise InvalidOperationError('CategoryDisplayTree.get_ancestor_list()')

    def _get_or_create_pre_ancestors(self, sn: SPIDNodePair, op_type: OpType, source_tree: DisplayTree) -> ContainerNode:
        """Pre-ancestors are those nodes (either logical or pointing to real data) which are higher up than the source tree.
        Last pre-ancestor is easily derived and its prescence indicates whether its ancestors were already created"""

        tree_type: int = sn.spid.tree_type
        assert tree_type != TREE_TYPE_MIXED, f'For {sn.spid}'

        last_pre_ancestor_nid: str = self._build_tree_nid(tree_type, self.root_path, op_type)
        last_pre_ancestor = self._category_tree.get_node(last_pre_ancestor_nid)
        if last_pre_ancestor:
            return last_pre_ancestor

        # else we need to create pre-ancestors...

        parent_node = self.root_node
        if self.show_whole_forest:
            # Create tree type root (e.g. 'GDrive' or 'Local Disk')
            nid = str(tree_type)
            treetype_node = self._category_tree.get_node(nid)
            if not treetype_node:
                treetype_node = RootTypeNode(node_identifier=SinglePathNodeIdentifier(UID(tree_type), ROOT_PATH, tree_type))
                treetype_node.identifier = nid
                logger.debug(f'[{self.tree_id}] Creating TreeType node: {treetype_node.node_identifier}')
                self._category_tree.add_node(node=treetype_node, parent=parent_node)
            parent_node = treetype_node

        cat_node = self._category_tree.get_node(last_pre_ancestor_nid)
        if not cat_node:
            # Create category display node. This may be the "last pre-ancestor".
            cat_node = CategoryNode(node_identifier=SinglePathNodeIdentifier(NULL_UID, self.root_path, tree_type), op_type=op_type)
            cat_node.identifier = last_pre_ancestor_nid
            logger.debug(f'Creating Category node: {cat_node.node_identifier}')
            self._category_tree.add_node(node=cat_node, parent=parent_node)
        parent_node = cat_node

        if self.show_whole_forest:
            # Create remaining pre-ancestors:
            path_segments: List[str] = file_util.split_path(self.root_path)
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
                    # uid = self.app.cacheman.get_uid_for_path(path_so_far)
                    nid = self._build_tree_nid(tree_type, path_so_far, op_type)
                    node_identifier = self.node_identifier_factory.for_values(tree_type=tree_type, full_path=path_so_far, uid=NULL_UID)
                    child_node = ContainerNode(node_identifier=node_identifier)
                    child_node.identifier = nid
                    logger.debug(f'[{self.tree_id}] Creating dummy DIR node: {node_identifier}')
                    self._category_tree.add_node(node=child_node, parent=parent_node)
                parent_node = child_node

        # this is the last pre-ancestor.
        return parent_node

    def get_ops(self) -> Iterable[Op]:
        return self._op_list

    def get_op_for_node(self, node: Node) -> Optional[Op]:
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

    def add_node(self, sn: SPIDNodePair, op: Op, source_tree: DisplayTree):
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
        assert op is not None, f'For node: {sn}'
        self._append_op(op)

        op_type_for_display = op.op_type
        if op_type_for_display == OpType.MKDIR:
            # Group "mkdir" with "copy" for display purposes:
            op_type_for_display = OpType.CP

        parent: Node = self._get_or_create_pre_ancestors(sn, op_type_for_display, source_tree)

        if isinstance(parent, HasChildList):
            parent.add_meta_metrics(sn.node)

        stack: Deque = deque()
        full_path = sn.spid.get_single_path()
        tree_type = sn.spid.tree_type
        assert full_path, f'SPID does not have a path: {sn.spid}'
        assert full_path.startswith(self.root_path), f'ItemPath="{full_path}", TreeRootPath="{self.root_path}"'
        # Walk up the source tree and compose a list of ancestors:
        while full_path != self.root_path:
            # Go up one dir:
            full_path: str = str(pathlib.Path(full_path).parent)
            # Get standard UID for path (note: this is a kludge for non-local trees, but should be OK because we just need a UID which
            # is unique for this tree)
            # uid = self.app.cacheman.get_uid_for_path(full_path)
            ancestor_spid = SinglePathNodeIdentifier(NULL_UID, full_path, tree_type)
            nid = self._build_tree_nid(tree_type, full_path, op_type_for_display)
            parent = self._category_tree.get_node(nid=nid)
            if parent:
                break
            else:
                dir_node = ContainerNode(ancestor_spid)
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
            # Finally add the node itself. No need to muck around with NIDs now - should be unique globally

            if SUPER_DEBUG:
                logger.info(f'[{self.tree_id}] Adding node: {sn.node.node_identifier} ({sn.node.identifier}) '
                            f'to parent: {parent.node_identifier} ({parent.identifier})')
            self._category_tree.add_node(node=sn.node, parent=parent)
        except DuplicatedNodeIdError:
            # TODO: configurable handling of conflicts. Google Drive allows nodes with the same path and name, which is not allowed on local FS
            conflict_node = self._category_tree.get_node(sn.node.identifier)
            if conflict_node.md5 == sn.node.md5:
                self.count_conflict_warnings += 1
                if SUPER_DEBUG:
                    logger.warning(f'[{self.tree_id}] Duplicate nodes for the same path! However, nodes have same MD5, so we will just ignore the new'
                                   f' node: existing={conflict_node} new={sn.node}')
            else:
                self.count_conflict_errors += 1
                if SUPER_DEBUG:
                    logger.error(f'[{self.tree_id}] Duplicate nodes for the same path and different content: existing={conflict_node} new={sn.node}')
                # raise

        if SUPER_DEBUG:
            self.print_tree_contents_debug()

    def __repr__(self):
        return f'CategoryDisplayTree(tree_id=[{self.tree_id}], {self.get_summary()})'

    def get_node_list_for_path_list(self, path_list: Union[str, List[str]]) -> List[Node]:
        raise InvalidOperationError('CategoryDisplayTree.get_node_list_for_path_list()')

    def get_summary(self) -> str:
        def make_cat_map():
            cm = {}
            for c in OP_TYPES:
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
                    for cat in OP_TYPES:
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
            for cat in OP_TYPES:
                cat_summaries.append(cat_map[cat])
            return ', '.join(cat_summaries)

    def refresh_stats(self, tree_id: str):
        logger.debug(f'[{tree_id}] Refreshing stats for display tree')
        stats_sw = Stopwatch()
        queue: Deque[Node] = deque()
        stack: Deque[Node] = deque()
        queue.append(self.root_node)
        stack.append(self.root_node)

        # go down tree, zeroing out existing stats and adding children to stack
        while len(queue) > 0:
            node: Node = queue.popleft()
            assert isinstance(node, HasChildList) and isinstance(node, Node)
            node.zero_out_stats()

            children = self.get_children(node)
            if children:
                for child in children:
                    if child.is_dir():
                        assert isinstance(child, Node)
                        queue.append(child)
                        stack.append(child)

        # now go back up the tree by popping the stack and building stats as we go:
        while len(stack) > 0:
            node = stack.pop()
            assert node.is_dir() and isinstance(node, HasChildList) and isinstance(node, Node)

            children = self.get_children(node)
            if children:
                for child in children:
                    node.add_meta_metrics(child)

        self._stats_loaded = True
        dispatcher.send(signal=actions.REFRESH_SUBTREE_STATS_DONE, sender=tree_id)
        logger.debug(f'[{tree_id}] {stats_sw} Refreshed stats for tree')
