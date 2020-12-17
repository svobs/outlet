import logging
import pathlib
from collections import deque
from typing import Deque, Dict, Iterable, List, Optional

from pydispatch import dispatcher

from constants import NULL_UID, ROOT_PATH, SUPER_DEBUG, SUPER_ROOT_UID, TREE_TYPE_GDRIVE, TREE_TYPE_LOCAL_DISK, TREE_TYPE_MIXED
from error import InvalidOperationError
from model.display_tree.display_tree import DisplayTree
from model.node.container_node import CategoryNode, ContainerNode, RootTypeNode
from model.node.decorator_node import DecoDirNode, DecoNode
from model.node.node import Node, SPIDNodePair
from model.node.trait import HasChildStats
from model.node_identifier import SinglePathNodeIdentifier
from model.uid import UID
from model.user_op import get_uid_for_op_and_tree_type, USER_OP_TYPES, UserOp, UserOpType
from ui.signal import Signal
from model.display_tree.filter_criteria import FilterCriteria
from util.simple_tree import NodeAlreadyPresentError, SimpleTree
from util.stopwatch_sec import Stopwatch

logger = logging.getLogger(__name__)


class ChangeDisplayTree(DisplayTree):
    """
    ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
    CLASS ChangeDisplayTree
    ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼
    """

    def __init__(self, backend, state, show_whole_forest=False):
        # Root node will never be displayed in the UI, but tree requires a root node, as does parent class
        super().__init__(backend, state)

        self._category_tree: SimpleTree = SimpleTree()

        # Root node is not even displayed, so is not terribly important.
        # Do not use its original UID, so as to disallow it from interfering with lookups
        root_node = ContainerNode(self.get_root_identifier())
        logger.debug(f'[{self.tree_id}] ChangeDisplayTree: inserting root node: {root_node}')
        self._category_tree.add_node(root_node, parent=None)

        self.show_whole_forest: bool = show_whole_forest

        self._op_dict: Dict[UID, UserOp] = {}
        """Lookup for target node UID -> UserOp. The target node will be the dst node if the UserOp has one; otherwise
        it will be the source node."""
        self._op_list: List[UserOp] = []
        """We want to keep track of change action creation order."""

        self.count_conflict_warnings = 0
        """For debugging only"""
        self.count_conflict_errors = 0
        """For debugging only"""

    def get_root_node(self) -> Node:
        return self._category_tree.get_root_node()

    def get_children_for_root(self, filter_criteria: FilterCriteria = None) -> Iterable[Node]:
        return self.get_children(self.get_root_node(), filter_criteria)

    def get_children(self, parent: Node, filter_criteria: FilterCriteria = None) -> Iterable[Node]:
        try:
            if filter_criteria:
                return filter_criteria.get_filtered_child_list(parent, self)
            else:
                child_list = self._category_tree.get_child_list(parent.identifier)
                return child_list
        except Exception:
            if logger.isEnabledFor(logging.DEBUG):
                self.print_tree_contents_debug()
            logger.error(f'[{self.tree_id}] While retrieving children for: {parent.identifier}')
            raise

    def print_tree_contents_debug(self):
        logger.debug(f'[{self.tree_id}] CategoryTree for "{self.get_root_sn().spid}": \n' + self._category_tree.show(show_identifier=True))

    def print_op_structs_debug(self):
        logger.debug(f'[{self.tree_id}] OpList size = {len(self._op_list)}:')
        for op_num, op in enumerate(self._op_list):
            logger.debug(f'[{self.tree_id}]     {op_num}: {op}')
        logger.debug(f'[{self.tree_id}] OpDict size = {len(self._op_dict)}:')
        for uid, op in self._op_dict.items():
            logger.debug(f'[{self.tree_id}]     {uid} -> {op}')

    def get_ancestor_list(self, spid: SinglePathNodeIdentifier) -> Deque[Node]:
        raise InvalidOperationError('ChangeDisplayTree.get_ancestor_list()')

    def get_ops(self) -> Iterable[UserOp]:
        return self._op_list

    def get_op_for_node(self, node: Node) -> Optional[UserOp]:
        return self._op_dict.get(node.uid, None)

    def _append_op(self, op: UserOp):
        logger.debug(f'[{self.tree_id}] Appending op: {op}')
        if op.dst_node:
            if self._op_dict.get(op.dst_node.uid, None):
                raise RuntimeError(f'Duplicate UserOp: 1st={op}; 2nd={self._op_dict.get(op.dst_node.uid)}')
            self._op_dict[op.dst_node.uid] = op
        else:
            if self._op_dict.get(op.src_node.uid, None):
                raise RuntimeError(f'Duplicate UserOp: 1st={op}; 2nd={self._op_dict.get(op.src_node.uid)}')
            self._op_dict[op.src_node.uid] = op
        self._op_list.append(op)

    def _build_tree_nid(self, tree_type: int, single_path: Optional[str], op: Optional[UserOpType]) -> UID:
        return self.backend.cacheman.get_uid_for_change_tree_node(tree_type, single_path, op)

    def _get_or_create_pre_ancestors(self, sn: SPIDNodePair, op_type: UserOpType) -> ContainerNode:
        """Pre-ancestors are those nodes (either logical or pointing to real data) which are higher up than the source tree.
        Last pre-ancestor is easily derived and its prescence indicates whether its ancestors were already created"""

        tree_type: int = sn.spid.tree_type
        assert tree_type != TREE_TYPE_MIXED, f'For {sn.spid}'

        cat_node_nid: UID = get_uid_for_op_and_tree_type(op_type, tree_type)
        cat_node = self._category_tree.get_node(cat_node_nid)
        if cat_node:
            logger.debug(f'[{self.tree_id}] Found existing CategoryNode with OpType={op_type.name} nid="{cat_node_nid}"')
            assert isinstance(cat_node, ContainerNode)
            return cat_node

        # else we need to create pre-ancestors...

        parent_node = self.get_root_node()

        if self.show_whole_forest:
            # Create tree type root (e.g. 'GDrive' or 'Local Disk')
            nid = self._build_tree_nid(tree_type, None, None)
            treetype_node = self._category_tree.get_node(nid)
            if not treetype_node:
                # see UID to root_UID of relevant tree
                treetype_node = RootTypeNode(node_identifier=SinglePathNodeIdentifier(UID(tree_type), ROOT_PATH, tree_type))
                logger.debug(f'[{self.tree_id}] Inserting new RootTypeNode: {treetype_node.node_identifier}')
                self._category_tree.add_node(node=treetype_node, parent=parent_node)
            parent_node = treetype_node

        assert not cat_node
        # Create category display node. This may be the "last pre-ancestor". (Use root node UID so its context menu points to root)
        cat_node = CategoryNode(node_identifier=SinglePathNodeIdentifier(cat_node_nid, self.root_path, tree_type),
                                op_type=op_type)
        logger.debug(f'[{self.tree_id}] Inserting new CategoryNode with OpType={op_type.name}: {cat_node.node_identifier}')
        cat_node.set_parent_uids(parent_node.identifier)
        self._category_tree.add_node(node=cat_node, parent=parent_node)
        parent_node = cat_node

        # this is the last pre-ancestor.
        return parent_node

    def _get_or_create_ancestors(self, sn: SPIDNodePair, op_type: UserOpType, parent: Node):
        stack: Deque = deque()
        full_path = sn.spid.get_single_path()
        tree_type = sn.spid.tree_type
        assert full_path, f'SPID does not have a path: {sn.spid}'
        assert full_path.startswith(self.root_path), f'ItemPath="{full_path}", TreeRootPath="{self.root_path}"'

        # Walk up the source tree and compose a list of ancestors:
        logger.debug(f'[{self.tree_id}] Looking for ancestors for path "{full_path}"')
        while True:
            # Go up one dir:
            full_path: str = str(pathlib.Path(full_path).parent)
            if full_path == self.root_path:
                break
            nid = self._build_tree_nid(tree_type, full_path, op_type)
            ancestor = self._category_tree.get_node(nid=nid)
            if ancestor:
                break
            else:
                ancestor_spid = SinglePathNodeIdentifier(nid, full_path, tree_type)
                ancestor = ContainerNode(ancestor_spid)
                stack.append(ancestor)

        # Walk down the ancestor list and create a node for each ancestor dir:
        while len(stack) > 0:
            child = stack.pop()
            if SUPER_DEBUG:
                logger.info(f'[{self.tree_id}] Inserting new dummy ancestor: node: {child} under parent: {parent}')
            self._category_tree.add_node(node=child, parent=parent)
            parent = child

        return parent

    def add_node(self, sn: SPIDNodePair, op: UserOp):
        """When we add the node, we add any necessary ancestors for it as well.
        1. Create and add "pre-ancestors": fake nodes which need to be displayed at the top of the tree but aren't
        backed by any actual data nodes. This includes possibly tree-type nodes and category nodes.
        2. Create and add "ancestors": dir nodes from the source tree for display.
        The "ancestors" are duplicated for each UserOpType, so we need to generate a separate unique identifier which includes the UserOpType.
        For this, we take advantage of the fact that each node has a separate "identifier" field which is nominally identical to its UID,
        but in this case it will be a string which includes the UserOpType name.
        3. Add a node for the node itself
        """
        assert isinstance(sn, SPIDNodePair), f'Wrong type: {type(sn)}'
        assert op is not None, f'For node: {sn}'
        self._append_op(op)

        op_type_for_display = op.op_type
        if op_type_for_display == UserOpType.MKDIR:
            # Group "mkdir" with "copy" for display purposes:
            op_type_for_display = UserOpType.CP

        # We can easily derive the UID/NID of the node's parent. Check to see if it exists in the tree - if so, we can save a lot of work.
        parent_nid = self._build_tree_nid(sn.spid.tree_type, sn.spid.get_single_parent_path(), op_type_for_display)
        parent: Node = self._category_tree.get_node(nid=parent_nid)
        if parent:
            logger.debug(f'[{self.tree_id}] Parent was already added to tree ({parent.node_identifier}')
        else:
            parent: Node = self._get_or_create_pre_ancestors(sn, op_type_for_display)

            parent: Node = self._get_or_create_ancestors(sn, op_type_for_display, parent)

        try:
            # Finally add the node itself.
            if SUPER_DEBUG:
                logger.info(f'[{self.tree_id}] Adding node: {sn.node.node_identifier} to parent {parent.node_identifier} (nid={parent.identifier})')

            nid = self._build_tree_nid(sn.spid.tree_type, sn.spid.get_single_path(), op_type_for_display)
            if sn.node.is_dir():
                deco_node = DecoDirNode(nid, parent_uid=parent.identifier, delegate_node=sn.node)
            else:
                deco_node = DecoNode(nid, parent_uid=parent.identifier, delegate_node=sn.node)

            self._category_tree.add_node(node=deco_node, parent=parent)
        except NodeAlreadyPresentError:
            # TODO: configurable handling of conflicts. Google Drive allows nodes with the same path and name, which is not allowed on local FS
            conflict_node: Node = self._category_tree.get_node(deco_node.identifier)
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
        return f'ChangeDisplayTree(tree_id=[{self.tree_id}], {self.get_summary()})'

    @staticmethod
    def _make_cat_map():
        cm = {}
        for c in USER_OP_TYPES:
            cm[c] = f'{CategoryNode.display_names[c]}: 0'
        return cm

    @staticmethod
    def _build_cat_summaries_str(cat_map) -> str:
        cat_summaries = []
        for op_type in USER_OP_TYPES:
            summary = cat_map.get(op_type, None)
            if summary:
                cat_summaries.append(summary)
        return ', '.join(cat_summaries)

    def _build_cat_map(self, identifier):
        include_empty_op_types = False
        cat_count = 0
        if include_empty_op_types:
            cat_map = ChangeDisplayTree._make_cat_map()
        else:
            cat_map = {}
        for child in self._category_tree.children(identifier):
            # assert isinstance(child, CategoryNode), f'For {child}'
            cat_count += 1
            cat_map[child.op_type] = f'{child.name}: {child.get_summary()}'
        if cat_count:
            return cat_map
        else:
            return None

    # FIXME: find new home for this
    def get_summary(self) -> str:
        # FIXME: this is broken
        return ''
        if self.show_whole_forest:
            # need to preserve ordering...
            type_summaries = []
            type_map = {}
            cat_count = 0
            for child in self._category_tree.children(self.get_root_node().identifier):
                assert isinstance(child, RootTypeNode), f'For {child}'
                cat_map = self._build_cat_map(child.identifier)
                if cat_map:
                    cat_count += 1
                    type_map[child.node_identifier.tree_type] = cat_map
            if cat_count == 0:
                return 'Contents are identical'
            for tree_type, tree_type_name in (TREE_TYPE_LOCAL_DISK, 'Local Disk'), (TREE_TYPE_GDRIVE, 'Google Drive'):
                cat_map = type_map.get(tree_type, None)
                if cat_map:
                    type_summaries.append(f'{tree_type_name}: {self._build_cat_summaries_str(cat_map)}')
            return '; '.join(type_summaries)
        else:
            cat_map = self._build_cat_map(self.get_root_node().identifier)
            if not cat_map:
                return 'Contents are identical'
            return self._build_cat_summaries_str(cat_map)

    # FIXME: find new home for this
    def refresh_stats(self, tree_id: str):
        logger.debug(f'[{tree_id}] Refreshing stats for category display tree')
        stats_sw = Stopwatch()
        queue: Deque[Node] = deque()
        stack: Deque[Node] = deque()
        queue.append(self.get_root_node())
        stack.append(self.get_root_node())

        # go down tree, zeroing out existing stats and adding children to stack
        while len(queue) > 0:
            node: Node = queue.popleft()
            assert isinstance(node, HasChildStats) and isinstance(node, Node)
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
            assert node.is_dir() and isinstance(node, HasChildStats) and isinstance(node, Node)

            children = self.get_children(node)
            if children:
                for child in children:
                    node.add_meta_metrics(child)

        dispatcher.send(signal=Signal.REFRESH_SUBTREE_STATS_DONE, sender=tree_id)
        logger.debug(f'[{tree_id}] {stats_sw} Refreshed stats for tree')
