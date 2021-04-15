import copy
import logging
import pathlib
from collections import deque
from typing import Deque, Dict, Iterable, List, Optional

from constants import NULL_UID, ROOT_PATH, SUPER_DEBUG, TreeType
from error import InvalidOperationError
from model.display_tree.display_tree import DisplayTree
from model.node.container_node import CategoryNode, ContainerNode, RootTypeNode
from model.node.decorator_node import DecoNode
from model.node.directory_stats import DirectoryStats
from model.node.node import Node, SPIDNodePair
from model.node_identifier import ChangeTreeSPID, GUID, SinglePathNodeIdentifier
from model.uid import UID
from model.user_op import UserOp, UserOpType
from util.simple_tree import NodeAlreadyPresentError, SimpleTree

logger = logging.getLogger(__name__)


class ChangeTree(DisplayTree):
    """
    ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
    CLASS ChangeTree

    NOTE: although this class inherits from DisplayTree, it is used exclusively by the backend.
    ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼
    """

    def __init__(self, backend, state, show_whole_forest=False):
        # Root node will never be displayed in the UI, but tree requires a root node, as does parent class
        super().__init__(backend, state)

        self._category_tree: SimpleTree = SimpleTree[GUID, SPIDNodePair](self._extract_identifier_func)

        # Root node is not even displayed, so is not terribly important.
        # Do not use its original UID, so as to disallow it from interfering with lookups
        root_node = ContainerNode(self.get_root_spid())
        logger.debug(f'[{self.tree_id}] ChangeTree: inserting root node: {root_node}')
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

    @staticmethod
    def _extract_identifier_func(self, sn: SPIDNodePair) -> GUID:
        assert isinstance(sn.spid, ChangeTreeSPID), f'Not a ChangeTreeSPID: {sn.spid}'
        return sn.spid.guid

    def get_root_node(self) -> SPIDNodePair:
        return self._category_tree.get_root_node()

    def get_child_list_for_root(self) -> Iterable[SPIDNodePair]:
        return self.get_child_list(self.get_root_node().spid)

    def get_child_list(self, parent_spid: SinglePathNodeIdentifier) -> Iterable[SPIDNodePair]:
        try:
            return self._category_tree.get_child_list_for_identifier(parent_spid.guid)
        except Exception:
            if logger.isEnabledFor(logging.DEBUG):
                self.print_tree_contents_debug()
            logger.error(f'[{self.tree_id}] While retrieving children for: {parent_spid}')
            raise

    def print_tree_contents_debug(self):
        logger.debug(f'[{self.tree_id}] ChangeTree for "{self.get_root_sn().spid}": \n' + self._category_tree.show(show_identifier=True))

    def print_op_structs_debug(self):
        logger.debug(f'[{self.tree_id}] OpList size = {len(self._op_list)}:')
        for op_num, op in enumerate(self._op_list):
            logger.debug(f'[{self.tree_id}]     {op_num}: {op}')
        logger.debug(f'[{self.tree_id}] OpDict size = {len(self._op_dict)}:')
        for uid, op in self._op_dict.items():
            logger.debug(f'[{self.tree_id}]     {uid} -> {op}')

    def get_ancestor_list(self, spid: SinglePathNodeIdentifier) -> Deque[Node]:
        raise InvalidOperationError('ChangeTree.get_ancestor_list()')

    def get_ops(self) -> List[UserOp]:
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

    def _get_or_create_pre_ancestors(self, sn: SPIDNodePair) -> SPIDNodePair:
        """Pre-ancestors are those nodes (either logical or pointing to real data) which are higher up than the source tree.
        Last pre-ancestor is easily derived and its prescence indicates whether its ancestors were already created"""

        tree_type: TreeType = sn.spid.tree_type
        assert isinstance(sn.spid, ChangeTreeSPID), f'Not a ChangeTreeSPID: {sn.spid}'
        op_type: UserOpType = sn.spid.op_type
        assert tree_type != TreeType.MIXED, f'For {sn.spid}'

        cat_spid = ChangeTreeSPID(NULL_UID, sn.spid.device_uid, self.root_path, tree_type, op_type)
        cat_sn = self._category_tree.get_node_for_uid(cat_spid.guid)
        if cat_sn:
            logger.debug(f'[{self.tree_id}] Found existing CategoryNode with OpType={op_type.name} guid="{cat_spid.guid}"')
            assert isinstance(cat_sn.node, ContainerNode)
            return cat_sn

        # else we need to create pre-ancestors...

        parent_sn: SPIDNodePair = self.get_root_node()

        if self.show_whole_forest:
            # Create device node (e.g. 'GDrive' or 'Local Disk')
            device_sn = self._category_tree.get_node_for_uid(parent_sn.spid.guid)
            if not device_sn:
                # set UID to root_UID of relevant tree
                node_uid = UID(tree_type)
                device_node = RootTypeNode(node_identifier=ChangeTreeSPID(node_uid, parent_sn.spid.device_uid, ROOT_PATH, tree_type, op_type))
                device_sn: SPIDNodePair = (device_node.node_identifier, device_node)
                logger.debug(f'[{self.tree_id}] Inserting new RootTypeNode: {device_sn.spid}')
                self._category_tree.add_node(node=device_sn, parent=parent_sn)
            parent_sn = device_sn

        assert not cat_sn
        cat_node = CategoryNode(node_identifier=cat_spid, op_type=op_type)
        # Create category display node. This may be the "last pre-ancestor". (Use root node UID so its context menu points to root)
        cat_sn: SPIDNodePair = (cat_node.node_identifier, cat_node)
        logger.debug(f'[{self.tree_id}] Inserting new CategoryNode with OpType={op_type.name}: {cat_node.node_identifier}')
        self._category_tree.add_node(node=cat_sn, parent=parent_sn)
        parent_sn = cat_sn

        # this is the last pre-ancestor.
        return parent_sn

    def _get_or_create_ancestors(self, sn: SPIDNodePair, parent_sn: SPIDNodePair):
        stack: Deque = deque()
        full_path = sn.spid.get_single_path()
        assert full_path, f'SPID does not have a path: {sn.spid}'
        assert full_path.startswith(self.root_path), f'ItemPath="{full_path}", TreeRootPath="{self.root_path}"'

        # Walk up the source tree and compose a list of ancestors:
        logger.debug(f'[{self.tree_id}] Looking for ancestors for path "{full_path}"')
        ancestor_spid: ChangeTreeSPID = copy.deepcopy(sn.spid)
        while True:
            # Go up one dir:
            full_path: str = str(pathlib.Path(ancestor_spid.get_single_path()).parent)
            if full_path == self.root_path:
                break
            ancestor_spid.set_path_list(full_path)
            ancestor = self._category_tree.get_node_for_uid(ancestor_spid.guid)
            if ancestor:
                break
            else:
                ancestor = ContainerNode(ancestor_spid)
                stack.append(ancestor)

        # Walk down the ancestor list and create a node for each ancestor dir:
        while len(stack) > 0:
            child = stack.pop()
            if SUPER_DEBUG:
                logger.info(f'[{self.tree_id}] Inserting new dummy ancestor: node: {child} under parent: {parent_sn}')
            self._category_tree.add_node(node=child, parent=parent_sn)
            parent_sn = child

        return parent_sn

    def add_node(self, sn: SPIDNodePair, op: UserOp):
        """When we add the node, we add any necessary ancestors for it as well.
        1. Create and add "pre-ancestors": fake nodes which need to be displayed at the top of the tree but aren't backed by any actual data nodes.
        This includes possibly tree-type nodes and category nodes.
        2. Create and add "ancestors": dir nodes from the source tree for display.
        The "ancestors" may be duplicated for each UserOpType, so we need to generate a separate unique identifier which includes the UserOpType.
        For this, we generate a UID from a combination of tree_type, op_type, and node's path for its tree_type, and decorate the node in an object
        which has this new global identifier ("nid")
        3. Add a node for the node itself.
        """
        assert isinstance(sn, SPIDNodePair), f'Wrong type: {type(sn)}'
        assert isinstance(sn.spid, ChangeTreeSPID), f'Not a ChangeTreeSPID: {sn.spid}'
        assert sn.spid.op_type == op.op_type, f'OpType in SPID ({sn.spid}) does not match op ({op})'
        self._append_op(op)

        # FIXME: op_type_for_display
        op_type_for_display = op.op_type
        if op_type_for_display == UserOpType.MKDIR:
            # Group "mkdir" with "copy" for display purposes:
            op_type_for_display = UserOpType.CP

        # We can easily derive the UID/NID of the node's parent. Check to see if it exists in the tree - if so, we can save a lot of work.
        parent_sn: SPIDNodePair = self._category_tree.get_node_for_uid(uid=sn.spid.guid)
        if parent_sn:
            logger.debug(f'[{self.tree_id}] Parent was already added to tree ({parent_sn.spid}')
        else:
            parent_sn = self._get_or_create_pre_ancestors(sn, op_type_for_display)

            parent_sn = self._get_or_create_ancestors(sn, op_type_for_display, parent_sn)

        try:
            # Finally add the node itself.
            if SUPER_DEBUG:
                logger.info(f'[{self.tree_id}] Adding node: {sn.node.node_identifier} to parent {parent_sn.spid}')

            # FIXME
            nid = self._build_tree_nid(sn.spid.device_uid, sn.spid.get_single_path(), op_type_for_display)
            deco_node = DecoNode(nid, parent_uid=parent.uid, delegate_node=sn.node)

            self._category_tree.add_node(node=deco_node, parent=parent)
        except NodeAlreadyPresentError:
            # TODO: configurable handling of conflicts. Google Drive allows nodes with the same path and name, which is not allowed on local FS
            conflict_node: Node = self._category_tree.get_node_for_uid(deco_node.uid)
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
        return f'ChangeTree(tree_id=[{self.tree_id}], {len(self._category_tree)})'

    def generate_dir_stats(self) -> Dict[UID, DirectoryStats]:
        return self._category_tree.generate_dir_stats(self.tree_id)
