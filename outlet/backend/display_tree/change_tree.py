import logging
import pathlib
from collections import deque
from typing import Deque, Dict, Iterable, List, Optional

from constants import ROOT_PATH, ROOT_PATH_UID, SUPER_DEBUG
from error import InvalidOperationError
from model.display_tree.display_tree import DisplayTree
from model.node.container_node import CategoryNode, ContainerNode, RootTypeNode
from model.node.directory_stats import DirectoryStats
from model.node.node import SPIDNodePair, Node, SPIDNodePair
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

        self._category_tree: SimpleTree = SimpleTree[GUID, SPIDNodePair](extract_identifier_func=self._extract_identifier_func,
                                                                         extract_node_func=self._extract_node_func)

        # Root node is not even displayed, so is not terribly important.
        # Do not use its original UID, so as to disallow it from interfering with lookups
        # self.root_cn = self._make_change_node_pair(self.state.root_sn, op_type=None)
        logger.debug(f'[{self.tree_id}] ChangeTree: inserting root node: {self.state.root_sn}')
        self._category_tree.add_node(self.state.root_sn, parent=None)

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
    def _extract_node_func(sn: SPIDNodePair) -> Node:
        return sn.node

    @staticmethod
    def _extract_identifier_func(sn: SPIDNodePair) -> GUID:
        # assert isinstance(sn.spid, ChangeTreeSPID), f'Not a ChangeTreeSPID: {sn.spid}'
        return sn.spid.guid

    def get_root_node(self) -> SPIDNodePair:
        return self._category_tree.get_root_node()

    def get_child_list_for_root(self) -> Iterable[SPIDNodePair]:
        return self.get_child_list_for_spid(self.get_root_node().spid)

    def get_child_list_for_spid(self, parent_spid: SinglePathNodeIdentifier) -> Iterable[SPIDNodePair]:
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

    def get_ancestor_list(self, spid: ChangeTreeSPID) -> Deque[Node]:
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

        assert isinstance(sn.spid, ChangeTreeSPID), f'Not a ChangeTreeSPID: {sn.spid}'
        op_type: UserOpType = sn.spid.op_type

        cat_spid = ChangeTreeSPID(self.get_root_sn().spid.path_uid, sn.spid.device_uid, self.root_path, op_type=sn.spid.op_type)
        cat_sn = self._category_tree.get_node_for_identifier(cat_spid.guid)
        if cat_sn:
            logger.debug(f'[{self.tree_id}] Found existing CategoryNode with OpType={op_type.name} guid="{cat_spid.guid}"')
            assert isinstance(cat_sn.node, ContainerNode)
            return cat_sn

        # else we need to create pre-ancestors...

        parent_sn: SPIDNodePair = self.get_root_node()

        if self.show_whole_forest:
            # Create device node (e.g. 'GDrive' or 'Local Disk')
            device_sn = self._category_tree.get_node_for_identifier(parent_sn.spid.guid)
            if not device_sn:
                # set UID to root_UID of relevant tree
                device_spid = ChangeTreeSPID(ROOT_PATH_UID, parent_sn.spid.device_uid, ROOT_PATH, op_type)
                device_node = RootTypeNode(node_identifier=device_spid)
                device_sn: SPIDNodePair = (device_spid, device_node)
                logger.debug(f'[{self.tree_id}] Inserting new RootTypeNode: {device_sn.spid.guid} (under parent: {parent_sn.spid.guid}')
                self._category_tree.add_node(node=device_sn, parent=parent_sn)
            parent_sn = device_sn

        assert not cat_sn
        cat_node = CategoryNode(node_identifier=cat_spid, op_type=op_type)
        # Create category display node. This may be the "last pre-ancestor". (Use root node UID so its context menu points to root)
        cat_sn: SPIDNodePair = SPIDNodePair(cat_spid, cat_node)
        logger.debug(f'[{self.tree_id}] Inserting new CategoryNode: {cat_spid.guid}')
        self._category_tree.add_node(node=cat_sn, parent=parent_sn)

        # this is the last pre-ancestor.
        return cat_sn

    def _get_or_create_ancestors(self, sn: SPIDNodePair, parent_sn: SPIDNodePair):
        stack: Deque[SPIDNodePair] = deque()
        full_path = sn.spid.get_single_path()
        assert full_path, f'SPID does not have a path: {sn.spid}'
        assert full_path.startswith(self.root_path), f'ItemPath="{full_path}", TreeRootPath="{self.root_path}"'

        # Walk up the source tree and compose a list of ancestors:
        logger.debug(f'[{self.tree_id}] Looking for ancestors for path "{full_path}"')
        ancestor_spid: ChangeTreeSPID = sn.spid
        while True:
            # Go up one dir:
            full_path: str = str(pathlib.Path(ancestor_spid.get_single_path()).parent)
            if full_path == self.root_path:
                break
            # Need some work to assemble the GUID to look up the ancestor:
            ancestor_path_uid = self.backend.cacheman.get_uid_for_local_path(full_path)
            ancestor_spid: ChangeTreeSPID = ChangeTreeSPID(ancestor_path_uid, ancestor_spid.device_uid, full_path, ancestor_spid.op_type)
            ancestor_sn = self._category_tree.get_node_for_identifier(ancestor_spid.guid)
            if ancestor_sn:
                parent_sn = ancestor_sn
                break
            else:
                # create ancestor & push to stack for later insertion in correct order
                ancestor = ContainerNode(ancestor_spid)
                stack.append(SPIDNodePair(ancestor_spid, ancestor))

        # Walk down the ancestor list and create a node for each ancestor dir:
        while len(stack) > 0:
            child_sn = stack.pop()
            if SUPER_DEBUG:
                logger.info(f'[{self.tree_id}] Inserting new dummy ancestor: node: {child_sn} under parent: {parent_sn}')
            self._category_tree.add_node(node=child_sn, parent=parent_sn)
            parent_sn = child_sn

        return parent_sn

    @staticmethod
    def _make_change_node_pair(from_sn: SPIDNodePair, op_type: Optional[UserOpType]) -> SPIDNodePair:
        change_spid = ChangeTreeSPID(path_uid=from_sn.spid.path_uid, device_uid=from_sn.spid.device_uid,
                                     full_path=from_sn.spid.get_single_path(), op_type=op_type)
        return SPIDNodePair(change_spid, from_sn.node)

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

        sn = self._make_change_node_pair(sn, op.op_type)
        if sn.spid.op_type == UserOpType.MKDIR:
            # For display purposes, group "mkdir" with "copy":
            sn.spid.op_type = UserOpType.CP

        self._append_op(op)

        # We can easily derive the UID/NID of the node's parent. Check to see if it exists in the tree - if so, we can save a lot of work.
        parent_sn: SPIDNodePair = self._category_tree.get_node_for_identifier(identifier=sn.spid.guid)
        if parent_sn:
            logger.debug(f'[{self.tree_id}] Parent was already added to tree ({parent_sn.spid}')
        else:
            parent_sn = self._get_or_create_pre_ancestors(sn)

            parent_sn = self._get_or_create_ancestors(sn, parent_sn)

        try:
            # Finally add the node itself.
            if SUPER_DEBUG:
                logger.info(f'[{self.tree_id}] Adding change node: {sn.spid} to parent {parent_sn.spid}')

            self._category_tree.add_node(node=sn, parent=parent_sn)
        except NodeAlreadyPresentError:
            # TODO: does this error still occur?
            # TODO: configurable handling of conflicts. Google Drive allows nodes with the same path and name, which is not allowed on local FS
            conflict_sn: SPIDNodePair = self._category_tree.get_node_for_identifier(sn.spid.guid)
            if conflict_sn.node.md5 == sn.node.md5:
                self.count_conflict_warnings += 1
                if SUPER_DEBUG:
                    logger.warning(f'[{self.tree_id}] Duplicate nodes for the same path! However, nodes have same MD5, so we will just ignore the new'
                                   f' node: existing={conflict_sn.node} new={sn.node}')
            else:
                self.count_conflict_errors += 1
                if SUPER_DEBUG:
                    logger.error(f'[{self.tree_id}] Duplicate nodes for the same path & different content: existing={conflict_sn.node} new={sn.node}')
                # raise

        if SUPER_DEBUG:
            self.print_tree_contents_debug()

    def __repr__(self):
        return f'ChangeTree(tree_id=[{self.tree_id}], {len(self._category_tree)})'

    def generate_dir_stats(self) -> Dict[GUID, DirectoryStats]:
        return self._category_tree.generate_dir_stats(self.tree_id)
