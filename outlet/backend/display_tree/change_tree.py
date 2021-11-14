import copy
import logging
import pathlib
from collections import deque
from typing import Deque, Dict, Iterable, List, Optional

from constants import DIFF_DEBUG_ENABLED, ROOT_PATH, SUPER_DEBUG_ENABLED, TRACE_ENABLED
from error import InvalidOperationError
from model.display_tree.display_tree import DisplayTree
from model.node.container_node import CategoryNode, ContainerNode, RootTypeNode
from model.node.directory_stats import DirectoryStats
from model.node.node import Node, SPIDNodePair
from model.node_identifier import ChangeTreeSPID, GUID, SinglePathNodeIdentifier
from model.user_op import OpTypeMeta, UserOp, UserOpType
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

        self._op_dict: Dict[GUID, UserOp] = {}
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
        assert sn.node, f'SPIDNodePair is missing node: {sn}'
        return sn.node

    @staticmethod
    def _extract_identifier_func(sn: SPIDNodePair) -> GUID:
        # assert isinstance(sn.spid, ChangeTreeSPID), f'Not a ChangeTreeSPID: {sn.spid}'
        return sn.spid.guid

    def get_sn_for_guid(self, guid: GUID) -> SPIDNodePair:
        return self._category_tree.get_node_for_identifier(guid)

    def get_root_node(self) -> SPIDNodePair:
        return self._category_tree.get_root_node()

    def get_child_list_for_root(self) -> Iterable[SPIDNodePair]:
        return self.get_child_list_for_spid(self.get_root_node().spid)

    def get_child_list_for_spid(self, parent_spid: SinglePathNodeIdentifier, is_expanding_parent: bool = False) -> Iterable[SPIDNodePair]:
        try:
            # note: don't need to use is_expanding_parent (this is not a FE tree!)
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

    def get_op_list(self) -> List[UserOp]:
        return self._op_list

    def get_op_for_guid(self, guid: GUID) -> Optional[UserOp]:
        return self._op_dict.get(guid, None)

    def _append_op(self, sn: SPIDNodePair, op: UserOp):
        guid = sn.spid.guid
        if SUPER_DEBUG_ENABLED:
            logger.debug(f'[{self.tree_id}] Inserting op for GUID "{guid}": {op}')

        # Validation:
        if not ((op.dst_node and sn.node.node_identifier == op.dst_node.node_identifier) or (sn.node.node_identifier == op.src_node.node_identifier)):
            raise RuntimeError(f'SN being added ({sn.node.node_identifier}) does not match either src or dst node of op ({op})')

        if self._op_dict.get(guid, None):
            raise RuntimeError(f'Duplicate UserOp for GUID "{guid}": 1st={op}; 2nd={self._op_dict.get(guid)}')

        # Insertion:
        self._op_dict[guid] = op
        self._op_list.append(op)

    def _get_or_create_pre_ancestors(self, sn: SPIDNodePair) -> SPIDNodePair:
        """Pre-ancestors are those nodes (either logical or pointing to real data) which are higher up than the source tree.
        Last pre-ancestor is easily derived and its prescence indicates whether its ancestors were already created"""

        assert isinstance(sn.spid, ChangeTreeSPID), f'Not a ChangeTreeSPID: {sn.spid}'
        op_type: UserOpType = sn.spid.op_type

        cat_spid = ChangeTreeSPID(self.get_root_sn().spid.path_uid, sn.spid.device_uid, self.root_path, op_type=sn.spid.op_type)
        cat_sn = self._category_tree.get_node_for_identifier(cat_spid.guid)
        if cat_sn:
            if SUPER_DEBUG_ENABLED:
                logger.debug(f'[{self.tree_id}] Found existing CategoryNode with OpType={op_type.name} guid="{cat_spid.guid}"')
            assert isinstance(cat_sn.node, ContainerNode)
            return cat_sn

        # else we need to create pre-ancestors...
        if SUPER_DEBUG_ENABLED:
            logger.debug(f'[{self.tree_id}] Creating pre-ancestors for {sn.spid}')

        parent_sn: SPIDNodePair = self.get_root_node()

        if self.show_whole_forest:
            # Create device node (e.g. 'GDrive' or 'Local Disk')
            # set UID to root_UID of relevant tree
            device_spid = self.backend.node_identifier_factory.get_device_root_spid(sn.spid.device_uid)
            device_sn = self._category_tree.get_node_for_identifier(device_spid.guid)
            if not device_sn:
                device_node = RootTypeNode(node_identifier=device_spid)
                device_sn: SPIDNodePair = SPIDNodePair(device_spid, device_node)
                if SUPER_DEBUG_ENABLED:
                    logger.debug(f'[{self.tree_id}] Inserting new RootTypeNode: {device_sn.spid.guid} (under parent: {parent_sn.spid.guid}')
                self._category_tree.add_node(node=device_sn, parent=parent_sn)
            parent_sn = device_sn

        assert not cat_sn
        cat_node = CategoryNode(node_identifier=cat_spid, op_type=op_type)
        # Create category display node. This may be the "last pre-ancestor". (Use root node UID so its context menu points to root)
        cat_sn: SPIDNodePair = SPIDNodePair(cat_spid, cat_node)
        if SUPER_DEBUG_ENABLED:
            logger.debug(f'[{self.tree_id}] Inserting new CategoryNode: {cat_spid.guid}')
        self._category_tree.add_node(node=cat_sn, parent=parent_sn)

        # this is the last pre-ancestor.
        return cat_sn

    def _get_or_create_ancestors(self, sn: SPIDNodePair, parent_sn: SPIDNodePair):
        full_path = sn.spid.get_single_path()
        existing_ancestor_path = parent_sn.spid.get_single_path()
        if not full_path:
            raise RuntimeError(f'SPID does not have a path: {sn.spid}')
        if not full_path.startswith(self.root_path):
            raise RuntimeError(f'Cannot insert node ({sn.spid}): its path does not start with tree root path ("{self.get_root_spid()}")')

        # Walk up the source tree and compose a list of ancestors:
        if SUPER_DEBUG_ENABLED:
            logger.debug(f'[{self.tree_id}] Looking for ancestors to fill in for path "{full_path}", stopping before "{existing_ancestor_path}"')

        ancestor_spid: ChangeTreeSPID = sn.spid
        dummy_stack: Deque[SPIDNodePair] = deque()

        while True:
            # Go up one dir:
            full_path: str = str(pathlib.Path(ancestor_spid.get_single_path()).parent)
            if full_path == self.root_path:
                break

            if full_path == ROOT_PATH:
                # This indicates a gap in our logic somewhere
                raise RuntimeError(f'While checking ancestors: somehow we skipped existing ancestor "{existing_ancestor_path}" '
                                   f'(while inserting node with path "{sn.spid.get_single_path()}")')

            if SUPER_DEBUG_ENABLED:
                logger.debug(f'[{self.tree_id}] Checking for ancestor with path: "{full_path}"')

            # Need some work to assemble the GUID to look up the ancestor:
            ancestor_path_uid = self.backend.cacheman.get_uid_for_local_path(full_path)  # TODO: take "_local" out of this
            ancestor_spid: ChangeTreeSPID = ChangeTreeSPID(ancestor_path_uid, ancestor_spid.device_uid, full_path, ancestor_spid.op_type)
            ancestor_sn = self._category_tree.get_node_for_identifier(ancestor_spid.guid)
            if ancestor_sn:
                if SUPER_DEBUG_ENABLED:
                    logger.debug(f'[{self.tree_id}] Ancestor already exists: {ancestor_spid.guid}; will start adding descendents to this"')
                # already added ancestor to tree
                parent_sn = ancestor_sn
                break
            else:
                if SUPER_DEBUG_ENABLED:
                    logger.debug(f'[{self.tree_id}] Ancestor not found: {ancestor_spid.guid}; creating dummy node"')
                # create ancestor & push to stack for later insertion in correct order
                ancestor_dir = ContainerNode(ancestor_spid)
                ancestor_dir.set_icon(OpTypeMeta.icon_src_dir(ancestor_spid.op_type))
                dummy_stack.append(SPIDNodePair(ancestor_spid, ancestor_dir))

        if SUPER_DEBUG_ENABLED:
            logger.debug(f'[{self.tree_id}] Need to add {len(dummy_stack)} ancestors"')

        # Walk down the ancestor list and create a node for each ancestor dir:
        while len(dummy_stack) > 0:
            dummy_child_sn = dummy_stack.pop()
            if SUPER_DEBUG_ENABLED:
                logger.debug(f'[{self.tree_id}] Inserting new dummy ancestor: {dummy_child_sn.spid} under parent: {parent_sn.spid}')
            self._category_tree.add_node(node=dummy_child_sn, parent=parent_sn)

            parent_sn = dummy_child_sn

        # Walk up the source tree and compose a list of ancestors:
        if SUPER_DEBUG_ENABLED:
            logger.debug(f'[{self.tree_id}] Done adding ancestors. Returning parent: {parent_sn.spid}')
        return parent_sn

    @staticmethod
    def _make_change_node_pair(from_sn: SPIDNodePair, op: UserOp) -> SPIDNodePair:
        change_spid = ChangeTreeSPID(path_uid=from_sn.spid.path_uid, device_uid=from_sn.spid.device_uid,
                                     full_path=from_sn.spid.get_single_path(), op_type=op.op_type)
        if change_spid.op_type == UserOpType.MKDIR:
            assert from_sn.node.is_dir(), f'Should be a dir: {from_sn.node}'
            # For display purposes, group "mkdir" with "copy":
            change_spid.op_type = UserOpType.CP
        change_node = copy.deepcopy(from_sn.node)
        change_node.set_icon(OpTypeMeta.get_icon_for(change_node.device_uid, change_node.uid, op))
        return SPIDNodePair(change_spid, change_node)

    def add_sn_and_op(self, sn: SPIDNodePair, op: UserOp):
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

        sn = self._make_change_node_pair(sn, op)

        self._append_op(sn, op)

        # We can easily derive the UID/NID of the node's parent. Check to see if it exists in the tree - if so, we can save a lot of work.
        existing_sn: SPIDNodePair = self._category_tree.get_node_for_identifier(identifier=sn.spid.guid)
        if existing_sn:
            logger.debug(f'[{self.tree_id}] Already present in tree ({existing_sn}')
            if not existing_sn.node.is_container_node():
                raise RuntimeError(f'Node already exists in the tree and is not a dummy node: {sn.spid}')

            self._category_tree.swap_with_existing_node(sn)
            logger.debug(f'[{self.tree_id}] Replaced dummy node with real change node: ({existing_sn.spid}')
        else:
            parent_sn = self._get_or_create_pre_ancestors(sn)

            parent_sn = self._get_or_create_ancestors(sn, parent_sn)

            try:
                if sn.spid.guid == parent_sn.spid.guid:
                    logger.error(f'[{self.tree_id}] Something is wrong: these should not have the same identifiers: sn={sn}, parent_sn={parent_sn}')
                    raise RuntimeError(f'Internal error: got a bad parent node while inserting: {sn.spid}')

                # Finally add the node itself.
                if SUPER_DEBUG_ENABLED:
                    logger.debug(f'[{self.tree_id}] Adding change node: {sn.spid} to parent {parent_sn.spid}')

                self._category_tree.add_node(node=sn, parent=parent_sn)
            except NodeAlreadyPresentError:
                # TODO: does this error still occur?
                # TODO: configurable handling of conflicts. Google Drive allows nodes with the same path and name, which is not allowed on local FS
                conflict_sn: SPIDNodePair = self._category_tree.get_node_for_identifier(sn.spid.guid)
                if conflict_sn.node.is_dir() or sn.node.is_dir():
                    self.count_conflict_errors += 1
                    if SUPER_DEBUG_ENABLED:
                        logger.error(f'[{self.tree_id}] Duplicate nodes for the same path & at least 1 is a dir: old={conflict_sn.node} new={sn.node}')
                elif conflict_sn.node.md5 == sn.node.md5:
                    self.count_conflict_warnings += 1
                    if SUPER_DEBUG_ENABLED:
                        logger.warning(f'[{self.tree_id}] Duplicate file nodes for the same path! However, both have same MD5, '
                                       f'so new node will be ignored: old={conflict_sn.node} new={sn.node}')
                else:
                    self.count_conflict_errors += 1
                    if SUPER_DEBUG_ENABLED:
                        logger.error(f'[{self.tree_id}] Duplicate nodes for the same path & different content: old={conflict_sn.node} new={sn.node}')

        if DIFF_DEBUG_ENABLED:
            self.print_tree_contents_debug()

    def __repr__(self):
        return f'ChangeTree(state=[{self.state}], {len(self._category_tree)} nodes, ' \
               f'errors={self.count_conflict_errors}, warnings={self.count_conflict_warnings})'

    def generate_dir_stats(self) -> Dict[GUID, DirectoryStats]:
        return self._category_tree.generate_dir_stats(self.tree_id)
