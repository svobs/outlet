import logging
import os
from collections import Counter, defaultdict, deque
from typing import DefaultDict, Deque, Dict, List, Optional, Tuple, Union

from constants import GDRIVE_ROOT_UID, ROOT_PATH, SUPER_DEBUG_ENABLED, TRACE_ENABLED, TreeType
from error import GDriveItemNotFoundError, NodeNotPresentError
from model.gdrive_meta import GDriveUser
from model.node.gdrive_node import GDriveFile, GDriveFolder, GDriveNode
from model.node.node import Node, SPIDNodePair
from model.node_identifier import GDriveIdentifier, GDriveSPID, NodeIdentifier, SinglePathNodeIdentifier
from model.node_identifier_factory import NodeIdentifierFactory
from model.uid import UID
from util import file_util
from util.base_tree import BaseTree

logger = logging.getLogger(__name__)


class GDriveWholeTree(BaseTree):
    """
    ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
    CLASS GDriveWholeTree

    Represents a user's entire GDrive tree.
    ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼
    """

    def __init__(self, backend, device_uid: UID):
        super().__init__()

        self.backend = backend

        self.node_identifier_factory: NodeIdentifierFactory = self.backend.node_identifier_factory
        """This is sometimes needed for lookups"""

        self.device_uid: UID = device_uid
        """Uniquely identifies this GDrive account"""

        # Keep track of parentless nodes. These include the 'My Drive' node, as well as shared nodes.
        self.uid_dict: Dict[UID, GDriveNode] = {}
        """ Forward lookup table: nodes are indexed by UID"""

        self.parent_child_dict: Dict[UID, List[GDriveNode]] = {}
        """ Reverse lookup table: 'parent_uid' -> list of child nodes """

        self.me: Optional[GDriveUser] = None

    def get_root_node(self) -> Optional[GDriveNode]:
        return self.uid_dict[GDRIVE_ROOT_UID]

    @property
    def node_identifier(self):
        return self.backend.node_identifier_factory.get_root_constant_gdrive_identifier(self.device_uid)

    def upsert_folder_and_children(self, parent_node: GDriveFolder, child_list: List[GDriveNode]) -> List[GDriveNode]:
        """Adds or replaces the given parent_node and its children. Any previous children which are not in the given list are
        unlinked from the parent, and if they had no other parents, become root-level nodes."""
        if SUPER_DEBUG_ENABLED:
            logger.debug(f'Entered upsert_folder_and_children(): parent_node={parent_node}, child_list size={len(child_list)}')

        upserted_node_list: List[GDriveNode] = [self.upsert_node(parent_node)]

        former_child_list: List[GDriveNode] = self.parent_child_dict.get(parent_node.uid)
        self.parent_child_dict[parent_node.uid] = child_list

        former_child_count = len(former_child_list)

        for child in child_list:
            upserted_node_list.append(self.upsert_node(child))
            GDriveWholeTree._remove_from_list(former_child_list, child)

        orphan_list = []
        if former_child_list:
            for potential_orphan in former_child_list:
                potential_orphan.remove_parent(parent_node.uid)
                if potential_orphan.has_no_parents():
                    # If child has no parents, add as child of pseudo-root.
                    # May get expensive for very large number of children...
                    self._upsert_root(potential_orphan)
                    orphan_list.append(potential_orphan)

        if orphan_list:
            logger.debug(f'After updating parent node (uid={parent_node.uid}) with {len(child_list)} nodes ({child_list}), the following '
                         f'{len(orphan_list)} nodes (of {former_child_count} originally) were orphaned: {orphan_list}')
            # TODO: separate these and query GDrive to see if they still exist; remove them (they are probably deleted on the server)
            upserted_node_list += orphan_list

        return upserted_node_list

    def upsert_node(self, node: GDriveNode) -> GDriveNode:
        """Adds a node. Assumes that the node has all necessary parent info filled in already,
        and does the heavy lifting and populates all data structures appropriately."""

        # Build forward dictionary
        existing_node = self.uid_dict.get(node.uid, None)
        if existing_node:
            if node == existing_node:
                logger.debug(f'upsert_node(): identical to existing; updating node {node.uid} sync_ts to {node.sync_ts}')
                existing_node.set_sync_ts(node.sync_ts)
                return existing_node

            is_name_changed = existing_node.name != node.name

            logger.debug(f'upsert_node(): found existing node with same ID (will attempt to merge nodes): existing: {existing_node}; new={node}')
            new_parent_uids, removed_parent_uids = _merge_into_existing(existing_node, node)
            node = existing_node

            # If any parents were removed, update parent dict (the "add" case is handled further below)
            if removed_parent_uids:
                for removed_parent_uid in removed_parent_uids:
                    self._remove_from_parent_dict(removed_parent_uid, node)

            # If path was affected, need to recompute paths for the node and all its descendants:
            if is_name_changed or new_parent_uids or removed_parent_uids:
                self.recompute_path_list_for_subtree(node.uid)

        else:
            new_parent_uids: List[UID] = node.get_parent_uids()
            self.uid_dict[node.uid] = node

        # build reverse dictionary for any added parents
        if len(new_parent_uids) > 0:
            for parent_uid in new_parent_uids:
                self._add_to_parent_dict(parent_uid, node)

        if node.has_no_parents() and node.uid != GDRIVE_ROOT_UID:  # do not make root a child of itself
            self._upsert_root(node)

        # Generate full_path for node, if not already done (we assume this is a newly created node)
        self.recompute_path_list_for_uid(node.uid)

        # this may actually be an existing node (we favor that if it exists)
        return node

    def recompute_path_list_for_subtree(self, subtree_root_uid: UID):
        def action_func(visited_node):
            self.recompute_path_list_for_uid(visited_node.uid)

        self.for_each_node_breadth_first(action_func=action_func, subtree_root_identifier=subtree_root_uid)

    def remove_node(self, node: GDriveNode, fail_if_children_present: bool = True) -> Optional[GDriveNode]:
        """Remove given node from all data structures in this tree. Returns the node which was removed (which may be a different object
        than the parameter) or None if node is not in tree.
        If fail_if_children_present==True, an exception will be raised if the node is a folder and it has child nodes linked to it.
        If false and the node is a folder and it has child nodes, the child nodes will be unlinked from the parent, and if it was their
        only parent, the child nodes will become root nodes"""
        if node.uid not in self.uid_dict:
            logger.warning(f'Cannot remove node from in-memory tree: it was not found in the tree: {node}')
            return None

        if not node.get_path_list():
            # (Kind of a kludge): we need the old path list so that downstream processes can work properly.
            self.recompute_path_list_for_uid(node.uid)

        if node.is_dir():
            child_list = self.get_child_list_for_node(node)
            if child_list:
                if fail_if_children_present:
                    raise RuntimeError(f'Cannot remove non-empty folder: {node}')
                else:
                    # Remove parent->children link from dict...
                    child_list = self.parent_child_dict.pop(node.uid, [])
                    if child_list:
                        # Now remove child->parent link from each child
                        for child in child_list:
                            child.remove_parent(node.uid)
                            # If child has no parents, add as child of pseudo-root.
                            # May get expensive for very large number of children...
                            if child.has_no_parents():
                                logger.debug(f'Deletion of parent ({node.uid}) resulted in orphaned child ({child.uid}): making child a root')
                                self._upsert_root(child)

                            # Paths were updated:
                            self.recompute_path_list_for_subtree(child.uid)

        # Unlink node from all its parents
        if node.get_parent_uids():
            for parent_uid in node.get_parent_uids():
                # this may get expensive for folders with lots of nodes...may want to monitor performance
                self._remove_from_parent_dict(parent_uid, node)
        else:
            # Remove from roots if present (if not, do nothing)
            self._remove_root(node)

        removed_node = self.uid_dict.pop(node.uid, None)

        if SUPER_DEBUG_ENABLED:
            logger.debug(f'GDriveNode removed from in-memory tree: {removed_node}')

        return removed_node

    def add_parent_mapping(self, node_uid: UID, parent_uid: UID):
        """Assuming that an node with the given UID has already been added to this tree, this method
        adds all the references to the various data structures which are needed to assign it a single parent."""
        assert node_uid
        assert parent_uid
        node = self.uid_dict.get(node_uid)
        if not node:
            raise RuntimeError(f'Cannot add parent mapping: Item not found with UID: {node_uid} (for parent_uid={parent_uid})')
        assert isinstance(node, GDriveNode)

        # Add to dict:
        self._add_to_parent_dict(parent_uid, node)

        # Add ref in node:
        if parent_uid not in node.get_parent_uids():
            if TRACE_ENABLED:
                logger.debug(f'add_parent_mapping(): Adding parent {parent_uid} to node {node.uid} (device_uid={self.device_uid})')
            node.add_parent(parent_uid)

    def _remove_root(self, node: GDriveNode):
        root_list = self.get_child_list_for_root()
        for root in root_list:
            if root.uid == node.uid:
                root_list.remove(root)
                return root
        return None

    def _upsert_root(self, node: GDriveNode):
        root_list = self.get_child_list_for_root()
        for root in root_list:
            if root.uid == node.uid:
                # already present: do nothing
                return
        root_list.append(node)

    def _add_to_parent_dict(self, parent_uid: UID, node: GDriveNode):
        child_list: List[GDriveNode] = self.parent_child_dict.get(parent_uid)
        if not child_list:
            child_list: List[GDriveNode] = []
            self.parent_child_dict[parent_uid] = child_list
        child_list.append(node)

    def _remove_from_parent_dict(self, parent_uid: UID, child_to_remove: GDriveNode):
        child_list: List[GDriveNode] = self.parent_child_dict.get(parent_uid, [])
        self._remove_from_list(child_list, child_to_remove)

    @staticmethod
    def _remove_from_list(node_list: List[GDriveNode], node_to_remove: GDriveNode):
        if node_list:
            for node in node_list:
                if node.uid == node_to_remove.uid:
                    node_list.remove(node)
                    return

    def get_all_files_and_folders_for_subtree(self, subtree_root: GDriveIdentifier) -> Tuple[List[GDriveFile], List[GDriveFolder]]:
        file_list: List[GDriveFile] = []
        folder_list: List[GDriveFolder] = []
        queue: Deque[GDriveNode] = deque()
        node = self.get_node_for_uid(uid=subtree_root.node_uid)
        if node:
            queue.append(node)

        while len(queue) > 0:
            node: GDriveNode = queue.popleft()
            if node.is_dir():
                assert isinstance(node, GDriveFolder)
                folder_list.append(node)
                for child in self.get_child_list_for_node(node):
                    assert isinstance(child, GDriveNode)
                    queue.append(child)
            else:
                assert isinstance(node, GDriveFile)
                file_list.append(node)

        return file_list, folder_list

    def get_node_list_for_path_list(self, path_list: List[str]) -> List[GDriveNode]:
        identifiers_found: List[NodeIdentifier] = self.get_identifier_list_for_path_list(path_list)

        # In Google Drive it is legal to have two different files with the same path
        if SUPER_DEBUG_ENABLED:
            logger.debug(f'Found {len(identifiers_found)} nodes for path list: "{path_list}"')
        return list(map(lambda x: self.get_node_for_uid(x.node_uid), identifiers_found))

    def get_identifier_list_for_path_list(self, path_list: List[str], error_if_not_found: bool = False):
        assert isinstance(path_list, List)
        identifiers_found: List[NodeIdentifier] = []

        for single_path in path_list:
            try:
                identifiers = self._get_identifier_list_for_single_path(single_path, error_if_not_found)
                if identifiers:
                    identifiers_found = identifiers_found + identifiers
            except GDriveItemNotFoundError:
                logger.warning(f'No node identifier(s) found for path, skipping: "{single_path}"')
        return identifiers_found

    def _get_identifier_list_for_single_path(self, full_path: str, error_if_not_found: bool) -> List[NodeIdentifier]:
        """Try to match the given file-system-like path, mapping the root of this tree to the first segment of the path.
        Since GDrive allows for multiple parents per child, it is possible for multiple matches to occur. This
        returns them all.
        NOTE: returns FileNotFoundError if not even one ID could be matched
        """
        if SUPER_DEBUG_ENABLED:
            logger.debug(f'GDriveWholeTree._get_identifier_list_for_single_path() requested for full_path: "{full_path}", '
                         f'error_if_not_found={error_if_not_found}')

        current_seg_nodes: List[GDriveNode] = [self.get_root_node()]

        if full_path == ROOT_PATH:
            return [n.node_identifier for n in current_seg_nodes]

        if not full_path.startswith('/'):
            raise RuntimeError(f'Bad path: "{full_path}"')

        target_path = full_path.lower()  # case insensitive search
        path_so_far = ''
        next_seg_nodes: List[GDriveNode] = []

        for name_seg in file_util.split_path(target_path[1:]):
            path_so_far = path_so_far + '/' + name_seg

            for current_seg_node in current_seg_nodes:
                matching_children: List[Node] = [n for n in self.get_child_list_for_node(current_seg_node) if n.name.lower() == name_seg]
                if SUPER_DEBUG_ENABLED and len(matching_children) > 1:
                    logger.info(f'get_identifier_list_for_single_path(): Multiple child IDs ({len(matching_children)}) found for parent "'
                                f'{current_seg_node.node_identifier}", path_so_far "{path_so_far}"')
                    for num, match in enumerate(matching_children):
                        logger.info(f'Match {num}: {match}')
                next_seg_nodes += matching_children

            if next_seg_nodes:
                # transfer next_seg_nodes to current list, then clear current_seg_node
                current_seg_nodes = next_seg_nodes
                next_seg_nodes = []
            else:
                if SUPER_DEBUG_ENABLED:
                    logger.debug(f'get_identifier_list_for_single_path(): Segment not found: "{name_seg}"'
                                 f' (target_path: "{target_path}", path_so_far="{path_so_far}")')
                if error_if_not_found:
                    err_node_identifier = self.node_identifier_factory.for_values(device_uid=self.device_uid, tree_type=TreeType.GDRIVE,
                                                                                  path_list=full_path)
                    raise GDriveItemNotFoundError(node_identifier=err_node_identifier, offending_path=path_so_far)
                else:
                    return []

        matching_node_identifiers: List[NodeIdentifier] = list(map(lambda x: x.node_identifier, current_seg_nodes))
        for node_identifier in matching_node_identifiers:
            assert full_path in node_identifier.get_path_list(), \
                f'path just built ({full_path}) not found in path list of node identifeir ({node_identifier})'
            # node_identifier.add_path_if_missing(full_path)
        if SUPER_DEBUG_ENABLED:
            logger.debug(f'get_identifier_list_for_single_path(): Found for path "{path_so_far}": {matching_node_identifiers}')
        return matching_node_identifiers

    @staticmethod
    def is_path_in_subtree(full_path: Union[str, List[str]], subtree_root_path: str = None):
        """Checks if the given path(s) have this tree's root (or if given, subtree_root_path) as their base path.
        Note: for GDriveWholeTree, if subtree_root_path is not provided, any non-empty value of full_path will return True."""
        if not full_path:
            raise RuntimeError('is_path_in_subtree(): full_path not provided!')

        if not subtree_root_path:
            # This tree start with '/': it contains everything
            return True

        if isinstance(full_path, list):
            for p in full_path:
                # i.e. any
                if p.startswith(subtree_root_path):
                    return True
            return False

        return full_path.startswith(subtree_root_path)

    def get_parent_list_for_node(self, node: GDriveNode, required_subtree_path: str = None) -> List[GDriveNode]:
        if node.device_uid != self.device_uid:
            logger.debug(f'get_parent_list_for_node(): node has wrong device_uid (got {node.device_uid}, expected {self.device_uid}) returning None')
            return []

        assert isinstance(node, GDriveNode)
        parent_uids = node.get_parent_uids()
        if parent_uids:
            resolved_parents = []
            for par_id in parent_uids:
                parent = self.get_node_for_uid(par_id)
                if parent and (not required_subtree_path or self.is_path_in_subtree(parent.get_path_list(), required_subtree_path)):
                    resolved_parents.append(parent)
            return resolved_parents
        return []

    def to_sn(self, node, single_path) -> SPIDNodePair:
        spid = self.backend.node_identifier_factory.for_values(uid=node.uid, device_uid=node.device_uid, tree_type=node.tree_type,
                                                               path_list=single_path, must_be_single_path=True)
        return SPIDNodePair(spid, node)

    def to_sn_from_node_and_parent_spid(self, node: GDriveNode, parent_spid: SinglePathNodeIdentifier) -> SPIDNodePair:
        # derive single child path from single parent path
        child_path: str = os.path.join(parent_spid.get_single_path(), node.name)
        # Yuck...this is more expensive than preferred... at least there's no network call
        return self.to_sn(node, child_path)

    def get_parent_for_sn(self, sn: SPIDNodePair) -> Optional[SPIDNodePair]:
        parent_uids = sn.node.get_parent_uids()
        if parent_uids:
            parent_path = sn.spid.get_single_parent_path()
            for par_id in sn.node.get_parent_uids():
                parent_node = self.get_node_for_uid(par_id)
                if parent_node and parent_path in parent_node.get_path_list():
                    return self.to_sn(node=parent_node, single_path=parent_path)
        return None

    def validate(self):
        logger.debug(f'Validating GDriveWholeTree')
        # Validate parent dict:
        for parent_uid, children in self.parent_child_dict.items():
            unique_child_uids = {}
            for child in children:
                if not self.get_node_for_uid(child.uid):
                    logger.error(f'Child present in child list of parent {parent_uid} but not found in id_dict: {child}')
                duplicate_child = unique_child_uids.get(child.uid)
                if duplicate_child:
                    logger.error(f'Child already present in list of parent {parent_uid}: orig={duplicate_child} dup={child}')
                else:
                    unique_child_uids[child.uid] = child

        for node_uid, node in self.uid_dict.items():
            if node_uid != node.uid:
                logger.error(f'[!!!] Node actual UID does not match its key in the UID dict ({node_uid}): {node}')
            if len(node.get_parent_uids()) > 1:
                resolved_parent_ids = [x for x in node.get_parent_uids() if self.get_node_for_uid(x)]
                if len(resolved_parent_ids) > 1:
                    logger.error(f'Found multiple valid parent_uids for node: {node}: parent_uids={resolved_parent_ids}')

        logger.debug(f'Done validating GDriveWholeTree')

    @property
    def tree_type(self) -> TreeType:
        return TreeType.GDRIVE

    def get_child_list_for_identifier(self, node_uid: UID) -> List[GDriveNode]:
        if node_uid not in self.uid_dict:
            raise NodeNotPresentError(f'Cannot get children: parent "{node_uid}" is not in the tree (device_uid={self.device_uid})!')
        return self.parent_child_dict.get(node_uid, [])

    def get_child_list_for_spid(self, parent_spid: SinglePathNodeIdentifier) -> List[SPIDNodePair]:
        """Raises NodeNotPresentError if parent is not in this tree"""
        assert isinstance(parent_spid, GDriveSPID), f'Expected GDriveSPID but got: {type(parent_spid)}: {parent_spid}'

        child_node_list = self.get_child_list_for_identifier(parent_spid.node_uid)
        if TRACE_ENABLED:
            logger.debug(f'get_child_list_for_spid(): got {len(child_node_list)} child nodes for node_uid={parent_spid.node_uid}')

        child_sn_list = []
        for child_node in child_node_list:
            child_sn_list.append(self.to_sn_from_node_and_parent_spid(child_node, parent_spid))

        return child_sn_list

    def get_child_list_for_node(self, node: GDriveNode) -> List[GDriveNode]:
        return self.parent_child_dict.get(node.uid, [])

    def get_node_for_goog_id_and_parent_uid(self, goog_id: str, parent_uid: UID) -> Optional[GDriveNode]:
        """Finds the GDrive node with the given goog_id. (Parent UID is needed so that we don't have to search the entire tree"""
        parent = self.get_node_for_uid(parent_uid)
        if parent:
            children = self.get_child_list_for_node(parent)
            if children:
                for child in children:
                    assert isinstance(child, GDriveNode)
                    if child.goog_id == goog_id:
                        return child
        return None

    def get_node_for_name_and_parent_uid(self, name: str, parent_uid: UID) -> Optional[GDriveNode]:
        """Returns the first GDrive node found with the given name and parent.
        This roughly matches the logic used to search for an node in Google Drive when we are unsure about its goog_id."""
        parent = self.get_node_for_uid(parent_uid)
        if parent:
            children = self.get_child_list_for_node(parent)
            if children:
                for child in children:
                    assert isinstance(child, GDriveNode)
                    if child.name == name:
                        return child
        return None

    def get_node_for_identifier(self, identifier: UID) -> Optional[GDriveNode]:
        return self.get_node_for_uid(identifier)

    def get_node_for_uid(self, uid: UID) -> Optional[GDriveNode]:
        assert uid
        return self.uid_dict.get(uid, None)

    def resolve_uids_to_goog_ids(self, uids: List[UID], fail_if_missing: bool = True) -> List[str]:
        goog_ids: List[str] = []
        for uid in uids:
            node = self.get_node_for_uid(uid)
            if node and node.goog_id:
                goog_ids.append(node.goog_id)
            elif fail_if_missing:
                if not node:
                    raise RuntimeError(f'Could not resolve goog_id: could not find node in master GDrive tree with UID: {uid}')
                if not node.goog_id:
                    raise RuntimeError(f'Could not resolve goog_id for UID {uid}: node has no goog_id: {node}')
        return goog_ids

    def recompute_path_list_for_uid(self, uid: UID) -> List[str]:
        """Derives the list filesystem-like-paths for the node with the given UID, sets them, and returns them.
        Stops when a parent cannot be found, or the root of the tree is reached."""
        current_node: GDriveNode = self.get_node_for_uid(uid)
        if not current_node:
            raise RuntimeError(f'Cannot recompute path list: node not found in tree for UID {uid}')

        logger.debug(f'Recomputing path for node {uid} ("{current_node.name}")')

        # TODO: it's possible to optimize this by using the parent paths, if available

        path_list: List[str] = []
        # Iterate backwards (the given ID is the last segment in the path
        current_segment_nodes: List[Tuple[GDriveNode, str]] = [(current_node, '')]
        next_segment_nodes: List[Tuple[GDriveNode, str]] = []
        while current_segment_nodes:
            for node, path_so_far in current_segment_nodes:
                if path_so_far == '':
                    # first node (leaf)
                    path_so_far = node.name
                else:
                    if node.name == ROOT_PATH:
                        # special case for root path: don't add an extra slash
                        path_so_far = '/' + path_so_far
                    else:
                        # Pre-pend parent name:
                        path_so_far = node.name + '/' + path_so_far

                parent_uids: List[UID] = node.get_parent_uids()
                if parent_uids:
                    if len(parent_uids) > 1:
                        # Make sure they are not dead links:
                        parent_uids: List[UID] = [x for x in parent_uids if self.get_node_for_uid(x)]
                        if len(parent_uids) > 1:
                            if SUPER_DEBUG_ENABLED:
                                logger.debug(f'Multiple parents found for {node.uid} ("{node.name}").')
                                for parent_index, parent_uid in enumerate(parent_uids):
                                    logger.debug(f'Parent {parent_index}: {parent_uid}')
                            # pass through
                        elif SUPER_DEBUG_ENABLED:
                            logger.warning(f'Found multiple parents for node but only one could be resolved: node={node.uid} ("{node.name}")')
                    for parent_uid in parent_uids:
                        parent_node: GDriveNode = self.get_node_for_uid(parent_uid)
                        if parent_node:
                            next_segment_nodes.append((parent_node, path_so_far))
                        else:
                            # Parent refs cannot be resolved == root of subtree
                            if SUPER_DEBUG_ENABLED:
                                logger.debug(f'Mapped ID "{uid}" to subtree path "{path_so_far}"')
                            if path_so_far not in path_list:
                                path_list.append(path_so_far)

                else:
                    # No parent refs. Root of Google Drive
                    path_list.append(path_so_far)
            current_segment_nodes = next_segment_nodes
            next_segment_nodes = []

        if TRACE_ENABLED:
            logger.debug(f'Computed path list "{path_list}" for node_identifier: {current_node.node_identifier}')
        elif SUPER_DEBUG_ENABLED:
            if path_list != current_node.node_identifier.get_path_list():
                logger.debug(f'Updating path_list for node_identifier ({current_node.node_identifier}) -> {path_list}')

        current_node.node_identifier.set_path_list(path_list)

        for path in path_list:
            if path.startswith('//'):
                logger.error(f'Generated invalid path ({path}) for node: {current_node}')

        return path_list

    def find_duplicate_node_names(self):
        """Finds and builds a list of all nodes which have the same name inside the same folder"""
        queue: Deque[GDriveNode] = deque()
        child_dict: DefaultDict[str, List[GDriveNode]] = defaultdict(list)

        duplicates: List[List[GDriveNode]] = []

        # roots ...
        for root in self.get_child_list_for_root():
            if root.is_dir():
                queue.append(root)
            child_dict[root.name].append(root)

        for node_list in child_dict.values():
            if len(node_list) > 1:
                duplicates.append(node_list)

        # everything else ...
        while len(queue) > 0:
            node: GDriveNode = queue.popleft()
            child_dict: DefaultDict[str, List[GDriveNode]] = defaultdict(list)

            children = self.get_child_list_for_node(node)
            if children:
                for child in children:
                    if child.is_dir():
                        assert isinstance(child, GDriveNode)
                        queue.append(child)
                    child_dict[child.name].append(child)

            for node_list in child_dict.values():
                if len(node_list) > 1:
                    duplicates.append(node_list)
                    # TODO: distinguish between files and folders... folders will be non-trivial
                    print_list = list(map(lambda x: f'{x.name}:{x.md5}', node_list))
                    logger.warning(f'Conflict: {print_list}')
                    # TODO: give user option of deleting the files which have identical MD5s

        logger.info(f'Tree contains {len(duplicates)} filename conflicts')

    def count_multiple_parents(self):
        counter: Counter = Counter()
        for uid, node in self.uid_dict.items():
            assert uid == node.uid
            num_parents: int = len(node.get_parent_uids())
            counter.update([num_parents])

        for num_parents, node_count in counter.items():
            logger.info(f'Nodes with {num_parents} parents: {node_count}')

    def show_tree(self, subroot_uid: UID) -> str:
        # TODO
        return 'TODO'


def _merge_into_existing(existing_node: GDriveNode, new_node: GDriveNode) -> Tuple[List[UID], List[UID]]:
    new_parent_uids: List[UID] = []
    for parent_uid in new_node.get_parent_uids():
        if parent_uid not in existing_node.get_parent_uids():
            new_parent_uids.append(parent_uid)

    removed_parent_uids: List[UID] = []
    for parent_uid in existing_node.get_parent_uids():
        if parent_uid not in new_node.get_parent_uids():
            removed_parent_uids.append(parent_uid)

    if existing_node.goog_id and existing_node.goog_id != new_node.goog_id:
        raise RuntimeError(f'Existing node goog_id ({existing_node.goog_id}) does not match new node goog_id ({new_node.goog_id})')

    if existing_node.is_dir() and new_node.is_dir():
        if existing_node.all_children_fetched and not new_node.all_children_fetched:
            if TRACE_ENABLED:
                logger.debug(f'Merging into existing node which has all_children_fetched=True; will set new node to True')
            new_node.all_children_fetched = True
        elif not existing_node.all_children_fetched and new_node.all_children_fetched:
            logger.debug(f'Overwriting node with all_children_fetched=False with one which is True: {new_node}')
    existing_node.update_from(new_node)

    # Need to return these so they can be added to reverse dict
    return new_parent_uids, removed_parent_uids
