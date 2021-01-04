import logging
from collections import Counter, defaultdict, deque
from typing import Callable, DefaultDict, Deque, Dict, List, Optional, Tuple, Union

from constants import COUNT_MULTIPLE_GDRIVE_PARENTS, FIND_DUPLICATE_GDRIVE_NODE_NAMES, GDRIVE_ROOT_UID, ROOT_PATH, SUPER_DEBUG, TrashStatus, \
    TREE_TYPE_GDRIVE
from error import GDriveItemNotFoundError
from model.gdrive_meta import GDriveUser
from model.has_get_children import HasGetChildren
from model.node.gdrive_node import GDriveFile, GDriveFolder, GDriveNode
from model.node.node import Node
from model.node_identifier import GDriveIdentifier, NodeIdentifier
from model.node_identifier_factory import NodeIdentifierFactory
from model.uid import UID
from model.display_tree.filter_criteria import FilterCriteria
from util import file_util, format
from util.stopwatch_sec import Stopwatch

logger = logging.getLogger(__name__)


class GDriveWholeTree(HasGetChildren):
    """
    ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
    CLASS GDriveWholeTree

    Represents the entire GDrive tree. We can't easily map this to DisplayTree, because the GDriveWholeTree can have multiple roots.
    ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼
    """
    def __init__(self, node_identifier_factory: NodeIdentifierFactory):
        self.node_identifier_factory: NodeIdentifierFactory = node_identifier_factory
        """This is sometimes needed for lookups"""

        # Keep track of parentless nodes. These include the 'My Drive' node, as well as shared nodes.
        self.uid_dict: Dict[UID, GDriveNode] = {}
        """ Forward lookup table: nodes are indexed by UID"""

        # It's a lot cleaner to have a single root node, even if it does not map to anything in GDrive:
        root = GDriveWholeTree.get_super_root()
        self.uid_dict[root.uid] = root

        self.parent_child_dict: Dict[UID, List[GDriveNode]] = {}
        """ Reverse lookup table: 'parent_uid' -> list of child nodes """

        # init root list:
        self.parent_child_dict[GDRIVE_ROOT_UID] = []

        self.me: Optional[GDriveUser] = None

    @staticmethod
    def get_super_root():
        # basically a fake / logical node which serves as the parent of My GDrive, shares, etc.
        return GDriveFolder(NodeIdentifierFactory.get_gdrive_root_constant_identifier(), None, None, TrashStatus.NOT_TRASHED, None, None,
                            None, None, None, None, None, False)

    @property
    def node_identifier(self):
        return NodeIdentifierFactory.get_gdrive_root_constant_identifier()

    def compute_path_list_for_node(self, node: GDriveNode) -> List[str]:
        """Gets the absolute path for the node. Also sets its 'path_list' attribute for future use"""
        path_list = node.get_path_list()
        if path_list:
            # Does node already have a full_path? Just return that (huge speed gain):
            return path_list

        # Set in the node for future use:
        path_list: List[str] = self._compute_path_list_for_uid(node.uid)
        node.node_identifier.set_path_list(path_list)
        return path_list

    def upsert_folder_and_children(self, parent_node: GDriveFolder, child_list: List[GDriveNode]) -> List[GDriveNode]:
        """Adds or replaces the given parent_node and its children. Any previous children which are not in the given list are
        unlinked from the parent, and if they had no other parents, become root-level nodes."""
        upserted_node_list: List[GDriveNode] = [self.upsert_node(parent_node)]

        former_child_list: List[GDriveNode] = self.parent_child_dict.get(parent_node.uid)
        if former_child_list:
            self.parent_child_dict[parent_node.uid] = []

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
            logger.debug(f'After updating parent node ({parent_node.uid}) with {len(child_list)} nodes, the following '
                         f'{len(orphan_list)} nodes were orphaned: {orphan_list}')
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
                logger.debug(f'upsert_node(): identical to existing; updating node {node.uid} sync_ts={node.sync_ts}')
                existing_node.set_sync_ts(node.sync_ts)
                return existing_node

            logger.debug(f'upsert_node(): found existing node with same ID (will attempt to merge nodes): existing: {existing_node}; new={node}')
            new_parent_uids, removed_parent_uids = _merge_into_existing(existing_node, node)
            node = existing_node

            if len(removed_parent_uids) > 0:
                for removed_parent_uid in removed_parent_uids:
                    self._remove_from_parent_dict(removed_parent_uid, node)

        else:
            new_parent_uids: List[UID] = node.get_parent_uids()
            self.uid_dict[node.uid] = node

        # build reverse dictionary
        if len(new_parent_uids) > 0:
            for parent_uid in new_parent_uids:
                self._add_to_parent_dict(parent_uid, node)

        self._upsert_root(node)

        # this may actually be an existing node (we favor that if it exists)
        return node

    def remove_node(self, node: GDriveNode, fail_if_children_present: bool = True) -> Optional[GDriveNode]:
        """Remove given node from all data structures in this tree. Returns the node which was removed (which may be a different object
        than the parameter) or None if node is not in tree.
        If fail_if_children_present==True, an exception will be raised if the node is a folder and it has child nodes linked to it.
        If false and the node is a folder and it has child nodes, the child nodes will be unlinked from the parent, and if it was their
        only parent, the child nodes will become root nodes"""
        if node.uid not in self.uid_dict:
            logger.warning(f'Cannot remove node from in-memory tree: it was not found in the tree: {node}')
            return None

        if node.is_dir():
            child_list = self.get_children(node)
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
                            self._upsert_root(child)

        # Unlink node from all its parents
        if node.get_parent_uids():
            for parent_uid in node.get_parent_uids():
                # this may get expensive for folders with lots of nodes...may want to monitor performance
                self._remove_from_parent_dict(parent_uid, node)
        else:
            # Remove from roots if present (if not, do nothing)
            self._remove_root(node)

        removed_node = self.uid_dict.pop(node.uid, None)

        if SUPER_DEBUG:
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
        node.add_parent(parent_uid)

    def _remove_root(self, node: GDriveNode):
        root_list = self.get_children_for_root()
        for root in root_list:
            if root.uid == node.uid:
                root_list.remove(root)
                return root
        return None

    def _upsert_root(self, node: GDriveNode):
        if node.has_no_parents():
            root_list = self.get_children_for_root()
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
        node = self.get_node_for_uid(uid=subtree_root.uid)
        if node:
            queue.append(node)

        while len(queue) > 0:
            node: GDriveNode = queue.popleft()
            if node.is_dir():
                assert isinstance(node, GDriveFolder)
                folder_list.append(node)
                for child in self.get_children(node):
                    assert isinstance(child, GDriveNode)
                    queue.append(child)
            else:
                assert isinstance(node, GDriveFile)
                file_list.append(node)

        return file_list, folder_list

    def get_identifier_list_for_path_list(self, path_list: List[str], error_if_not_found: bool = False):
        identifiers_found: List[NodeIdentifier] = []

        for single_path in path_list:
            try:
                identifiers = self._get_identifier_list_for_single_path(single_path, error_if_not_found)
                if identifiers:
                    identifiers_found += identifiers
            except GDriveItemNotFoundError:
                logger.warning(f'No node identifier(s) found for path, skipping: "{single_path}"')
        return identifiers_found

    def get_node_list_for_path_list(self, path_list: List[str]) -> List[GDriveNode]:
        identifiers_found: List[NodeIdentifier] = self.get_identifier_list_for_path_list(path_list)

        # In Google Drive it is legal to have two different files with the same path
        logger.debug(f'Found {len(identifiers_found)} nodes for path list: "{path_list}"')
        return list(map(lambda x: self.get_node_for_uid(x.uid), identifiers_found))

    def _get_identifier_list_for_single_path(self, full_path: str, error_if_not_found: bool) -> List[NodeIdentifier]:
        """Try to match the given file-system-like path, mapping the root of this tree to the first segment of the path.
        Since GDrive allows for multiple parents per child, it is possible for multiple matches to occur. This
        returns them all.
        NOTE: returns FileNotFoundError if not even one ID could be matched
        """
        if SUPER_DEBUG:
            logger.debug(f'GDriveWholeTree.get_identifier_list_for_single_path() requested for full_path: "{full_path}"')
        if full_path == ROOT_PATH:
            return [NodeIdentifierFactory.get_gdrive_root_constant_identifier()]
        name_segments = file_util.split_path(full_path)
        if len(name_segments) == 0:
            raise RuntimeError(f'Bad path: "{full_path}"')
        # name_segments = list(map(lambda x: x.lower(), name_segments))
        iter_name_segs = iter(name_segments)
        try:
            seg = next(iter_name_segs)
        except StopIteration:
            seg = ''
        if seg == '/':
            # Strip off root prefix if there is one
            try:
                seg = next(iter_name_segs)
            except StopIteration:
                seg = ''
        path_so_far = '/' + seg
        current_seg_nodes: List[GDriveNode] = [x for x in self.get_children_for_root() if x.name.lower() == seg.lower()]
        next_seg_nodes: List[GDriveNode] = []
        path_found = '/'
        if current_seg_nodes:
            path_found += current_seg_nodes[0].name

        for name_seg in iter_name_segs:
            path_so_far = path_so_far + '/' + name_seg
            for current in current_seg_nodes:
                current_id: UID = current.uid
                children: List[Node] = self.get_children(current)
                if not children:
                    if SUPER_DEBUG:
                        logger.debug(f'Item has no children: id="{current_id}" path_so_far="{path_so_far}"')
                    break
                matches: List[Node] = [x for x in children if x.name.lower() == name_seg.lower()]
                if SUPER_DEBUG and len(matches) > 1:
                    logger.info(f'get_identifier_list_for_single_path(): Multiple child IDs ({len(matches)}) found for parent ID "'
                                f'{current_id}", path_so_far "{path_so_far}"')
                    for num, match in enumerate(matches):
                        logger.info(f'Match {num}: {match}')
                next_seg_nodes += matches

            if len(next_seg_nodes) == 0:
                if SUPER_DEBUG:
                    logger.debug(f'get_identifier_list_for_single_path(): Segment not found: "{name_seg}" (target_path: "{full_path}"')
                if error_if_not_found:
                    raise GDriveItemNotFoundError(node_identifier=self.node_identifier_factory.for_values(
                        tree_type=TREE_TYPE_GDRIVE, path_list=full_path), offending_path=path_so_far)
            else:
                path_found = path_found + '/' + next_seg_nodes[0].name

            current_seg_nodes = next_seg_nodes
            next_seg_nodes = []
        matching_node_identifiers: List[NodeIdentifier] = list(map(lambda x: x.node_identifier, current_seg_nodes))
        for node_identifier in matching_node_identifiers:
            # Needs to be filled in:
            node_identifier.add_path_if_missing(path_found)
        if SUPER_DEBUG:
            logger.debug(f'get_identifier_list_for_single_path(): Found for path "{path_so_far}": {matching_node_identifiers}')
        if error_if_not_found and not matching_node_identifiers:
            raise GDriveItemNotFoundError(node_identifier=self.node_identifier_factory.for_values(
                tree_type=TREE_TYPE_GDRIVE, path_list=full_path), offending_path=path_so_far)
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
        if node.get_tree_type() != TREE_TYPE_GDRIVE:
            logger.debug(f'get_parent_list_for_node(): node has wrong tree type ({node.get_tree_type()}); returning None')
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
    def tree_type(self) -> int:
        return TREE_TYPE_GDRIVE

    def get_children_for_root(self) -> List[GDriveNode]:
        # Pseudo-root GDRIVE_ROOT_UID has the root nodes as its children:
        return self.parent_child_dict.get(GDRIVE_ROOT_UID, [])

    def get_children(self, node: GDriveNode, filter_criteria: FilterCriteria = None) -> List[Node]:
        if filter_criteria:
            return filter_criteria.get_filtered_child_list(node, self)
        else:
            if node.uid == GDRIVE_ROOT_UID:
                child_list = self.get_children_for_root()
            else:
                child_list = self.parent_child_dict.get(node.uid, [])
            return child_list

    def get_node_for_goog_id_and_parent_uid(self, goog_id: str, parent_uid: UID) -> Optional[GDriveNode]:
        """Finds the GDrive node with the given goog_id. (Parent UID is needed so that we don't have to search the entire tree"""
        parent = self.get_node_for_uid(parent_uid)
        if parent:
            children = self.get_children(parent)
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
            children = self.get_children(parent)
            if children:
                for child in children:
                    assert isinstance(child, GDriveNode)
                    if child.name == name:
                        return child
        return None

    def get_subtree_bfs(self, subtree_root: GDriveNode) -> List[GDriveNode]:
        """Returns all nodes in the subtree in BFS order"""
        queue: Deque[GDriveNode] = deque()
        bfs_list: List[GDriveNode] = []

        queue.append(subtree_root)
        bfs_list.append(subtree_root)

        # everything else ...
        while len(queue) > 0:
            node: GDriveNode = queue.popleft()

            children = self.get_children(node)
            if children:
                for child in children:
                    assert isinstance(child, GDriveNode)
                    bfs_list.append(child)

                    if child.is_dir():
                        assert isinstance(child, GDriveNode)
                        queue.append(child)

        return bfs_list

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

    def _compute_path_list_for_uid(self, uid: UID) -> List[str]:
        """Gets the filesystem-like-path for the node with the given GoogID.
        Stops when a parent cannot be found, or the root of the tree is reached."""
        current_node: GDriveNode = self.get_node_for_uid(uid)
        if not current_node:
            raise RuntimeError(f'Item not found: id={uid}')

        # TODO: it's possible to optimize this by using the parent paths, if available

        path_list: List[str] = []
        # Iterate backwards (the given ID is the last segment in the path
        current_nodes: List[Tuple[GDriveNode, str]] = [(current_node, '')]
        next_segment_nodes: List[Tuple[GDriveNode, str]] = []
        while current_nodes:
            for node, path_so_far in current_nodes:
                if path_so_far == '':
                    # first node (leaf)
                    path_so_far = node.name
                else:
                    # Pre-pend parent name:
                    path_so_far = node.name + '/' + path_so_far

                parent_uids: List[UID] = node.get_parent_uids()
                if parent_uids:
                    if len(parent_uids) > 1:
                        # Make sure they are not dead links:
                        parent_uids: List[UID] = [x for x in parent_uids if self.get_node_for_uid(x)]
                        if len(parent_uids) > 1:
                            if SUPER_DEBUG:
                                logger.debug(f'Multiple parents found for {node.uid} ("{node.name}").')
                                for parent_index, parent_uid in enumerate(parent_uids):
                                    logger.debug(f'Parent {parent_index}: {parent_uid}')
                            # pass through
                        elif SUPER_DEBUG:
                            logger.warning(f'Found multiple parents for node but only one could be resolved: node={node.uid} ("{node.name}")')
                    for parent_uid in parent_uids:
                        parent_node: GDriveNode = self.get_node_for_uid(parent_uid)
                        if parent_node:
                            next_segment_nodes.append((parent_node, path_so_far))
                        else:
                            # Parent refs cannot be resolved == root of subtree
                            if SUPER_DEBUG:
                                logger.debug(f'Mapped ID "{uid}" to subtree path "{path_so_far}"')
                            if path_so_far not in path_list:
                                path_list.append(path_so_far)

                else:
                    # No parent refs. Root of Google Drive
                    path_list.append('/' + path_so_far)
            current_nodes = next_segment_nodes
            next_segment_nodes = []
        return path_list

    def get_summary(self) -> str:
        # FIXME: this is broken
        super_root_node = self.get_node_for_uid(GDRIVE_ROOT_UID)
        if super_root_node.is_stats_loaded():
            size_bytes = 0
            trashed_bytes = 0
            file_count = 0
            dir_count = 0
            trashed_file_count = 0
            trashed_dir_count = 0
            for root in self.get_children_for_root():
                if root.get_trashed_status() == TrashStatus.NOT_TRASHED:
                    if root.get_size_bytes():
                        size_bytes += root.get_size_bytes()

                        if root.is_dir():
                            assert isinstance(root, GDriveFolder)
                            dir_count += root.dir_count + 1
                            file_count += root.file_count
                        else:
                            file_count += 1
                else:
                    # trashed:
                    if root.is_dir():
                        assert isinstance(root, GDriveFolder)
                        if root.get_size_bytes():
                            trashed_bytes += root.get_size_bytes()
                        if root.trashed_bytes:
                            trashed_bytes += root.trashed_bytes
                        trashed_dir_count += root.dir_count + root.trashed_dir_count + 1
                        trashed_file_count += root.file_count + root.trashed_file_count
                    else:
                        trashed_file_count += 1
                        if root.get_size_bytes():
                            trashed_bytes += root.get_size_bytes()

                if root.is_dir():
                    trashed_bytes += root.trashed_bytes
                    file_count += root.file_count
                else:
                    file_count += 1

            size_hf = format.humanfriendlier_size(size_bytes)
            trashed_size_hf = format.humanfriendlier_size(trashed_bytes)
            return f'{size_hf} total in {file_count:n} files & {dir_count:n} folders (including {trashed_size_hf} in ' \
                   f'{trashed_file_count:n} files & {trashed_dir_count:n} folders trashed) in Google Drive'
        else:
            return 'Loading stats...'

    def find_duplicate_node_names(self):
        """Finds and builds a list of all nodes which have the same name inside the same folder"""
        queue: Deque[GDriveNode] = deque()
        child_dict: DefaultDict[str, List[GDriveNode]] = defaultdict(list)

        duplicates: List[List[GDriveNode]] = []

        # roots ...
        for root in self.get_children_for_root():
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

            children = self.get_children(node)
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

    def for_each_node_breadth_first(self, action_func: Callable, subtree_root_uid: Optional[UID] = None):
        dir_queue: Deque[GDriveFolder] = deque()
        if subtree_root_uid:
            # Partial
            subtree_root = self.get_node_for_uid(subtree_root_uid)
            if not subtree_root:
                return

            action_func(subtree_root)

            if subtree_root.is_dir():
                assert isinstance(subtree_root, GDriveFolder)
                dir_queue.append(subtree_root)
        else:
            for root in self.get_children_for_root():
                if root.is_dir():
                    assert isinstance(root, GDriveFolder)
                    dir_queue.append(root)
                action_func(root)

        while len(dir_queue) > 0:
            node: GDriveFolder = dir_queue.popleft()

            children = self.get_children(node)
            if children:
                for child in children:
                    if child.is_dir():
                        assert isinstance(child, GDriveFolder)
                        dir_queue.append(child)
                    action_func(child)

    def refresh_stats(self, subtree_root: Optional[GDriveFolder] = None, tree_id: str = None):
        # FIXME: this is broken
        # Calculates the stats for all the directories
        logger.debug(f'[{tree_id}] Refreshing stats for GDrive tree (subtree={subtree_root})')
        stats_sw = Stopwatch()
        second_pass_stack: Deque[GDriveFolder] = deque()

        if subtree_root:
            subtree_root_uid = subtree_root.uid
        else:
            subtree_root_uid = None

        def zero_out_stats_and_add_dirs_to_stack(n):
            if n.is_dir():
                assert isinstance(n, GDriveFolder)
                n.zero_out_stats()
                second_pass_stack.append(n)

        self.for_each_node_breadth_first(action_func=zero_out_stats_and_add_dirs_to_stack, subtree_root_uid=subtree_root_uid)

        # 2nd pass:
        while len(second_pass_stack) > 0:
            node = second_pass_stack.pop()
            assert node.is_dir()

            children = self.get_children(node)
            if children:
                for child in children:
                    node.add_meta_metrics(child)
            else:
                node.set_stats_for_no_children()

            # if SUPER_DEBUG:
            #     logger.debug(f'Folder node {node.uid} ("{node.name}") has size={node.get_size_bytes()}, etc={node.get_etc()}')

        # TODO: make use of this later
        if FIND_DUPLICATE_GDRIVE_NODE_NAMES:
            self.find_duplicate_node_names()

        if COUNT_MULTIPLE_GDRIVE_PARENTS:
            self.count_multiple_parents()

        logger.debug(f'{stats_sw} Refreshed stats for Google Drive tree')

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

    existing_node.update_from(new_node)

    # Need to return these so they can be added to reverse dict
    return new_parent_uids, removed_parent_uids