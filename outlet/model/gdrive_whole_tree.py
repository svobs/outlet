import logging
from collections import defaultdict, deque
from typing import Callable, DefaultDict, Deque, Dict, List, Optional, Tuple

from pydispatch import dispatcher

import constants
from util import file_util, format
from index.error import GDriveItemNotFoundError
from index.uid.uid import UID
from model.node_identifier import GDriveIdentifier, NodeIdentifier
from model.node.gdrive_node import GDriveFile, GDriveFolder, GDriveNode
from model.node_identifier_factory import NodeIdentifierFactory
from util.stopwatch_sec import Stopwatch
from ui import actions

logger = logging.getLogger(__name__)

SUPER_DEBUG = False


"""
◤━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━◥
    CLASS UserMeta
◣━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━◢
"""


class UserMeta:
    def __init__(self, display_name, permission_id, email_address, photo_link):
        self.display_name = display_name
        self.permission_id = permission_id
        self.email_address = email_address
        self.photo_link = photo_link


"""
◤━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━◥
    CLASS GDriveWholeTree
◣━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━◢
"""


class GDriveWholeTree:
    """
    Represents the entire GDrive tree. We can't easily map this to DisplayTree, because the GDriveWholeTree can have multiple roots.
    """
    def __init__(self, node_identifier_factory):
        self.node_identifier_factory = node_identifier_factory
        self.node_identifier = NodeIdentifierFactory.get_gdrive_root_constant_identifier()
        """This is sometimes needed for lookups"""

        self._stats_loaded = False

        # Keep track of parentless nodes. These include the 'My Drive' item, as well as shared items.
        self.roots: List[GDriveNode] = []
        self.id_dict: Dict[UID, GDriveNode] = {}
        """ Forward lookup table: nodes are indexed by GOOG ID"""

        self.first_parent_dict: Dict[UID, List[GDriveNode]] = {}
        """ Reverse lookup table: 'parent_uid' -> list of child nodes """

        self.me: Optional[UserMeta] = None
        self.path_dict = None
        self.owner_dict = {}
        self.mime_types = {}
        self.shortcuts = {}

    def get_full_path_for_item(self, item: GDriveNode) -> List[str]:
        """Gets the absolute path for the item. Also sets its 'full_path' attribute for future use"""
        if item.full_path:
            # Does item already have a full_path? Just return that (huge speed gain):
            return item.full_path

        # Set in the item for future use:
        full_paths: List[str] = self.get_all_paths_for_id(item.uid)
        if len(full_paths) == 1:
            item.node_identifier.full_path = full_paths[0]
        else:
            item.node_identifier.full_path = full_paths
        return full_paths

    def add_item(self, item: GDriveNode):
        """Adds an item. Assumes that the item has all necessary parent info filled in already,
        and does the heavy lifting and populates all data structures appropriately."""

        parent_uids: List[UID] = item.get_parent_uids()

        # Build forward dictionary
        existing_item = self.id_dict.get(item.uid, None)
        if existing_item:
            logger.debug(f'add_item(): found existing item with same ID (will attempt to merge items): existing: {existing_item}; new={item}')
            parent_uids = _merge_items(existing_item, item)
        else:
            self.id_dict[item.uid] = item

        # build reverse dictionary
        if len(parent_uids) > 0:
            for parent_uid in parent_uids:
                self._add_to_parent_dict(parent_uid, item)

        if not item.get_parent_uids():
            self.roots.append(item)

    def remove_item(self, item: GDriveNode):
        """Remove given item from all data structures in this tree."""
        if item.uid not in self.id_dict:
            logger.warning(f'Cannot remove item from in-memory tree: it was not found in the tree: {item}')
            return

        if item.is_dir():
            # FIXME
            raise RuntimeError(f'Cannot remove folders yet: {item}')
        else:
            for parent_uid in item.get_parent_uids():
                child_list = self.first_parent_dict.get(parent_uid, [])
                if child_list:
                    # this may get expensive for folders with lots of items...may want to monitor performance
                    child_list.remove(item)

        if item in self.roots:
            self.roots.remove(item)

        self.id_dict.pop(item.uid, None)
        logger.debug(f'GDriveNode removed from in-memory tree: {item}')

    def add_parent_mapping(self, item_uid: UID, parent_uid: UID):
        """Assuming that an item with the given UID has already been added to this tree, this method
        adds all the references to the various data structures which are needed to assign it a single parent."""
        assert item_uid
        assert parent_uid
        item = self.id_dict.get(item_uid)
        if not item:
            raise RuntimeError(f'Cannot add parent mapping: Item not found with UID: {item_uid}')
        assert isinstance(item, GDriveNode)

        # Add to dict:
        self._add_to_parent_dict(parent_uid, item)

        # Add ref in item:
        item.add_parent(parent_uid)

    def _add_to_parent_dict(self, parent_uid: UID, item):
        child_list: List[GDriveNode] = self.first_parent_dict.get(parent_uid)
        if not child_list:
            child_list: List[GDriveNode] = []
            self.first_parent_dict[parent_uid] = child_list
        child_list.append(item)

    def get_all_files_and_folders_for_subtree(self, subtree_root: GDriveIdentifier) -> Tuple[List[GDriveFile], List[GDriveFolder]]:
        file_list: List[GDriveFile] = []
        folder_list: List[GDriveFolder] = []
        queue: Deque[GDriveNode] = deque()
        node = self.get_item_for_uid(uid=subtree_root.uid)
        if node:
            queue.append(node)

        while len(queue) > 0:
            node: GDriveNode = queue.popleft()
            if node.is_dir():
                assert isinstance(node, GDriveFolder)
                folder_list.append(node)
                for child in self.get_children(node):
                    queue.append(child)
            else:
                assert isinstance(node, GDriveFile)
                file_list.append(node)

        return file_list, folder_list

    def get_all_identifiers_for_path(self, path: str) -> List[NodeIdentifier]:
        """Try to match the given file-system-like path, mapping the root of this tree to the first segment of the path.
        Since GDrive allows for multiple parents per child, it is possible for multiple matches to occur. This
        returns them all.
        NOTE: returns FileNotFoundError if not even one ID could be matched
        """
        if SUPER_DEBUG:
            logger.debug(f'get_all_identifiers_for_path() requested for path: "{path}"')
        if path == constants.ROOT_PATH:
            return [NodeIdentifierFactory.get_gdrive_root_constant_identifier()]
        name_segments = file_util.split_path(path)
        if len(name_segments) == 0:
            raise RuntimeError(f'Bad path: "{path}"')
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
        current_seg_items: List[GDriveNode] = [x for x in self.roots if x.name.lower() == seg.lower()]
        next_seg_items: List[GDriveNode] = []
        path_found = '/'
        if current_seg_items:
            path_found += current_seg_items[0].name

        for name_seg in iter_name_segs:
            path_so_far = path_so_far + '/' + name_seg
            for current in current_seg_items:
                current_id: UID = current.uid
                children: List[GDriveNode] = self.get_children(current)
                if not children:
                    if SUPER_DEBUG:
                        logger.debug(f'Item has no children: id="{current_id}" path_so_far="{path_so_far}"')
                    break
                matches: List[GDriveNode] = [x for x in children if x.name.lower() == name_seg.lower()]
                if SUPER_DEBUG and len(matches) > 1:
                    logger.info(f'get_all_identifiers_for_path(): Multiple child IDs ({len(matches)}) found for parent ID "'
                                f'{current_id}", path_so_far "{path_so_far}"')
                    for num, match in enumerate(matches):
                        logger.info(f'Match {num}: {match}')
                next_seg_items += matches

            if len(next_seg_items) == 0:
                if SUPER_DEBUG:
                    logger.debug(f'Segment not found: "{name_seg}" (target_path: "{path}"')
                raise GDriveItemNotFoundError(node_identifier=self.node_identifier_factory.for_values(
                    tree_type=constants.TREE_TYPE_GDRIVE, full_path=path), offending_path=path_so_far)
            else:
                path_found = path_found + '/' + next_seg_items[0].name

            current_seg_items = next_seg_items
            next_seg_items = []
        matching_ids = list(map(lambda x: x.node_identifier, current_seg_items))
        for node_id in matching_ids:
            # Needs to be filled in:
            node_id.full_path = path_found
        if SUPER_DEBUG:
            logger.debug(f'Found for path "{path_so_far}": {matching_ids}')
        if not matching_ids:
            raise GDriveItemNotFoundError(node_identifier=self.node_identifier_factory.for_values(
                tree_type=constants.TREE_TYPE_GDRIVE, full_path=path), offending_path=path_so_far)
        return matching_ids

    def is_in_subtree(self, path: str, subtree_root_path: str):
        if isinstance(path, list):
            for p in path:
                # i.e. any
                if p.startswith(subtree_root_path):
                    return True
            return False

        return path.startswith(subtree_root_path)

    def get_parent_for_item(self, item: GDriveNode, required_subtree_path: str = None) -> Optional[GDriveNode]:
        parent_uids = item.get_parent_uids()
        if parent_uids:
            resolved_parents = []
            for par_id in parent_uids:
                parent = self.get_item_for_uid(par_id)
                if parent and (not required_subtree_path or self.is_in_subtree(parent.full_path, required_subtree_path)):
                    resolved_parents.append(parent)
            if len(resolved_parents) > 1:
                logger.error(f'Found multiple valid parents for item: {item}: parents={resolved_parents}')
            if len(resolved_parents) == 1:
                return resolved_parents[0]
        return None

    def get_ancestors(self, item: GDriveNode, stop_before_func: Callable[[GDriveNode], bool] = None) -> Deque[GDriveNode]:
        ancestors: Deque[GDriveNode] = deque()

        # Walk up the source tree, adding ancestors as we go, until we reach either a node which has already
        # been added to this tree, or the root of the source tree
        ancestor = item
        while ancestor:
            if stop_before_func is not None and stop_before_func(ancestor):
                return ancestors
            ancestor = self.get_parent_for_item(ancestor)
            if ancestor:
                ancestors.appendleft(ancestor)

        return ancestors

    def validate(self):
        logger.debug(f'Validating GDriveWholeTree')
        # Validate parent dict:
        for parent_uid, children in self.first_parent_dict.items():
            unique_child_ids = {}
            for child in children:
                if not self.get_item_for_uid(child.uid):
                    logger.error(f'Child present in child list of parent {parent_uid} but not found in id_dict: {child}')
                duplicate_child = unique_child_ids.get(child.uid)
                if duplicate_child:
                    logger.error(f'Child already present in list of parent {parent_uid}: orig={duplicate_child} dup={child}')
                else:
                    unique_child_ids[child.uid] = child

        for item_id, item in self.id_dict.items():
            if item_id != item.uid:
                logger.error(f'[!!!] Item actual ID does not match its key in the ID dict ({item_id}): {item}')
            if len(item.get_parent_uids()) > 1:
                resolved_parent_ids = [x for x in item.get_parent_uids() if self.get_item_for_uid(x)]
                if len(resolved_parent_ids) > 1:
                    logger.error(f'Found multiple valid parent_uids for item: {item}: parent_uids={resolved_parent_ids}')

        logger.debug(f'Done validating GDriveWholeTree')

    @property
    def tree_type(self) -> int:
        return constants.TREE_TYPE_GDRIVE

    def get_children_for_root(self) -> List[GDriveNode]:
        return self.roots

    def get_children(self, node: GDriveNode) -> List[GDriveNode]:
        return self.first_parent_dict.get(node.uid, [])

    def get_item_for_goog_id_and_parent_uid(self, goog_id: str, parent_uid: UID) -> Optional[GDriveNode]:
        """Finds the GDrive node with the given goog_id. (Parent UID is needed so that we don't have to search the entire tree"""
        parent = self.get_item_for_uid(parent_uid)
        if parent:
            children = self.get_children(parent)
            if children:
                for child in children:
                    if child.goog_id == goog_id:
                        return child
        return None

    def get_item_for_name_and_parent_uid(self, name: str, parent_uid: UID) -> Optional[GDriveNode]:
        """Returns the first GDrive node found with the given name and parent.
        This roughly matches the logic used to search for an item in Google Drive when we are unsure about its goog_id."""
        parent = self.get_item_for_uid(parent_uid)
        if parent:
            children = self.get_children(parent)
            if children:
                for child in children:
                    if child.name == name:
                        return child
        return None

    def get_item_for_uid(self, uid: UID) -> Optional[GDriveNode]:
        assert uid
        return self.id_dict.get(uid, None)

    def resolve_uids_to_goog_ids(self, uids: List[UID]):
        goog_ids: List[str] = []
        for uid in uids:
            item = self.get_item_for_uid(uid)
            if not item:
                raise RuntimeError(f'Could not resolve parent UID: {uid}')
            if not item.goog_id:
                raise RuntimeError(f'Item is missing Google ID: {item}')
            goog_ids.append(item.goog_id)
        return goog_ids

    def get_all_paths_for_id(self, uid: UID, stop_before_id: str = None) -> List[str]:
        """Gets the filesystem-like-path for the item with the given GoogID.
        If stop_before_id is given, treat it as the subtree root and stop before including it; otherwise continue
        until a parent cannot be found, or until the root of the tree is reached"""
        current_item: GDriveNode = self.get_item_for_uid(uid)
        if not current_item:
            raise RuntimeError(f'Item not found: id={uid}')

        # TODO: it's possible to optimize this by using the parent paths, if available

        path_list: List[str] = []
        # Iterate backwards (the given ID is the last segment in the path
        current_items: List[Tuple[GDriveNode, str]] = [(current_item, '')]
        next_segment_items: List[Tuple[GDriveNode, str]] = []
        while current_items:
            for item, path_so_far in current_items:
                if item.uid == stop_before_id:
                    path_list.append(path_so_far)
                    continue

                if path_so_far == '':
                    path_so_far = item.name
                else:
                    path_so_far = item.name + '/' + path_so_far

                parent_uids: List[UID] = item.get_parent_uids()
                if parent_uids:
                    if len(parent_uids) > 1:
                        # Make sure they are not dead links:
                        parent_uids: List[UID] = [x for x in parent_uids if self.get_item_for_uid(x)]
                        if len(parent_uids) > 1:
                            if SUPER_DEBUG:
                                logger.debug(f'Multiple parents found for {item.uid} ("{item.name}").')
                                for parent_num, p in enumerate(parent_uids):
                                    logger.info(f'Parent {parent_num}: {p}')
                            # pass through
                        elif SUPER_DEBUG:
                            logger.debug(f'Found multiple parents for item but only one is valid: item={item.uid} ("{item.name}")')
                    for parent_uid in parent_uids:
                        parent_item = self.get_item_for_uid(parent_uid)
                        if parent_item:
                            next_segment_items.append((parent_item, path_so_far))
                        else:
                            # Parent refs cannot be resolved == root of subtree
                            if SUPER_DEBUG:
                                logger.debug(f'Mapped ID "{uid}" to subtree path "{path_so_far}"')
                            path_list.append(path_so_far)

                else:
                    # No parent refs. Root of Google Drive
                    path_list.append('/' + path_so_far)
            current_items = next_segment_items
            next_segment_items = []
        return path_list

    def get_summary(self):
        if self._stats_loaded:
            size_bytes = 0
            trashed_bytes = 0
            file_count = 0
            dir_count = 0
            trashed_file_count = 0
            trashed_dir_count = 0
            for root in self.roots:
                if root.trashed == constants.NOT_TRASHED:
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

    def find_duplicate_node_names(self, tree_id):
        """Finds and builds a list of all nodes which have the same name inside the same folder"""
        queue: Deque[GDriveNode] = deque()
        stack: Deque[GDriveNode] = deque()
        child_dict: DefaultDict[str, List[GDriveNode]] = defaultdict(list)

        duplicates: List[List[GDriveNode]] = []

        # roots ...
        for root in self.roots:
            if root.is_dir():
                queue.append(root)
                stack.append(root)
            child_dict[root.name].append(root)

        for node_list in child_dict.values():
            if len(node_list) > 1:
                duplicates.append(node_list)

        # everything else ...
        while len(queue) > 0:
            item: GDriveNode = queue.popleft()
            child_dict: DefaultDict[str, List[GDriveNode]] = defaultdict(list)

            children = self.get_children(item)
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

    def refresh_stats(self, tree_id):
        # Calculates the stats for all the directories
        stats_sw = Stopwatch()
        queue: Deque[GDriveFolder] = deque()
        stack: Deque[GDriveFolder] = deque()
        for root in self.roots:
            if root.is_dir():
                assert isinstance(root, GDriveFolder)
                queue.append(root)
                stack.append(root)

        while len(queue) > 0:
            item: GDriveFolder = queue.popleft()
            item.zero_out_stats()

            children = self.get_children(item)
            if children:
                for child in children:
                    if child.is_dir():
                        assert isinstance(child, GDriveFolder)
                        queue.append(child)
                        stack.append(child)

        while len(stack) > 0:
            item = stack.pop()
            assert item.is_dir()

            children = self.get_children(item)
            if children:
                for child in children:
                    item.add_meta_metrics(child)

        self._stats_loaded = True
        actions.set_status(sender=tree_id, status_msg=self.get_summary())
        dispatcher.send(signal=actions.SUBTREE_STATS_UPDATED, sender=tree_id)

        # TODO: make use of this later
        self.find_duplicate_node_names(tree_id)

        logger.debug(f'{stats_sw} Refreshed stats for whole Google Drive tree')


def _merge_items(existing_item: GDriveNode, new_item: GDriveNode) -> List[UID]:
    # Assume items are identical but each references a different parent (most likely flattened for SQL)
    assert len(existing_item.get_parent_uids()) >= 1 and len(
        new_item.get_parent_uids()) == 1, f'Expected 1 parent each but found: {existing_item.get_parent_uids()} and {new_item.get_parent_uids()}'

    new_parent_ids: List[UID] = []
    for parent_uid in new_item.get_parent_uids():
        if parent_uid not in existing_item.get_parent_uids():
            new_parent_ids.append(parent_uid)

    # Merge into existing item:
    existing_item.set_parent_uids(existing_item.get_parent_uids() + new_parent_ids)

    # Need to return these so they can be added to reverse dict
    return new_parent_ids