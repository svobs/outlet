import logging
from typing import Dict, List, Optional, Tuple, Union, ValuesView

import constants
import file_util
from index.error import GDriveItemNotFoundError
from index.uid_generator import AtomicIntUidGenerator, UID, UidGenerator
from model import node_identifier
from model.node_identifier import NodeIdentifier, NodeIdentifierFactory
from model.goog_node import GoogNode
from model.planning_node import PlanningNode

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
    CLASS GDriveTree
◣━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━◢
"""


class GDriveTree:
    def __init__(self):
        pass

"""
◤━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━◥
    CLASS GDriveWholeTree
    Represents the entire GDrive tree
◣━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━◢
"""


class GDriveWholeTree:
    def __init__(self):
        super().__init__()

        # Keep track of parentless nodes. These include the 'My Drive' item, as well as shared items.
        self.roots: List[GoogNode] = []
        self.id_dict: Dict[UID, GoogNode] = {}
        """ Forward lookup table: nodes are indexed by GOOG ID"""

        self.first_parent_dict: Dict[UID, List[GoogNode]] = {}
        """ Reverse lookup table: 'parent_uid' -> list of child nodes """

        self.me: Optional[UserMeta] = None
        self.path_dict = None
        self.owner_dict = {}
        self.mime_types = {}
        self.shortcuts = {}

    @property
    def node_identifier(self):
        return NodeIdentifierFactory.get_gdrive_root_constant_identifier()

    def get_full_path_for_item(self, item: GoogNode) -> List[str]:
        """Gets the absolute path for the item"""
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

    def add_item(self, item: GoogNode):
        """Adds an item. Assumes that the item has all necessary parent info filled in already,
        and does the heavy lifting and populates all data structures appropriately."""

        assert not isinstance(item, PlanningNode)

        parent_uids: List[UID] = item.parent_uids

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

        if not item.parent_uids:
            self.roots.append(item)

    def add_parent_mapping(self, item_uid: UID, parent_uid: UID):
        """Assuming that an item with the given UID has already been added to this tree, this method
        adds all the references to the various data structures which are needed to assign it a single parent."""
        assert item_uid
        assert parent_uid
        item = self.id_dict.get(item_uid)
        if not item:
            raise RuntimeError(f'Item not found: {item_uid}')

        # Add to dict:
        self._add_to_parent_dict(parent_uid, item)

        # Add ref in item:
        item.add_parent(parent_uid)

    def _add_to_parent_dict(self, parent_uid: UID, item):
        child_list: List[GoogNode] = self.first_parent_dict.get(parent_uid)
        if not child_list:
            child_list: List[GoogNode] = []
            self.first_parent_dict[parent_uid] = child_list
        child_list.append(item)

    def get_all(self) -> ValuesView[GoogNode]:
        """Returns the complete set of all unique items from this subtree."""
        return self.id_dict.values()

    def get_all_ids_for_path(self, path: str) -> List[NodeIdentifier]:
        """Try to match the given file-system-like path, mapping the root of this tree to the first segment of the path.
        Since GDrive allows for multiple parents per child, it is possible for multiple matches to occur. This
        returns them all.
        NOTE: returns FileNotFoundError if not even one ID could be matched
        """
        if SUPER_DEBUG:
            logger.debug(f'get_all_ids_for_path() requested for path: "{path}"')
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
        current_seg_items: List[GoogNode] = [x for x in self.roots if x.name.lower() == seg.lower()]
        next_seg_items: List[GoogNode] = []
        path_found = '/'
        if current_seg_items:
            path_found += current_seg_items[0].name

        for name_seg in iter_name_segs:
            path_so_far = path_so_far + '/' + name_seg
            for current in current_seg_items:
                current_id: UID = current.uid
                children: List[GoogNode] = self.get_children(current_id)
                if not children:
                    logger.debug(f'Item has no children: id="{current_id}" path_so_far="{path_so_far}"')
                    break
                matches: List[GoogNode] = [x for x in children if x.name.lower() == name_seg.lower()]
                if SUPER_DEBUG and len(matches) > 1:
                    logger.info(f'get_all_ids_for_path(): Multiple child IDs ({len(matches)}) found for parent ID "'
                                f'{current_id}", path_so_far "{path_so_far}"')
                    for num, match in enumerate(matches):
                        logger.info(f'Match {num}: {match}')
                next_seg_items += matches

            if len(next_seg_items) == 0:
                if SUPER_DEBUG:
                    logger.debug(f'Segment not found: "{name_seg}" (target_path: "{path}"')
                raise GDriveItemNotFoundError(node_identifier=NodeIdentifierFactory.for_values(tree_type=constants.OBJ_TYPE_GDRIVE, full_path=path),
                                              offending_path=path_so_far)
            else:
                path_found = path_found + '/' + next_seg_items[0].name

            current_seg_items = next_seg_items
            next_seg_items = []
        matching_ids = list(map(lambda x: x.node_identifier, current_seg_items))
        for node_identifier in matching_ids:
            # Needs to be filled in:
            node_identifier.full_path = path_found
        if SUPER_DEBUG:
            logger.debug(f'Found for path "{path_so_far}": {matching_ids}')
        if not matching_ids:
            raise GDriveItemNotFoundError(node_identifier=NodeIdentifierFactory.for_values(tree_type=constants.OBJ_TYPE_GDRIVE, full_path=path),
                                          offending_path=path_so_far)
        return matching_ids

    def validate(self):
        logger.debug(f'Validating GDriveWholeTree')
        # Validate parent dict:
        for parent_uid, children in self.first_parent_dict.items():
            unique_child_ids = {}
            for child in children:
                if not self.get_item_for_id(child.uid):
                    logger.error(f'Child present in child list of parent {parent_uid} but not found in id_dict: {child}')
                duplicate_child = unique_child_ids.get(child.uid)
                if duplicate_child:
                    logger.error(f'Child already present in list of parent {parent_uid}: orig={duplicate_child} dup={child}')
                else:
                    unique_child_ids[child.uid] = child

        for item_id, item in self.id_dict.items():
            if item_id != item.uid:
                logger.error(f'[!!!] Item actual ID does not match its key in the ID dict ({item_id}): {item}')
            if len(item.parent_uids) > 1:
                resolved_parent_ids = [x for x in item.parent_uids if self.get_item_for_id(x)]
                if len(resolved_parent_ids) > 1:
                    logger.error(f'Found multiple valid parent_uids for item: {item}: parent_uids={resolved_parent_ids}')

        logger.debug(f'Done validating GDriveWholeTree')

    @property
    def tree_type(self) -> int:
        return constants.OBJ_TYPE_GDRIVE

    def get_children(self, parent_uid: Union[UID, NodeIdentifier]) -> List[GoogNode]:
        if isinstance(parent_uid, NodeIdentifier):
            parent_uid: UID = parent_uid.uid

        return self.first_parent_dict.get(parent_uid, [])

    def get_item_for_id(self, uid: UID) -> Optional[GoogNode]:
        assert uid
        return self.id_dict.get(uid, None)

    def resolve_uids_to_goog_ids(self, uids: List[UID]):
        goog_ids: List[str] = []
        for uid in uids:
            item = self.get_item_for_id(uid)
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
        current_item: GoogNode = self.get_item_for_id(uid)
        if not current_item:
            raise RuntimeError(f'Item not found: id={uid}')

        path_list: List[str] = []
        # Iterate backwards (the given ID is the last segment in the path
        current_items: List[Tuple[GoogNode, str]] = [(current_item, '')]
        next_segment_items: List[Tuple[GoogNode, str]] = []
        while current_items:
            for item, path_so_far in current_items:
                if item.uid == stop_before_id:
                    path_list.append(path_so_far)
                    continue

                if path_so_far == '':
                    path_so_far = item.name
                else:
                    path_so_far = item.name + '/' + path_so_far

                parent_uids: List[UID] = item.parent_uids
                if parent_uids:
                    if len(parent_uids) > 1:
                        # Make sure they are not dead links:
                        parent_uids: List[UID] = [x for x in parent_uids if self.get_item_for_id(x)]
                        if len(parent_uids) > 1:
                            if SUPER_DEBUG:
                                logger.debug(f'Multiple parents found for {item.uid} ("{item.name}").')
                                for parent_num, p in enumerate(parent_uids):
                                    logger.info(f'Parent {parent_num}: {p}')
                            # pass through
                        elif SUPER_DEBUG:
                            logger.debug(f'Found multiple parents for item but only one is valid: item={item.uid} ("{item.name}")')
                    for parent_uid in parent_uids:
                        parent_item = self.get_item_for_id(parent_uid)
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
        file_count = 0
        folder_count = 0
        for item in self.id_dict.values():
            if item.is_dir():
                folder_count += 1
            else:
                file_count += 1
        return f'{file_count:n} files and {folder_count:n} folders in Google Drive '


def _merge_items(existing_item: GoogNode, new_item: GoogNode) -> List[UID]:
    # Assume items are identical but each references a different parent (most likely flattened for SQL)
    assert len(existing_item.parent_uids) >= 1 and len(
        new_item.parent_uids) == 1, f'Expected 1 parent each but found: {existing_item.parent_uids} and {new_item.parent_uids}'

    new_parent_ids: List[UID] = []
    for parent_uid in new_item.parent_uids:
        if parent_uid not in existing_item.parent_uids:
            new_parent_ids.append(parent_uid)

    # Merge into existing item:
    existing_item.parent_uids = existing_item.parent_uids + new_parent_ids

    # Need to return these so they can be added to reverse dict
    return new_parent_ids