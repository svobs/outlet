import errno
import logging
import os
from typing import Dict, List, Optional, Tuple, Union, ValuesView

import constants
import file_util
from model.display_id import GDriveIdentifier, Identifier
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

    @classmethod
    def get_root_constant_identifier(cls):
        return GDriveIdentifier(uid=constants.ROOT, full_path=constants.ROOT)


"""
◤━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━◥
    CLASS GDriveWholeTree
    Represents the entire GDrive tree
◣━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━◢
"""


class GDriveWholeTree(GDriveTree):
    def __init__(self):
        super().__init__()
        # Keep track of parentless nodes. These include the 'My Drive' item, as well as shared items.
        self.roots: List[GoogNode] = []
        self.id_dict: Dict[str, GoogNode] = {}
        """ Forward lookup table: nodes are indexed by GOOG ID"""

        self.first_parent_dict: Dict[str, List[GoogNode]] = {}
        """ Reverse lookup table: 'parent_id' -> list of child nodes """

        self.ids_with_multiple_parents: List[str] = []
        """List of item_ids which have more than 1 parent"""

        self.me: Optional[UserMeta] = None
        self.path_dict = None
        self.owner_dict = {}
        self.mime_types = {}
        self.shortcuts = {}

    @property
    def identifier(self):
        return GDriveTree.get_root_constant_identifier()

    def get_full_paths_for_item(self, item: GoogNode) -> List[str]:
        """Gets the absolute path for the item"""
        if item.full_path:
            # Does item already have a full_path? Just return that (huge speed gain):
            return item.full_path

        # Set in the item for future use:
        full_paths: List[str] = self.get_all_paths_for_id(item.uid)
        if len(full_paths) == 1:
            item.identifier.full_path = full_paths[0]
        else:
            item.identifier.full_path = full_paths
        return full_paths

    def add_item(self, item: GoogNode):
        """Called when adding from Google API"""

        assert not isinstance(item, PlanningNode)

        parent_ids: List[str] = item.parent_ids

        # Build forward dictionary
        existing_item = self.id_dict.get(item.uid, None)
        if existing_item:
            if SUPER_DEBUG:
                logger.debug(f'add_item(): found existing item with same ID (will attempt to merge items): existing: {existing_item}; new={item}')
            parent_ids = _merge_items(existing_item, item)
        else:
            self.id_dict[item.uid] = item

        # build reverse dictionary
        if len(parent_ids) > 0:
            for parent_id in parent_ids:
                self._add_to_parent_dict(parent_id, item)

        if item and not item.parent_ids:
            self.roots.append(item)

        # This may not be the same object which came in
        return item

    def _add_to_parent_dict(self, parent_id: str, item):
        child_list: List[GoogNode] = self.first_parent_dict.get(parent_id)
        if not child_list:
            child_list: List[GoogNode] = []
            self.first_parent_dict[parent_id] = child_list
        child_list.append(item)

    def get_all(self) -> ValuesView[GoogNode]:
        """Returns the complete set of all unique items from this subtree."""
        return self.id_dict.values()

    def get_all_ids_for_path(self, path: str) -> List[Identifier]:
        """Try to match the given file-system-like path, mapping the root of this tree to the first segment of the path.
        Since GDrive allows for multiple parents per child, it is possible for multiple matches to occur. This
        returns them all.
        NOTE: returns FileNotFoundError if not even one ID could be matched
        """
        if SUPER_DEBUG:
            logger.debug(f'get_all_ids_for_path() requested for path: "{path}"')
        name_segments = file_util.split_path(path)
        if len(name_segments) == 0:
            raise RuntimeError(f'Bad path: "{path}"')
        # name_segments = list(map(lambda x: x.lower(), name_segments))
        iter_name_segs = iter(name_segments)
        seg = next(iter_name_segs)
        if seg == '/':
            # Strip off root prefix if there is one
            seg = next(iter_name_segs)
        path_so_far = '/' + seg
        current_seg_items: List[GoogNode] = [x for x in self.roots if x.name.lower() == seg.lower()]
        next_seg_items: List[GoogNode] = []
        path_found = '/'
        if current_seg_items:
            path_found += current_seg_items[0].name

        for name_seg in iter_name_segs:
            path_so_far = path_so_far + '/' + name_seg
            for current in current_seg_items:
                current_id = current.uid
                children: List[GoogNode] = self.get_children(current_id)
                if not children:
                    logger.debug(f'Item has no children: id="{current_id}" path_so_far="{path_so_far}"')
                    break
                matches: List[GoogNode] = [x for x in children if x.name.lower() == name_seg.lower()]
                if len(matches) > 1:
                    logger.info(f'get_all_ids_for_path(): Multiple child IDs ({len(matches)}) found for parent ID"'
                                f'{current_id}", path_so_far "{path_so_far}"')
                    for num, match in enumerate(matches):
                        logger.info(f'Match {num}: {match}')
                next_seg_items += matches

            if len(next_seg_items) == 0:
                if SUPER_DEBUG:
                    logger.debug(f'Segment not found: "{name_seg}" (target_path: "{path}"')
                raise FileNotFoundError(errno.ENOENT, os.strerror(errno.ENOENT), path_so_far)
            else:
                path_found = path_found + '/' + next_seg_items[0].name

            current_seg_items = next_seg_items
            next_seg_items = []
        matching_ids = list(map(lambda x: x.identifier, current_seg_items))
        for identifier in matching_ids:
            # Needs to be filled in:
            identifier.full_path = path_found
        if SUPER_DEBUG:
            logger.debug(f'Found for path "{path_so_far}": {matching_ids}')
        return matching_ids

    def validate(self):
        logger.debug(f'Validating GDriveWholeTree')
        # Validate parent dict:
        for parent_id, children in self.first_parent_dict.items():
            unique_child_ids = {}
            for child in children:
                if not self.get_item_for_id(child.uid):
                    logger.error(f'Child present in child list of parent {parent_id} but not found in id_dict: {child}')
                duplicate_child = unique_child_ids.get(child.uid)
                if duplicate_child:
                    logger.error(f'Child already present in list of parent {parent_id}: orig={duplicate_child} dup={child}')
                else:
                    unique_child_ids[child.uid] = child

        for item_id, item in self.id_dict.items():
            if item_id != item.uid:
                logger.error(f'[!!!] Item actual ID does not match its key in the ID dict ({item_id}): {item}')
            if len(item.parent_ids) > 1:
                resolved_parent_ids = [x for x in item.parent_ids if self.get_item_for_id(x)]
                if len(resolved_parent_ids) > 1:
                    logger.error(f'Found multiple valid parent_ids for item: {item}: parent_ids={resolved_parent_ids}')

        logger.debug(f'Done validating GDriveWholeTree')

    @property
    def tree_type(self) -> int:
        return constants.OBJ_TYPE_GDRIVE

    def get_children(self, parent_id: Union[str, Identifier]) -> List[GoogNode]:
        if isinstance(parent_id, Identifier):
            parent_id = parent_id.uid

        return self.first_parent_dict.get(parent_id, [])

    def get_item_for_id(self, goog_id: str) -> Optional[GoogNode]:
        assert goog_id
        return self.id_dict.get(goog_id, None)

    def get_all_paths_for_id(self, goog_id: str, stop_before_id: str = None) -> List[str]:
        """Gets the filesystem-like-path for the item with the given GoogID.
        If stop_before_id is given, treat it as the subtree root and stop before including it; otherwise continue
        until a parent cannot be found, or until the root of the tree is reached"""
        current_item: GoogNode = self.get_item_for_id(goog_id)
        if not current_item:
            raise RuntimeError(f'Item not found: id={goog_id}')

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

                parent_ids: List[str] = item.parent_ids
                if parent_ids:
                    if len(parent_ids) > 1:
                        # Make sure they are not dead links:
                        parent_ids = [x for x in parent_ids if self.get_item_for_id(x)]
                        if len(parent_ids) > 1:
                            if SUPER_DEBUG:
                                logger.debug(f'Multiple parents found for {item.uid} ("{item.name}").')
                                for parent_num, p in enumerate(parent_ids):
                                    logger.info(f'Parent {parent_num}: {p}')
                            # pass through
                        elif SUPER_DEBUG:
                            logger.debug(f'Found multiple parents for item but only one is valid: item={item.uid} ("{item.name}")')
                    for parent_id in parent_ids:
                        parent_item = self.get_item_for_id(parent_id)
                        if parent_item:
                            next_segment_items.append((parent_item, path_so_far))
                        else:
                            # Parent refs cannot be resolved == root of subtree
                            if SUPER_DEBUG:
                                logger.debug(f'Mapped ID "{goog_id}" to subtree path "{path_so_far}"')
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


def _merge_items(existing_item: GoogNode, new_item: GoogNode) -> List[str]:
    # Assume items are identical but each references a different parent (most likely flattened for SQL)
    assert len(existing_item.parent_ids) >= 1 and len(
        new_item.parent_ids) == 1, f'Expected 1 parent each but found: {existing_item.parent_ids} and {new_item.parent_ids}'

    new_parent_ids = []
    for parent_id in new_item.parent_ids:
        if parent_id not in existing_item.parent_ids:
            new_parent_ids.append(parent_id)

    # Merge into existing item:
    existing_item.parent_ids = existing_item.parent_ids + new_parent_ids

    # Need to return these so they can be added to reverse dict
    return new_parent_ids
