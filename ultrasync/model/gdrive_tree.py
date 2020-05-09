import errno
import logging
import os
from typing import Any, Dict, List, Optional, Union, ValuesView

import constants
import file_util
from index.two_level_dict import Md5BeforeUidDict
from model.category import Category
from model.display_id import GDriveIdentifier, Identifier
from model.goog_node import FolderToAdd, GoogFile, GoogFolder, GoogNode
from model.planning_node import FileDecoratorNode, PlanningNode
from model.subtree_snapshot import SubtreeSnapshot

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
        self.id_dict: Dict[str, GoogNode] = {}
        """ Forward lookup table: nodes are indexed by GOOG ID"""

        self.first_parent_dict: Dict[str, List[GoogNode]] = {}
        """ Reverse lookup table: 'parent_id' -> list of child nodes """

    @property
    def tree_type(self) -> int:
        return constants.OBJ_TYPE_GDRIVE

    @classmethod
    def get_root_constant_identifier(cls):
        return GDriveIdentifier(uid=constants.ROOT, full_path=constants.ROOT)

    def get_children(self, parent_id: Union[str, Identifier]) -> List[GoogNode]:
        if isinstance(parent_id, Identifier):
            parent_id = parent_id.uid

        return self.first_parent_dict.get(parent_id, [])

    def get_for_id(self, goog_id) -> Optional[GoogNode]:
        assert goog_id
        return self.id_dict.get(goog_id, None)

    def get_path_for_id(self, goog_id: str, stop_before_id: str = None) -> str:
        """Gets the filesystem-like-path for the item with the given GoogID.
        If stop_before_id is given, treat it as the subtree root and stop before including it; otherwise continue
        until a parent cannot be found, or until the root of the tree is reached"""
        item = self.get_for_id(goog_id)
        if not item:
            raise RuntimeError(f'Item not found: id={goog_id}')

        # Iterate backwards (the given ID is the last segment in the path
        path = ''
        while True:
            if item.uid == stop_before_id:
                return path
            if path == '':
                path = item.name
            else:
                path = item.name + '/' + path
            parent_ids: List[str] = item.parent_ids
            if parent_ids:
                if len(parent_ids) > 1:
                    resolved_parent_ids = [x for x in parent_ids if self.get_for_id(x)]
                    if len(resolved_parent_ids) > 1:
                        # If we see this, need to investigate
                        logger.warning(f'Multiple parents found for {item.uid} ("{item.name}"). Picking the first one.')
                        for parent_num, p in enumerate(resolved_parent_ids):
                            logger.info(f'Parent {parent_num}: {p}')
                        # pass through
                    elif SUPER_DEBUG:
                        logger.debug(f'Found multiple parents for item but only one is valid: item={item.uid} ("{item.name}")')
                    item = self.get_for_id(resolved_parent_ids[0])
                    # pass through
                else:
                    item = self.get_for_id(parent_ids[0])

                if not item:
                    # Parent refs cannot be resolved == root of subtree
                    if SUPER_DEBUG:
                        logger.debug(f'Mapped ID "{goog_id}" to subtree path "{path}"')
                    return path
            else:
                # No parent refs. Root of Google Drive
                return '/' + path

    def add_item(self, item: GoogNode):
        """Called when adding from Google API, or when slicing a metastore"""

        # Build forward dictionary
        existing_item = self.id_dict.get(item.uid, None)
        if existing_item:
            if SUPER_DEBUG:
                logger.debug(f'add_item(): found existing item with same ID (will attempt to merge items): existing: {existing_item}; new={item}')
            _merge_items(existing_item, item)
        else:
            self.id_dict[item.uid] = item

        # build reverse dictionary
        parent_ids: List[str] = item.parent_ids
        if len(parent_ids) > 0:
            for parent_id in parent_ids:
                self._add_to_parent_dict(parent_id, item)

        # This may not be the same object which came in
        return item

    def _add_to_parent_dict(self, parent_id: str, item):
        child_list: List[GoogNode] = self.first_parent_dict.get(parent_id)
        if not child_list:
            child_list: List[GoogNode] = []
            self.first_parent_dict[parent_id] = child_list
        child_list.append(item)


def _merge_items(existing_item: GoogNode, new_item: GoogNode):
    # Assume items are identical but each references a different parent (most likely flattened for SQL)
    assert len(existing_item.parent_ids) == 1 and len(
        new_item.parent_ids) == 1, f'Expected 1 parent each but found: {existing_item.parent_ids} and {new_item.parent_ids}'
    # Just merge into the existing item
    existing_item.parent_ids = list(set(existing_item.parent_ids) | set(new_item.parent_ids))


"""
◤━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━◥
    CLASS GDriveWholeTree
    Represents the entire GDrive tree
◣━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━◢
"""


class GDriveWholeTree(GDriveTree):
    def __init__(self):
        # Keep track of parentless nodes. These include the 'My Drive' item, as well as shared items.
        super().__init__()
        self.roots: List[GoogNode] = []

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

    def get_full_path_for_item(self, item: GoogNode) -> str:
        """Gets the absolute path for the item"""
        if item.full_path:
            # Does item already have a full_path? Just return that (huge speed gain):
            return item.full_path

        # Set in the item for future use:
        full_path = self.get_path_for_id(item.uid)
        item.identifier.full_path = full_path
        return full_path

    def add_item(self, item):
        """Called when adding from Google API, or when slicing a metastore"""
        added_item = super().add_item(item)

        if added_item and len(added_item.parent_ids) == 0:
            self.roots.append(item)

        return item

    def get_all_ids_for_path(self, path: str) -> List[Identifier]:
        """Try to match the given file-system-like path, mapping the root of this tree to the first segment of the path.
        Since GDrive allows for multiple parents per child, it is possible for multiple matches to occur. This
        returns them all.
        """
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
        current_seg_items: List = [x for x in self.roots if x.name.lower() == seg.lower()]
        next_seg_items = []
        path_found = '/'
        if current_seg_items:
            path_found += current_seg_items[0].name

        for name_seg in iter_name_segs:
            path_so_far = path_so_far + '/' + name_seg
            for current in current_seg_items:
                current_id = current.uid
                children = self.get_children(current_id)
                if not children:
                    logger.debug(f'Item has no children: id="{current_id}" path_so_far="{path_so_far}"')
                    break
                matches: List = [x for x in children if x.name.lower() == name_seg.lower()]
                if len(matches) > 1:
                    logger.info(f'get_all_ids_for_path(): Multiple child IDs ({len(matches)}) found for parent ID"'
                                f'{current_id}", path_so_far "{path_so_far}"')
                    for num, match in enumerate(matches):
                        logger.info(f'Match {num}: {match}')
                next_seg_items += matches

            if len(next_seg_items) == 0:
                logger.debug(f'Segment not found: {name_seg}')
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


"""
◤━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━◥
    CLASS GDriveSubtree
    Represents a slice of the whole tree.
    Has categories and MD5 for comparison
◣━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━◢
"""


class GDriveSubtree(GDriveTree, SubtreeSnapshot):
    def __init__(self, root_node: GoogNode):
        GDriveTree.__init__(self)
        SubtreeSnapshot.__init__(self, root_identifier=root_node.identifier)
        self.root_node = root_node

        self._md5_dict: Md5BeforeUidDict = Md5BeforeUidDict()

        self._cat_dict: Dict[Category, List[GoogNode]] = {Category.Ignored: [],
                                                          Category.Added: [],
                                                          Category.Deleted: [],
                                                          Category.Moved: [],
                                                          Category.Updated: [],
                                                          }

    def create_empty_subtree(self, subtree_root_node: GoogNode):
        return GDriveSubtree(subtree_root_node)

    def create_identifier(self, full_path, category):
        return GDriveIdentifier(uid=full_path, full_path=full_path, category=category)

    @property
    def root_path(self):
        return self.root_node.full_path

    @property
    def root_id(self):
        return self.root_node.uid

    def __eq__(self, other):
        if self.uid == constants.ROOT:
            return other.uid == constants.ROOT
        return super().__eq__(other)

    def __ne__(self, other):
        return not self.__eq__(other)

    def __repr__(self):
        return f'GDriveSubtree(root_id={self.root_id} root_path="{self.root_path}" id_count={len(self.id_dict)} ' \
               f'parent_count={len(self.first_parent_dict)} md5_count={self._md5_dict.total_entries})'

    def get_for_cat(self, category: Category):
        return self._cat_dict[category]

    def get_for_md5(self, md5) -> Union[List[GoogNode], ValuesView[GoogNode]]:
        uid_dict = self._md5_dict.get_second_dict(md5)
        if uid_dict:
            return uid_dict.values()
        return []

    def get_md5_set(self):
        return self._md5_dict.keys()

    def get_all(self) -> ValuesView[GoogNode]:
        """Returns the complete set of all unique items from this subtree."""
        return self.id_dict.values()

    def get_full_path_for_item(self, item: GoogNode) -> str:
        """Gets the absolute path for the item"""
        if item.full_path:
            # Does item already have a full_path? Just return that (huge speed gain):
            return item.full_path

        rel_path = self.get_path_for_id(item.uid, self.root_id)
        full_path = os.path.join(self.root_path, rel_path)
        # Set in the item for future use:
        item.identifier.full_path = full_path
        return full_path

    def get_for_path(self, path: str, include_ignored=False) -> Optional[GoogNode]:
        """Try to get a singular item corresponding to the given file-system-like
        path, mapping the root of this tree to the first segment of the path.

        We can probably cache this mapping in the future if performance is a concern
        """
        relative_path = file_util.strip_root(path, self.root_path)
        name_segments = file_util.split_path(relative_path)
        current = self.root_node
        path_so_far = ''
        for name_seg in name_segments:
            path_so_far = os.path.join(path_so_far, name_seg)
            children = self.get_children(current.uid)
            if children:
                matches = [x for x in children if x.name == name_seg]
                if matches:
                    if len(matches) > 1:
                        logger.error(f'get_for_path(): Multiple child IDs ({len(matches)}) found for parent ID "{current.uid}", '
                                     f'tree "{self.root_path}", path "{path_so_far}". Choosing the first found')
                        for num, match in enumerate(matches):
                            logger.warning(f'Match {num}: {match}')
                    current = matches[0]
                    continue
                # fall through

            if SUPER_DEBUG:
                logger.debug(f'No match found for path: {path_so_far}')
            return None

        if SUPER_DEBUG:
            logger.debug(f'Found for path "{path}": {current}')
        return current

    def add_item(self, item):
        """Called when adding from Google API, or when slicing a metastore"""

        # Some extra validation here to be safe:
        is_planning_node = isinstance(item, PlanningNode)
        existing_item = self.id_dict.get(item.uid, None)
        if existing_item is not None:
            if is_planning_node:
                if not isinstance(existing_item, PlanningNode):
                    raise RuntimeError(f'Attempt to overwrite type {type(existing_item)} with PlanningNode!')

        if isinstance(item, FileDecoratorNode):
            # Add fake parents
            self.make_parents_if_not_exist(item)

            if SUPER_DEBUG:
                logger.debug(f'Adding new PlanningNode: {item.name}')

        # Do the parent work here:
        added_item = super().add_item(item)

        # Do this after any merging we do above
        if not added_item.is_dir() and added_item.md5:
            if file_util.is_target_type(added_item.name, constants.VALID_SUFFIXES):
                previous = self._md5_dict.put(added_item)
                if previous:
                    logger.warning(f'Overwrote existing MD5/ID pair: {previous}')
            else:
                added_item.identifier.category = Category.Ignored

        if item.category != Category.NA:
            self._cat_dict[item.category].append(item)

    def validate(self):
        logger.debug(f'Validating GDriveSubtree "{self.root_path}"...')
        if not self.root_id:
            logger.error('No root ID!')

        if not self.root_path:
            logger.error('No root path!')

        # Validate parent dict:
        for parent_id, children in self.first_parent_dict.items():
            unique_child_ids = {}
            for child in children:
                if not self.get_for_id(child.uid):
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
                resolved_parent_ids = [x for x in item.parent_ids if self.get_for_id(x)]
                if len(resolved_parent_ids) > 1:
                    logger.error(f'Found multiple valid parent_ids for item: {item}: parent_ids={resolved_parent_ids}')

        logger.debug(f'Done validating GDriveSubtree "{self.root_path}"')

    def clear_categories(self):
        for cat, cat_list in self._cat_dict.items():
            if cat != Category.Ignored:
                cat_list.clear()

    def validate_categories(self):
        # TODO
        pass

    def get_ancestor_identifiers_as_list(self, item: GoogNode) -> List[Identifier]:
        identifiers = []
        while True:
            if item.parent_ids:
                if len(item.parent_ids) > 1:
                    resolved_parent_ids = [x for x in item.parent_ids if self.get_for_id(x)]
                    if len(resolved_parent_ids) > 1:
                        logger.error(f'Found multiple valid parents for item: {item}: parents={resolved_parent_ids}')
                    assert len(resolved_parent_ids) == 1
                    item = self.get_for_id(resolved_parent_ids[0])
                else:
                    item = self.get_for_id(item.parent_ids[0])
                if item and item.uid != self.identifier.uid:
                    identifiers.append(item.identifier)
                    continue
            identifiers.reverse()
            return identifiers

    def get_relative_path_for_item(self, goog_node: GoogNode):
        if goog_node.full_path:
            return file_util.strip_root(goog_node.full_path, self.root_path)
        # Get the path for the given ID, relative to the root of this subtree
        return self.get_path_for_id(goog_node.uid, self.root_id)

    def get_summary(self):
        file_count = 0
        folder_count = 0
        for item in self.id_dict.values():
            if item.is_dir():
                folder_count += 1
            else:
                file_count += 1
                folder_count += 1
        return f'{file_count:n} files and {folder_count:n} folders in subtree '

    def categorize(self, item: GoogNode, category: Category):
        assert category != Category.NA
        # param item should already be a member of this tree
        assert self.get_for_id(goog_id=item.uid) == item
        item.identifier.category = category
        return self._cat_dict[category].append(item)

    def get_category_summary_string(self):
        summary = []
        for cat in self._cat_dict.keys():
            length = len(self._cat_dict[cat])
            summary.append(f'{cat.name}={length}')
        return ' '.join(summary)

    def make_parents_if_not_exist(self, item: FileDecoratorNode):
        """
        Compare this to get_for_path(). TODO: combine these
        """
        relative_path = file_util.strip_root(item.dest_path, self.root_path)
        name_segments = file_util.split_path(relative_path)
        # Skip last item (it's the file name)
        name_segments.pop()
        current: GoogNode = self.root_node
        path_so_far = self.root_node.full_path
        for name_seg in name_segments:
            path_so_far = os.path.join(path_so_far, name_seg)
            children: List[GoogNode] = self.get_children(current.uid)
            if children:
                matches = [x for x in children if x.name == name_seg]
                if len(matches) > 1:
                    logger.error(f'get_for_path(): Multiple child IDs ({len(matches)}) found for parent ID"{current.uid}", '
                                 f'tree "{self.root_path}" Choosing the first found')
                    for num, match in enumerate(matches):
                        logger.info(f'Match {num}: {match}')
                    current = matches[0]
                    continue
                elif len(matches) == 1:
                    current = matches[0]
                    continue

            if SUPER_DEBUG:
                logger.debug(f'Creating new fake folder for: {path_so_far}')
            new_folder: FolderToAdd = FolderToAdd(dest_path=path_so_far)
            new_folder.parent_ids = current.uid
            self.add_item(new_folder)
            current = new_folder

        item.parent_ids = current.uid
