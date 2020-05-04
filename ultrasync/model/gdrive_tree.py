import logging
import os
from typing import Any, Dict, List, Optional, Union

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

    def get_children(self, parent_id: Union[str, Identifier]):
        if isinstance(parent_id, Identifier):
            parent_id = parent_id.uid

        return self.first_parent_dict.get(parent_id, None)

    def get_for_id(self, goog_id) -> Optional[GoogNode]:
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
            parents = item.parents
            if parents:
                if len(parents) > 1:
                    resolved_parents = [x for x in parents if self.get_for_id(x)]
                    if len(resolved_parents) > 1:
                        # If we see this, need to investigate
                        logger.warning(f'Multiple parents found for {item.uid} ("{item.name}"). Picking the first one.')
                        for parent_num, p in enumerate(resolved_parents):
                            logger.info(f'Parent {parent_num}: {p}')
                        # pass through
                    elif SUPER_DEBUG:
                        logger.debug(f'Found multiple parents for item but only one is valid: item={item.uid} ("{item.name}")')
                    item = self.get_for_id(resolved_parents[0])
                    # pass through
                else:
                    item = self.get_for_id(parents[0])

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
        parents = item.parents
        if len(parents) > 0:
            for parent_id in parents:
                self._add_to_parent_dict(parent_id, item)

        # This may not be the same object which came in
        return item

    def _add_to_parent_dict(self, parent_id, item):
        child_list = self.first_parent_dict.get(parent_id)
        if not child_list:
            child_list = []
            self.first_parent_dict[parent_id] = child_list
        child_list.append(item)


def _merge_items(existing_item: GoogNode, new_item: GoogNode):
    # Assume items are identical but each references a different parent (most likely flattened for SQL)
    assert len(existing_item.parents) == 1 and len(
        new_item.parents) == 1, f'Expected 1 parent each but found: {existing_item.parents} and {new_item.parents}'
    # Just merge into the existing item
    existing_item.parents = list(set(existing_item.parents) | set(new_item.parents))


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

    def add_item(self, item):
        """Called when adding from Google API, or when slicing a metastore"""
        added_item = super().add_item(item)

        if added_item and len(added_item.parents) == 0:
            self.roots.append(item)

        return item


"""
◤━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━◥
    CLASS GDriveSubtree
    Represents a slice of the whole tree.
    Has categories and MD5 for comparison
◣━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━◢
"""


class GDriveSubtree(GDriveTree, SubtreeSnapshot):
    def __init__(self, root_identifier: GDriveIdentifier):
        GDriveTree.__init__(self)
        SubtreeSnapshot.__init__(self, root_identifier=root_identifier)
        """Filesystem-like-path. Used for reference when comparing to FMetaTree"""

        """GoogID for where to start"""

        self._md5_dict: Md5BeforeUidDict = Md5BeforeUidDict()

        self._cat_dict: Dict[Category, List[GoogNode]] = {Category.Ignored: [],
                                                          Category.Added: [],
                                                          Category.Deleted: [],
                                                          Category.Moved: [],
                                                          Category.Updated: [],
                                                          }

    def create_identifier(self, full_path, category):
        return GDriveIdentifier(uid=full_path, full_path=full_path, category=category)

    @property
    def root_path(self):
        return self.identifier.full_path

    @property
    def root_id(self):
        return self.identifier.uid

    def __repr__(self):
        return f'GDriveSubtree(root_id={self.root_id} root_path="{self.root_path}" id_count={len(self.id_dict)} ' \
               f'parent_count={len(self.first_parent_dict)} md5_count={self._md5_dict.total_entries})'

    def get_for_cat(self, category: Category):
        return self._cat_dict[category]

    def get_for_md5(self, md5) -> Optional[List[GoogNode]]:
        return self._md5_dict.get(md5, None)

    def get_md5_set(self):
        return self._md5_dict.keys()

    def get_full_path_for_item(self, item: GoogNode) -> str:
        """Gets the absolute path for the item"""
        rel_path = self.get_path_for_id(item.uid, self.root_id)
        return os.path.join(self.root_path, rel_path)

    def get_for_path(self, path: str, include_ignored=False) -> Optional[GoogNode]:
        """Try to get a singular item corresponding to the given file-system-like
        path, mapping the root of this tree to the first segment of the path.

        We can probably cache this mapping in the future if performance is a concern
        """
        relative_path = file_util.strip_root(path, self.root_path)
        name_segments = file_util.split_path(relative_path)
        current_id = self.root_id
        current = None
        path_so_far = ''
        for name_seg in name_segments:
            path_so_far = os.path.join(path_so_far, name_seg)
            children = self.get_children(current_id)
            if not children:
                raise RuntimeError(f'Item has no children: id="{current_id}" path_so_far="{path_so_far}"')
            matches = [x for x in children if x.name == name_seg]
            if len(matches) > 1:
                logger.error(f'get_for_path(): Multiple child IDs ({len(matches)}) found for parent ID"{current_id}", '
                             f'tree "{self.root_path}", path "{path_so_far}". Choosing the first found')
                for num, match in enumerate(matches):
                    logger.info(f'Match {num}: {match}')
            elif len(matches) == 0:
                if SUPER_DEBUG:
                    logger.debug(f'No match found for path: {path_so_far}')
                return None
            current = matches[0]
            current_id = current.uid
        if SUPER_DEBUG:
            logger.debug(f'Found for path "{path}": {self.get_for_id(current_id)}')
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
        if isinstance(added_item, GoogFile) and added_item.md5:
            if file_util.is_target_type(added_item.name, constants.VALID_SUFFIXES):
                self._md5_dict.get(added_item.md5, added_item.uid)
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
            if len(item.parents) > 1:
                resolved_parents = [x for x in item.parents if self.get_for_id(x)]
                if len(resolved_parents) > 1:
                    logger.error(f'Found multiple valid parents for item: {item}: parents={resolved_parents}')

        logger.debug(f'Done validating GDriveSubtree "{self.root_path}"')

    def clear_categories(self):
        for cat, cat_list in self._cat_dict.items():
            if cat != Category.Ignored:
                cat_list.clear()

    def validate_categories(self):
        # TODO
        pass

    def get_relative_path_for_item(self, goog_node: GoogNode):
        if goog_node.full_path:
            return file_util.strip_root(goog_node.full_path, self.root_path)
        # Get the path for the given ID, relative to the root of this subtree
        return self.get_path_for_id(goog_node.uid, self.root_id)

    def get_summary(self):
        file_count = 0
        folder_count = 0
        for item in self.id_dict.values():
            if isinstance(item, GoogFile):
                file_count += 1
            elif isinstance(item, GoogFolder):
                folder_count += 1
        return f'{file_count} files and {folder_count} folders'

    def categorize(self, item, category: Category):
        assert category != Category.NA
        # param fmeta should already be a member of this tree
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
        current_id = self.root_id
        path_so_far = ''
        for name_seg in name_segments:
            path_so_far = os.path.join(path_so_far, name_seg)
            children = self.get_children(current_id)
            if children:
                matches = [x for x in children if x.name == name_seg]
                if len(matches) > 1:
                    logger.error(f'get_for_path(): Multiple child IDs ({len(matches)}) found for parent ID"{current_id}", '
                                 f'tree "{self.root_path}" Choosing the first found')
                    for num, match in enumerate(matches):
                        logger.info(f'Match {num}: {match}')
                    current_id = matches[0].uid
                    continue
                elif len(matches) == 1:
                    current_id = matches[0].uid
                    continue

            if SUPER_DEBUG:
                logger.debug(f'Creating new fake folder for: {path_so_far}')
            new_folder = FolderToAdd(dest_path=path_so_far)
            new_folder.parents = current_id
            self.add_item(new_folder)
            current_id = new_folder.uid

        item.parents = current_id
