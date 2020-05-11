import errno
import logging
import os
from queue import Queue
from typing import Any, Dict, List, Optional, Tuple, Union, ValuesView

import constants
import file_util
from index.two_level_dict import Md5BeforeUidDict
from model.category import Category
from model.display_id import GDriveIdentifier, Identifier
from model.goog_node import FolderToAdd, GoogFile, GoogFolder, GoogNode
from model.planning_node import FileDecoratorNode, PlanningNode
from model.subtree_snapshot import SubtreeSnapshot
from stopwatch_sec import Stopwatch

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

        logger.debug(f'Done validating GDriveSubtree')

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



"""
◤━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━◥
    CLASS GDriveSubtree
    Represents a slice of the whole tree.
    Has categories and MD5 for comparison
◣━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━◢
"""


class GDriveSubtree(GDriveTree, SubtreeSnapshot):
    def __init__(self, whole_tree: GDriveWholeTree, root_node: GoogNode):
        GDriveTree.__init__(self)
        SubtreeSnapshot.__init__(self, root_identifier=root_node.identifier)

        self._whole_tree = whole_tree
        self._root_node = root_node
        self._ignored_items: List[GoogNode] = []

    @property
    def root_node(self):
        return self._root_node

    @classmethod
    def create_empty_subtree(cls, subtree_root_node: GoogNode) -> SubtreeSnapshot:
        return GDriveSubtree(subtree_root_node)

    @classmethod
    def create_identifier(cls, full_path, uid, category) -> Identifier:
        return GDriveIdentifier(uid=uid, full_path=full_path, category=category)

    @property
    def root_path(self):
        return self._root_node.full_path

    @property
    def root_id(self):
        return self._root_node.uid

    def get_ignored_items(self):
        return self._ignored_items

    # def __eq__(self, other):
    #     if self.uid == constants.ROOT:
    #         return other.uid == constants.ROOT
    #     return super().__eq__(other)
    #
    # def __ne__(self, other):
    #     return not self.__eq__(other)

    def __repr__(self):
        # FIXME
        return 'WIP'
        # return f'GDriveSubtree(root_id={self.root_id} root_path="{self.root_path}" id_count={len(self.id_dict)} ' \
        #        f'parent_count={len(self.first_parent_dict)})'

    def get_for_md5(self, md5) -> Union[List[GoogNode], ValuesView[GoogNode]]:
        uid_dict = self._md5_dict.get_second_dict(md5)
        if uid_dict:
            return uid_dict.values()
        return []

    def get_md5_dict(self):
        md5_set_stopwatch = Stopwatch()

        md5_dict: Md5BeforeUidDict = Md5BeforeUidDict()

        q = Queue()
        q.put(self.root_node)

        while not q.empty():
            item: GoogNode = q.get()
            if item.is_dir():
                child_list = self._whole_tree.get_children(item.uid)
                if child_list:
                    for child in child_list:
                        q.put(child)
            elif item.md5:
                md5_dict.put(item)

        logger.info(f'{md5_set_stopwatch} Found {md5_dict.total_entries} MD5s')
        return md5_dict

    def get_md5_set(self):
        md5_set_stopwatch = Stopwatch()

        md5_dict: Md5BeforeUidDict = Md5BeforeUidDict()

        q = Queue()
        q.put(self.root_node)

        while not q.empty():
            item: GoogNode = q.get()
            if item.is_dir():
                child_list = self._whole_tree.get_children(item.uid)
                if child_list:
                    for child in child_list:
                        q.put(child)
            elif item.md5:
                md5_dict.put(item)

        logger.info(f'{md5_set_stopwatch} Found {md5_dict.total_entries} MD5s')
        return md5_dict

    def get_children(self, parent_id: Union[str, Identifier]) -> List[GoogNode]:
        return self._whole_tree.get_children(parent_id=parent_id)

    def get_all(self) -> ValuesView[GoogNode]:
        """Returns the complete set of all unique items from this subtree."""
        # Should remove this method from the parent class at some point
        raise RuntimeError('You should never do this for a GDriveSubtree!')

    def get_full_path_for_item(self, item: GoogNode) -> List[str]:
        return self._whole_tree.get_full_paths_for_item(item)

    def in_this_subtree(self, path: str):
        return path.startswith(self.root_path)

    def get_for_path(self, path: str, include_ignored=False) -> List[GoogNode]:
        if not self.in_this_subtree(path):
            raise RuntimeError(f'Not in this tree: "{path}" (tree root: {self.root_path}')
        try:
            identifiers = self._whole_tree.get_all_ids_for_path(path)
        except FileNotFoundError:
            return []

        if len(identifiers) == 1:
            return [self._whole_tree.get_item_for_id(identifiers[0].uid)]

        logger.warning(f'Found {len(identifiers)} identifiers for path: "{path}"). Returning the whole list')
        return list(map(lambda x: self._whole_tree.get_item_for_id(x.uid), identifiers))

    def add_item(self, item):
        raise RuntimeError('Cannot do this from a subtree!')

    def get_ancestor_chain(self, item: GoogNode) -> List[Identifier]:
        identifiers = []

        # kind of a kludge but I don't care right now
        if isinstance(item, FileDecoratorNode) and not item.parent_ids:
            relative_path = file_util.strip_root(item.dest_path, self.root_path)
            name_segments = file_util.split_path(relative_path)
            # Skip last item (it's the file name)
            name_segments.pop()
            current_identifier: Identifier = self._root_node.identifier
            path_so_far = current_identifier.full_path
            for name_seg in name_segments:
                path_so_far = os.path.join(path_so_far, name_seg)
                children: List[GoogNode] = self._whole_tree.get_children(current_identifier.uid)
                if children:
                    matches = [x for x in children if x.name == name_seg]
                    if len(matches):
                        if len(matches) > 1:
                            logger.error(f'get_ancestor_chain(): Multiple child IDs ({len(matches)}) found for parent ID"{current_identifier.uid}", '
                                         f'tree "{self.root_path}" Choosing the first found')
                            for num, match in enumerate(matches):
                                logger.info(f'Match {num}: {match}')

                        current_identifier = matches[0].identifier
                        identifiers.append(current_identifier)
                        continue

                if SUPER_DEBUG:
                    logger.debug(f'Deriving new fake ancestor for: {path_so_far}')
                current_identifier = GDriveIdentifier(uid=path_so_far, full_path=path_so_far, category=Category.Added)
                identifiers.append(current_identifier)

            return identifiers

        while True:
            if item.parent_ids:
                if len(item.parent_ids) > 1:
                    resolved_parent_ids = []
                    for par_id in item.parent_ids:
                        par = self._whole_tree.get_item_for_id(par_id)
                        if par and self.in_this_subtree(par.full_path):
                            resolved_parent_ids.append(par_id)
                    if len(resolved_parent_ids) > 1:
                        logger.error(f'Found multiple valid parents for item: {item}: parents={resolved_parent_ids}')
                    # assert len(resolved_parent_ids) == 1
                    item = self._whole_tree.get_item_for_id(resolved_parent_ids[0])
                else:
                    item = self._whole_tree.get_item_for_id(item.parent_ids[0])
                if item and item.uid != self.identifier.uid:
                    identifiers.append(item.identifier)
                    continue
            identifiers.reverse()
            return identifiers

    def get_relative_path_for_item(self, goog_node: GoogNode):
        """Get the path for the given ID, relative to the root of this subtree"""
        if not goog_node.full_path:
            node_full_path = self._whole_tree.get_all_paths_for_id(goog_node.uid)
        else:
            node_full_path = goog_node.full_path
        if isinstance(node_full_path, list):
            # Use the first path we find which is under this subtree:
            for full_path in node_full_path:
                if self.in_this_subtree(full_path):
                    return file_util.strip_root(full_path, self.root_path)
            raise RuntimeError(f'Could not get relative path for {node_full_path} in "{self.root_path}"')
        return file_util.strip_root(node_full_path, self.root_path)

    def get_summary(self):
        # FIXME
        file_count = 0
        folder_count = 0
        # for item in self.id_dict.values():
        #     if item.is_dir():
        #         folder_count += 1
        #     else:
        #         file_count += 1
        #         folder_count += 1
        return f'{file_count:n} files and {folder_count:n} folders in subtree '
