import logging
import logging
import os
from queue import Queue
from typing import List, Union, ValuesView

import file_util
from index.two_level_dict import Md5BeforeUidDict
from model.category import Category
from model.display_id import GDriveIdentifier, Identifier
from model.gdrive_whole_tree import GDriveTree, GDriveWholeTree
from model.goog_node import GoogNode
from model.planning_node import FileDecoratorNode
from model.subtree_snapshot import SubtreeSnapshot
from stopwatch_sec import Stopwatch

logger = logging.getLogger(__name__)

SUPER_DEBUG = False

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
