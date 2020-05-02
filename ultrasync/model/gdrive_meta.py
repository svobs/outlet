import copy
import logging
import os
from typing import Dict, List, Optional

import file_util
from index.two_level_dict import Md5BeforeIdDict
from model.category import Category
from model.goog_node import GoogFile, GoogNode

logger = logging.getLogger(__name__)

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
    CLASS GDriveMeta
◣━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━◢
"""


class GDriveMeta:
    def __init__(self, root_id, root_path):
        self.root_id = root_id
        """GoogID for where to start"""

        self.root_path = root_path
        """Filesystem-like-path. Used for reference when comparing to FMetaTree"""

        # Keep track of parentless nodes. These include the 'My Drive' item, as well as shared items.
        self.roots: List[GoogNode] = []

        self.id_dict: Dict[str, GoogNode] = {}
        """ Forward lookup table: nodes are indexed by GOOG ID"""

        self.first_parent_dict: Dict[str, List[GoogNode]] = {}
        """ Reverse lookup table: 'parent_id' -> list of child nodes """

        self._md5_dict: Md5BeforeIdDict = Md5BeforeIdDict()

        self._cat_dict: Dict[Category, List[GoogNode]] = {Category.Ignored: [],
                                                          Category.Added: [],
                                                          Category.Deleted: [],
                                                          Category.Moved: [],
                                                          Category.Updated: [],
                                                          }

        # self.ids_with_multiple_parents: List[str] = []
        """List of item_ids which have more than 1 parent"""

        self.me: Optional[UserMeta] = None
        self.path_dict = None
        self.owner_dict = {}
        self.mime_types = {}
        self.shortcuts = {}

    def get_children(self, parent_id):
        return self.first_parent_dict.get(parent_id, None)

    def get_for_id(self, goog_id) -> Optional[GoogNode]:
        return self.id_dict.get(goog_id, None)

    def get_for_md5(self, md5) -> Optional[List[GoogNode]]:
        return self._md5_dict.get(md5, None)

    def get_md5_set(self):
        return self._md5_dict.keys()

    def get_path_for_id(self, goog_id: str) -> str:
        """Gets the filesystem-like-path for the item with the given GoogID, relative to the root of this subtree"""
        item = self.get_for_id(goog_id)
        if not item:
            raise RuntimeError(f'Item not found: id={goog_id}')

        path = ''
        while True:
            if path == '':
                path = item.name
            else:
                path = item.name + '/' + path
            path = item.name + path
            parents = item.parents
            if parents:
                if len(parents) > 1:
                    logger.warning(f'Multiple parents found for {item.id} ("{item.name}"). Picking the first one.')
                    # pass through
                item = self.get_for_id(parents[0])
                if not item:
                    # reached root of subtree
                    logger.debug(f'Mapped ID "{goog_id}" to subtree path "{path}"')
                    return path
            else:
                # Root of Google Drive
                return '/' + path

    def get_for_path(self, path: str) -> Optional[GoogNode]:
        """Try to get a singular item corresponding to the given file-system-like
        path, mapping the root of this tree to the first segment of the path."""
        name_segments = file_util.split_path(path)
        current_id = self.root_id
        path_so_far = ''
        for name_seg in name_segments:
            path_so_far = os.path.join(path_so_far, name_seg)
            children = self.get_children(current_id)
            matches = [x for x in children if x.name == name_seg]
            if len(matches) > 1:
                logger.error(f'Multiple IDs map found for segment {path_so_far}. Choosing the first found')
            elif len(matches) == 0:
                logger.debug(f'No match found for path: {path_so_far}')
                return None
            else:
                current_id = matches[0].id
        logger.debug(f'Found for path "{path}": {self.get_for_id(current_id)}')
        return current_id

    def add_item(self, item):
        """Called when adding from Google API, or when slicing a metastore"""

        # Build forward dictionary
        existing_item = self.id_dict.get(item.id, None)
        if existing_item:
            item = _try_to_merge(existing_item, item)
            if item:
                self.id_dict[item.id] = item
            else:
                return
        else:
            self.id_dict[item.id] = item

        # Do this after any merging we do above, so we are consistent
        if isinstance(item, GoogFile) and item.md5:
            self._md5_dict.get(item.md5, item.id)
            previous = self._md5_dict.put(item)
            # if previous:
            #     logger.debug(f'Overwrite existing MD5/ID pair')

        # build reverse dictionary
        parents = item.parents
        if len(parents) == 0:
            self.roots.append(item)
        else:
            for parent_id in parents:
                self._add_to_parent_dict(parent_id, item)

    def _add_to_parent_dict(self, parent_id, item):
        child_list = self.first_parent_dict.get(parent_id)
        if not child_list:
            child_list = []
            self.first_parent_dict[parent_id] = child_list
        child_list.append(item)

    def clear_categories(self):
        for cat, cat_list in self._cat_dict.items():
            if cat != Category.Ignored:
                cat_list.clear()

    def validate_categories(self):
        pass

    def get_relative_path_of(self, goog_node: GoogNode):
        return self.get_path_for_id(goog_node.id)


def _try_to_merge(existing_item: GoogNode, new_item: GoogNode) -> Optional[GoogNode]:
    # Let's be safe and clone the data if there's a conflict. We don't know whether we're loading from
    # cache or whether we're slicing a subtree
    if new_item.is_newer_than(existing_item):
        clone = copy.deepcopy(new_item)
    else:
        clone = copy.deepcopy(existing_item)
    clone.parents = list(set(existing_item.parents) | set(new_item.parents))
    return clone
