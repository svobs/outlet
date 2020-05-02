import copy
import logging
from typing import Dict, List, Optional

from index.two_level_dict import Md5BeforeIdDict
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
    def __init__(self):
        # Keep track of parentless nodes. These include the 'My Drive' item, as well as shared items.
        self.roots: List[GoogNode] = []

        self.id_dict: Dict[str, GoogNode] = {}
        """ Forward lookup table: nodes are indexed by GOOG ID"""

        self.first_parent_dict: Dict[str, List[GoogNode]] = {}
        """ Reverse lookup table: 'parent_id' -> list of child nodes """

        self.md5_dict: Md5BeforeIdDict = Md5BeforeIdDict()

        self.ids_with_multiple_parents = []
        """List of item_ids which have more than 1 parent"""

        self.me = None
        self.path_dict = None
        self.owner_dict = {}
        self.mime_types = {}
        self.shortcuts = {}

    def get_children(self, parent_id):
        return self.first_parent_dict.get(parent_id, None)

    def get_for_id(self, goog_id):
        return self.id_dict.get(goog_id, None)

    def get_for_md5(self, md5) -> List[GoogNode]:
        return self.md5_dict.get(md5, None)

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
            self.md5_dict.get(item.md5, item.id)
            previous = self.md5_dict.put(item)
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


def _try_to_merge(existing_item: GoogNode, new_item: GoogNode) -> Optional[GoogNode]:
    # Let's be safe and clone the data if there's a conflict. We don't know whether we're loading from
    # cache or whether we're slicing a subtree
    if new_item.is_newer_than(existing_item):
        clone = copy.deepcopy(new_item)
    else:
        clone = copy.deepcopy(existing_item)
    clone.parents = list(set(existing_item.parents) | set(new_item.parents))
    return clone
