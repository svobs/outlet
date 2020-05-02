import copy
import logging
from abc import ABC
from typing import Dict, List, Optional, Union

from constants import OBJ_TYPE_GDRIVE
from index.two_level_dict import Md5BeforeIdDict
from model.category import Category
from model.display_node import DisplayId, DisplayNode, ensure_int
from ui.assets import ICON_GENERIC_DIR, ICON_GENERIC_FILE, ICON_TRASHED_DIR, ICON_TRASHED_FILE

logger = logging.getLogger(__name__)

NOT_TRASHED = 0
EXPLICITLY_TRASHED = 1
IMPLICITLY_TRASHED = 2

TRASHED_STATUS = ['No', 'UserTrashed', 'Trashed']

"""
◤━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━◥
    CLASS GDriveDisplayId
◣━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━◢
"""


class GDriveDisplayId(DisplayId):
    def __init__(self, id_string):
        super().__init__(id_string=id_string)

    @property
    def tree_type(self) -> int:
        return OBJ_TYPE_GDRIVE


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
    CLASS GoogNode
◣━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━◢
"""


class GoogNode(DisplayNode, ABC):

    def __init__(self, item_id, item_name, trashed, drive_id, my_share, sync_ts, category=Category.NA):
        super().__init__(category)

        self._parent:  Optional[Union[GoogNode, List[GoogNode]]] = None
        """ Most items will have only one parent, so store that way for efficiency"""

        self.id = item_id
        """Google ID"""

        self.name = item_name

        if trashed < 0 or trashed > 2:
            raise RuntimeError(f'Invalid value for "trashed": {trashed}')
        self.trashed = trashed

        self.drive_id = drive_id
        """This will only ever contain other users' drive_ids."""

        self.my_share = my_share
        """If true, I own it but I have shared it with other users"""

        self.sync_ts = sync_ts

    # TODO: rewrite this as compare_to()
    def is_newer_than(self, other_folder):
        return self.sync_ts > other_folder.sync_ts

    @property
    def parents(self):
        if not self._parent:
            return []
        if isinstance(self._parent, list):
            return self._parent
        return [self._parent]

    @parents.setter
    def parents(self, parents):
        """Can be a list of GoogFolders, or a single instance, or None"""
        if not parents:
            self._parent = None
        elif isinstance(parents, list):
            if len(parents) == 0:
                self._parent = None
            elif len(parents) == 1:
                self._parent = parents[0]
            else:
                self._parent = parents
        else:
            self._parent = parents

    def add_parent(self, parent):
        current_parents = self.parents
        if len(current_parents) == 0:
            self.parents = parent
        else:
            for current_parent in current_parents:
                if current_parent.id == parent.id:
                    logger.debug(f'Parent is already in list; skipping: {parent.id}')
                    return
            current_parents.append(parent)
            self.parents = current_parents

    @property
    def display_id(self) -> DisplayId:
        return GDriveDisplayId(id_string=self.id)

    def get_icon(self):
        if self.trashed != NOT_TRASHED:
            return ICON_TRASHED_DIR
        return ICON_GENERIC_DIR

    @classmethod
    def is_dir(cls):
        return True

    def get_name(self):
        return self.name

    @classmethod
    def has_path(cls):
        return True

    @property
    def trashed_str(self):
        if self.trashed is None:
            return ' '
        return TRASHED_STATUS[self.trashed]

    def make_tuple(self, parent_id):
        return self.id, self.name, parent_id, self.trashed, self.drive_id, self.my_share, self.sync_ts


"""
◤━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━◥
    CLASS GoogFolder
◣━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━◢
"""


class GoogFolder(GoogNode):
    def __init__(self, item_id, item_name, trashed, drive_id, my_share, sync_ts, all_children_fetched, category=Category.NA):
        super().__init__(item_id, item_name, trashed, drive_id, my_share, sync_ts, category)

        self.all_children_fetched = all_children_fetched
        """If true, all its children have been fetched from Google"""

    def __repr__(self):
        return f'Folder:(id="{self.id}" name="{self.name}" trashed={self.trashed_str} drive_id={self.drive_id} ' \
               f'my_share={self.my_share} sync_ts={self.sync_ts} parents={self.parents} children_fetched={self.all_children_fetched} ]'



"""
◤━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━◥
    CLASS GoogFile
◣━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━◢
"""


class GoogFile(GoogNode):
    # TODO: handling of shortcuts... does a shortcut have an ID?
    # TODO: handling of special chars in file systems

    def __init__(self, item_id, item_name, trashed, drive_id, version, head_revision_id, md5,
                 my_share, create_ts, modify_ts, size_bytes, owner_id, sync_ts):
        super().__init__(item_id=item_id, item_name=item_name, trashed=trashed,
                         drive_id=drive_id, my_share=my_share, sync_ts=sync_ts)
        self.version = version
        self.head_revision_id = head_revision_id
        self.md5 = md5
        self.my_share = my_share
        self.create_ts = ensure_int(create_ts)
        self.modify_ts = ensure_int(modify_ts)
        self._size_bytes = ensure_int(size_bytes)
        self.owner_id = owner_id

    def __repr__(self):
        return f'GoogFile(id="{self.id}" name="{self.name}" trashed={self.trashed_str}  size={self.size_bytes} ' \
               f'md5="{self.md5} create_ts={self.create_ts} modify_ts={self.modify_ts} owner_id={self.owner_id} ' \
               f'drive_id={self.drive_id} my_share={self.my_share} version={self.version} head_rev_id="{self.head_revision_id}" ' \
               f'sync_ts={self.sync_ts} parents={self.parents})'

    def is_newer_than(self, other_folder):
        if self.modify_ts and other_folder.modify_ts:
            delta = self.modify_ts - other_folder.modify_ts
            if delta != 0:
                return delta
        return self.sync_ts > other_folder.sync_ts

    @classmethod
    def is_dir(cls):
        return False

    def get_icon(self):
        if self.trashed != NOT_TRASHED:
            return ICON_TRASHED_FILE
        return ICON_GENERIC_FILE

    def make_tuple(self, parent_id):
        return (self.id, self.name, parent_id, self.trashed, self._size_bytes, self.md5, self.create_ts, self.modify_ts,
                self.owner_id, self.drive_id, self.my_share, self.version, self.head_revision_id, self.sync_ts)


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
