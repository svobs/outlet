import logging
from typing import Dict, List, Optional, Union

from constants import OBJ_TYPE_GDRIVE
from model.category import Category
from model.display_node import DisplayId, DisplayNode, ensure_int
from ui.assets import ICON_GENERIC_DIR, ICON_GENERIC_FILE, ICON_TRASHED_DIR, ICON_TRASHED_FILE

logger = logging.getLogger(__name__)

NOT_TRASHED = 0
EXPLICITLY_TRASHED = 1
IMPLICITLY_TRASHED = 2

TRASHED_STATUS = ['No', 'UserTrashed', 'Trashed']


class GDriveDisplayId(DisplayId):
    def __init__(self, id_string):
        super().__init__(id_string=id_string)

    @property
    def tree_type(self) -> int:
        return OBJ_TYPE_GDRIVE


class UserMeta:
    def __init__(self, display_name, permission_id, email_address, photo_link):
        self.display_name = display_name
        self.permission_id = permission_id
        self.email_address = email_address
        self.photo_link = photo_link


class GoogFolder(DisplayNode):

    def __init__(self, item_id, item_name, trashed, drive_id, my_share, sync_ts, all_children_fetched, category=Category.NA):
        super().__init__(category)

        self._parent:  Optional[Union[GoogFolder, List[GoogFolder]]] = None
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

        self.all_children_fetched = all_children_fetched
        """If true, all its children have been fetched from Google"""

    def __repr__(self):
        return f'Folder:(id="{self.id}" name="{self.name}" trashed={self.trashed_str} drive_id={self.drive_id} ' \
                   f'my_share={self.my_share} sync_ts={self.sync_ts} parents={self.parents} children_fetched={self.all_children_fetched} ]'

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
        return self.id, self.name, parent_id, self.trashed, self.drive_id, self.my_share, self.sync_ts, self.all_children_fetched


class GoogFile(GoogFolder):
    # TODO: handling of shortcuts... does a shortcut have an ID?
    # TODO: handling of special chars in file systems

    def __init__(self, item_id, item_name, trashed, drive_id, version, head_revision_id, md5,
                 my_share, create_ts, modify_ts, size_bytes, owner_id, sync_ts):
        super().__init__(item_id=item_id, item_name=item_name, trashed=trashed,
                         drive_id=drive_id, my_share=my_share, sync_ts=sync_ts,
                         all_children_fetched=False)  # all_children_fetched is not used
        self.version = version
        self.head_revision_id = head_revision_id
        self.md5 = md5
        self.my_share = my_share
        self.create_ts = ensure_int(create_ts)
        self.modify_ts = ensure_int(modify_ts)
        self._size_bytes = ensure_int(size_bytes)
        self.owner_id = owner_id

    def __repr__(self):
        return f'GoogFile(id="{self.id}" name="{self.name}" trashed={self.trashed_str}  size={self.size_bytes}' \
               f'md5="{self.md5} create_ts={self.create_ts} modify_ts={self.modify_ts} owner_id={self.owner_id} ' \
               f'drive_id={self.drive_id} my_share={self.my_share} version={self.version} head_rev_id="{self.head_revision_id}" ' \
               f'sync_ts={self.sync_ts} parents={self.parents})'

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


class GDriveMeta:
    def __init__(self):
        # Keep track of parentless nodes. These include the 'My Drive' item, as well as shared items.
        self.roots: List[GoogFolder] = []

        self.id_dict: Dict[str, GoogFolder] = {}
        """ Forward lookup table: nodes are indexed by GOOG ID"""

        self.first_parent_dict: Dict[str, List[GoogFolder]] = {}
        """ Reverse lookup table: 'parent_id' -> list of child nodes """

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

    def add_item(self, item):
        """Called when adding from Google API"""
        existing_item = self.id_dict.get(item.id, None)
        if existing_item:
            # If we have a conflict at all, it should mean we are loading from cache and are combining two goog_folder entries
            assert len(existing_item.parents) >= 1 and len(item.parents) == 1, f'Expected exactly 1 parent for existing and new item!'
            existing_parents = existing_item.parents
            new_parents = item.parents
            for parent_id in new_parents:
                if parent_id in existing_parents:
                    logger.error(f'Duplicate entry found; skipping: {item.id} (parent_id={parent_id})')
                    return
                existing_parents.append(item)
                # Make sure this is initialized:
                existing_item.parents = existing_parents
            item = existing_item
        else:
            self.id_dict[item.id] = item

        # build reverse dictionaries
        parents = item.parents
        if len(parents) == 0:
            self.roots.append(item)
        else:
            has_multiple_parents = (len(parents) > 1)
            parent_index = 0
            if has_multiple_parents:
                logger.debug(f'Item has multiple parents:  [{item.id}] {item.name}')
                self.ids_with_multiple_parents.append((item.id,))
            for parent_id in parents:
                self._add_to_parent_dict(parent_id, item)
                if has_multiple_parents:
                    parent_index += 1
                    logger.debug(f'\tParent {parent_index}: [{parent_id}]')

    def _add_to_parent_dict(self, parent_id, item):
        child_list = self.first_parent_dict.get(parent_id)
        if not child_list:
            child_list = []
            self.first_parent_dict[parent_id] = child_list
        child_list.append(item)
