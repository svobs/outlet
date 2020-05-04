import os
from abc import ABC
import logging
from typing import List, Optional, Union

from constants import NOT_TRASHED, TRASHED_STATUS
from model.category import Category
from model.display_id import GDriveIdentifier
from model.display_node import DisplayNode, ensure_int
from model.planning_node import PlanningNode
from ui.assets import ICON_GENERIC_DIR, ICON_GENERIC_FILE, ICON_TRASHED_DIR, ICON_TRASHED_FILE

logger = logging.getLogger(__name__)


"""
◤━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━◥
    CLASS GoogNode
◣━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━◢
"""


class GoogNode(DisplayNode, ABC):

    def __init__(self, item_id, item_name, trashed, drive_id, my_share, sync_ts, category=Category.NA):
        super().__init__(GDriveIdentifier(item_id, None, category))

        self._parent:  Optional[Union[GoogNode, List[GoogNode]]] = None
        """ Most items will have only one parent, so store that way for efficiency"""

        self._name = item_name

        if trashed < 0 or trashed > 2:
            raise RuntimeError(f'Invalid value for "trashed": {trashed}')
        self.trashed = trashed

        self.drive_id = drive_id
        """This will only ever contain other users' drive_ids."""

        self.my_share = my_share
        """If true, I own it but I have shared it with other users"""

        self.sync_ts = sync_ts

    def get_relative_path(self, parent_tree):
        return parent_tree.get_relative_path_for_item(self)

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
                if current_parent.uid == parent.uid:
                    logger.debug(f'Parent is already in list; skipping: {parent.uid}')
                    return
            current_parents.append(parent)
            self.parents = current_parents

    def get_icon(self):
        if self.trashed == NOT_TRASHED:
            return ICON_GENERIC_DIR
        return ICON_TRASHED_DIR

    @classmethod
    def is_dir(cls):
        return True

    @property
    def name(self):
        return self._name

    @name.setter
    def name(self, name):
        self._name = name

    @classmethod
    def has_path(cls):
        return True

    @property
    def trashed_str(self):
        if self.trashed is None:
            return ' '
        return TRASHED_STATUS[self.trashed]

    def make_tuple(self, parent_id):
        return self.uid, self.name, parent_id, self.trashed, self.drive_id, self.my_share, self.sync_ts


"""
◤━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━◥
    CLASS GoogFolder
◣━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━◢
"""


class GoogFolder(GoogNode):
    def __init__(self, item_id, item_name, trashed, drive_id, my_share, sync_ts, all_children_fetched):
        super().__init__(item_id, item_name, trashed, drive_id, my_share, sync_ts)

        self.all_children_fetched = all_children_fetched
        """If true, all its children have been fetched from Google"""

    def __repr__(self):
        return f'Folder:(id="{self.uid}" name="{self.name}" trashed={self.trashed_str} drive_id={self.drive_id} ' \
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
        return f'GoogFile(id="{self.uid}" name="{self.name}" trashed={self.trashed_str}  size={self.size_bytes} ' \
               f'md5="{self.md5} create_ts={self.create_ts} modify_ts={self.modify_ts} owner_id={self.owner_id} ' \
               f'drive_id={self.drive_id} my_share={self.my_share} version={self.version} head_rev_id="{self.head_revision_id}" ' \
               f'sync_ts={self.sync_ts} parents={self.parents})'

    def is_newer_than(self, other_folder):
        if self.modify_ts and other_folder.modify_ts:
            delta = self.modify_ts - other_folder.modify_ts
            if delta != 0:
                return delta
        return self.sync_ts > other_folder.sync_ts

    @property
    def size_bytes(self):
        return self._size_bytes

    @classmethod
    def is_dir(cls):
        return False

    def get_icon(self):
        if self.trashed != NOT_TRASHED:
            return ICON_TRASHED_FILE
        return ICON_GENERIC_FILE

    def make_tuple(self, parent_id):
        return (self.uid, self.name, parent_id, self.trashed, self._size_bytes, self.md5, self.create_ts, self.modify_ts,
                self.owner_id, self.drive_id, self.my_share, self.version, self.head_revision_id, self.sync_ts)


class FolderToAdd(PlanningNode, GoogNode):
    def __init__(self, dest_path):
        GoogNode.__init__(self, item_id=dest_path, item_name=os.path.basename(dest_path), trashed=NOT_TRASHED, drive_id=None,
                          my_share=False, sync_ts=None, category=Category.ADDED)
        self.dest_path = dest_path

    def get_icon(self):
        # TODO: added folder
        return ICON_GENERIC_DIR

    @classmethod
    def is_dir(cls):
        return True

    @classmethod
    def has_path(cls):
        return True
