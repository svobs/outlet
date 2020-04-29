import logging

from model.category import Category
from model.display_node import DisplayId, DisplayNode, ensure_int

logger = logging.getLogger(__name__)

NOT_TRASHED = 0
EXPLICITLY_TRASHED = 1
IMPLICITLY_TRASHED = 2

TRASHED_STATUS = ['No', 'UserTrashed', 'Trashed']


class UserMeta:
    def __init__(self, display_name, permission_id, email_address, photo_link):
        self.display_name = display_name
        self.permission_id = permission_id
        self.email_address = email_address
        self.photo_link = photo_link


class GoogFolder(DisplayNode):

    def __init__(self, item_id, item_name, trashed, drive_id, my_share, sync_ts, category=Category.NA):
        super().__init__(category)
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

    def __repr__(self):
        return f'Folder:(id="{self.id}" name="{self.name}" trashed={self.trashed_str} drive_id={self.drive_id} ' \
                   f'my_share={self.my_share} sync_ts={self.sync_ts} ]'

    @property
    def display_id(self) -> DisplayId:
        return DisplayId(id_string=self.id)

    @classmethod
    def is_dir(cls):
        return True

    def get_name(self):
        return self.name

    @classmethod
    def has_path(cls):
        # TODO: make this return True in the future
        return False

    @property
    def trashed_str(self):
        if self.trashed is None:
            return ' '
        return TRASHED_STATUS[self.trashed]

    def make_tuple(self, parent_id):
        return self.id, self.name, parent_id, self.trashed, self.drive_id, self.my_share, self.sync_ts


class GoogFile(GoogFolder):
    # TODO: handling of shortcuts... does a shortcut have an ID?
    # TODO: handling of special chars in file systems

    def __init__(self, item_id, item_name, trashed, drive_id, version, head_revision_id, md5,
                 my_share, create_ts, modify_ts, size_bytes, owner_id, sync_ts):
        super().__init__(item_id=item_id, item_name=item_name, trashed=trashed, drive_id=drive_id, my_share=my_share, sync_ts=sync_ts)
        self.version = version
        self.head_revision_id = head_revision_id
        self.md5 = md5
        self.my_share = my_share
        self.create_ts = ensure_int(create_ts)
        self.modify_ts = ensure_int(modify_ts)
        self._size_bytes = ensure_int(size_bytes)
        self._size_bytes = ensure_int(size_bytes)
        self.owner_id = owner_id

    def __repr__(self):
        return f'GoogFile(id="{self.id}" name="{self.name}" trashed={self.trashed_str}  size={self.size_bytes}' \
               f'md5="{self.md5} create_ts={self.create_ts} modify_ts={self.modify_ts} owner_id={self.owner_id} ' \
               f'drive_id={self.drive_id} my_share={self.my_share} version={self.version} head_rev_id="{self.head_revision_id}" ' \
               f'sync_ts={self.sync_ts})'

    @classmethod
    def is_dir(cls):
        return False

    def make_tuple(self, parent_id):
        return (self.id, self.name, parent_id, self.trashed, self._size_bytes, self.md5, self.create_ts, self.modify_ts,
                self.owner_id, self.drive_id, self.my_share, self.version, self.head_revision_id, self.sync_ts)


class GDriveMeta:
    def __init__(self):
        # Keep track of parentless nodes. These include the 'My Drive' item, as well as shared items.
        self.roots = []

        # 'parent_id' -> list of child nodes
        self.first_parent_dict = {}

        # List of item_ids which have more than 1 parent:
        self.ids_with_multiple_parents = []

        self.me = None
        self.path_dict = None
        self.owner_dict = {}
        self.mime_types = {}
        self.shortcuts = {}

    def get_children(self, parent_id):
        return self.first_parent_dict.get(parent_id, None)

    def add_item_with_parents(self, parents, item):
        if len(parents) == 0:
            self.add_root(item)
        else:
            has_multiple_parents = (len(parents) > 1)
            parent_index = 0
            if has_multiple_parents:
                logger.debug(f'Item has multiple parents:  [{item.id}] {item.name}')
                self.add_id_with_multiple_parents(item)
            for parent_id in parents:
                self.add_to_parent_dict(parent_id, item)
                if has_multiple_parents:
                    parent_index += 1
                    logger.debug(f'\tParent {parent_index}: [{parent_id}]')

    def add_to_parent_dict(self, parent_id, item):
        if not parent_id:
            self.add_root(item)
            return

        child_list = self.first_parent_dict.get(parent_id)
        if not child_list:
            child_list = []
            self.first_parent_dict[parent_id] = child_list
        child_list.append(item)

    def add_id_with_multiple_parents(self, item):
        self.ids_with_multiple_parents.append((item.id,))

    def add_root(self, item):
        self.roots.append(item)
