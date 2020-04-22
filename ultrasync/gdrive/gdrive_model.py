import logging


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


class GoogFolder:
    def __init__(self, item_id, item_name, trashed, drive_id, my_share):
        self.id = item_id
        """Google ID"""

        self.name = item_name

        if trashed < 0 or trashed > 2:
            raise RuntimeError(f'Invalid value for "trashed": {trashed}')
        self.trashed = trashed

        self.drive_id = drive_id
        """Verify this against my Drive ID."""

        self.my_share = my_share
        """If true, I own it but I have shared it with other users"""

    def __repr__(self):
        return f'Folder:(id="{self.id}" name="{self.name}" trashed={self.trashed_str} drive_id={self.drive_id} ' \
                   f'my_share={self.my_share} ]'

    @property
    def trashed_str(self):
        if self.trashed is None:
            return ' '
        return TRASHED_STATUS[self.trashed]

    def is_dir(self):
        return True

    def make_tuple(self, parent_id):
        return self.id, self.name, parent_id, self.trashed, self.drive_id, self.my_share


class GoogFile(GoogFolder):
    def __init__(self, item_id, item_name, trashed, drive_id, version, head_revision_id, md5,
                 my_share, create_ts, modify_ts, size_bytes, owner_id):
        super().__init__(item_id=item_id, item_name=item_name, trashed=trashed, drive_id=drive_id, my_share=my_share)
        self.version = version
        self.head_revision_id = head_revision_id
        self.md5 = md5
        self.my_share = my_share
        self.create_ts = create_ts
        self.modify_ts = modify_ts
        self.size_bytes = size_bytes
        self.owner_id = owner_id

    def __repr__(self):
        return f'GoogFile(id="{self.id}" name="{self.name}" size={self.size_bytes} trashed={self.trashed_str} ' \
               f'drive_id={self.drive_id} owner_id={self.owner_id} my_share={self.my_share} ' \
               f'version={self.version} head_rev_id="{self.head_revision_id}" md5="{self.md5} ' \
               f'create_ts={self.create_ts} modify_ts={self.modify_ts}' \
               f')'

    def is_dir(self):
        return False

    def make_tuple(self, parent_id):
        return (self.id, self.name, parent_id, self.trashed, self.size_bytes, self.md5, self.create_ts, self.modify_ts,
                self.owner_id, self.drive_id, self.my_share, self.version, self.head_revision_id)


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
