import logging


logger = logging.getLogger(__name__)

NOT_TRASHED = 0
EXPLICITLY_TRASHED = 1
IMPLICITLY_TRASHED = 2

TRASHED_STATUS = ['No', 'UserTrashed', 'Trashed']


class GoogFolder:
    def __init__(self, item_id, item_name, trashed):
        self.id = item_id
        self.name = item_name
        if trashed < 0 or trashed > 2:
            raise RuntimeError(f'Invalid value for "trashed": {trashed}')
        self.trashed = trashed
        # TODO: shared?

    def to_str(self):
        return f'Folder:[id="{self.id}" name="{self.name}" trashed={self.trashed_str}]'

    @property
    def trashed_str(self):
        if self.trashed is None:
            return ' '
        return TRASHED_STATUS[self.trashed]

    def is_dir(self):
        return True

    def make_tuple(self, parent_id):
        return self.id, self.name, parent_id, self.trashed


class GoogFile(GoogFolder):
    def __init__(self, item_id, item_name, original_filename, version, head_revision_id, md5,
                 shared, create_ts, modify_ts, size_bytes, owner_id, trashed):
        super().__init__(item_id, item_name, trashed)
        self.original_filename = original_filename # TODO: remove
        self.version = version
        self.head_revision_id = head_revision_id
        self.md5 = md5
        self.shared = shared
        self.create_ts = create_ts
        self.modify_ts = modify_ts
        self.size_bytes = size_bytes
        self.owner_id = owner_id

    def to_str(self):
        return f'GoogFile[id="{self.id}" name="{self.name}" trashed={self.trashed_str} ' \
               f'version={self.version} md5="{self.md5} modify_ts={self.modify_ts} create_ts={self.create_ts}"]'

    def is_dir(self):
        return False

    def make_tuple(self, parent_id):
        return (self.id, self.name, parent_id, self.trashed, self.original_filename, self.version, self.head_revision_id,
                self.md5, self.shared, self.create_ts, self.modify_ts, self.size_bytes, self.owner_id)


class GDriveMeta:
    def __init__(self):
        # Keep track of parentless nodes. These include the 'My Drive' item, as well as shared items.
        self.roots = []

        # 'parent_id' -> list of child nodes
        self.first_parent_dict = {}

        # List of item_ids which have more than 1 parent:
        self.ids_with_multiple_parents = []

        self.path_dict = None

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
