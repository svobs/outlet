from enum import Enum
import itertools
import logging


logger = logging.getLogger(__name__)


_TRASHED = {
    0: ['NOT_TRASHED', '[ ]'],
    1: ['EXPLICITLY_TRASHED', '[X]'],
    2: ['TRASHED', '[x]'],
}
Trashed = Enum(
    value='Trashed',
    names=itertools.chain.from_iterable(
        itertools.product(v, [k]) for k, v in _TRASHED.items()
    )
)


class DirNode:
    def __init__(self, item_id, item_name, trashed=False, explicitly_trashed=False, trashed_status=Trashed.NOT_TRASHED):
        self.id = item_id
        self.name = item_name
        if explicitly_trashed:
            self.trashed = Trashed.EXPLICITLY_TRASHED
        elif trashed:
            self.trashed = Trashed.TRASHED
        else:
            self.trashed = trashed_status

    def trash_status_str(self):
        if self.trashed == Trashed.EXPLICITLY_TRASHED:
            return '[X]'
        elif self.trashed == Trashed.TRASHED:
            return '[x]'
        else:
            return '[ ]'


class FileNode(DirNode):
    def __init__(self, item_id, item_name, original_filename, version, head_revision_id, md5, shared, created_ts,
                 modified_ts, size_bytes, owner_id, trashed=False, explicitly_trashed=False,
                 trashed_status=Trashed.NOT_TRASHED):
        super().__init__(item_id, item_name, trashed, explicitly_trashed, trashed_status)
        self.original_filename = original_filename
        self.version = version
        self.head_revision_id = head_revision_id
        self.md5 = md5
        self.shared = shared
        self.created_ts = created_ts
        self.modified_ts = modified_ts
        self.size_bytes = size_bytes
        self.owner_id = owner_id

    def make_tuple(self, parent_id):
        return (self.id, self.name, parent_id, self.trashed.value, self.original_filename, self.version, self.head_revision_id,
                self.md5, self.shared, self.created_ts, self.modified_ts, self.size_bytes, self.owner_id)


class IntermediateMeta:
    def __init__(self):
        # Keep track of parentless nodes. These usually indicate shared folder roots,
        # but sometimes indicate something else screwy
        self.roots = []

        # 'parent_id' -> list of its DirNode children
        self.first_parent_dict = {}

        # List of item_ids which have more than 1 parent:
        self.ids_with_multiple_parents = []

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

    def add_to_parent_dict(self, parent_id, item: DirNode):
        child_list = self.first_parent_dict.get(parent_id)
        if not child_list:
            child_list = []
            self.first_parent_dict[parent_id] = child_list
        child_list.append(item)

    def add_id_with_multiple_parents(self, item):
        self.ids_with_multiple_parents.append((item.id,))

    def add_root(self, item):
        self.roots.append(item)
