from enum import Enum
import itertools


_TRASHED = {
    0: ['[ ]', 'NOT_TRASHED'],
    1: ['[X]', 'EXPLICITLY_TRASHED'],
    2: ['[x]', 'TRASHED'],
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


    @property
    def trash_status_str(self):
        if self.trashed == Trashed.EXPLICITLY_TRASHED:
            return '[X]'
        elif self.trashed == Trashed.TRASHED:
            return '[x]'
        else:
            return '[ ]'


class IntermediateMeta:
    def __init__(self):
        # Keep track of parentless nodes. These usually indicate shared folder roots,
        # but sometimes indicate something else screwy
        self.roots = []

        # 'parent_id' -> list of its DirNode children
        self.first_parent_dict = {}

        # List of item_ids which have more than 1 parent:
        self.ids_with_multiple_parents = []

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
