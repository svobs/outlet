class DirNode:
    def __init__(self, item_id, item_name, trashed=None, explicitly_trashed=None):
        self.id = item_id
        self.name = item_name
        self.trashed = trashed
        self.explicitly_trashed = explicitly_trashed


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

    def add_id_with_multiple_parents(self, item_id):
        self.ids_with_multiple_parents.append((item_id,))

    def add_root(self, item):
        self.roots.append(item)

