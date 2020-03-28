'''
TABLE
UniqueItem
item_id
signature
len_bytes


TABLE
ItemChange
item_id
file_path
modify_ts
item_status = Valid, Deleted
sync_ts

'''


class FMeta:
    def __init__(self, signature, length, sync_ts, modify_ts, file_path, status = 1):
        self.signature = signature
        self.length = length
        self.sync_ts = sync_ts
        self.modify_ts = modify_ts
        self.file_path = file_path
        self.status = status

    def __iter__(self):
        yield self.signature
        yield self.length
        yield self.sync_ts
        yield self.modify_ts
        yield self.file_path
        yield self.status

    def is_content_equal(self, other_entry):
        return isinstance(other_entry, FMeta) and self.signature == other_entry.signature and self.length == other_entry.length

    def is_meta_equal(self, other_entry):
        return isinstance(other_entry, FMeta) and self.file_path == other_entry.file_path and self.status == other_entry.status

    def matches(self, other_entry):
        return self.is_content_equal(other_entry) and self.is_meta_equal(other_entry)

    def is_valid(self):
        return self.status == 1

    def is_moved(self):
        return self.status == 2

    def is_deleted(self):
        return self.status == 3


class FMetaSet:
    def __init__(self):
        # Each item contains a list of entries
        self.sig_dict = {}
        # Each item is an entry
        self.path_dict = {}

    def add(self, item):
        self.sig_dict[item.signature] = item
        self.path_dict[item.signature] = item
