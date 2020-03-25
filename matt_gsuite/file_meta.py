'''
TODO: rename to 'SyncItem'
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
class FileEntry:
    def __init__(self, signature, length, sync_ts, file_path, deleted = 1):
        self.signature = signature
        self.length = length
        self.sync_ts = sync_ts
        self.modify_ts = None # TODO
        self.file_path = file_path
        self.deleted = deleted

    def __iter__(self):
        yield self.signature
        yield self.length
        yield self.sync_ts
        yield self.file_path
        yield self.deleted

    def is_content_equal(self, other_entry):
        return isinstance(other_entry, FileEntry) and self.signature == other_entry.signature and self.length == other_entry.length

    def is_meta_equal(self, other_entry):
        return isinstance(other_entry, FileEntry) and self.file_path == other_entry.file_path and self.deleted == other_entry.deleted

    def matches(self, other_entry):
        return self.is_content_equal(other_entry) and self.is_meta_equal(other_entry)

    def is_valid(self):
        return self.deleted == 1

    def is_moved(self):
        return self.deleted == 2

    def is_deleted(self):
        return self.deleted == 3


class FilesMeta:
    def __init__(self):
        # Each item contains a list of entries
        self.sig_dict = {}
        # Each item is an entry
        self.path_dict = {}


class SyncSet:
    def __init__(self):
        self.local_adds = []
        self.local_updates = []
        self.local_dels = []
        self.remote_adds = []
        self.remote_dels = []