
class FileEntry:
    def __init__(self, signature, length, sync_ts, file_path, deleted = False):
        self.signature = signature
        self.length = length
        self.sync_ts = sync_ts
        self.file_path = file_path
        self.deleted = deleted

    def __iter__(self):
        yield self.signature
        yield self.length
        yield self.sync_ts
        yield self.file_path
        yield self.deleted


class FilesMeta:
    def __init__(self):
        self.sig_dict = {}
        self.path_dict = {}
