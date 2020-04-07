import humanfriendly


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

    @classmethod
    def is_dir(cls):
        return False

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


class FMetaMoved(FMeta):
    def __init__(self, fmeta, prev_path):
        """ FMeta contains new file path; prev_path specifies old file path"""
        super().__init__(fmeta.signature, fmeta.length, fmeta.sync_ts, fmeta.modify_ts, fmeta.file_path, fmeta.status)
        self.prev_path = prev_path


class DMeta:
    """For directories"""
    def __init__(self, file_path):
        self.file_path = file_path
        self.items = 0
        self.total_size_bytes = 0

    def add_meta(self, fmeta):
        self.items += 1
        self.total_size_bytes += fmeta.length

    @classmethod
    def is_dir(cls):
        return True

    def get_summary(self):
        size = humanfriendly.format_size(self.total_size_bytes)
        return f'{size} in {self.items} files'


class FMetaSet:
    def __init__(self, root_path):
        self.root_path = root_path
        # Each item contains a list of entries
        self.sig_dict = {}
        # Each item is an entry
        self.path_dict = {}
        # These are not included in the previous two structures
        self.ignored_files = []
        self._dup_count = 0
        self._total_size_bytes = 0

    def add(self, item):
        set_matching_sig = self.sig_dict.get(item.signature, None)
        if set_matching_sig is None:
            set_matching_sig = [item]
            self.sig_dict[item.signature] = set_matching_sig
        else:
            set_matching_sig.append(item)
            self._dup_count += 1
        item_matching_path = self.path_dict.get(item.file_path, None)
        if item_matching_path is not None:
            print(f'WARNING: overwriting metadata for path: {item.file_path}')
            self._total_size_bytes -= item_matching_path.length
        self._total_size_bytes += item.length
        self.path_dict[item.signature] = item

    def add_ignored_file(self, item):
        self.ignored_files.append(item)

    def print_stats(self):
        print(f'FMetaSet=[sigs:{len(self.sig_dict)} paths:{len(self.path_dict)} duplicates:{self._dup_count}]')

    def get_summary(self):
        size = humanfriendly.format_size(self._total_size_bytes)
        return f'{size} in {len(self.path_dict)} files'


class ChangeSet:
    def __init__(self, src_root_path, dst_root_path):
        # TODO: refactor each of these into FMetaSet.
        # TODO: then include them in each status bar
        self.adds = []
        self.updates = []
        self.dels = []
        self.moves = []
        self.src_root_path = src_root_path
        self.dst_root_path = dst_root_path