import humanfriendly
from enum import Enum, auto
"""
Category = IntEnum(
    value='Category',
    names=[
        ('NA', 0),
        ('Ignored', 1),
        ('Added', 2),
        ('Updated', 3),
        ('Moved', 4),
        ('Deleted', 5)
    ]
)"""

class Category(Enum):
    NA = 0
    Ignored = 1
    Added = 2
    Updated = 3
    Moved = 4
    Deleted = 5


"""
class Category(IntEnum):
    NONE = (0, 'None')
    IGNORED = (1, 'Ignored')
    ADDED = (2, 'Added')
    UPDATED = (3, 'Updated')
    DELETED = (4, 'Deleted')
    MOVED = (5, 'Moved')

    def __init__(self, ordinal, pretty_name):
        super().__init__(ordinal)
        self._value_ = ordinal
        self._pretty_name = pretty_name

    @property
    def name(self):
        return self._pretty_name
"""

class FMeta:
    def __init__(self, signature, length, sync_ts, modify_ts, file_path, category=0):
        self.signature = signature
        self.length = length
        self.sync_ts = sync_ts
        self.modify_ts = modify_ts
        self.file_path = file_path
        self.category = category

    def __iter__(self):
        yield self.signature
        yield self.length
        yield self.sync_ts
        yield self.modify_ts
        yield self.file_path
        yield self.category

    @classmethod
    def is_dir(cls):
        return False

    def is_content_equal(self, other_entry):
        return isinstance(other_entry, FMeta) and self.signature == other_entry.signature and self.length == other_entry.length

    def is_meta_equal(self, other_entry):
        return isinstance(other_entry, FMeta) and self.file_path == other_entry.file_path and self.category == other_entry.category

    def matches(self, other_entry):
        return self.is_content_equal(other_entry) and self.is_meta_equal(other_entry)

    # TODO
    def is_valid(self):
        return self.category == 1

    def is_moved(self):
        return self.category == 2

    def is_deleted(self):
        return self.category == 3


class FMetaMoved(FMeta):
    def __init__(self, fmeta, prev_path):
        """ FMeta contains new file path; prev_path specifies old file path"""
        super().__init__(fmeta.signature, fmeta.length, fmeta.sync_ts, fmeta.modify_ts, fmeta.file_path, fmeta.category)
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


class FMetaList:
    def __init__(self):
        self.list = []
        self._total_count = 0
        self._total_size_bytes = 0

    def add(self, item):
        self.list.append(item)
        self._total_size_bytes += item.length
        self._total_count += 1


class FMetaTree:
    def __init__(self, root_path):
        self.root_path = root_path
        # Each item contains a list of entries
        self.sig_dict = {}
        # Each item is an entry
        self.path_dict = {}
        self.cat_dict = {Category.Ignored: FMetaList(),
                         Category.Moved: FMetaList(),
                         Category.Deleted: FMetaList(),
                         Category.Updated: FMetaList(),
                         Category.Added: FMetaList()}
        self._dup_count = 0
        self._total_size_bytes = 0

    def categorize(self, fmeta, category: Category):
        assert category != Category.NA
        fmeta.category = category
        return self.cat_dict[category].add(fmeta)

    def clear_categories(self):
        for cat, list in self.cat_dict.items():
            if cat != Category.Ignored:
                list.list.clear()

    def get_category_summary_string(self):
        summary = []
        for cat in self.cat_dict.keys():
            length = len(self.cat_dict[cat].list)
            summary.append(f'{cat.name}={length}')
        return ' '.join(summary)

    def get_for_cat(self, category: Category):
        return self.cat_dict[category].list

    def get_for_sig(self, signature):
        return self.sig_dict.get(signature, None)

    def add(self, item: FMeta):
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

        cat = Category(int(item.category))
        if cat != Category.NA:
            self.cat_dict[cat].add(item)

    def add_ignored_file(self, item):
        self.cat_dict[Category.Ignored].add(item)

    def print_stats(self):
        print(f'FMetaTree=[sigs:{len(self.sig_dict)} paths:{len(self.path_dict)} duplicates:{self._dup_count}]')

    def get_summary(self):
        size = humanfriendly.format_size(self._total_size_bytes)
        return f'{size} in {len(self.path_dict)} files'
