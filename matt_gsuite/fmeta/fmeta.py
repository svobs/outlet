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
    def __init__(self, signature, size_bytes, sync_ts, modify_ts, file_path, category=Category.NA):
        self.signature = signature
        self.size_bytes = size_bytes
        self.sync_ts = sync_ts
        self.modify_ts = modify_ts
        self.file_path = file_path
        self.category = category

    def __iter__(self):
        yield self.signature
        yield self.size_bytes
        yield self.sync_ts
        yield self.modify_ts
        yield self.file_path
        yield self.category.value

    @property
    def category(self):
        assert type(self._category) == Category
        return self._category

    @category.setter
    def category(self, category):
        if type(category) == int:
            self._category = Category(category)
        else:
            assert type(category) == Category
            self._category = category

    @classmethod
    def is_dir(cls):
        return False

    def is_content_equal(self, other_entry):
        return isinstance(other_entry, FMeta) and self.signature == other_entry.signature and self.size_bytes == other_entry.size_bytes

    def is_meta_equal(self, other_entry):
        return isinstance(other_entry, FMeta) and self.file_path == other_entry.file_path and self.category == other_entry.category

    def matches(self, other_entry):
        return self.is_content_equal(other_entry) and self.is_meta_equal(other_entry)

    def is_ignored(self):
        return self.category == Category.Ignored


class FMetaMoved(FMeta):
    def __init__(self, fmeta, prev_path):
        """ FMeta contains new file path; prev_path specifies old file path"""
        super().__init__(fmeta.signature, fmeta.size_bytes, fmeta.sync_ts, fmeta.modify_ts, fmeta.file_path, fmeta.category)
        self.prev_path = prev_path


class DirNode:
    """For directories"""
    def __init__(self, file_path):
        self.file_path = file_path
        self.items = 0
        self.size_bytes = 0

    def add_meta(self, fmeta):
        self.items += 1
        self.size_bytes += fmeta.size_bytes

    @classmethod
    def is_dir(cls):
        return True

    def get_summary(self):
        size = humanfriendly.format_size(self.size_bytes)
        return f'{size} in {self.items} files'


class CategoryNode(DirNode):
    def __init__(self, category):
        super().__init__('')
        self.category = category


class FMetaList:
    def __init__(self):
        self.list = []
        self._total_count = 0
        self._total_size_bytes = 0

    def add(self, item):
        self.list.append(item)
        self._total_size_bytes += item.size_bytes
        self._total_count += 1

    @property
    def size_bytes(self):
        return self._total_size_bytes

    @property
    def file_count(self):
        return self._total_count


class FMetaTree:
    def __init__(self, root_path):
        self.root_path = root_path
        # Each item is an entry
        self._path_dict = {}
        # Each item contains a list of entries
        self.sig_dict = {}
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

    def get_all(self):
        return self._path_dict.values()

    def get_for_cat(self, category: Category):
        return self.cat_dict[category].list

    def get_for_path(self, file_path, include_ignored=False):
        matching_list = self._path_dict.get(file_path, None)
        if matching_list is None or include_ignored:
            return matching_list
        else:
            return [fmeta for fmeta in matching_list if fmeta.category != Category.Ignored]

    def get_for_sig(self, signature):
        return self.sig_dict.get(signature, None)

    def add(self, item: FMeta):
        # ignored files may not have signatures
        if item.category != Category.Ignored:
            set_matching_sig = self.sig_dict.get(item.signature, None)
            if set_matching_sig is None:
                set_matching_sig = [item]
                self.sig_dict[item.signature] = set_matching_sig
            else:
                set_matching_sig.append(item)
                self._dup_count += 1

        item_matching_path = self._path_dict.get(item.file_path, None)
        if item_matching_path is not None:
            print(f'WARNING: overwriting metadata for path: {item.file_path}')
            self._total_size_bytes -= item_matching_path.size_bytes
        self._total_size_bytes += item.size_bytes
        self._path_dict[item.signature] = item

        cat = Category(item.category)
        if cat != Category.NA:
            self.cat_dict[cat].add(item)

    def get_stats_string(self):
        return f'FMetaTree=[sigs:{len(self.sig_dict)} paths:{len(self._path_dict)} duplicates:{self._dup_count}]'

    def get_summary(self):
        ignored_count = self.cat_dict[Category.Ignored].file_count
        ignored_size = self.cat_dict[Category.Ignored].size_bytes

        total_size = self._total_size_bytes - ignored_size
        size_hf = humanfriendly.format_size(total_size)

        count = len(self._path_dict) - ignored_count

        summary_string = f'{size_hf} in {count} files'
        if ignored_count > 0:
            ignored_size_hf = humanfriendly.format_size(ignored_size)
            summary_string += f' (+{ignored_size_hf} in {ignored_count} ignored files)'
        return summary_string
