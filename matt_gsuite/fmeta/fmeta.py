import humanfriendly
import itertools
from enum import Enum, auto

# See: https://www.notinventedhere.org/articles/python/how-to-use-strings-as-name-aliases-in-python-enums.html
_CATEGORIES = {
    0: ['None', 'NA'],
    1: ['Ignored', 'IGNORED'],
    2: ['Added', 'ADDED'],
    3: ['Deleted', 'DELETED'],
    4: ['Updated', 'UPDATED'],
    5: ['Moved', 'MOVED'],
}
Category = Enum(
    value='Category',
    names=itertools.chain.from_iterable(
        itertools.product(v, [k]) for k, v in _CATEGORIES.items()
    )
)


class FMeta:
    def __init__(self, signature, size_bytes, sync_ts, modify_ts, file_path, category=Category.NA, prev_path=None):
        self.signature = signature
        self.size_bytes = size_bytes
        self.sync_ts = sync_ts
        self.modify_ts = modify_ts
        self.file_path = file_path
        self.category = category
        # Only used if category == MOVED
        self.prev_path = prev_path

    def __iter__(self):
        yield self.signature
        yield self.size_bytes
        yield self.sync_ts
        yield self.modify_ts
        yield self.file_path
        yield self.category.value
        yield self.prev_path

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


class DirNode:
    """For directories"""
    def __init__(self, file_path, category):
        self.file_path = file_path
        self.items = 0
        self.size_bytes = 0
        self.category = category

    def add_meta(self, fmeta):
        if fmeta.category != self.category:
            print(f'BAD CATEGORY: expected={self.category} found={fmeta.category} path={fmeta.file_path}')
        assert fmeta.category == self.category
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
        super().__init__('', category)


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
        self._cat_dict = {Category.Ignored: FMetaList(),
                          Category.Added: FMetaList(),
                          Category.Deleted: FMetaList(),
                          Category.Moved: FMetaList(),
                          Category.Updated: FMetaList(),
                          }
        self._dup_sig_count = 0
        self._total_size_bytes = 0

    def categorize(self, fmeta, category: Category):
        assert category != Category.NA
        fmeta.category = category
        return self._cat_dict[category].add(fmeta)

    def clear_categories(self):
        for cat, cat_list in self._cat_dict.items():
            if cat != Category.Ignored:
                cat_list.list.clear()

    def validate_categories(self, print_debug=False):
        errors = 0
        for cat, cat_list in self._cat_dict.items():
            if print_debug:
                print(f'Examining Category {cat.name}')
            by_path = {}
            for fmeta in cat_list.list:
                if print_debug:
                    print(f'Examining FMeta path: {fmeta.file_path}')
                if fmeta.category != cat:
                    if print_debug:
                        print(f'BAD CATEGORY: found: {fmeta.category}')
                    errors += 1
                existing = by_path.get(fmeta.file_path, None)
                if existing is None:
                    by_path[fmeta.file_path] = fmeta
                else:
                    if print_debug:
                        print(f'DUP IN CATEGORY')
                    errors += 1
        if errors > 0:
            raise RuntimeError(f'Category valication found {errors} errors')

    def get_category_summary_string(self):
        summary = []
        for cat in self._cat_dict.keys():
            length = len(self._cat_dict[cat].list)
            summary.append(f'{cat.name}={length}')
        return ' '.join(summary)

    def get_all(self):
        return self._path_dict.values()

    def get_for_cat(self, category: Category):
        return self._cat_dict[category].list

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
                self._dup_sig_count += 1

        item_matching_path = self._path_dict.get(item.file_path, None)
        if item_matching_path is not None:
            print(f'WARNING: overwriting metadata for path: {item.file_path}')
            self._total_size_bytes -= item_matching_path.size_bytes
        self._total_size_bytes += item.size_bytes
        self._path_dict[item.signature] = item

        cat = Category(item.category)
        if cat != Category.NA:
            self._cat_dict[cat].add(item)

    def get_stats_string(self):
        return f'FMetaTree=[sigs:{len(self.sig_dict)} paths:{len(self._path_dict)} duplicate sigs:{self._dup_sig_count}]'

    def get_summary(self):
        ignored_count = self._cat_dict[Category.Ignored].file_count
        ignored_size = self._cat_dict[Category.Ignored].size_bytes

        total_size = self._total_size_bytes - ignored_size
        size_hf = humanfriendly.format_size(total_size)

        count = len(self._path_dict) - ignored_count

        summary_string = f'{size_hf} in {count} files'
        if ignored_count > 0:
            ignored_size_hf = humanfriendly.format_size(ignored_size)
            summary_string += f' (+{ignored_size_hf} in {ignored_count} ignored files)'
        return summary_string
