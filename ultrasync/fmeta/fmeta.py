import humanfriendly
import itertools
import logging
import os
from enum import Enum, auto
import file_util

logger = logging.getLogger(__name__)

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
    def __init__(self, signature, size_bytes, sync_ts, modify_ts, change_ts, file_path, category=Category.NA, prev_path=None):
        self.signature = signature
        self.size_bytes = size_bytes
        self.sync_ts = sync_ts
        self.modify_ts = modify_ts
        self.change_ts = change_ts
        self.file_path = file_path
        self.category = category
        # Only used if category == ADDED or MOVED
        self.prev_path = prev_path

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
    """Note: each FMeta object should be unique within its tree. Each FMeta should not be shared
    between trees, and should be cloned if needed"""
    def __init__(self, root_path):
        self.root_path = root_path
        # Each item is an entry
        self._path_dict = {}
        # Each item contains a list of entries
        self._sig_dict = {}
        self._cat_dict = {Category.Ignored: FMetaList(),
                          Category.Added: FMetaList(),
                          Category.Deleted: FMetaList(),
                          Category.Moved: FMetaList(),
                          Category.Updated: FMetaList(),
                          }
        self._dup_sig_count = 0
        self._total_size_bytes = 0

    def categorize(self, fmeta, category: Category):
        """Convenience method to use when building the tree.
        Changes the category of the given fmeta, then adds it to the category dict.
        Important: this method assumes the fmeta has already been assigned to the sig_dict
        and path_dict"""
        assert category != Category.NA
        # param fmeta should already be a member of this tree
        assert self.get_for_path(file_path=fmeta.file_path, include_ignored=True) == fmeta
        fmeta.category = category
        return self._cat_dict[category].add(fmeta)

    def clear_categories(self):
        for cat, cat_list in self._cat_dict.items():
            if cat != Category.Ignored:
                cat_list.list.clear()

    def validate_categories(self):
        errors = 0
        for cat, cat_list in self._cat_dict.items():
            if logger.isEnabledFor(logging.DEBUG):
                logger.debug(f'Examining Category {cat.name}')
            by_path = {}
            for fmeta in cat_list.list:
                if logger.isEnabledFor(logging.DEBUG):
                    logger.debug(f'Examining FMeta path: {fmeta.file_path}')
                if fmeta.category != cat:
                    if logger.isEnabledFor(logging.DEBUG):
                        logger.debug(f'ERROR: BAD CATEGORY: found: {fmeta.category}')
                    errors += 1
                existing = by_path.get(fmeta.file_path, None)
                if existing is None:
                    by_path[fmeta.file_path] = fmeta
                else:
                    if logger.isEnabledFor(logging.DEBUG):
                        logger.debug(f'ERROR: DUP IN CATEGORY: {fmeta.category.name}')
                    errors += 1
        if errors > 0:
            raise RuntimeError(f'Category validation found {errors} errors')

    def get_all(self):
        """
        Gets the complete set of all unique FMetas from this FMetaTree.
        Returns: List of FMetas from list of unique paths
        """
        return self._path_dict.values()

    def get_for_cat(self, category: Category):
        return self._cat_dict[category].list

    def get_for_path(self, file_path, include_ignored=False):
        if os.path.isabs(file_path):
            file_path = file_util.strip_root(file_path, self.root_path)
        fmeta = self._path_dict.get(file_path, None)
        if fmeta is None or include_ignored:
            return fmeta
        elif fmeta.category != Category.Ignored:
            return fmeta

    def get_sig_set(self):
        return self._sig_dict.keys()

    def get_for_sig(self, signature):
        return self._sig_dict.get(signature, None)

    def remove(self, file_path, sig, ok_if_missing=False):
        """Removes from this FMetaTree the FMeta which matches the given file path and signature.
        Does sanity checks and raises exceptions if internal state is found to have problems.
        If match not found: returns None if ok_if_missing=True; raises exception otherwise.
        If match found for both file path and sig, it is removed and the removed element is returned.
        """
        match = self._path_dict.pop(file_path, None)
        if match is None:
            if ok_if_missing:
                return None
            else:
                raise RuntimeError(f'Could not find FMeta for path: {file_path}')

        if match.category == Category.Ignored:
            # Will not be present in sig_dict
            return match

        matching_sig_list = self.get_for_sig(sig)
        if matching_sig_list is None:
            # This indicates a serious data problem
            raise RuntimeError(f'FMeta found for path: {file_path} but not sig: {sig}')

        path_matches = list(filter(lambda f: f.file_path == file_path, matching_sig_list))
        path_matches_count = len(path_matches)
        if path_matches_count == 0:
            raise RuntimeError(f'FMeta found for path: {file_path} but not signature: {sig}')
        elif path_matches_count > 1:
            raise RuntimeError(f'Multiple FMeta ({path_matches}) found for path: {file_path} and sig: {sig}')
        else:
            matching_sig_list.remove(path_matches[0])

        # (Don't worry about category list)

        return match

    def add(self, item: FMeta):
        if item.category == Category.Ignored:
            logger.debug(f'Found ignored file: {item.file_path}')
        else:
            # ignored files may not have signatures
            set_matching_sig = self._sig_dict.get(item.signature, None)
            if set_matching_sig is None:
                set_matching_sig = [item]
                self._sig_dict[item.signature] = set_matching_sig
            else:
                set_matching_sig.append(item)
                self._dup_sig_count += 1

        item_matching_path = self._path_dict.get(item.file_path, None)
        if item_matching_path is not None:
            logger.warning(f'Overwriting metadata for path: {item.file_path}')
            self._total_size_bytes -= item_matching_path.size_bytes
        self._total_size_bytes += item.size_bytes
        self._path_dict[item.file_path] = item

        if item.category != Category.NA:
            self._cat_dict[item.category].add(item)

    def get_category_summary_string(self):
        summary = []
        for cat in self._cat_dict.keys():
            length = len(self._cat_dict[cat].list)
            summary.append(f'{cat.name}={length}')
        return ' '.join(summary)

    def get_stats_string(self):
        """
        For internal use only
        """
        cats_string = self.get_category_summary_string()
        return f'FMetaTree=[sigs:{len(self._sig_dict)} paths:{len(self._path_dict)} dup_sigs:{self._dup_sig_count} cats=[{cats_string}]'

    def get_summary(self):
        """
        Returns: summary of the aggregate FMeta in this tree.

        Remember: path dict contains ALL file meta, including faux-meta such as
        'deleted' meta, as well as 'ignored' meta. We subtract that out here.

        """
        ignored_count = self._cat_dict[Category.Ignored].file_count
        ignored_size = self._cat_dict[Category.Ignored].size_bytes

        deleted_count = self._cat_dict[Category.Deleted].file_count
        deleted_size = self._cat_dict[Category.Deleted].size_bytes

        total_size = self._total_size_bytes - ignored_size - deleted_size
        size_hf = humanfriendly.format_size(total_size)

        count = len(self._path_dict) - ignored_count - deleted_count

        summary_string = f'{size_hf} total in {count} files'
        if ignored_count > 0:
            ignored_size_hf = humanfriendly.format_size(ignored_size)
            summary_string += f' (+{ignored_size_hf} in {ignored_count} ignored files)'
        return summary_string
