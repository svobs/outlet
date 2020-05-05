import logging
from typing import Dict, List, Optional, Union

import humanfriendly

import file_util
from model.category import Category
from model.display_id import LocalFsIdentifier
from model.display_node import DisplayNode
from model.fmeta import FMeta
from model.planning_node import PlanningNode
from model.subtree_snapshot import SubtreeSnapshot

logger = logging.getLogger(__name__)

"""
━━━━━━━━━━━━━━━━━┛ ✠ ┗━━━━━━━━━━━━━━━━━
                  FMetaList
━━━━━━━━━━━━━━━━━┓ ✠ ┏━━━━━━━━━━━━━━━━━
"""


class FMetaList:
    def __init__(self):
        self.list: List[FMeta] = []
        self._total_count: int = 0
        self._total_size_bytes: int = 0

    def add(self, item):
        self.list.append(item)
        if item.size_bytes:
            self._total_size_bytes += item.size_bytes
        # else:
        #     logger.debug(f'Object has no size: {item}')
        self._total_count += 1

    @property
    def size_bytes(self):
        return self._total_size_bytes

    @property
    def file_count(self):
        return self._total_count


"""
━━━━━━━━━━━━━━━━━┛ ✠ ┗━━━━━━━━━━━━━━━━━
                  FMetaTree
━━━━━━━━━━━━━━━━━┓ ✠ ┏━━━━━━━━━━━━━━━━━
"""


class FMetaTree(SubtreeSnapshot):
    """🢄🢄🢄 Note: each FMeta object should be unique within its tree. Each FMeta should not be shared
    between trees, and should be cloned if needed"""

    def __init__(self, root_path: str):
        super().__init__(LocalFsIdentifier(root_path))
        # Each item is an entry
        self._path_dict: Dict[str, FMeta] = {}
        # Each item contains a list of entries
        self._md5_dict: Dict[str, List[FMeta]] = {}
        self._cat_dict: Dict[Category, FMetaList] = {Category.Ignored: FMetaList(),
                                                     Category.Added: FMetaList(),
                                                     Category.Deleted: FMetaList(),
                                                     Category.Moved: FMetaList(),
                                                     Category.Updated: FMetaList(),
                                                     }
        self._dup_md5_count = 0
        self._total_size_bytes = 0

    def create_identifier(self, full_path, category):
        return LocalFsIdentifier(full_path=full_path, category=category)

    def categorize(self, fmeta, category: Category):
        """🢄🢄🢄 Convenience method to use when building the tree.
        Changes the category of the given fmeta, then adds it to the category dict.
        Important: this method assumes the fmeta has already been assigned to the md5_dict
        and path_dict"""
        assert category != Category.NA
        # param fmeta should already be a member of this tree
        assert self.get_for_path(path=fmeta.full_path, include_ignored=True) == fmeta
        fmeta.identifier.category = category
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
                    logger.debug(f'Examining FMeta path: {fmeta.full_path}')
                if fmeta.category != cat:
                    if logger.isEnabledFor(logging.DEBUG):
                        logger.debug(f'ERROR: BAD CATEGORY: found: {fmeta.category}')
                    errors += 1
                existing = by_path.get(fmeta.full_path, None)
                if existing is None:
                    by_path[fmeta.full_path] = fmeta
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

    def get_full_path_for_item(self, item: FMeta) -> str:
        # Trivial for FMetas
        return item.full_path

    def get_for_path(self, path, include_ignored=False) -> Optional[FMeta]:
        fmeta = self._path_dict.get(path, None)
        if fmeta is None or include_ignored:
            return fmeta
        elif fmeta.category != Category.Ignored:
            return fmeta

    def get_md5_set(self):
        return self._md5_dict.keys()

    def get_for_md5(self, md5):
        return self._md5_dict.get(md5, None)

    def get_relative_path_for_item(self, fmeta: FMeta):
        assert fmeta.full_path.startswith(self.root_path), f'FMeta full path ({fmeta.full_path}) does not contain root ({self.root_path})'
        return file_util.strip_root(fmeta.full_path, self.root_path)

    def remove(self, full_path, md5, remove_old_md5=False, ok_if_missing=False):
        """
        🢂 Removes from this FMetaTree the FMeta which matches the given file path and md5.
        Does sanity checks and raises exceptions if internal state is found to have problems.
        If match not found: returns None if ok_if_missing=True; raises exception otherwise.
        If remove_old_md5=True: ignore the value of 'md5' and instead remove the one found from the path search
        If match found for both file path and md5, it is removed and the removed element is returned.
        """
        match = self._path_dict.pop(full_path, None)
        if match is None:
            if ok_if_missing:
                logger.debug(f'Did not remove because not found in path dict: {full_path}')
                return None
            else:
                raise RuntimeError(f'Could not find FMeta for path: {full_path}')

        if match.category == Category.Ignored:
            # Will not be present in md5_dict
            return match

        if remove_old_md5:
            md5_to_find = match.md5
        else:
            if logger.isEnabledFor(logging.DEBUG) and md5 is not None and md5 != match.md5:
                logger.debug(f'Ignoring md5 ({match.md5}) from path match; removing specified md5 instead ({md5})')
            md5_to_find = md5

        matching_md5_list = self.get_for_md5(md5_to_find)
        if matching_md5_list is None:
            # This indicates a serious data problem
            raise RuntimeError(f'FMeta found for path: {full_path} but not md5: {md5_to_find}')

        path_matches = list(filter(lambda f: f.full_path == full_path, matching_md5_list))
        path_matches_count = len(path_matches)
        if path_matches_count == 0:
            raise RuntimeError(f'FMeta found for path: {full_path} but not md5: {md5_to_find}')
        elif path_matches_count > 1:
            raise RuntimeError(f'Multiple FMeta ({path_matches}) found for path: {full_path} and md5: {md5_to_find}')
        else:
            matching_md5_list.remove(path_matches[0])

        # (Don't worry about category list)

        return match

    def add_item(self, item: FMeta):
        assert item.full_path.startswith(self.root_path), f'FMeta (cat={item.category.name}) full path (' \
                                                          f'{item.full_path}) is not under this tree ({self.root_path})'

        if item.category == Category.Ignored:
            # ignored files may not have md5s
            logger.debug(f'Found ignored file: {item.full_path}')
        elif not item.md5:
            logger.debug(f'File has no MD5: {item.full_path}')
        else:
            set_matching_md5 = self._md5_dict.get(item.md5, None)
            if set_matching_md5 is None:
                set_matching_md5 = [item]
                self._md5_dict[item.md5] = set_matching_md5
            else:
                set_matching_md5.append(item)
                self._dup_md5_count += 1

        is_planning_node = isinstance(item, PlanningNode)

        item_matching_path = self._path_dict.get(item.full_path, None)
        if item_matching_path is not None:
            if is_planning_node:
                if not isinstance(item_matching_path, PlanningNode):
                    raise RuntimeError(f'Attempt to overwrite type {type(item_matching_path)} with PlanningNode! '
                                       f'Orig={item_matching_path}; New={item}')
            else:
                self._total_size_bytes -= item_matching_path.size_bytes
            logger.warning(f'Overwriting path: {item.full_path}')

        if not is_planning_node:
            self._total_size_bytes += item.size_bytes

        self._path_dict[item.full_path] = item

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
        return self.__repr__()

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

    def __repr__(self):
        cats_string = self.get_category_summary_string()
        return f'FMetaTree(Paths={len(self._path_dict)} MD5s={len(self._md5_dict)} Dup_MD5s={self._dup_md5_count} Root="{self.root_path}" cats=[{cats_string}])'
