import os
from typing import Any, Callable, Dict, List, Optional, Union
import logging

logger = logging.getLogger(__name__)


"""
🡻🡻🡻 ① KEY FUNCS 🡻🡻🡻
"""


def get_md5(item):
    return item.md5


def get_uid(item):
    return item.uid


def get_sha256(item):
    return item.sha256


def get_full_path(item):
    return item.full_path


def get_parent_path(item):
    return os.path.dirname(item.full_path)


def get_file_name(item):
    return os.path.basename(item.full_path)


def overwrite_newer_ts(old, new) -> bool:
    if old.is_newer_than(new):
        logger.error('Existing item is newer than new item - will not overwrite in cache')
        return False
    return True


"""
🡻🡻🡻 ② Generic classes 🡻🡻🡻
"""


class OneLevelDict:
    def __init__(self, key_func1: Callable[[Any], str],
                 should_overwrite: Callable[[Any, Any], bool]):
        """
        Args:
            key_func1: Takes 'item' as single arg, and returns a key value for the
                lookup
            should_overwrite: function which takes 'old' and 'new' as args,
                and returns True if new should replace old; False if not.
                Will only be called if put() finds an existing item with matching
                keys.
        """
        self._dict: Dict[str, Dict[str, Any]] = {}
        self._key_func1 = key_func1
        self._should_overwrite = should_overwrite
        self.total_entries = 0

    def put(self, item, expected_existing=None):
        assert item, 'trying to insert None!'
        key1 = self._key_func1(item)
        if not key1:
            raise RuntimeError(f'Key1 is null for item: {item}')
        existing = self._dict.get(key1, None)
        if not existing:
            self._dict[key1] = item
            self.total_entries += 1
        elif expected_existing:
            if expected_existing != existing:
                logger.error(f'Replacing a different entry ({existing}) than expected ({expected_existing})!')
            # Overwrite either way...
            self._dict[key1] = item
        elif self._should_overwrite(existing, item):
            self._dict[key1] = item
        return existing

    def get(self, key: str):
        assert key, 'key is empty!'
        return self._dict.get(key, None)

    def remove(self, key):
        assert key, 'key is empty!'
        return self._dict.pop(key, None)

    def keys(self):
        return self._dict.keys()


class TwoLevelDict:
    def __init__(self, key_func1: Callable[[Any], Union[str, int]],
                 key_func2: Callable[[Any], Union[str, int]],
                 should_overwrite: Callable[[Any, Any], bool]):
        """
        Args:
            key_func1: Takes 'item' as single arg, and returns a key value for the
                first lookup
            key_func2: Takes 'item' as single arg, and returns a key value for the
                second lookup
            should_overwrite: function which takes 'old' and 'new' as args,
                and returns True if new should replace old; False if not.
                Will only be called if put() finds an existing item with matching
                keys.
        """
        self._dict: Dict[Union[str, int], Dict[Union[str, int], Any]] = {}
        self._key_func1 = key_func1
        self._key_func2 = key_func2
        self._should_overwrite = should_overwrite
        self.total_entries = 0

    def put(self, item, expected_existing=None) -> Optional[Any]:
        assert item, 'trying to insert None!'
        key1 = self._key_func1(item)
        if not key1:
            raise RuntimeError(f'Key1 is null for item: {item}')
        dict2 = self._dict.get(key1, None)
        if dict2 is None:
            dict2 = {}
            self._dict[key1] = dict2
        key2 = self._key_func2(item)
        if not key2:
            raise RuntimeError(f'Key2 is null for item: {item}')
        existing = dict2.get(key2, None)
        if not existing:
            dict2[key2] = item
            self.total_entries += 1
        elif expected_existing:
            if expected_existing != existing:
                logger.error(f'Replacing a different entry ({existing}) than expected ({expected_existing})!')
            # Overwrite either way...
            dict2[key2] = item
        elif self._should_overwrite(existing, item):
            dict2[key2] = item
        return existing

    def get_second_dict(self, key1: Union[str, int]) -> Dict[Union[str, int], Any]:
        dict2 = self._dict.get(key1, None)
        if dict2:
            return dict2
        return {}

    def get_single(self, key1: Union[str, int], key2: Union[str, int]) -> Optional[Any]:
        """If only one arg is provided, returns the entire dict which matches the first key.
        If two are provided, returns the item matching both keys, or None if not found"""
        assert key1, 'key1 is empty!'
        if key2 is None:
            print('TODO')
        assert key2, 'key2 is empty!'
        dict2 = self._dict.get(key1, None)
        if not dict2:
            return None
        return dict2.get(key2, None)

    def remove(self, key1: Union[str, int], key2: Union[str, int]) -> Optional[Any]:
        """Removes and returns the item matching both keys, or None if not found"""
        assert key1, 'key1 is empty!'
        assert key2, 'key2 is empty!'
        dict2 = self._dict.get(key1, None)
        if dict2 is None:
            return None
        return dict2.pop(key2, None)

    def keys(self):
        return self._dict.keys()

    def get_all(self) -> List[Any]:
        all_vals = []
        for d in self._dict.values():
            if d:
                all_vals += d.values()
        return all_vals

"""
🡻🡻🡻 ③ Parameterized classes 🡻🡻🡻
"""


class FullPathDict(OneLevelDict):
    def __init__(self):
        super().__init__(get_full_path, overwrite_newer_ts)


class ParentPathBeforeFileNameDict(TwoLevelDict):
    def __init__(self):
        super().__init__(get_parent_path, get_file_name, overwrite_newer_ts)


class FullPathBeforeMd5Dict(TwoLevelDict):
    def __init__(self):
        super().__init__(get_full_path, get_md5, overwrite_newer_ts)


class FullPathBeforeUidDict(TwoLevelDict):
    def __init__(self):
        super().__init__(get_full_path, get_uid, overwrite_newer_ts)


class Md5BeforeUidDict(TwoLevelDict):
    def __init__(self):
        super().__init__(get_md5, get_uid, overwrite_newer_ts)


class Md5BeforePathDict(TwoLevelDict):
    def __init__(self):
        super().__init__(get_md5, get_full_path, overwrite_newer_ts)


class Sha256BeforePathDict(TwoLevelDict):
    def __init__(self):
        super().__init__(get_sha256, get_full_path, overwrite_newer_ts)


class PathBeforeSha256Dict(TwoLevelDict):
    def __init__(self):
        super().__init__(get_full_path, get_sha256, overwrite_newer_ts)
