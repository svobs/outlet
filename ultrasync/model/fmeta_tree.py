import logging
import os
import pathlib
from typing import Dict, List, Optional, Union, ValuesView

import constants
import file_util
import format_util
from index import uid_generator
from index.atomic_counter import AtomicCounter
from index.two_level_dict import Md5BeforePathDict, Md5BeforeUidDict
from index.uid_generator import UID
from model.category import Category
from model.node_identifier import NodeIdentifier, LocalFsIdentifier
from model.display_node import DirNode, DisplayNode
from model.fmeta import FMeta
from model.planning_node import PlanningNode
from model.subtree_snapshot import SubtreeSnapshot
from stopwatch_sec import Stopwatch

logger = logging.getLogger(__name__)

"""
━━━━━━━━━━━━━━━━━┛ ✠ ┗━━━━━━━━━━━━━━━━━
                  FMetaTree
━━━━━━━━━━━━━━━━━┓ ✠ ┏━━━━━━━━━━━━━━━━━
"""


class FMetaTree(SubtreeSnapshot):
    """🢄🢄🢄 Note: each FMeta object should be unique within its tree. Each FMeta should not be shared
    between trees, and should be cloned if needed"""

    def __init__(self, root_path: str):
        super().__init__(LocalFsIdentifier(full_path=root_path))
        # Each item is an entry
        self._path_dict: Dict[str, FMeta] = {}
        # Each item contains a list of entries
        self._ignored_items: List[FMeta] = []
        self._total_size_bytes = 0

    @property
    def root_node(self):
        return self.create_identifier(full_path=self.root_path, uid=uid_generator.NULL_UID, category=Category.NA)

    @property
    def tree_type(self):
        return constants.OBJ_TYPE_LOCAL_DISK

    @classmethod
    def create_identifier(cls, full_path: str, uid: UID, category) -> NodeIdentifier:
        return LocalFsIdentifier(full_path=full_path, uid=uid, category=category)

    def get_parent_for_item(self, item) -> Optional[DisplayNode]:
        # FIXME: add support for storing dir metadata in FMetaTree. Ditch this fake stuff
        parent = str(pathlib.Path(item.full_path).parent)
        if parent.startswith(self.root_path):
            identifer = LocalFsIdentifier(full_path=parent)
            return DirNode(identifer)
        return None

    def get_all(self) -> ValuesView[FMeta]:
        """
        Gets the complete set of all unique FMetas from this FMetaTree.
        Returns: List of FMetas from list of unique paths
        """
        return self._path_dict.values()

    def get_ignored_items(self):
        return self._ignored_items

    def get_full_path_for_item(self, item: FMeta) -> str:
        # Trivial for FMetas
        return item.full_path

    def get_for_path(self, path, include_ignored=False) -> List[FMeta]:
        fmeta = self._path_dict.get(path, None)
        if fmeta is None:
            return []
        if include_ignored or fmeta.category != Category.Ignored:
            return [fmeta]

    def get_md5_dict(self):
        md5_set_stopwatch = Stopwatch()

        md5_dict: Md5BeforePathDict = Md5BeforePathDict()
        for item in self._path_dict.values():
            if item.md5:
                md5_dict.put(item)

        logger.info(f'{md5_set_stopwatch} Found {md5_dict.total_entries} MD5s')
        return md5_dict

    def get_relative_path_for_full_path(self, full_path: str):
        assert full_path.startswith(self.root_path), f'Full path ({full_path}) does not contain root ({self.root_path})'
        return file_util.strip_root(full_path, self.root_path)

    def get_relative_path_for_item(self, fmeta: FMeta):
        return self.get_relative_path_for_full_path(fmeta.full_path)

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

        return match

    def add_item(self, item: Union[FMeta, PlanningNode]):
        assert item.full_path.startswith(self.root_path), f'FMeta (cat={item.category.name}) full path (' \
                                                          f'{item.full_path}) is not under this tree ({self.root_path})'

        if item.category == Category.Ignored:
            logger.debug(f'Found ignored file: {item.full_path}')
            self._ignored_items.append(item)

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

    def get_summary(self):
        """
        Returns: summary of the aggregate FMeta in this tree.

        Remember: path dict contains ALL file meta, including faux-meta such as
        'deleted' meta, as well as 'ignored' meta. We subtract that out here.

        """
        ignored_count = len(self._ignored_items)
        ignored_size = 0
        for ignored in self._ignored_items:
            if ignored.size_bytes:
                ignored_size += ignored.size_bytes

        total_size = self._total_size_bytes - ignored_size
        size_hf = format_util.humanfriendlier_size(total_size)

        count = len(self._path_dict) - ignored_count

        summary_string = f'{size_hf} total in {format_util.with_commas(count)} files'
        if ignored_count > 0:
            ignored_size_hf = format_util.humanfriendlier_size(ignored_size)
            summary_string += f' (+{ignored_size_hf} in {ignored_count} ignored files)'
        return summary_string

    def __repr__(self):
        return f'FMetaTree(Paths={len(self._path_dict)} Root="{self.root_path}"])'
