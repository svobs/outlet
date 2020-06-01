import logging
import os
import pathlib
from typing import Dict, Iterable, List, Optional, Union, ValuesView

import constants
import file_util
import format_util
from index.two_level_dict import Md5BeforePathDict, Md5BeforeUidDict
from index.uid_generator import UID, UidGenerator
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
    """🢄 Just a shell of its former self!"""

    def __init__(self, root_identifier: LocalFsIdentifier, application):
        assert isinstance(root_identifier, LocalFsIdentifier)
        super().__init__(root_identifier)
        self.cache_manager = application.cache_manager

    @classmethod
    def create_identifier(cls, full_path: str, uid: UID, category) -> NodeIdentifier:
        return LocalFsIdentifier(full_path=full_path, uid=uid, category=category)

    def get_parent_for_item(self, item: FMeta) -> Optional[DisplayNode]:
        parent = str(pathlib.Path(item.full_path).parent)
        if parent.startswith(self.root_path):
            return self.cache_manager.get_parent_for_item(item)
        return None

    def get_all(self) -> List[FMeta]:
        """
        Gets the complete set of all unique FMetas from this FMetaTree.
        Returns: List of FMetas from list of unique paths
        """
        return self.cache_manager.get_all_files_for_subtree(self.node_identifier)

    def get_children_for_root(self) -> Iterable[DisplayNode]:
        return self.cache_manager.get_children(self.node_identifier)

    def get_children(self, parent_identifier: NodeIdentifier) -> Iterable[DisplayNode]:
        assert parent_identifier.tree_type == constants.TREE_TYPE_LOCAL_DISK, f'For: {parent_identifier}'
        return self.cache_manager.get_children(parent_identifier)

    def get_full_path_for_item(self, item: FMeta) -> str:
        # Trivial for FMetas
        return item.full_path

    def get_for_path(self, path: str, include_ignored=False) -> List[FMeta]:
        item = self.cache_manager.get_for_local_path(path)
        if item:
            if item.full_path.startswith(self.root_path):
                return [item]
        return []

    def get_md5_dict(self):
        md5_set_stopwatch = Stopwatch()

        md5_dict: Md5BeforePathDict = Md5BeforePathDict()
        for item in self.get_all():
            if item.md5:
                md5_dict.put(item)

        logger.info(f'{md5_set_stopwatch} Found {md5_dict.total_entries} MD5s in {self.root_path}')
        return md5_dict

    def get_relative_path_for_full_path(self, full_path: str):
        assert full_path.startswith(self.root_path), f'Full path ({full_path}) does not contain root ({self.root_path})'
        return file_util.strip_root(full_path, self.root_path)

    def get_relative_path_for_item(self, fmeta: FMeta):
        return self.get_relative_path_for_full_path(fmeta.full_path)

    def remove(self, node: FMeta):
        raise RuntimeError('Can no longer do this in FMetaTree!')

    def add_item(self, item: Union[FMeta, PlanningNode]):
        raise RuntimeError('Can no longer do this in FMetaTree!')

    def get_summary(self):
        """
        Returns: summary of the aggregate FMeta in this tree.
        """

        total_size = 0  # TODO!
        size_hf = format_util.humanfriendlier_size(total_size)

        count = 0  # TODO!

        summary_string = f'{size_hf} total in {format_util.with_commas(count)} files'
        return summary_string

    def __repr__(self):
        return f'FMetaTree(root="{self.node_identifier}"])'
