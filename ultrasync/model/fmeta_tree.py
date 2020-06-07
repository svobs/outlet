import logging
import pathlib
from collections import deque
from typing import Iterable, List, Optional, Tuple, Union

import constants
import file_util
import format_util
from index.two_level_dict import Md5BeforePathDict
from index.uid_generator import UID
from model.display_node import DirNode, DisplayNode
from model.fmeta import FMeta
from model.node_identifier import LocalFsIdentifier, NodeIdentifier
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

    def __init__(self, root_node: DirNode, application):
        assert isinstance(root_node.node_identifier, LocalFsIdentifier)
        super().__init__(root_node.node_identifier)
        self.root_node = root_node
        self.cache_manager = application.cache_manager

        self._stats_loaded = False

    @classmethod
    def create_identifier(cls, full_path: str, uid: UID, category) -> NodeIdentifier:
        return LocalFsIdentifier(full_path=full_path, uid=uid, category=category)

    def get_parent_for_item(self, item: FMeta) -> Optional[DisplayNode]:
        parent_path: str = str(pathlib.Path(item.full_path).parent)
        if parent_path.startswith(self.root_path):
            return self.cache_manager.get_for_local_path(parent_path)
        return None

    def get_all(self) -> List[FMeta]:
        """
        Gets the complete set of all unique FMetas from this FMetaTree.
        Returns: List of FMetas from list of unique paths
        """
        return self.cache_manager.get_all_files_for_subtree(self.node_identifier)

    def get_children_for_root(self) -> Iterable[DisplayNode]:
        return self.cache_manager.get_children(self.root_node)

    def get_children(self, node: DisplayNode) -> Iterable[DisplayNode]:
        assert node.node_identifier.tree_type == constants.TREE_TYPE_LOCAL_DISK, f'For: {node.node_identifier}'
        return self.cache_manager.get_children(node)

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

    def refresh_stats(self):
        stats_sw = Stopwatch()
        queue = deque()
        stack = deque()
        queue.append(self.root_node)
        stack.append(self.root_node)

        # Loop over all dirs in the tree and zero out their stats. Also create a stack
        while len(queue) > 0:
            item = queue.popleft()
            item.zero_out_stats()

            children = self.get_children(item)
            if children:
                for child in children:
                    if child.is_dir():
                        queue.append(child)
                        stack.append(child)

        while len(stack) > 0:
            item = stack.pop()
            assert item.is_dir()

            children = self.get_children(item)
            if children:
                for child in children:
                    item.add_meta_metrics(child)

        self._stats_loaded = True

        logger.debug(f'{stats_sw} Refreshed stats for FMetaTree')

    def get_summary(self):
        if self._stats_loaded:
            size_hf = format_util.humanfriendlier_size(self.root_node.size_bytes)
            return f'{size_hf} total in {self.root_node.file_count:n} files and {self.root_node.dir_count:n} dirs'
        else:
            return 'Loading stats...'

    def __repr__(self):
        return f'FMetaTree(root="{self.node_identifier}"])'
