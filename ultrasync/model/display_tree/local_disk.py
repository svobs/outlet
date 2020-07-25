import logging
import pathlib
from typing import Iterable, List, Optional

import constants
from util import file_util, format
from index.two_level_dict import Md5BeforePathDict
from model.node.display_node import DisplayNode
from model.node.local_disk_node import LocalDirNode, LocalFileNode
from model.node_identifier import LocalFsIdentifier
from model.display_tree.display_tree import DisplayTree
from util.stopwatch_sec import Stopwatch

logger = logging.getLogger(__name__)

"""
━━━━━━━━━━━━━━━━━┛ ✠ ┗━━━━━━━━━━━━━━━━━
           LocalDiskSubtree
━━━━━━━━━━━━━━━━━┓ ✠ ┏━━━━━━━━━━━━━━━━━
"""


class LocalDiskSubtree(DisplayTree):
    """🢄 Just a shell of its former self!"""

    def __init__(self, root_node: LocalDirNode, application):
        assert isinstance(root_node.node_identifier, LocalFsIdentifier)
        super().__init__(root_node)
        self.root_node = root_node
        self.cache_manager = application.cache_manager

        self._stats_loaded = False

    def get_parent_for_item(self, item: LocalFileNode) -> Optional[DisplayNode]:
        assert item.full_path, f'No full_path for item: {item}'
        parent_path: str = str(pathlib.Path(item.full_path).parent)
        if parent_path.startswith(self.root_path):
            return self.cache_manager.get_node_for_local_path(parent_path)
        return None

    def get_children_for_root(self) -> Iterable[DisplayNode]:
        return self.cache_manager.get_children(self.root_node)

    def get_children(self, node: DisplayNode) -> Iterable[DisplayNode]:
        assert node.node_identifier.tree_type == constants.TREE_TYPE_LOCAL_DISK, f'For: {node.node_identifier}'
        return self.cache_manager.get_children(node)

    def get_full_path_for_item(self, item: LocalFileNode) -> str:
        # Trivial for FMetas
        return item.full_path

    def get_for_path(self, path: str, include_ignored=False) -> List[LocalFileNode]:
        item = self.cache_manager.get_node_for_local_path(path)
        if item:
            if item.full_path.startswith(self.root_path):
                return [item]
        return []

    def get_md5_dict(self):
        md5_set_stopwatch = Stopwatch()

        md5_dict: Md5BeforePathDict = Md5BeforePathDict()
        files_list, dir_list = self.cache_manager.get_all_files_and_dirs_for_subtree(self.node_identifier)
        for item in files_list:
            if item.exists() and item.md5:
                md5_dict.put(item)

        logger.info(f'{md5_set_stopwatch} Found {md5_dict.total_entries} MD5s in {self.root_path}')
        return md5_dict

    def get_relative_path_for_full_path(self, full_path: str):
        assert full_path.startswith(self.root_path), f'Full path ({full_path}) does not contain root ({self.root_path})'
        return file_util.strip_root(full_path, self.root_path)

    def get_relative_path_for_item(self, item: LocalFileNode):
        return self.get_relative_path_for_full_path(item.full_path)

    def remove(self, node: LocalFileNode):
        raise RuntimeError('Can no longer do this in LocalDiskSubtree!')

    def get_summary(self):
        if self._stats_loaded:
            size_hf = format.humanfriendlier_size(self.root_node.get_size_bytes())
            return f'{size_hf} total in {self.root_node.file_count:n} files and {self.root_node.dir_count:n} dirs'
        else:
            return 'Loading stats...'

    def __repr__(self):
        return f'LocalDiskSubtree(root="{self.node_identifier}"])'