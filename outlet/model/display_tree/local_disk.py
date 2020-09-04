import logging
import pathlib
from typing import Iterable, List, Optional

from pydispatch import dispatcher

import constants
from ui import actions
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
        self.root_node: LocalDirNode = root_node
        self.cache_manager = application.cache_manager

        self._stats_loaded = False

    def get_parent_for_node(self, node: LocalFileNode) -> Optional[DisplayNode]:
        if node.get_tree_type() != constants.TREE_TYPE_LOCAL_DISK:
            return None

        return self.cache_manager.get_parent_for_node(node, self.root_path)

    def get_children_for_root(self) -> Iterable[DisplayNode]:
        return self.cache_manager.get_children(self.root_node)

    def get_children(self, parent: DisplayNode) -> Iterable[DisplayNode]:
        assert parent.node_identifier.tree_type == constants.TREE_TYPE_LOCAL_DISK, f'For: {parent.node_identifier}'
        return self.cache_manager.get_children(parent)

    def get_full_path_for_node(self, node: LocalFileNode) -> str:
        # Trivial for FMetas
        return node.full_path

    def get_for_path(self, path: str, include_ignored=False) -> List[LocalFileNode]:
        node = self.cache_manager.get_node_for_local_path(path)
        if node:
            if node.full_path.startswith(self.root_path):
                return [node]
        return []

    def get_md5_dict(self):
        md5_set_stopwatch = Stopwatch()

        md5_dict: Md5BeforePathDict = Md5BeforePathDict()
        files_list, dir_list = self.cache_manager.get_all_files_and_dirs_for_subtree(self.node_identifier)
        for node in files_list:
            if node.exists() and node.md5:
                md5_dict.put(node)

        logger.info(f'{md5_set_stopwatch} Found {md5_dict.total_entries} MD5s in {self.root_path}')
        return md5_dict

    def get_relative_path_for_full_path(self, full_path: str):
        assert full_path.startswith(self.root_path), f'Full path ({full_path}) does not contain root ({self.root_path})'
        return file_util.strip_root(full_path, self.root_path)

    def get_relative_path_for_node(self, node: LocalFileNode):
        return self.get_relative_path_for_full_path(node.full_path)

    def remove(self, node: LocalFileNode):
        raise RuntimeError('Can no longer do this in LocalDiskSubtree!')

    def get_summary(self):
        if self._stats_loaded:
            size_hf = format.humanfriendlier_size(self.root_node.get_size_bytes())
            return f'{size_hf} total in {self.root_node.file_count:n} files and {self.root_node.dir_count:n} dirs'
        else:
            return 'Loading stats...'

    def refresh_stats(self, tree_id: str):
        self.cache_manager.refresh_stats(tree_id, self.root_node)
        self._stats_loaded = True
        dispatcher.send(signal=actions.REFRESH_SUBTREE_STATS_DONE, sender=tree_id)
        dispatcher.send(signal=actions.SET_STATUS, sender=tree_id, status_msg=self.get_summary())

    def __repr__(self):
        return f'LocalDiskSubtree(root="{self.node_identifier}"])'
