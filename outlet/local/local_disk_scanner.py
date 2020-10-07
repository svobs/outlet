import errno
import logging
import os
from pathlib import Path
from typing import Optional

from pydispatch import dispatcher

import ui.actions as actions
from local.local_tree_recurser import LocalTreeRecurser
from model.node.local_disk_node import LocalFileNode, LocalDirNode
from model.local_disk_tree import LocalDiskTree
from model.node_identifier import LocalFsIdentifier, NodeIdentifier

logger = logging.getLogger(__name__)

# disabled:
VALID_SUFFIXES = None


def meta_matches(file_path: str, node: LocalFileNode):
    stat = os.stat(file_path)
    size_bytes = int(stat.st_size)
    modify_ts = int(stat.st_mtime * 1000)
    assert modify_ts > 100000000000, f'modify_ts too small: {modify_ts} (for path: {file_path})'
    change_ts = int(stat.st_ctime * 1000)
    assert change_ts > 100000000000, f'change_ts too small: {change_ts} (for path: {file_path})'

    is_equal = node.exists() and node.get_size_bytes() == size_bytes and node.modify_ts == modify_ts and node.change_ts == change_ts

    if False and logger.isEnabledFor(logging.DEBUG):
        logger.debug(f'Meta Exp=[{node.get_size_bytes()} {node.modify_ts} {node.change_ts}]' +
                     f' Act=[{size_bytes} {modify_ts} {change_ts}] -> {is_equal}')

    return is_equal


def _check_update_sanity(old_node: LocalFileNode, new_node: LocalFileNode):
    try:
        if not isinstance(old_node, LocalFileNode):
            # Internal error; try to recover
            logger.error(f'Invalid node type for old_node: {type(old_node)}. Will overwrite cache entry')
            return

        if not isinstance(new_node, LocalFileNode):
            raise RuntimeError(f'Invalid node type for new_node: {type(new_node)}')

        if new_node.modify_ts < old_node.modify_ts:
            logger.warning(f'File "{new_node.full_path}": update has older modify_ts ({new_node.modify_ts}) than prev version ({old_node.modify_ts})')

        if new_node.change_ts < old_node.change_ts:
            logger.warning(f'File "{new_node.full_path}": update has older change_ts ({new_node.change_ts}) than prev version ({old_node.change_ts})')

        if new_node.get_size_bytes() != old_node.get_size_bytes() and new_node.md5 == old_node.md5 and old_node.md5:
            logger.warning(f'File "{new_node.full_path}": update has same MD5 ({new_node.md5}) ' +
                           f'but different size: (old={old_node.get_size_bytes()}, new={new_node.get_size_bytes()})')
    except Exception:
        logger.error(f'Error checking update sanity! Old={old_node} New={new_node}')
        raise


# SUPPORT CLASSES ####################


class FileCounter(LocalTreeRecurser):
    """
    Does a quick walk of the filesystem and counts the files which are of interest
    """

    def __init__(self, root_path):
        LocalTreeRecurser.__init__(self, root_path, valid_suffixes=None)
        self.files_to_scan = 0
        self.dirs_to_scan = 0

    def handle_target_file_type(self, file_path):
        self.files_to_scan += 1

    def handle_non_target_file(self, file_path):
        self.files_to_scan += 1

    def handle_dir(self, dir_path: str):
        self.dirs_to_scan += 1


class LocalDiskScanner(LocalTreeRecurser):
    """
    Walks the filesystem for a subtree (LocalDiskDisplayTree), using a cache if configured,
    to generate an up-to-date list of FMetas.
    """

    def __init__(self, application, root_node_identifer: NodeIdentifier, tree_id=None):
        LocalTreeRecurser.__init__(self, Path(root_node_identifer.full_path), valid_suffixes=None)
        assert isinstance(root_node_identifer, LocalFsIdentifier), f'type={type(root_node_identifer)}, for {root_node_identifer}'
        self.application = application
        self.cache_manager = application.cache_manager
        self.root_node_identifier: LocalFsIdentifier = root_node_identifer
        self.tree_id = tree_id  # For sending progress updates
        self.progress = 0
        self.total = 0

        self._local_tree: Optional[LocalDiskTree] = None

    def _find_total_files_to_scan(self):
        # First survey our local files:
        logger.info(f'[{self.tree_id}] Scanning path: {self.root_path}')
        file_counter = FileCounter(self.root_path)
        file_counter.recurse_through_dir_tree()

        logger.debug(f'[{self.tree_id}] Found {file_counter.files_to_scan} files and {file_counter.dirs_to_scan} dirs to scan.')
        return file_counter.files_to_scan

    def handle_file(self, file_path: str):
        target_node = self.cache_manager.build_local_file_node(full_path=file_path)
        if target_node:
            self._local_tree.add_to_tree(target_node)

        if self.tree_id:
            actions.get_dispatcher().send(actions.PROGRESS_MADE, sender=self.tree_id, progress=1)
            self.progress += 1
            msg = f'Scanning file {self.progress} of {self.total}'
            actions.get_dispatcher().send(actions.SET_PROGRESS_TEXT, sender=self.tree_id, msg=msg)

    def handle_target_file_type(self, file_path):
        self.handle_file(file_path)

    def handle_non_target_file(self, file_path):
        self.handle_file(file_path)

    def handle_dir(self, dir_path: str):
        dir_node: LocalDirNode = self.cache_manager.get_node_for_local_path(dir_path)
        if dir_node:
            # logger.debug(f'[{self.tree_id}] Found existing dir node: {dir_node.node_identifier}')
            dir_node.set_exists(True)
        else:
            uid = self.cache_manager.get_uid_for_path(dir_path)
            dir_node = LocalDirNode(node_identifier=LocalFsIdentifier(full_path=dir_path, uid=uid), exists=True)
            logger.debug(f'[{self.tree_id}] Adding dir node: {dir_node.node_identifier}')

        self._local_tree.add_to_tree(dir_node)

    def scan(self) -> LocalDiskTree:
        """Recurse over disk tree. Gather current stats for each file, and compare to the stale tree.
        For each current file found, remove from the stale tree.
        When recursion is complete, what's left in the stale tree will be deleted/moved files"""

        assert self.root_node_identifier.full_path == str(self.root_path), \
            f'Expected match: (1)="{self.root_node_identifier.full_path}", (2)="{self.root_path}"'
        if not os.path.exists(self.root_node_identifier.full_path):
            raise FileNotFoundError(errno.ENOENT, os.strerror(errno.ENOENT), self.root_node_identifier.full_path)

        self._local_tree = LocalDiskTree(self.application)
        if not os.path.isdir(self.root_node_identifier.full_path):
            logger.debug(f'[{self.tree_id}] Root is a file; returning tree with a single node')
            root_node = self.cache_manager.build_local_file_node(full_path=self.root_node_identifier.full_path)
            self._local_tree.add_node(node=root_node, parent=None)
            return self._local_tree

        root_node = LocalDirNode(node_identifier=self.root_node_identifier, exists=True)
        self._local_tree.add_node(node=root_node, parent=None)

        self.total = self._find_total_files_to_scan()
        if self.tree_id:
            logger.debug(f'[{self.tree_id}] Sending START_PROGRESS with total={self.total}')
            dispatcher.send(signal=actions.START_PROGRESS, sender=self.tree_id, total=self.total)
        try:
            self.recurse_through_dir_tree()
            logger.debug(f'Scanned {self.total} files')
            return self._local_tree
        finally:
            if self.tree_id:
                logger.debug(f'Sending STOP_PROGRESS for tree_id: {self.tree_id}')
                actions.get_dispatcher().send(actions.STOP_PROGRESS, sender=self.tree_id)
