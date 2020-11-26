import errno
import logging
import os
from pathlib import Path
from typing import Optional

from pydispatch import dispatcher

import ui.actions as actions
from constants import TrashStatus
from model.node.local_disk_node import LocalDirNode
from model.local_disk_tree import LocalDiskTree
from model.node_identifier import LocalNodeIdentifier, NodeIdentifier
from store.local.local_tree_recurser import LocalTreeRecurser

logger = logging.getLogger(__name__)

# disabled:
VALID_SUFFIXES = None


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
    Walks the filesystem for a subtree (DisplayTree), using a cache if configured,
    to generate an up-to-date list of FMetas.
    """

    def __init__(self, app, root_node_identifer: LocalNodeIdentifier, tree_id=None):
        LocalTreeRecurser.__init__(self, Path(root_node_identifer.get_single_path()), valid_suffixes=None)
        assert isinstance(root_node_identifer, LocalNodeIdentifier), f'type={type(root_node_identifer)}, for {root_node_identifer}'
        self.app = app
        self.cacheman = app.cacheman
        self.root_node_identifier: LocalNodeIdentifier = root_node_identifer
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
        target_node = self.cacheman.build_local_file_node(full_path=file_path)
        if target_node:
            self._local_tree.add_to_tree(target_node)

        if self.tree_id:
            dispatcher.send(actions.PROGRESS_MADE, sender=self.tree_id, progress=1)
            self.progress += 1
            msg = f'Scanning file {self.progress} of {self.total}'
            dispatcher.send(actions.SET_PROGRESS_TEXT, sender=self.tree_id, msg=msg)

    def handle_target_file_type(self, file_path):
        self.handle_file(file_path)

    def handle_non_target_file(self, file_path):
        self.handle_file(file_path)

    def handle_dir(self, dir_path: str):
        dir_node: LocalDirNode = self.cacheman.get_node_for_local_path(dir_path)
        if dir_node:
            # logger.debug(f'[{self.tree_id}] Found existing dir node: {dir_node.node_identifier}')
            dir_node.set_is_live(True)
        else:
            uid = self.cacheman.get_uid_for_path(dir_path)
            dir_node = LocalDirNode(node_identifier=LocalNodeIdentifier(path_list=dir_path, uid=uid), trashed=TrashStatus.NOT_TRASHED, is_live=True)
            logger.debug(f'[{self.tree_id}] Adding dir node: {dir_node.node_identifier}')

        self._local_tree.add_to_tree(dir_node)

    def scan(self) -> LocalDiskTree:
        """Recurse over disk tree. Gather current stats for each file, and compare to the stale tree.
        For each current file found, remove from the stale tree.
        When recursion is complete, what's left in the stale tree will be deleted/moved files"""

        assert self.root_node_identifier.get_single_path() == str(self.root_path), \
            f'Expected match: (1)="{self.root_node_identifier.get_single_path()}", (2)="{self.root_path}"'
        if not os.path.exists(self.root_node_identifier.get_single_path()):
            raise FileNotFoundError(errno.ENOENT, os.strerror(errno.ENOENT), self.root_node_identifier.get_single_path())

        self._local_tree = LocalDiskTree(self.app)
        if not os.path.isdir(self.root_node_identifier.get_single_path()):
            logger.debug(f'[{self.tree_id}] Root is a file; returning tree with a single node')
            root_node = self.cacheman.build_local_file_node(full_path=self.root_node_identifier.get_single_path())
            self._local_tree.add_node(node=root_node, parent=None)
            return self._local_tree

        root_node = LocalDirNode(node_identifier=self.root_node_identifier, trashed=TrashStatus.NOT_TRASHED, is_live=True)
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
                dispatcher.send(actions.STOP_PROGRESS, sender=self.tree_id)
