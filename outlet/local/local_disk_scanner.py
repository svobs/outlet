import errno
import logging
import os
from pathlib import Path

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


def _check_update_sanity(old_fmeta, new_fmeta):
    if new_fmeta.modify_ts < old_fmeta.modify_ts:
        logger.warning(f'File "{new_fmeta.full_path}": update has older modify_ts ({new_fmeta.modify_ts}) than prev version ({old_fmeta.modify_ts})')

    if new_fmeta.change_ts < old_fmeta.change_ts:
        logger.warning(f'File "{new_fmeta.full_path}": update has older change_ts ({new_fmeta.change_ts}) than prev version ({old_fmeta.change_ts})')

    if new_fmeta.get_size_bytes() != old_fmeta.get_size_bytes() and new_fmeta.md5 == old_fmeta.md5:
        logger.warning(f'File "{new_fmeta.full_path}": update has same md5 ({new_fmeta.md5}) ' +
                       f'but different size: (old={old_fmeta.get_size_bytes()}, new={new_fmeta.get_size_bytes()})')


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
        self.cache_manager = application.cache_manager
        self.root_node_identifier: LocalFsIdentifier = root_node_identifer
        self.tree_id = tree_id  # For sending progress updates
        self.progress = 0
        self.total = 0

        self._local_tree: LocalDiskTree = LocalDiskTree(application)
        root_node = LocalDirNode(node_identifier=root_node_identifer, exists=os.path.exists(root_node_identifer.full_path))
        self._local_tree.add_node(node=root_node, parent=None)

        self.added_count = 0
        self.updated_count = 0
        self.deleted_count = 0
        self.unchanged_count = 0

    def _find_total_files_to_scan(self):
        # First survey our local files:
        logger.info(f'Scanning path: {self.root_path}')
        file_counter = FileCounter(self.root_path)
        file_counter.recurse_through_dir_tree()

        total = file_counter.files_to_scan
        logger.debug(f'Found {total} files to scan.')
        return total

    def handle_file(self, file_path: str):
        stale_fmeta: LocalFileNode = self.cache_manager.get_node_for_local_path(file_path)

        if stale_fmeta:
            if meta_matches(file_path, stale_fmeta):
                # No change from cache
                self.unchanged_count += 1
                target_fmeta = stale_fmeta
            else:
                # this can fail (e.g. broken symlink). If it does, we'll treat it like a deleted file
                target_fmeta = self.cache_manager.build_local_file_node(full_path=file_path)
                if target_fmeta:
                    self.updated_count += 1
                    if logger.isEnabledFor(logging.DEBUG):
                        _check_update_sanity(stale_fmeta, target_fmeta)
        else:
            # Not in cache (i.e. new):
            target_fmeta = self.cache_manager.build_local_file_node(full_path=file_path)
            if target_fmeta:
                self.added_count += 1

        if target_fmeta:
            self._local_tree.add_to_tree(target_fmeta)

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
        stale_dir: LocalDirNode = self.cache_manager.get_node_for_local_path(dir_path)
        if stale_dir:
            # logger.debug(f'Found existing dir node: {stale_dir.node_identifier}')
            stale_dir.set_exists(True)
        else:
            uid = self.cache_manager.get_uid_for_path(dir_path)
            dir_node = LocalDirNode(node_identifier=LocalFsIdentifier(full_path=dir_path, uid=uid), exists=True)
            logger.debug(f'Adding dir node: {dir_node.node_identifier}')
            self._local_tree.add_to_tree(dir_node)

    def scan(self) -> LocalDiskTree:
        """Recurse over disk tree. Gather current stats for each file, and compare to the stale tree.
        For each current file found, remove from the stale tree.
        When recursion is complete, what's left in the stale tree will be deleted/moved files"""

        if not os.path.exists(self.root_path):
            raise FileNotFoundError(errno.ENOENT, os.strerror(errno.ENOENT), self.root_path)

        if self.tree_id:
            self.total = self._find_total_files_to_scan()
            logger.debug(f'Sending START_PROGRESS with total={self.total} for tree_id: {self.tree_id}')
            dispatcher.send(signal=actions.START_PROGRESS, sender=self.tree_id, total=self.total)
        try:
            self.recurse_through_dir_tree()

            # FIXME: deleted_count never actually populated
            logger.info(f'Result: {self.added_count} new, {self.updated_count} updated, ? deleted, '
                        f'and {self.unchanged_count} unchanged from cache')

            return self._local_tree
        finally:
            if self.tree_id:
                logger.debug(f'Sending STOP_PROGRESS for tree_id: {self.tree_id}')
                actions.get_dispatcher().send(actions.STOP_PROGRESS, sender=self.tree_id)
