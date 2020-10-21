import logging
from typing import Dict, Optional, Tuple

from constants import LOCAL_ROOT_UID, ROOT_PATH
from model.local_disk_tree import LocalDiskTree
from model.node.container_node import RootTypeNode
from model.node.display_node import DisplayNode
from model.node.local_disk_node import LocalFileNode, LocalNode
from model.node_identifier import LocalFsIdentifier
from store.local.master_local import SUPER_DEBUG
from util.two_level_dict import Md5BeforePathDict, Sha256BeforePathDict

logger = logging.getLogger(__name__)


# CLASS LocalDiskMemoryStore
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
class LocalDiskMemoryStore:
    def __init__(self, app):
        self.use_md5 = app.config.get('cache.enable_md5_lookup')
        if self.use_md5:
            self.md5_dict = Md5BeforePathDict()
        else:
            self.md5_dict = None

        self.use_sha256 = app.config.get('cache.enable_sha256_lookup')
        if self.use_sha256:
            self.sha256_dict = Sha256BeforePathDict()
        else:
            self.sha256_dict = None

        # Each node inserted here will have an entry created for its dir.
        # self.parent_path_dict = ParentPathBeforeFileNameDict()
        # But we still need a dir tree to look up child dirs:
        self.master_tree = LocalDiskTree(app)
        root_node = RootTypeNode(node_identifier=LocalFsIdentifier(full_path=ROOT_PATH, uid=LOCAL_ROOT_UID))
        self.master_tree.add_node(node=root_node, parent=None)

        self.expected_node_moves: Dict[str, str] = {}
        """When the FileSystemEventHandler gives us MOVE notifications for a tree, it gives us a separate notification for each
        and every node. Since we want our tree move to be an atomic operation, we do it all at once, but then keep track of the
        nodes we've moved so that we know exactly which notifications to ignore after that.
        Dict is key-value pair of [old_file_path -> new_file_path]"""

    def remove_single_node(self, node: LocalNode):
        """Removes the given node from all in-memory structs (does nothing if it is not found in some or any of them).
        Will raise an exception if trying to remove a non-empty directory."""
        logger.debug(f'Removing LocalNode from memory cache: {node}')

        existing: DisplayNode = self.master_tree.get_node(node.uid)
        if existing:
            if existing.is_dir():
                children = self.master_tree.children(existing.identifier)
                if children:
                    # maybe allow deletion of dir with children in the future, but for now be careful
                    raise RuntimeError(f'Cannot remove dir from cache because it has {len(children)} children: {node}')

            count_removed = self.master_tree.remove_node(node.uid)
            assert count_removed <= 1, f'Deleted {count_removed} nodes at {node.full_path}'
        else:
            logger.warning(f'Cannot remove node because it has already been removed from cache: {node}')

        if self.use_md5 and node.md5:
            self.md5_dict.remove(node.md5, node.full_path)
        if self.use_sha256 and node.sha256:
            self.sha256_dict.remove(node.sha256, node.full_path)

    def upsert_single_node(self, node: LocalNode, update_only: bool = False) -> Tuple[Optional[LocalNode], bool]:
        """If a node already exists, the new node is merged into it and returned; otherwise the given node is returned.
        Second item in the tuple is True if update contained changes which should be saved to disk; False if otherwise"""

        if SUPER_DEBUG:
            logger.debug(f'Upserting LocalNode to memory cache: {node}')

        # 1. Validate UID:
        if not node.uid:
            raise RuntimeError(f'Cannot upsert node to cache because it has no UID: {node}')

        existing_node: LocalNode = self.master_tree.get_node(node.uid)
        if existing_node:
            if existing_node.exists() and not node.exists():
                # In the future, let's close this hole with more elegant logic
                logger.warning(f'Cannot replace a node which exists with one which does not exist; skipping cache update for {node.node_identifier}')
                return None, False

            if existing_node.is_dir() and not node.is_dir():
                # need to replace all descendants...not ready to do this yet
                raise RuntimeError(f'Cannot replace a directory with a file: "{node.full_path}"')

            if existing_node == node:
                if SUPER_DEBUG:
                    logger.debug(f'Node being upserted is identical to node already in the cache; skipping cache update '
                                 f'(CachedNode={existing_node}; NewNode={node}')
                return existing_node, False
            else:
                # Signature may have changed. Simplify things by just removing prev node before worrying about updated node
                if existing_node.md5 and self.use_md5:
                    self.md5_dict.remove(existing_node)
                if existing_node.sha256 and self.use_sha256:
                    self.sha256_dict.remove(existing_node)

            # just update the existing - much easier
            if SUPER_DEBUG:
                logger.debug(f'Merging node (PyID {id(node)}) into existing_node (PyID {id(existing_node)})')
            if node.is_file() and existing_node.is_file():
                assert isinstance(node, LocalFileNode) and isinstance(existing_node, LocalFileNode)
                _copy_signature_if_possible(existing_node, node)
                if SUPER_DEBUG:
                    _check_update_sanity(existing_node, node)
            existing_node.update_from(node)
            node = existing_node
        elif update_only:
            if SUPER_DEBUG:
                logger.debug(f'Skipping update of node {node.uid} because it is not in the cache')
            return node, False
        else:
            # new file or directory insert
            self.master_tree.add_to_tree(node)

        # do this after the above, to avoid cache corruption in case of failure
        if node.md5 and self.use_md5:
            self.md5_dict.put(node)
        if node.sha256 and self.use_sha256:
            self.sha256_dict.put(node)

        return node, True


def _copy_signature_if_possible(src: LocalFileNode, dst: LocalFileNode):
    if src.modify_ts == dst.modify_ts and src.change_ts == dst.change_ts and src.get_size_bytes() == dst.get_size_bytes():
        # It is possible for the stored cache copy to be missing a signature. If so, the src may not have an MD5/SHA256.
        # In that case, do not overwrite possible real data with null values, but do check to make sure we don't overwrite one value with a different
        if dst.md5:
            if src.md5 and dst.md5 != src.md5:
                logger.error(f'Dst node already has MD5 but it is unexpected: {dst} (expected {src}')
        elif src.md5:
            if SUPER_DEBUG:
                logger.debug(f'Copying MD5 for: {dst.node_identifier}')
            dst.md5 = src.md5
        if dst.sha256:
            if src.sha256 and dst.sha256 != src.sha256:
                logger.error(f'Dst node already has SHA256 but it is unexpected: {dst} (expected {src}')
        elif src.sha256:
            if SUPER_DEBUG:
                logger.debug(f'Copying SHA256 for: {dst.node_identifier}')
            dst.sha256 = src.sha256


def _check_update_sanity(old_node: LocalFileNode, new_node: LocalFileNode):
    try:
        if not old_node:
            raise RuntimeError(f'old_node is empty!')

        if not isinstance(old_node, LocalFileNode):
            # Internal error; try to recover
            logger.error(f'Invalid node type for old_node: {type(old_node)}. Will overwrite cache entry')
            return

        if not new_node:
            raise RuntimeError(f'new_node is empty!')

        if not isinstance(new_node, LocalFileNode):
            raise RuntimeError(f'Invalid node type for new_node: {type(new_node)}')

        if not old_node.modify_ts:
            logger.info(f'old_node has no modify_ts. Skipping modify_ts comparison (Old={old_node} New={new_node}')
        elif not new_node.modify_ts:
            raise RuntimeError(f'new_node is missing modify_ts!')
        elif new_node.modify_ts < old_node.modify_ts:
            logger.warning(
                f'File "{new_node.full_path}": update has older modify_ts ({new_node.modify_ts}) than prev version ({old_node.modify_ts})')

        if not old_node.change_ts:
            logger.info(f'old_node has no change_ts. Skipping change_ts comparison (Old={old_node} New={new_node}')
        elif not new_node.change_ts:
            raise RuntimeError(f'new_node is missing change_ts!')
        elif new_node.change_ts < old_node.change_ts:
            logger.warning(
                f'File "{new_node.full_path}": update has older change_ts ({new_node.change_ts}) than prev version ({old_node.change_ts})')

        if new_node.get_size_bytes() != old_node.get_size_bytes() and new_node.md5 == old_node.md5 and old_node.md5:
            logger.warning(f'File "{new_node.full_path}": update has same MD5 ({new_node.md5}) ' +
                           f'but different size: (old={old_node.get_size_bytes()}, new={new_node.get_size_bytes()})')
    except Exception as e:
        logger.error(f'Error checking update sanity! Old={old_node} New={new_node}: {repr(e)}')
        raise

