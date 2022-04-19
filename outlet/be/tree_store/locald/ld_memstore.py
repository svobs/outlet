import logging
from typing import Optional, Tuple

from be.tree_store.locald.ld_tree import LocalDiskTree
from constants import LOCAL_ROOT_UID, ROOT_PATH
from logging_constants import SUPER_DEBUG_ENABLED, TRACE_ENABLED
from model.node.container_node import RootTypeNode
from model.node.locald_node import LocalDirNode, LocalFileNode, LocalNode
from model.node.node import TNode
from model.node_identifier import LocalNodeIdentifier
from model.uid import UID
from util import file_util
from util.two_level_dict import Md5BeforeUidDict, Sha256BeforeUidDict

logger = logging.getLogger(__name__)


class LocalDiskMemoryStore:
    """
    ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
    CLASS LocalDiskMemoryStore
    ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼
    """
    def __init__(self, backend, device_uid: UID):
        self.backend = backend
        self.device_uid: UID = device_uid
        self.use_md5 = backend.get_config('cache.enable_md5_lookup')
        if self.use_md5:
            self.md5_dict: Optional[Md5BeforeUidDict] = Md5BeforeUidDict()
        else:
            self.md5_dict: Optional[Md5BeforeUidDict] = None

        self.use_sha256 = backend.get_config('cache.enable_sha256_lookup')
        if self.use_sha256:
            self.sha256_dict: Optional[Sha256BeforeUidDict] = Sha256BeforeUidDict()
        else:
            self.sha256_dict: Optional[Sha256BeforeUidDict] = None

        # Each node inserted here will have an entry created for its dir.
        # But we still need a dir tree to look up child dirs:
        self.master_tree = LocalDiskTree(backend)
        root_node = RootTypeNode(node_identifier=LocalNodeIdentifier(full_path=ROOT_PATH, uid=LOCAL_ROOT_UID, device_uid=self.device_uid))
        self.master_tree.add_node(node=root_node, parent=None)

    def remove_single_node(self, node: LocalNode):
        """Removes the given node from all in-memory structs (does nothing if it is not found in some or any of them).
        Will raise an exception if trying to remove a non-empty directory."""
        logger.debug(f'Removing LocalNode from memory cache: {node}')

        cached_node: TNode = self.master_tree.get_node_for_uid(node.uid)
        if cached_node:
            if cached_node.is_dir():
                children = self.master_tree.get_child_list_for_identifier(cached_node.uid)
                if children:
                    # maybe allow deletion of dir with children in the future, but for now be careful
                    raise RuntimeError(f'Cannot remove dir from cache because it has {len(children)} children: {node}')

            count_removed = self.master_tree.remove_node(node.uid)
            assert count_removed <= 1, f'Deleted {count_removed} nodes at {node.node_identifier}'
        else:
            logger.warning(f'Cannot remove node because it has already been removed from cache: {node}')

    def upsert_single_node(self, node: LocalNode, update_only: bool = False) -> Tuple[Optional[LocalNode], bool]:
        """If a node already exists, the new node is merged into it and returned; otherwise the given node is returned.
        Second item in the tuple is True if update contained changes which should be saved to disk; False if otherwise"""

        # FIXME: replace returned Tuple with object which specifies whether UI should be updated + whether DB should be updated
        # FIXME: currently we are sending everything to the FE. We should send to the FE only if changed OR icon updated

        if SUPER_DEBUG_ENABLED:
            logger.debug(f'Upserting to memstore: {node}')

        assert self.master_tree

        if file_util.normalize_path(node.get_single_path()) != node.get_single_path():
            # Sanity check. We can really get messed up if path isn't exactly as expected (e.g. no trailing '/' for dirs!)
            raise RuntimeError(f'File path is not normalized: {node}')

        # Validate UID:
        if not node.uid:
            raise RuntimeError(f'Cannot upsert node to cache because it has no UID: {node}')

        # Update icon (this may be the only thing changed)
        self.backend.cacheman.update_node_icon(node)
        if SUPER_DEBUG_ENABLED:
            logger.debug(f'TNode {node.device_uid}:{node.uid} has icon={node.get_icon().name}, custom_icon={node.get_custom_icon()}')

        cached_node: LocalNode = self.master_tree.get_node_for_uid(node.uid)
        if cached_node:
            if cached_node.is_dir() and not node.is_dir():
                # Not allowed. Need to first delete all descendants via other ops.
                raise RuntimeError(f'Cannot replace a directory with a file: "{node.node_identifier}"')

            if not node.is_live():
                if cached_node.is_live():
                    if cached_node.get_icon() != node.get_icon():
                        cached_node.set_icon(node.get_icon())
                        logger.debug(f'Will not overwrite live node with non-live, but will copy its icon: {node}')
                        return node, False

                    # In the future, let's close this hole with more elegant logic
                    logger.debug(f'Will not replace a live node with non-live; skipping memstore update for {node.node_identifier}')
                    return None, False
                elif not cached_node.is_live():
                    # this shouldn't really happen
                    logger.warning(f'Updating non-live node with another non-live-node: {node.node_identifier}')

            elif node.is_file() and cached_node.is_file():
                # Check for freshly scanned files which are missing signatures. If their other meta checks out, copy from the cache before doing
                # equals comparison
                assert isinstance(node, LocalFileNode) and isinstance(cached_node, LocalFileNode)
                assert node.is_live(), f'Expected to be live: {node}'  # remember, non-live nodes probably don't have create_ts or modify_ts
                if TRACE_ENABLED:
                    logger.debug(f'Before merging: cached_node={cached_node} fresh_node={node}')
                node.copy_signature_if_is_meta_equal(cached_node, self.backend.cacheman.is_seconds_precision_enough)

            if cached_node == node:
                if SUPER_DEBUG_ENABLED:
                    logger.debug(f'TNode being upserted is identical to node already in the cache; skipping memstore update '
                                 f'(CachedNode={cached_node}; NewNode={node}')
                return cached_node, False

            # just update the existing - much easier
            if SUPER_DEBUG_ENABLED:
                logger.debug(f'Merging node (PyID {id(node)}) into cached_node (PyID {id(cached_node)})')

            if cached_node.is_dir() and node.is_dir():
                assert isinstance(cached_node, LocalDirNode), f'Wrong type: {cached_node}'
                if cached_node.all_children_fetched and not node.all_children_fetched:
                    if TRACE_ENABLED:
                        logger.debug(f'Merging into existing node which has all_children_fetched=True; will set new node to True')
                    node.all_children_fetched = True
                elif not cached_node.all_children_fetched and node.all_children_fetched:
                    logger.debug(f'Overwriting node with all_children_fetched=False with one which is True: {node}')

            cached_node.update_from(node)
            node = cached_node

        elif update_only:
            if SUPER_DEBUG_ENABLED:
                logger.debug(f'Skipping update of node {node.node_identifier.guid} because it is not in memstore')
            return node, False
        else:
            # new file or directory insert
            self.master_tree.add_to_tree(node)

        if SUPER_DEBUG_ENABLED:
            logger.debug(f'TNode {node.node_identifier.guid} was upserted into memstore')
        return node, True
