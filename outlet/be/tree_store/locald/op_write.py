import logging
from abc import ABC, abstractmethod
from typing import List

from pydispatch import dispatcher

from logging_constants import SUPER_DEBUG_ENABLED, TRACE_ENABLED
from model.node.locald_node import LocalNode
from model.node_identifier import LocalNodeIdentifier, NodeIdentifier
from be.tree_store.locald.ld_memstore import LocalDiskMemoryStore
from be.sqlite.local_db import LocalDiskDatabase
from signal_constants import Signal
from signal_constants import ID_GLOBAL_CACHE

logger = logging.getLogger(__name__)


class LocalSubtree(ABC):
    """
    ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
    CLASS LocalSubtree

    Just a collection of nodes to be upserted and removed, all descendants of the same subtree
    ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼
    """

    def __init__(self, subtree_root: NodeIdentifier, remove_node_list: List[LocalNode], upsert_node_list: List[LocalNode]):
        self.subtree_root: NodeIdentifier = subtree_root
        self.remove_node_list: List[LocalNode] = [] if remove_node_list is None else remove_node_list
        self.upsert_node_list: List[LocalNode] = [] if upsert_node_list is None else upsert_node_list

    def __repr__(self):
        return f'LocalSubtree({self.subtree_root} remove_nodes={len(self.remove_node_list)} upsert_nodes={len(self.upsert_node_list)}'


class LocalWriteThroughOp(ABC):
    """
    ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
    ABSTRACT CLASS LocalWriteThroughOp
    ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼
    """

    @abstractmethod
    def update_memstore(self, data: LocalDiskMemoryStore):
        pass

    @classmethod
    def is_single_node_op(cls) -> bool:
        return False

    @abstractmethod
    def send_signals(self):
        pass

    # Single-node
    # ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼


class LocalDiskSingleNodeOp(LocalWriteThroughOp, ABC):
    """
    ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
    ABSTRACT CLASS LocalDiskSingleNodeOp
    ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼
    """

    def __init__(self, node: LocalNode):
        assert node, f'No node for operation: {type(self)}'
        self.node: LocalNode = node

    @abstractmethod
    def update_diskstore(self, cache: LocalDiskDatabase):
        pass

    @classmethod
    def is_single_node_op(cls) -> bool:
        return True


class UpsertSingleNodeOp(LocalDiskSingleNodeOp):
    """
    ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
    CLASS UpsertSingleNodeOp
    ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼
    """

    def __init__(self, node: LocalNode, update_only: bool = False):
        super().__init__(node)
        self.was_updated: bool = True
        self.update_only: bool = update_only

    def update_memstore(self, memstore: LocalDiskMemoryStore):
        node, self.was_updated = memstore.upsert_single_node(self.node, self.update_only)
        if node:
            self.node = node
        elif SUPER_DEBUG_ENABLED:
            logger.debug(f'UpsertSingleNodeOp: upsert_single_node() returned None for input node: {self.node}')

    def update_diskstore(self, cache: LocalDiskDatabase):
        if not self.node.is_live():
            if SUPER_DEBUG_ENABLED:
                logger.debug(f'UpsertSingleNodeOp: node is not live; skipping save to disk: {self.node}')
            return

        if not self.was_updated:
            if SUPER_DEBUG_ENABLED:
                logger.debug(f'UpsertSingleNodeOp: node was not updated in memstore; skipping save to disk: {self.node}')
            return

        logger.debug(f'UpsertSingleNodeOp: upserting LocalNode to diskstore: {self.node}')
        cache.upsert_single_node(self.node, commit=False)

    def send_signals(self):
        if SUPER_DEBUG_ENABLED:
            logger.debug(f'UpsertSingleNodeOp: sending upsert signal for {self.node.node_identifier}')
        dispatcher.send(signal=Signal.NODE_UPSERTED_IN_CACHE, sender=ID_GLOBAL_CACHE, node=self.node)

    def __repr__(self):
        return f'UpsertSingleNodeOp({self.node.node_identifier}, update_only={self.update_only})'


class DeleteSingleNodeOp(UpsertSingleNodeOp):
    """
    ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
    CLASS DeleteSingleNodeOp
    ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼
    """

    def __init__(self, node: LocalNode, to_trash: bool = False):
        super().__init__(node)
        self.to_trash: bool = to_trash

        # 1. Validate
        if not node.uid:
            raise RuntimeError(f'Cannot remove node from cache because it has no UID: {node}')

        if to_trash:
            # TODO
            raise RuntimeError(f'Not supported: to_trash=true!')

    def update_memstore(self, memstore: LocalDiskMemoryStore):
        memstore.remove_single_node(self.node)

    def update_diskstore(self, cache: LocalDiskDatabase):
        cache.delete_single_node(self.node, commit=False)

    def send_signals(self):
        dispatcher.send(signal=Signal.NODE_REMOVED_IN_CACHE, sender=ID_GLOBAL_CACHE, node=self.node)

    def __repr__(self):
        return f'DeleteSingleNodeOp({self.node.node_identifier} to_trash={self.to_trash})'

    # Multi-node
    # ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼


class LocalDiskMultiNodeOp(LocalWriteThroughOp, ABC):
    """
    ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
    ABSTRACT CLASS LocalDiskMultiNodeOp
    ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼
    """

    @abstractmethod
    def get_subtree_list(self) -> List[LocalSubtree]:
        pass

    @abstractmethod
    def update_diskstore(self, cache: LocalDiskDatabase, subtree: LocalSubtree):
        """
            Only nodes which were updated in the memstore and for which is_live()==true should be written to disk.
            However, any nodes which were updated (whether live or not) should have signals sent for them.
        """
        pass


class BatchChangesOp(LocalDiskMultiNodeOp):
    """
    ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
    CLASS BatchChangesOp

    REMEMBER: ALWAYS REMOVE BEFORE ADDING!
    ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼
    """

    def __init__(self, subtree_list: List[LocalSubtree] = None,
                 subtree_root: LocalNodeIdentifier = None, upsert_node_list: List[LocalNode] = None, remove_node_list: List[LocalNode] = None):
        if subtree_list:
            self.subtree_list = subtree_list
        else:
            self.subtree_list = [LocalSubtree(subtree_root, remove_node_list, upsert_node_list)]

    def get_subtree_list(self) -> List[LocalSubtree]:
        return self.subtree_list

    def update_memstore(self, memstore: LocalDiskMemoryStore):
        for subtree in self.subtree_list:
            if SUPER_DEBUG_ENABLED:
                logger.debug(f'Upserting {len(subtree.upsert_node_list)} & removing {len(subtree.remove_node_list)} nodes at memstore subroot '
                             f'"{subtree.subtree_root}"')
            # Deletes must occur from bottom up:
            if subtree.remove_node_list:
                for node in reversed(subtree.remove_node_list):
                    memstore.remove_single_node(node)

            if subtree.upsert_node_list:
                new_upsert_node_list = []

                # Only nodes which were updated in the memstore and for which is_live()==true should be written to disk.
                # But in this case, I am too lazy to rework this API to make it work; should be fine to do some extra writes
                for node in subtree.upsert_node_list:
                    master_node, was_updated = memstore.upsert_single_node(node)
                    if TRACE_ENABLED:
                        logger.debug(f'TNode {node.uid} was updated: {was_updated}')
                    if master_node:
                        node = master_node
                    new_upsert_node_list.append(node)

                subtree.upsert_node_list = new_upsert_node_list

    def update_diskstore(self, cache: LocalDiskDatabase, subtree: LocalSubtree):
        if SUPER_DEBUG_ENABLED:
            logger.debug(f'Removing {len(subtree.remove_node_list)} & upserting {len(subtree.upsert_node_list)} nodes in diskstore')

        if subtree.remove_node_list:
            cache.delete_files_and_dirs(subtree.remove_node_list, commit=False)
        else:
            if SUPER_DEBUG_ENABLED:
                logger.debug(f'No nodes to remove from diskstore')

        if subtree.upsert_node_list:
            # do not write non-live nodes to disk
            live_node_list = list(filter(lambda n: n.is_live(), subtree.upsert_node_list))
            if live_node_list:
                cache.upsert_files_and_dirs(live_node_list, commit=False)
        else:
            if SUPER_DEBUG_ENABLED:
                logger.debug(f'No nodes to upsert to diskstore')

    def send_signals(self):
        # All nodes in our lists (whether live or not) should have signals sent for them.
        for subtree in self.subtree_list:
            dispatcher.send(signal=Signal.SUBTREE_NODES_CHANGED_IN_CACHE, sender=ID_GLOBAL_CACHE, subtree_root=subtree.subtree_root,
                            upserted_node_list=subtree.upsert_node_list, removed_node_list=list(reversed(subtree.remove_node_list)))

    def __repr__(self):
        return f'BatchChangesOp({self.subtree_list})'


class DeleteSubtreeOp(BatchChangesOp):
    """
    ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
    CLASS DeleteSubtreeOp
    ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼
    """

    def __init__(self, subtree_root: LocalNodeIdentifier, node_list: List[LocalNode]):
        super().__init__(subtree_root=subtree_root, upsert_node_list=[], remove_node_list=node_list)


class RefreshDirEntriesOp(BatchChangesOp):
    """
    ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
    CLASS RefreshDirEntriesOp
    ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼
    """

    def __init__(self, subtree_root: LocalNodeIdentifier, upsert_node_list: List[LocalNode], remove_node_list: List[LocalNode]):
        super().__init__(subtree_root=subtree_root, upsert_node_list=upsert_node_list, remove_node_list=remove_node_list)