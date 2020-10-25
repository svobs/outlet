import logging
from abc import ABC, abstractmethod
from typing import List

from pydispatch import dispatcher

from constants import SUPER_DEBUG
from model.node.local_disk_node import LocalNode
from model.node_identifier import LocalNodeIdentifier, NodeIdentifier
from store.local.master_local_memory import LocalDiskMemoryStore
from store.sqlite.local_db import LocalDiskDatabase
from ui import actions
from ui.actions import ID_GLOBAL_CACHE

logger = logging.getLogger(__name__)


# CLASS LocalSubtree
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
class LocalSubtree(ABC):
    """Just a collection of nodes to be upserted and removed, all descendants of the same subtree"""
    def __init__(self, subtree_root: NodeIdentifier, remove_node_list: List[LocalNode], upsert_node_list: List[LocalNode]):
        self.subtree_root: NodeIdentifier = subtree_root
        self.remove_node_list: List[LocalNode] = remove_node_list
        self.upsert_node_list: List[LocalNode] = upsert_node_list

    def __repr__(self):
        return f'LocalSubtree({self.subtree_root} remove_nodes={len(self.remove_node_list)} upsert_nodes={len(self.upsert_node_list)}'


# ABSTRACT CLASS LocalWriteThroughOp
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
class LocalWriteThroughOp(ABC):
    @abstractmethod
    def update_memstore(self, data: LocalDiskMemoryStore):
        pass

    @classmethod
    def is_subtree_op(cls) -> bool:
        return False

    @abstractmethod
    def send_signals(self):
        pass


# ABSTRACT CLASS LocalWriteThroughOp
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
class LocalDiskSingleNodeOp(LocalWriteThroughOp, ABC):
    def __init__(self, node: LocalNode):
        assert node, f'No node for operation: {type(self)}'
        self.node: LocalNode = node

    @abstractmethod
    def update_diskstore(self, cache: LocalDiskDatabase):
        pass


# ABSTRACT CLASS LocalWriteThroughOp
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
class LocalDiskSubtreeOp(LocalWriteThroughOp, ABC):
    @abstractmethod
    def get_subtree_list(self) -> List[LocalSubtree]:
        pass

    @abstractmethod
    def update_diskstore(self, cache: LocalDiskDatabase, subtree: LocalSubtree):
        pass

    @classmethod
    def is_subtree_op(cls) -> bool:
        return True


# CLASS UpsertSingleNodeOp
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
class UpsertSingleNodeOp(LocalDiskSingleNodeOp):
    def __init__(self, node: LocalNode, update_only: bool = False):
        super().__init__(node)
        self.was_updated: bool = True
        self.update_only: bool = update_only

    def update_memstore(self, memstore: LocalDiskMemoryStore):
        node, self.was_updated = memstore.upsert_single_node(self.node, self.update_only)
        if node:
            self.node = node
        elif SUPER_DEBUG:
            logger.debug(f'upsert_single_node() returned None for input node: {self.node}')

    def update_diskstore(self, cache: LocalDiskDatabase):
        if self.was_updated:
            if SUPER_DEBUG:
                logger.debug(f'Upserting LocalNode to disk cache: {self.node}')
            cache.upsert_single_node(self.node, commit=False)

    def send_signals(self):
        if self.was_updated:
            dispatcher.send(signal=actions.NODE_UPSERTED, sender=ID_GLOBAL_CACHE, node=self.node)

    def __repr__(self):
        return f'UpsertSingleNodeOp({self.node.node_identifier}, update_only={self.update_only})'


# CLASS DeleteSingleNodeOp
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
class DeleteSingleNodeOp(UpsertSingleNodeOp):
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
        dispatcher.send(signal=actions.NODE_REMOVED, sender=ID_GLOBAL_CACHE, node=self.node)

    def __repr__(self):
        return f'DeleteSingleNodeOp({self.node.node_identifier} to_trash={self.to_trash})'


# CLASS BatchChangesOp
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
class BatchChangesOp(LocalDiskSubtreeOp):
    """ALWAYS REMOVE BEFORE ADDING!"""

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
            logger.debug(f'Upserting {len(subtree.upsert_node_list)} and removing {len(subtree.remove_node_list)} nodes at memstore subroot '
                         f'"{subtree.subtree_root.get_path_list()}"')
            # Deletes must occur from bottom up:
            if subtree.remove_node_list:
                for node in reversed(subtree.remove_node_list):
                    memstore.remove_single_node(node)
            if subtree.upsert_node_list:
                for node_index, node in enumerate(subtree.upsert_node_list):
                    master_node, was_updated = memstore.upsert_single_node(node)
                    if master_node:
                        subtree.upsert_node_list[node_index] = master_node

    def update_diskstore(self, cache: LocalDiskDatabase, subtree: LocalSubtree):
        if subtree.remove_node_list:
            cache.delete_files_and_dirs(subtree.remove_node_list, commit=False)
        else:
            logger.debug(f'No nodes to remove from diskstore')
        if subtree.upsert_node_list:
            cache.upsert_files_and_dirs(subtree.upsert_node_list, commit=False)
        else:
            logger.debug(f'No nodes to upsert to diskstore')

    def send_signals(self):
        for subtree in self.subtree_list:
            for node in reversed(subtree.remove_node_list):
                dispatcher.send(signal=actions.NODE_REMOVED, sender=ID_GLOBAL_CACHE, node=node)
            for node in subtree.upsert_node_list:
                dispatcher.send(signal=actions.NODE_UPSERTED, sender=ID_GLOBAL_CACHE, node=node)

    def __repr__(self):
        return f'BatchChangesOp({self.subtree_list})'


# CLASS DeleteSubtreeOp
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
class DeleteSubtreeOp(BatchChangesOp):
    def __init__(self, subtree_root: LocalNodeIdentifier, node_list: List[LocalNode]):
        super().__init__(subtree_root=subtree_root, upsert_node_list=[], remove_node_list=node_list)
