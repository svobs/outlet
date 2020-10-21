import logging
from abc import ABC, abstractmethod
from typing import Dict, List, Optional

from pydispatch import dispatcher

from constants import SUPER_DEBUG, TREE_TYPE_LOCAL_DISK
from model.node.local_disk_node import LocalNode
from model.node_identifier import LocalFsIdentifier, NodeIdentifier
from store.cache_manager import PersistedCacheInfo
from store.local.local_disk_store import LocalDiskDiskStore
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


# ABSTRACT CLASS LocalDiskOp
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
class LocalDiskOp(ABC):
    @abstractmethod
    def update_memory_cache(self, data: LocalDiskMemoryStore):
        pass

    @classmethod
    def is_subtree_op(cls) -> bool:
        return False

    @abstractmethod
    def send_signals(self):
        pass


# ABSTRACT CLASS LocalDiskOp
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
class LocalDiskSingleNodeOp(LocalDiskOp, ABC):
    def __init__(self, node: LocalNode):
        assert node, f'No node for operation: {type(self)}'
        self.node: LocalNode = node

    @abstractmethod
    def update_disk_cache(self, cache: LocalDiskDatabase):
        pass


# ABSTRACT CLASS LocalDiskOp
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
class LocalDiskSubtreeOp(LocalDiskOp, ABC):
    @abstractmethod
    def get_subtree_list(self) -> List[LocalSubtree]:
        pass

    @abstractmethod
    def update_disk_cache(self, cache: LocalDiskDatabase, subtree: LocalSubtree):
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

    def update_memory_cache(self, memstore: LocalDiskMemoryStore):
        node, self.was_updated = memstore.upsert_single_node(self.node, self.update_only)
        if node:
            self.node = node
        elif SUPER_DEBUG:
            logger.debug(f'upsert_single_node() returned None for input node: {self.node}')

    def update_disk_cache(self, cache: LocalDiskDatabase):
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

    def update_memory_cache(self, memstore: LocalDiskMemoryStore):
        memstore.remove_single_node(self.node)

    def update_disk_cache(self, cache: LocalDiskDatabase):
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
                 subtree_root: LocalFsIdentifier = None, upsert_node_list: List[LocalNode] = None, remove_node_list: List[LocalNode] = None):
        if subtree_list:
            self.subtree_list = subtree_list
        else:
            self.subtree_list = [LocalSubtree(subtree_root, remove_node_list, upsert_node_list)]

    def get_subtree_list(self) -> List[LocalSubtree]:
        return self.subtree_list

    def update_memory_cache(self, memstore: LocalDiskMemoryStore):
        for subtree in self.subtree_list:
            logger.debug(f'Upserting {len(subtree.upsert_node_list)} and removing {len(subtree.remove_node_list)} nodes at memstore subroot '
                         f'"{subtree.subtree_root.full_path}"')
            # Deletes must occur from bottom up:
            if subtree.remove_node_list:
                for node in reversed(subtree.remove_node_list):
                    memstore.remove_single_node(node)
            if subtree.upsert_node_list:
                for node_index, node in enumerate(subtree.upsert_node_list):
                    master_node, was_updated = memstore.upsert_single_node(node)
                    if master_node:
                        subtree.upsert_node_list[node_index] = master_node

    def update_disk_cache(self, cache: LocalDiskDatabase, subtree: LocalSubtree):
        if subtree.remove_node_list:
            cache.delete_files_and_dirs(subtree.remove_node_list, commit=False)
        if subtree.upsert_node_list:
            cache.upsert_files_and_dirs(subtree.upsert_node_list, commit=False)

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
    def __init__(self, subtree_root: LocalFsIdentifier, node_list: List[LocalNode]):
        super().__init__(subtree_root=subtree_root, upsert_node_list=[], remove_node_list=node_list)


# CLASS LocalDiskOpExecutor
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
class LocalDiskOpExecutor:
    def __init__(self, app, memstore: LocalDiskMemoryStore, disk_store: LocalDiskDiskStore):
        self.app = app
        self._memstore: LocalDiskMemoryStore = memstore
        self._disk_store: LocalDiskDiskStore = disk_store

    def execute(self, operation: LocalDiskOp):
        if SUPER_DEBUG:
            logger.debug(f'Executing operation: {operation}')
        operation.update_memory_cache(self._memstore)

        cacheman = self.app.cacheman
        if cacheman.enable_save_to_disk:
            if operation.is_subtree_op():
                assert isinstance(operation, LocalDiskSubtreeOp)
                self._update_disk_cache_for_subtree(operation)
            else:
                assert isinstance(operation, LocalDiskSingleNodeOp)
                assert operation.node, f'No node for operation: {type(operation)}'
                cache_info: Optional[PersistedCacheInfo] = cacheman.find_existing_cache_info_for_subtree(operation.node.full_path,
                                                                                                         operation.node.get_tree_type())
                if not cache_info:
                    raise RuntimeError(f'Could not find a cache associated with file path: {operation.node.full_path}')

                cache = self._disk_store.get_or_open_db(cache_info)
                operation.update_disk_cache(cache)
                cache.commit()

        else:
            logger.debug(f'Save to disk is disabled: skipping save to disk for operation')

        operation.send_signals()

    def _update_disk_cache_for_subtree(self, op: LocalDiskSubtreeOp):
        """Attempt to come close to a transactional behavior by writing to all caches at once, and then committing all at the end"""
        cache_man = self.app.cacheman
        if not cache_man.enable_save_to_disk:
            logger.debug(f'Save to disk is disabled: skipping save of {len(op.get_subtree_list())} subtrees')
            return

        cache_dict: Dict[str, LocalDiskDatabase] = {}

        for subtree in op.get_subtree_list():
            cache_info: Optional[PersistedCacheInfo] = cache_man.find_existing_cache_info_for_subtree(subtree.subtree_root.full_path,
                                                                                                      TREE_TYPE_LOCAL_DISK)
            if not cache_info:
                raise RuntimeError(f'Could not find a cache associated with file path: {subtree.subtree_root.full_path}')

            cache = self._disk_store.get_or_open_db(cache_info)
            cache_dict[cache_info.cache_location] = cache
            op.update_disk_cache(cache, subtree)

        for cache in cache_dict.values():
            cache.commit()
