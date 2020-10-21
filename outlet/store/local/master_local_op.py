import logging
from abc import ABC, abstractmethod
from typing import List, Optional

from pydispatch import dispatcher

from constants import SUPER_DEBUG, TREE_TYPE_LOCAL_DISK
from model.node.local_disk_node import LocalNode
from model.node_identifier import LocalFsIdentifier, NodeIdentifier
from store.cache_manager import PersistedCacheInfo
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
    def __init__(self, app, memstore: LocalDiskMemoryStore):
        self.app = app
        self._memstore: LocalDiskMemoryStore = memstore

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

                with LocalDiskDatabase(cache_info.cache_location, self.app) as cache:
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

        physical_cache_list: List[PersistedCacheInfo] = []
        logical_cache_list: List[PersistedCacheInfo] = []

        for subtree in op.get_subtree_list():
            cache_info: Optional[PersistedCacheInfo] = cache_man.find_existing_cache_info_for_subtree(subtree.subtree_root.full_path,
                                                                                                      TREE_TYPE_LOCAL_DISK)
            if not cache_info:
                raise RuntimeError(f'Could not find a cache associated with file path: {subtree.subtree_root.full_path}')

            if not _is_cache_info_in_list(cache_info, physical_cache_list):
                physical_cache_list.append(cache_info)

            logical_cache_list.append(cache_info)

        if len(physical_cache_list) > 3:
            raise RuntimeError(f'Cannot update more than 3 disk caches simultaneously ({len(physical_cache_list)} needed)')

        if len(physical_cache_list) == 1:
            with LocalDiskDatabase(physical_cache_list[0].cache_location, self.app) as cache0:
                phys_cache_list = [cache0]
                log_cache_list = _map_physical_cache_list(phys_cache_list, logical_cache_list, physical_cache_list)
                _update_multiple_cache_files(log_cache_list, op)
                _commit(phys_cache_list)

        elif len(physical_cache_list) == 2:
            with LocalDiskDatabase(physical_cache_list[0].cache_location, self.app) as cache0, \
                    LocalDiskDatabase(physical_cache_list[1].cache_location, self.app) as cache1:
                phys_cache_list = [cache0, cache1]
                log_cache_list = _map_physical_cache_list(phys_cache_list, logical_cache_list, physical_cache_list)
                _update_multiple_cache_files(log_cache_list, op)
                _commit(phys_cache_list)

        elif len(physical_cache_list) == 3:
            with LocalDiskDatabase(physical_cache_list[0].cache_location, self.app) as cache0, \
                    LocalDiskDatabase(physical_cache_list[1].cache_location, self.app) as cache1, \
                    LocalDiskDatabase(physical_cache_list[2].cache_location, self.app) as cache2:
                phys_cache_list = [cache0, cache1, cache2]
                log_cache_list = _map_physical_cache_list(phys_cache_list, logical_cache_list, physical_cache_list)
                _update_multiple_cache_files(log_cache_list, op)
                _commit(phys_cache_list)


def _is_cache_info_in_list(cache_info, cache_list):
    for existing_cache in cache_list:
        if cache_info.cache_location == existing_cache.cache_location:
            return True
    return False


def _map_physical_cache(logical_cache: PersistedCacheInfo, cache_list: List[LocalDiskDatabase], physical_cache_list: List[PersistedCacheInfo]) \
        -> LocalDiskDatabase:
    for physical_index, physical_cache_info in enumerate(physical_cache_list):
        if logical_cache.cache_location == physical_cache_info.cache_location:
            return cache_list[physical_index]
    raise RuntimeError(f'Bad: {logical_cache}')


def _map_physical_cache_list(cache_list: List[LocalDiskDatabase], physical_cache_list: List[PersistedCacheInfo],
                             logical_cache_list: List[PersistedCacheInfo]) -> List[LocalDiskDatabase]:
    mapped_list: List[LocalDiskDatabase] = []
    for logical_cache in logical_cache_list:
        mapped_list.append(_map_physical_cache(logical_cache, cache_list, physical_cache_list))

    return mapped_list


def _commit(cache_list):
    for logical_index, cache in enumerate(cache_list):
        logger.debug(f'Committing cache {logical_index}: "{cache.db_path}"')
        cache.commit()


def _update_multiple_cache_files(logical_cache_list: List[LocalDiskDatabase], op: LocalDiskSubtreeOp):
    for cache, subtree in zip(logical_cache_list, op.get_subtree_list()):
        logger.debug(f'Writing subtree_root="{subtree.subtree_root.full_path}"')
        op.update_disk_cache(cache, subtree)

