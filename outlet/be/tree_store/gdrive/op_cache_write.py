import logging
from abc import ABC, abstractmethod
from collections import defaultdict
from typing import DefaultDict, List, Optional, Tuple

from pydispatch import dispatcher

from be.sqlite.gdrive_db import GDriveDatabase
from be.tree_store.cache_write_op import CacheWriteOp, NodeUpdateInfo
from be.tree_store.gdrive.client.change_observer import GDriveChange, GDriveNodeChange
from be.tree_store.gdrive.gd_memstore import GDriveMemoryStore
from constants import GDRIVE_ROOT_UID, TreeType
from logging_constants import SUPER_DEBUG_ENABLED, TRACE_ENABLED
from model.gdrive_meta import GDriveUser, MimeType
from model.node.gdrive_node import GDriveFile, GDriveFolder, GDriveNode
from model.uid import UID
from signal_constants import ID_GLOBAL_CACHE, Signal

logger = logging.getLogger(__name__)


# ABSTRACT CLASS GDCacheWriteOp
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
class GDCacheWriteOp(CacheWriteOp):
    @abstractmethod
    def update_memstore(self, memstore: GDriveMemoryStore):
        pass

    @abstractmethod
    def update_diskstore(self, cache: GDriveDatabase):
        pass

    @abstractmethod
    def send_signals(self):
        pass


class GDUpsertSingleNodeOp(GDCacheWriteOp):
    """
    ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
    CLASS GDUpsertSingleNodeOp
    ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼
    """
    def __init__(self, node: GDriveNode, update_only: bool = False):
        super().__init__()
        self.node: GDriveNode = node
        self.update_info: Optional[NodeUpdateInfo] = None
        self.parent_goog_ids = []
        self.update_only: bool = update_only

        # try to prevent cache corruption by doing some sanity checks
        if not node:
            raise RuntimeError(f'No node supplied!')
        if not node.uid:
            raise RuntimeError(f'TNode is missing UID: {node}')
        if node.node_identifier.tree_type != TreeType.GDRIVE:
            raise RuntimeError(f'Unrecognized tree type: {node.node_identifier.tree_type}')
        if not isinstance(node, GDriveNode):
            raise RuntimeError(f'Unrecognized node type: {node}')

    def update_memstore(self, memstore: GDriveMemoryStore):
        self.update_info = memstore.upsert_single_node(self.node)
        if self.update_info.node and self.update_info.needs_disk_update:
            parent_uids = self.node.get_parent_uids()
            if parent_uids:
                if len(parent_uids) == 1 and parent_uids[0] == GDRIVE_ROOT_UID:
                    logger.debug(f'Parent is GDrive root')
                    self.parent_goog_ids = [None]
                try:
                    self.parent_goog_ids = memstore.master_tree.resolve_uids_to_goog_ids(parent_uids, fail_if_missing=True)
                except RuntimeError:
                    logger.debug(f'Could not resolve goog_ids for parent UIDs ({parent_uids}); assuming parents do not exist')
            else:
                logger.debug(f'TNode has no parents; assuming it is a root node: {self.node}')

    def update_diskstore(self, cache: GDriveDatabase):
        if not self.update_info.needs_disk_update:
            if SUPER_DEBUG_ENABLED:
                logger.debug(f'TNode does not need disk update; skipping save to disk: {self.node}')
            return

        node = self.update_info.node
        if not node.is_live():
            if SUPER_DEBUG_ENABLED:
                logger.debug(f'TNode is not live; skipping save to disk: {node}')
            return

        if SUPER_DEBUG_ENABLED:
            logger.debug(f'GDUpsertSingleNodeOp: upserting GDriveNode to disk cache: {node}')

        parent_mappings = []
        parent_uids = node.get_parent_uids()
        if len(parent_uids) != len(self.parent_goog_ids):
            raise RuntimeError(f'Internal error: could not map all parent goog_ids ({len(self.parent_goog_ids)}) to parent UIDs '
                               f'({len(parent_uids)}) for node: {node}')
        for parent_uid, parent_goog_id in zip(parent_uids, self.parent_goog_ids):
            parent_mappings.append((node.uid, parent_uid, parent_goog_id, node.sync_ts))

        # Write new values:
        if parent_mappings:
            logger.debug(f'Writing {len(parent_mappings)} id-parent mappings to the GDrive master cache: {parent_mappings}')
            cache.upsert_parent_mappings_for_id(parent_mappings, node.uid, commit=False)

        if node.is_dir():
            logger.debug(f'Writing folder node to the GDrive master cache: {node}')
            assert isinstance(node, GDriveFolder)
            cache.upsert_gdrive_folder_list([node], commit=False)
        else:
            logger.debug(f'Writing file node to the GDrive master cache: {node}')
            assert isinstance(node, GDriveFile)
            cache.upsert_gdrive_file_list([node], commit=False)

        cache.commit()

    def send_signals(self):
        if self.update_info.needs_disk_update or self.update_info.has_icon_update:
            if SUPER_DEBUG_ENABLED:
                logger.debug(f'Sending signal {Signal.NODE_UPSERTED_IN_CACHE.name} with node: {self.node}')
            dispatcher.send(signal=Signal.NODE_UPSERTED_IN_CACHE, sender=ID_GLOBAL_CACHE, node=self.node)


class GDRemoveSingleNodeOp(GDCacheWriteOp):
    """
    ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
    CLASS GDRemoveSingleNodeOp
    ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼
    """
    def __init__(self, node: GDriveNode, to_trash: bool = False):
        assert isinstance(node, GDriveNode), f'For node: {node}'
        super().__init__()
        self.node: GDriveNode = node
        self.to_trash: bool = to_trash

    def update_memstore(self, memstore: GDriveMemoryStore):
        memstore.remove_single_node(self.node, self.to_trash)

    def update_diskstore(self, cache: GDriveDatabase):
        if SUPER_DEBUG_ENABLED:
            logger.debug(f'Removing GDriveNode from disk cache: {self.node}')

        cache.delete_single_node(self.node, commit=False)

    def send_signals(self):
        dispatcher.send(signal=Signal.NODE_REMOVED_IN_CACHE, sender=ID_GLOBAL_CACHE, node=self.node)


class GDRemoveSubtreeOp(GDCacheWriteOp):
    """
    ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
    CLASS GDRemoveSubtreeOp
    ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼
    """
    def __init__(self, subtree_root_node: GDriveNode, node_list: List[GDriveNode], to_trash: bool = False):
        super().__init__()
        self.subtree_root_node: GDriveNode = subtree_root_node
        self.node_list: List[GDriveNode] = node_list
        self.to_trash: bool = to_trash

    def update_memstore(self, memstore: GDriveMemoryStore):
        logger.debug(f'GDRemoveSubtreeOp: removing {len(self.node_list)} nodes from memory cache')
        for node in reversed(self.node_list):
            memstore.remove_single_node(node, self.to_trash)
        logger.debug(f'GDRemoveSubtreeOp: done removing nodes from memory cache')

    def update_diskstore(self, cache: GDriveDatabase):
        # TODO: bulk remove
        logger.debug(f'GDRemoveSubtreeOp: removing {len(self.node_list)} nodes from disk cache')
        for node in self.node_list:
            cache.delete_single_node(node, commit=False)
        logger.debug(f'GDRemoveSubtreeOp: done removing nodes from disk cache')

    def send_signals(self):
        logger.debug(f'GDRemoveSubtreeOp: sending "{Signal.NODE_REMOVED_IN_CACHE}" signal for {len(self.node_list)} nodes')
        for node in self.node_list:
            dispatcher.send(signal=Signal.NODE_REMOVED_IN_CACHE, sender=ID_GLOBAL_CACHE, node=node)


class BatchChangesOp(GDCacheWriteOp):
    """
    ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
    CLASS BatchChangesOp
    ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼
    """
    def __init__(self, backend, change_list: List[GDriveChange]):
        super().__init__()
        self.backend = backend
        self.change_list = BatchChangesOp._reduce_changes(change_list)

    @staticmethod
    def _reduce_changes(change_list: List[GDriveChange]) -> List[GDriveChange]:
        change_list_by_goog_id: DefaultDict[str, List[GDriveChange]] = defaultdict(lambda: list())
        for change in change_list:
            assert change.goog_id, f'No goog_id for change: {change}'
            change_list_by_goog_id[change.goog_id].append(change)

        reduced_changes: List[GDriveChange] = []
        for single_goog_id_change_list in change_list_by_goog_id.values():
            last_change = single_goog_id_change_list[-1]
            if last_change.node:
                reduced_changes.append(last_change)
            else:
                # skip this node
                logger.debug(f'No node found in cache for removed goog_id: "{last_change.goog_id}"; skipping')

        logger.debug(f'Reduced {len(change_list)} changes into {len(reduced_changes)} changes')
        return reduced_changes

    def update_memstore(self, memstore: GDriveMemoryStore):
        for change in self.change_list:
            if change.is_removed():
                # Some GDrive deletes (such as a hard delete of a folder) will cause a parent to be deleted before its descendants.
                removed_node = memstore.master_tree.remove_node(change.node, fail_if_children_present=False)
                if removed_node:
                    change.node = removed_node
                else:
                    # ensure full_path is populated:
                    memstore.master_tree.rebuild_path_list_for_uid(change.node.uid)
            else:
                assert isinstance(change, GDriveNodeChange)
                # need to use existing object if available to fulfill our contract (node will be sent via signals below)
                update_info = memstore.upsert_single_node(change.node)
                change.node = update_info.node

    def update_diskstore(self, cache: GDriveDatabase):
        mappings_list_list: List[List[Tuple]] = []
        file_uid_to_delete_list: List[UID] = []
        folder_uid_to_delete_list: List[UID] = []
        files_to_upsert: List[GDriveFile] = []
        folders_to_upsert: List[GDriveFolder] = []

        for change in self.change_list:
            if change.is_removed():
                if change.node.is_dir():
                    folder_uid_to_delete_list.append(change.node.uid)
                else:
                    file_uid_to_delete_list.append(change.node.uid)
            else:
                parent_mapping_list = []
                parent_uids = change.node.get_parent_uids()
                if parent_uids:
                    parent_goog_ids = self.backend.cacheman.get_goog_id_list_for_uid_list(change.node.device_uid, parent_uids)
                    if len(parent_uids) != len(parent_goog_ids):
                        raise RuntimeError(f'Internal error: could not map all parent goog_ids ({len(parent_goog_ids)}) to parent UIDs '
                                           f'({len(parent_uids)}) for node: {change.node}')
                    for parent_uid, parent_goog_id in zip(change.node.get_parent_uids(), parent_goog_ids):
                        parent_mapping_list.append((change.node.uid, parent_uid, parent_goog_id, change.node.sync_ts))
                    mappings_list_list.append(parent_mapping_list)

                if change.node.is_dir():
                    assert isinstance(change.node, GDriveFolder) and change.node.is_live(), f'Bad: {change.node}'
                    folders_to_upsert.append(change.node)
                else:
                    assert isinstance(change.node, GDriveFile) and change.node.is_live(), f'Bad: {change.node}'
                    files_to_upsert.append(change.node)

        if mappings_list_list:
            logger.debug(f'Upserting id-parent mappings for {len(mappings_list_list)} nodes to the GDrive master cache')
            cache.upsert_parent_mappings(mappings_list_list, commit=False)

        if len(file_uid_to_delete_list) + len(folder_uid_to_delete_list) > 0:
            logger.debug(f'Removing {len(file_uid_to_delete_list)} files and {len(folder_uid_to_delete_list)} folders from the GDrive master cache')
            cache.delete_nodes(file_uid_to_delete_list, folder_uid_to_delete_list, commit=False)

        if len(folders_to_upsert) > 0:
            logger.debug(f'Upserting {len(folders_to_upsert)} folders to the GDrive master cache')
            cache.upsert_gdrive_folder_list(folders_to_upsert, commit=False)

        if len(files_to_upsert) > 0:
            logger.debug(f'Upserting {len(files_to_upsert)} files to the GDrive master cache')
            cache.upsert_gdrive_file_list(files_to_upsert, commit=False)

    def send_signals(self):
        # TODO: consider optimizing by using SUBTREE_NODES_CHANGED_IN_CACHE (which requires sorting nodes into subtrees...)
        for change in self.change_list:
            assert change.node.get_path_list(), f'TNode is missing path list: {change.node}'
            if change.is_removed():
                dispatcher.send(signal=Signal.NODE_REMOVED_IN_CACHE, sender=ID_GLOBAL_CACHE, node=change.node)
            else:
                dispatcher.send(signal=Signal.NODE_UPSERTED_IN_CACHE, sender=ID_GLOBAL_CACHE, node=change.node)


class RefreshFolderOp(GDCacheWriteOp):
    """
    ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
    CLASS RefreshFolderOp

    Upserts a given folder and its immediate children to each cache.
    Any previous children which are not referenced in the given child_list will continue to exist but will be unlinked from
    the given parent folder.
    ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼
    """
    def __init__(self, backend, parent_folder: GDriveFolder, child_list: List[GDriveNode]):
        super().__init__()
        self.backend = backend
        assert parent_folder.all_children_fetched, f'Expected all_children_fetched==True for node: {parent_folder}'
        self.parent_folder: GDriveFolder = parent_folder
        self.child_list: List[GDriveNode] = child_list
        self._upserted_node_list: List[GDriveNode] = []

    def update_memstore(self, memstore: GDriveMemoryStore):
        logger.debug(f'RefreshFolderOp: upserting into memory cache: parent folder ({self.parent_folder}) and children: {self.child_list} '
                     f'children in memory cache')
        # FIXME: determine if nodes were removed from parents. If so, send notifications to ATM
        # TODO: only update what we was changed
        self._upserted_node_list = memstore.master_tree.upsert_folder_and_children(self.parent_folder, self.child_list)
        logger.debug(f'RefreshFolderOp: done upserting nodes to memory cache')

    def update_diskstore(self, cache: GDriveDatabase):
        logger.debug(f'RefreshFolderOp: upserting {len(self._upserted_node_list)} nodes in disk cache')

        mappings_list_list: List[List[Tuple]] = []
        files_to_upsert: List[GDriveFile] = []
        folders_to_upsert: List[GDriveFolder] = []

        for node in self._upserted_node_list:
            if not node.is_live():
                if SUPER_DEBUG_ENABLED:
                    logger.debug(f'Skipping save to disk for node because it is not live: {node.node_identifier}')
                continue

            parent_mapping_list = []
            parent_uids = node.get_parent_uids()
            if parent_uids:
                parent_goog_ids = self.backend.cacheman.get_goog_id_list_for_uid_list(node.device_uid, parent_uids)
                if len(parent_uids) != len(parent_goog_ids):
                    raise RuntimeError(f'Internal error: could not map all parent goog_ids ({len(parent_goog_ids)}) to parent UIDs '
                                       f'({len(parent_uids)}) for node: {node}')
                for parent_uid, parent_goog_id in zip(node.get_parent_uids(), parent_goog_ids):
                    parent_mapping_list.append((node.uid, parent_uid, parent_goog_id, node.sync_ts))
                mappings_list_list.append(parent_mapping_list)

            if node.is_dir():
                assert isinstance(node, GDriveFolder)
                folders_to_upsert.append(node)
            else:
                assert isinstance(node, GDriveFile)
                files_to_upsert.append(node)

        if mappings_list_list:
            logger.debug(f'Upserting id-parent mappings for {len(mappings_list_list)} nodes to the GDrive master cache')
            cache.upsert_parent_mappings(mappings_list_list, commit=False)

        if len(folders_to_upsert) > 0:
            logger.debug(f'Upserting {len(folders_to_upsert)} folders to the GDrive master cache')
            cache.upsert_gdrive_folder_list(folders_to_upsert, commit=False)

        if len(files_to_upsert) > 0:
            logger.debug(f'Upserting {len(files_to_upsert)} files to the GDrive master cache')
            cache.upsert_gdrive_file_list(files_to_upsert, commit=False)

        logger.debug(f'RefreshFolderOp: done with disk cache')

    def send_signals(self):
        if self._upserted_node_list:
            logger.debug(f'RefreshFolderOp: sending "{Signal.SUBTREE_NODES_CHANGED_IN_CACHE.name}" signal for {len(self._upserted_node_list)} nodes')
            # no need for removed_node list with GDrive
            dispatcher.send(signal=Signal.SUBTREE_NODES_CHANGED_IN_CACHE, sender=ID_GLOBAL_CACHE, subtree_root=self.parent_folder.node_identifier,
                            upserted_node_list=self._upserted_node_list, removed_node_list=[])
        elif SUPER_DEBUG_ENABLED:
            logger.debug(f'RefreshFolderOp: no need to send signal: no upserted nodes')


class CreateUserOp(GDCacheWriteOp):
    """
    ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
    CLASS CreateUserOp
    ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼
    """
    def __init__(self, user: GDriveUser):
        super().__init__()
        self.user: GDriveUser = user

    def update_memstore(self, memstore: GDriveMemoryStore):
        memstore.create_user(self.user)

    def update_diskstore(self, cache: GDriveDatabase):
        cache.upsert_user(self.user)

    def send_signals(self):
        pass


class UpsertMimeTypeOp(GDCacheWriteOp):
    """
    ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
    CLASS UpsertMimeTypeOp
    ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼
    """
    def __init__(self, mime_type_string: str):
        super().__init__()
        self._mime_type_string: str = mime_type_string
        self.mime_type: Optional[MimeType] = None
        """Note: this is accessed as the return value. Would be good to find a way to remove this dependency"""
        self._needs_insert: bool = True

    def update_memstore(self, memstore: GDriveMemoryStore):
        self.mime_type, self._needs_insert = memstore.get_or_create_mime_type(self._mime_type_string)

    def update_diskstore(self, cache: GDriveDatabase):
        if self._needs_insert:
            cache.upsert_mime_type(self.mime_type)

    def send_signals(self):
        pass


class DeleteAllDataOp(GDCacheWriteOp):
    """
    ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
    CLASS DeleteAllDataOp
    ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼
    """

    def update_memstore(self, memstore: GDriveMemoryStore):
        memstore.delete_all_gdrive_data()

    def update_diskstore(self, cache: GDriveDatabase):
        cache.delete_all_gdrive_data()

    def send_signals(self):
        pass