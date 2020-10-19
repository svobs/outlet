import logging
from abc import ABC, abstractmethod
from collections import defaultdict
from typing import DefaultDict, List, Tuple

from pydispatch import dispatcher

from constants import SUPER_DEBUG, TREE_TYPE_GDRIVE
from model.gdrive_whole_tree import GDriveWholeTree
from model.node.gdrive_node import GDriveFile, GDriveFolder, GDriveNode
from model.uid import UID
from store.gdrive.change_observer import GDriveChange, GDriveNodeChange
from store.sqlite.gdrive_db import GDriveDatabase
from ui import actions
from ui.actions import ID_GLOBAL_CACHE

logger = logging.getLogger(__name__)


# ABSTRACT CLASS GDriveCacheOp
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
class GDriveCacheOp(ABC):
    @abstractmethod
    def update_memory_cache(self, master_tree: GDriveWholeTree):
        pass

    @abstractmethod
    def update_disk_cache(self, cache: GDriveDatabase):
        pass

    @abstractmethod
    def send_signals(self):
        pass


# CLASS UpsertSingleNodeOp
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
class UpsertSingleNodeOp(GDriveCacheOp):
    def __init__(self, node: GDriveNode, uid_mapper, update_only: bool = False):
        self.node: GDriveNode = node
        self.uid_mapper = uid_mapper
        self.was_updated: bool = True
        self.parent_goog_ids = []
        self.update_only: bool = update_only

        # try to prevent cache corruption by doing some sanity checks
        if not node:
            raise RuntimeError(f'No node supplied!')
        if not node.uid:
            raise RuntimeError(f'Node is missing UID: {node}')
        if node.node_identifier.tree_type != TREE_TYPE_GDRIVE:
            raise RuntimeError(f'Unrecognized tree type: {node.node_identifier.tree_type}')
        if not isinstance(node, GDriveNode):
            raise RuntimeError(f'Unrecognized node type: {node}')

    def update_memory_cache(self, master_tree: GDriveWholeTree):
        if SUPER_DEBUG:
            logger.debug(f'Upserting GDriveNode to memory cache: {self.node}')

        # Detect whether it's already in the cache
        if self.node.goog_id:
            uid_from_mapper = self.uid_mapper.get_uid_for_goog_id(goog_id=self.node.goog_id)
            if self.node.uid != uid_from_mapper:
                logger.warning(f'Found node in cache with same GoogID ({self.node.goog_id}) but different UID ('
                               f'{uid_from_mapper}). Changing UID of node (was: {self.node.uid}) to match and overwrite previous node')
                self.node.uid = uid_from_mapper

        existing_node = master_tree.get_node_for_uid(self.node.uid)
        if existing_node:
            # it is ok if we have an existing node which doesn't have a goog_id; that will be replaced
            if existing_node.goog_id and existing_node.goog_id != self.node.goog_id:
                raise RuntimeError(f'Serious error: cache already contains UID {self.node.uid} but Google ID does not match '
                                   f'(existing="{existing_node.goog_id}"; new="{self.node.goog_id}")')

            if existing_node.exists() and not self.node.exists():
                # In the future, let's close this hole with more elegant logic
                logger.warning(f'Cannot replace a node which exists with one which does not exist; ignoring: {self.node}')
                self.was_updated = False
                return

            if existing_node.is_dir() and not self.node.is_dir():
                # need to replace all descendants...not ready to do this yet
                raise RuntimeError(f'Cannot replace a folder with a file: "{self.node.full_path}"')

            if existing_node == self.node:
                logger.info(f'Node being added (uid={self.node.uid}) is identical to node already in the cache; skipping cache update')
                dispatcher.send(signal=actions.NODE_UPSERTED, sender=ID_GLOBAL_CACHE, node=self.node)
                self.node = existing_node
                self.was_updated = False
                return
            logger.debug(f'Found existing node in cache with UID={existing_node.uid}: doing an update')
        elif self.update_only:
            logger.debug(f'Skipping update for node because it is not in the memory cache: {self.node}')
            self.was_updated = False
            return

        # Finally, update in-memory cache (tree). If an existing node is found with the same UID, it will update and return that instead:
        self.node = master_tree.add_node(self.node)

        # Generate full_path for node, if not already done (we assume this is a newly created node)
        master_tree.get_full_path_for_node(self.node)

        parent_uids = self.node.get_parent_uids()
        if parent_uids:
            try:
                self.parent_goog_ids = master_tree.resolve_uids_to_goog_ids(parent_uids, fail_if_missing=True)
            except RuntimeError:
                logger.debug(f'Could not resolve goog_ids for parent UIDs ({parent_uids}); assuming parents do not exist')
        else:
            logger.debug(f'Node has no parents; assuming it is a root node: {self.node}')

    def update_disk_cache(self, cache: GDriveDatabase):
        if SUPER_DEBUG:
            logger.debug(f'Upserting GDriveNode to disk cache: {self.node}')

        if not self.was_updated:
            logger.debug(f'Node does not need disk update; skipping save to disk: {self.node}')
            return

        if not self.node.exists():
            logger.debug(f'Node does not exist; skipping save to disk: {self.node}')
            return

        parent_mappings = []
        parent_uids = self.node.get_parent_uids()
        if len(parent_uids) != len(self.parent_goog_ids):
            raise RuntimeError(f'Internal error: could not map all parent goog_ids ({len(self.parent_goog_ids)}) to parent UIDs '
                               f'({len(parent_uids)}) for node: {self.node}')
        for parent_uid, parent_goog_id in zip(parent_uids, self.parent_goog_ids):
            parent_mappings.append((self.node.uid, parent_uid, parent_goog_id, self.node.sync_ts))

        # Write new values:
        if parent_mappings:
            logger.debug(f'Writing id-parent mappings to the GDrive master cache: {parent_mappings}')
            cache.upsert_parent_mappings_for_id(parent_mappings, self.node.uid, commit=False)

        if self.node.is_dir():
            logger.debug(f'Writing folder node to the GDrive master cache: {self.node}')
            assert isinstance(self.node, GDriveFolder)
            cache.upsert_gdrive_folder_list([self.node])
        else:
            logger.debug(f'Writing file node to the GDrive master cache: {self.node}')
            assert isinstance(self.node, GDriveFile)
            cache.upsert_gdrive_file_list([self.node])

    def send_signals(self):
        if self.was_updated:
            dispatcher.send(signal=actions.NODE_UPSERTED, sender=ID_GLOBAL_CACHE, node=self.node)


# CLASS DeleteSingleNodeOp
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
class DeleteSingleNodeOp(GDriveCacheOp):
    def __init__(self, node: GDriveNode, to_trash: bool = False):
        assert isinstance(node, GDriveNode), f'For node: {node}'
        self.node: GDriveNode = node
        self.to_trash: bool = to_trash

    def update_memory_cache(self, master_tree: GDriveWholeTree):
        if SUPER_DEBUG:
            logger.debug(f'Removing GDriveNode from memory cache: {self.node}')

        if self.node.is_dir():
            children: List[GDriveNode] = master_tree.get_children(self.node)
            if children:
                raise RuntimeError(f'Cannot remove GDrive folder from cache: it contains {len(children)} children!')

        existing_node = master_tree.get_node_for_uid(self.node.uid)
        if existing_node:
            master_tree.remove_node(existing_node)

    def update_disk_cache(self, cache: GDriveDatabase):
        if SUPER_DEBUG:
            logger.debug(f'Removing GDriveNode from disk cache: {self.node}')

        cache.delete_single_node(self.node, commit=False)

    def send_signals(self):
        dispatcher.send(signal=actions.NODE_REMOVED, sender=ID_GLOBAL_CACHE, node=self.node)


# CLASS DeleteSubtreeOp
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
class DeleteSubtreeOp(GDriveCacheOp):
    def __init__(self, subtree_root_node: GDriveNode, node_list: List[GDriveNode]):
        self.subtree_root_node: GDriveNode = subtree_root_node
        """If true, is a delete operation. If false, is upsert op."""
        self.node_list: List[GDriveNode] = node_list

    def update_memory_cache(self, master_tree: GDriveWholeTree):
        logger.debug(f'DeleteSubtreeOp: removing {len(self.node_list)} nodes from memory cache')
        for node in reversed(self.node_list):
            existing_node = master_tree.get_node_for_uid(node.uid)
            if existing_node:
                master_tree.remove_node(existing_node)
        logger.debug(f'DeleteSubtreeOp: done removing nodes from memory cache')

    def update_disk_cache(self, cache: GDriveDatabase):
        logger.debug(f'DeleteSubtreeOp: removing {len(self.node_list)} nodes from disk cache')
        for node in self.node_list:
            cache.delete_single_node(node, commit=False)
        logger.debug(f'DeleteSubtreeOp: done removing nodes from disk cache')

    def send_signals(self):
        logger.debug(f'DeleteSubtreeOp: sending "{actions.NODE_REMOVED}" signal for {len(self.node_list)} nodes')
        for node in self.node_list:
            dispatcher.send(signal=actions.NODE_REMOVED, sender=ID_GLOBAL_CACHE, node=node)


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
            logger.debug(f'No node found in cache for removed goog_id: "{last_change.goog_id}"')

    logger.debug(f'Reduced {len(change_list)} changes into {len(reduced_changes)} changes')
    return reduced_changes


# CLASS BatchChangesOp
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
class BatchChangesOp(GDriveCacheOp):
    def __init__(self, app, change_list: List[GDriveChange]):
        self.app = app
        self.change_list = _reduce_changes(change_list)

    def update_memory_cache(self, master_tree: GDriveWholeTree):
        for change in self.change_list:
            if change.is_removed():
                # Some GDrive deletes (such as a hard delete of a folder) will cause a parent to be deleted before its descendants.
                removed_node = master_tree.remove_node(change.node, fail_if_children_present=False)
                if removed_node:
                    change.node = removed_node
            else:
                assert isinstance(change, GDriveNodeChange)
                # need to use existing object if available to fulfill our contract (node will be sent via signals below)
                change.node = master_tree.add_node(change.node)

                # ensure full_path is populated
                master_tree.get_full_path_for_node(change.node)

    def update_disk_cache(self, cache: GDriveDatabase):
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
                    parent_goog_ids = self.app.cacheman.get_goog_id_list_for_uid_list(parent_uids)
                    if len(change.node.get_parent_uids()) != len(parent_goog_ids):
                        raise RuntimeError(f'Internal error: could not map all parent goog_ids ({len(parent_goog_ids)}) to parent UIDs '
                                           f'({len(parent_uids)}) for node: {change.node}')
                    for parent_uid, parent_goog_id in zip(change.node.get_parent_uids(), parent_goog_ids):
                        parent_mapping_list.append((change.node.uid, parent_uid, parent_goog_id, change.node.sync_ts))
                    mappings_list_list.append(parent_mapping_list)

                if change.node.is_dir():
                    assert isinstance(change.node, GDriveFolder)
                    folders_to_upsert.append(change.node)
                else:
                    assert isinstance(change.node, GDriveFile)
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
        for change in self.change_list:
            if change.is_removed():
                dispatcher.send(signal=actions.NODE_REMOVED, sender=ID_GLOBAL_CACHE, node=change.node)
            else:
                dispatcher.send(signal=actions.NODE_UPSERTED, sender=ID_GLOBAL_CACHE, node=change.node)

