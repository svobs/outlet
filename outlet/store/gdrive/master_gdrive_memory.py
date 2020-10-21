from typing import List, Optional, Tuple
import logging

from pydispatch import dispatcher

from constants import SUPER_DEBUG
from model.gdrive_whole_tree import GDriveWholeTree
from model.node.gdrive_node import GDriveNode
from store.uid.uid_mapper import UidGoogIdMapper
from ui import actions
from ui.actions import ID_GLOBAL_CACHE

logger = logging.getLogger(__name__)


# CLASS GDriveMemoryStore
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
class GDriveMemoryStore:
    def __init__(self, app, uid_mapper: UidGoogIdMapper):
        self.master_tree: Optional[GDriveWholeTree] = None
        self._uid_mapper: UidGoogIdMapper = uid_mapper

    def upsert_single_node(self, node: GDriveNode, update_only: bool = False) -> Tuple[GDriveNode, bool]:
        if SUPER_DEBUG:
            logger.debug(f'Upserting GDriveNode to memory cache: {node}')

        # Detect whether it's already in the cache
        if node.goog_id:
            uid_from_mapper = self._uid_mapper.get_uid_for_goog_id(goog_id=node.goog_id)
            if node.uid != uid_from_mapper:
                logger.warning(f'Found node in cache with same GoogID ({node.goog_id}) but different UID ('
                               f'{uid_from_mapper}). Changing UID of node (was: {node.uid}) to match and overwrite previous node')
                node.uid = uid_from_mapper

        existing_node = self.master_tree.get_node_for_uid(node.uid)
        if existing_node:
            # it is ok if we have an existing node which doesn't have a goog_id; that will be replaced
            if existing_node.goog_id and existing_node.goog_id != node.goog_id:
                raise RuntimeError(f'Serious error: cache already contains UID {node.uid} but Google ID does not match '
                                   f'(existing="{existing_node.goog_id}"; new="{node.goog_id}")')

            if existing_node.exists() and not node.exists():
                # In the future, let's close this hole with more elegant logic
                logger.warning(f'Cannot replace a node which exists with one which does not exist; ignoring: {node}')
                return node, False

            if existing_node.is_dir() and not node.is_dir():
                # need to replace all descendants...not ready to do this yet
                raise RuntimeError(f'Cannot replace a folder with a file: "{node.full_path}"')

            if existing_node == node:
                logger.info(f'Node being added (uid={node.uid}) is identical to node already in the cache; skipping cache update')
                dispatcher.send(signal=actions.NODE_UPSERTED, sender=ID_GLOBAL_CACHE, node=node)
                node = existing_node
                return node, False
            logger.debug(f'Found existing node in cache with UID={existing_node.uid}: doing an update')
        elif update_only:
            logger.debug(f'Skipping update for node because it is not in the memory cache: {node}')
            return node, False

        # Finally, update in-memory cache (tree). If an existing node is found with the same UID, it will update and return that instead:
        node = self.master_tree.add_node(node)

        # Generate full_path for node, if not already done (we assume this is a newly created node)
        self.master_tree.get_full_path_for_node(node)

        return node, True

    def remove_single_node(self, node: GDriveNode, to_trash: bool = False):
        if SUPER_DEBUG:
            logger.debug(f'Removing GDriveNode from memory cache: {node}')

        if to_trash:
            # TODO
            raise RuntimeError(f'Not supported: to_trash=true!')

        if node.is_dir():
            children: List[GDriveNode] = self.master_tree.get_children(node)
            if children:
                raise RuntimeError(f'Cannot remove GDrive folder from cache: it contains {len(children)} children!')

        existing_node = self.master_tree.get_node_for_uid(node.uid)
        if existing_node:
            self.master_tree.remove_node(existing_node)

