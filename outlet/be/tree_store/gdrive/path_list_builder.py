import logging
from typing import Callable, List, Tuple

from constants import ROOT_PATH
from logging_constants import SUPER_DEBUG_ENABLED, TRACE_ENABLED
from error import NodeNotPresentError
from model.node.gdrive_node import GDriveNode
from model.uid import UID

logger = logging.getLogger(__name__)


class GDrivePathListBuilder:

    def __init__(self, get_node_for_uid_func: Callable[[UID], GDriveNode]):
        self._get_node_for_uid_func: Callable[[UID], GDriveNode] = get_node_for_uid_func

    def rebuild_path_list_for_uid(self, uid: UID) -> GDriveNode:
        """Derives the list filesystem-like-paths for the node with the given UID, sets them, and returns the node with the paths populated.
        Stops when a parent cannot be found, or the root of the tree is reached.
        Note: the get_node_for_uid_func param should return a fully-formed GDriveNode, with the exception that its parent paths do not need
        to be populated."""
        current_node: GDriveNode = self._get_node_for_uid_func(uid)
        if not current_node:
            raise NodeNotPresentError(f'Cannot recompute path list: node not found in tree for UID {uid}')

        logger.debug(f'Recomputing path for node {uid} ("{current_node.name}")')

        # TODO: it's possible to optimize this by using the parent paths, if available

        path_list: List[str] = []
        # Iterate backwards (the given ID is the last segment in the path
        current_segment_nodes: List[Tuple[GDriveNode, str]] = [(current_node, '')]
        next_segment_nodes: List[Tuple[GDriveNode, str]] = []
        while current_segment_nodes:
            for node, path_so_far in current_segment_nodes:
                if path_so_far == '':
                    # first node (leaf)
                    path_so_far = node.name
                else:
                    if node.name == ROOT_PATH:
                        # special case for root path: don't add an extra slash
                        path_so_far = '/' + path_so_far
                    else:
                        # Pre-pend parent name:
                        path_so_far = node.name + '/' + path_so_far

                parent_uids: List[UID] = node.get_parent_uids()
                if parent_uids:
                    if len(parent_uids) > 1:
                        # Make sure they are not dead links:
                        parent_uids: List[UID] = [x for x in parent_uids if self._get_node_for_uid_func(x)]
                        if len(parent_uids) > 1:
                            if SUPER_DEBUG_ENABLED:
                                logger.debug(f'Multiple parents found for {node.uid} ("{node.name}").')
                                for parent_index, parent_uid in enumerate(parent_uids):
                                    logger.debug(f'Parent {parent_index}: {parent_uid}')
                            # pass through
                        elif SUPER_DEBUG_ENABLED:
                            logger.warning(f'Found multiple parents for node but only one could be resolved: node={node.uid} ("{node.name}")')
                    for parent_uid in parent_uids:
                        parent_node: GDriveNode = self._get_node_for_uid_func(parent_uid)
                        if parent_node:
                            next_segment_nodes.append((parent_node, path_so_far))
                        else:
                            # Parent refs cannot be resolved == root of subtree
                            if SUPER_DEBUG_ENABLED:
                                logger.debug(f'Mapped ID "{uid}" to subtree path "{path_so_far}"')
                            if path_so_far not in path_list:
                                path_list.append(path_so_far)

                else:
                    # No parent refs. Root of Google Drive
                    path_list.append(path_so_far)
            current_segment_nodes = next_segment_nodes
            next_segment_nodes = []

        if TRACE_ENABLED:
            logger.debug(f'Computed path list "{path_list}" for node_identifier: {current_node.node_identifier}')
        elif SUPER_DEBUG_ENABLED:
            if path_list != current_node.node_identifier.get_path_list():
                logger.debug(f'Updating path_list for node_identifier ({current_node.node_identifier}) -> {path_list}')

        current_node.node_identifier.set_path_list(path_list)

        for path in path_list:
            if path.startswith('//'):
                raise RuntimeError(f'Failed sanity check: generated invalid path ({path}) for node: {current_node}')

        return current_node
