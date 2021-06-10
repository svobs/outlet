import collections
import errno
import logging
import os
from collections import deque
from typing import Deque, Dict, List, Optional, Set

from pydispatch import dispatcher

from backend.display_tree.active_tree_meta import ActiveDisplayTreeMeta
from backend.display_tree.change_tree import ChangeTree
from backend.display_tree.filter_state import FilterState
from backend.display_tree.root_path_config import RootPathConfigPersister
from backend.realtime.live_monitor import LiveMonitor
from constants import CONFIG_DELIMITER, GDRIVE_ROOT_UID, NULL_UID, SUPER_DEBUG, TreeDisplayMode, TreeID, TreeType
from error import CacheNotLoadedError, GDriveItemNotFoundError
from model.display_tree.build_struct import DisplayTreeRequest, RowsOfInterest
from model.display_tree.display_tree import DisplayTree, DisplayTreeUiState
from model.display_tree.filter_criteria import FilterCriteria
from model.node.node import Node, NonexistentDirNode, SPIDNodePair
from model.node_identifier import GUID, LocalNodeIdentifier, SinglePathNodeIdentifier
from model.node_identifier_factory import NodeIdentifierFactory
from model.uid import UID
from signal_constants import ID_GLOBAL_CACHE, ID_LEFT_DIFF_TREE, ID_LEFT_TREE, ID_RIGHT_DIFF_TREE, ID_RIGHT_TREE, Signal
from util import file_util
from util.has_lifecycle import HasLifecycle
from util.root_path_meta import RootPathMeta
from util.stopwatch_sec import Stopwatch

logger = logging.getLogger(__name__)

SPIDNodePairWithParent = collections.namedtuple('SPIDNodePairWithParent', 'sn parent_guid')


class ActiveTreeManager(HasLifecycle):
    """
    ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
    CLASS ActiveTreeManager

    Central datatbase for DisplayTree information in the backend.
    Also encapsulates live monitoring, which monitors trees for changes in real time for display trees.
    ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼
    """

    def __init__(self, backend):
        HasLifecycle.__init__(self)
        self.backend = backend
        self._live_monitor: LiveMonitor = LiveMonitor(self.backend)
        """Sub-module of Cache Manager which, for displayed trees, provides [close to] real-time notifications for changes
         which originated from outside this backend"""

        self._display_tree_dict: Dict[TreeID, ActiveDisplayTreeMeta] = {}
        """Keeps track of which display trees are currently being used in the UI"""

        self._is_live_monitoring_enabled = backend.get_config('cache.monitoring.live_monitoring_enabled')

    def start(self):
        gdrive_live_monitor_enabled = self._is_live_monitoring_enabled and self._live_monitor.enable_gdrive_polling_thread
        if not gdrive_live_monitor_enabled and not self.backend.cacheman.sync_from_gdrive_on_cache_load:
            logger.warning(f'GDrive: live monitoring is disabled AND sync on cache load is disabled: GDrive cache will not be updated!')

        HasLifecycle.start(self)
        self.connect_dispatch_listener(signal=Signal.DEREGISTER_DISPLAY_TREE, receiver=self._deregister_display_tree)
        self.connect_dispatch_listener(signal=Signal.GDRIVE_RELOADED, receiver=self._on_gdrive_whole_tree_reloaded)
        self.connect_dispatch_listener(signal=Signal.COMPLETE_MERGE, receiver=self._on_merge_requested)
        self.connect_dispatch_listener(signal=Signal.EXIT_DIFF_MODE, receiver=self._on_exit_diff_mode_requested)

        # These take the signal from the cache and route it to the relevant display trees (if any) based on each node's location:
        self.connect_dispatch_listener(signal=Signal.NODE_UPSERTED_IN_CACHE, receiver=self._on_node_upserted)
        self.connect_dispatch_listener(signal=Signal.NODE_REMOVED_IN_CACHE, receiver=self._on_node_removed)

        self._live_monitor.start()

    def shutdown(self):
        logger.debug('ActiveTreeManager.shutdown() entered')
        HasLifecycle.shutdown(self)

        # Do this after destroying controllers, for a more orderly shutdown:
        try:
            if self._live_monitor:
                self._live_monitor.shutdown()
                self._live_monitor = None
        except (AttributeError, NameError):
            pass

    # SignalDispatcher callbacks
    # ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼

    def _get_filtered_snp(self, node: Node, full_path: str, filter_state: FilterState) -> Optional[SPIDNodePairWithParent]:
        sn = SPIDNodePair(self.backend.cacheman.make_spid_for(node_uid=node.uid, device_uid=node.device_uid, full_path=full_path), node)
        if not filter_state.has_criteria() or filter_state.matches(sn):
            parent_sn: SPIDNodePair = self.backend.cacheman.get_parent_for_sn(sn)
            if parent_sn:
                # Make sure to update the node icon before sending!
                self.backend.cacheman.update_node_icon(sn.node)
                return SPIDNodePairWithParent(sn, parent_sn.spid.guid)
            else:
                logger.warning(f'No parent found for: {sn.spid}. Will discard notification!')
                return None
        else:
            return None

    def _to_subtree_sn_list(self, node: Node, subtree_root_spid: SinglePathNodeIdentifier, filter_state: FilterState) -> List[SPIDNodePairWithParent]:
        cacheman = self.backend.cacheman

        if node.device_uid != subtree_root_spid.device_uid:
            return []

        # LocalDisk: easy: check path
        if node.tree_type == TreeType.LOCAL_DISK and node.node_identifier.has_path_in_subtree(subtree_root_spid.get_single_path()):
            snp = self._get_filtered_snp(node, node.get_single_path(), filter_state)
            if snp:
                return [snp]
            else:
                return []

        # GDrive: laborious
        ancestor_list = [node]
        while True:
            new_ancestor_list = []
            for ancestor in ancestor_list:
                if ancestor.uid == subtree_root_spid.node_uid:
                    # OK, yes: we are in subtree. Now just create SN:

                    subtree_root_path = subtree_root_spid.get_single_path()
                    return_list = []
                    for path in node.get_path_list():
                        if path.startswith(subtree_root_path):
                            snp = self._get_filtered_snp(node, path, filter_state)
                            if snp:
                                return_list.append(snp)

                    return return_list

                for parent_node in cacheman.get_parent_list_for_node(ancestor):
                    new_ancestor_list.append(parent_node)

            ancestor_list = new_ancestor_list

            if not ancestor_list:
                return []

    def _on_node_upserted(self, sender: str, node: Node):
        for tree_id, tree_meta in self._display_tree_dict.items():
            subtree_sn_list = self._to_subtree_sn_list(node, tree_meta.root_sn.spid, tree_meta.filter_state)
            if SUPER_DEBUG:
                logger.debug(f'Upserted node {node.device_uid}:{node.uid} resolved to {len(subtree_sn_list)} SPIDs in {tree_id}')

            for snp in subtree_sn_list:
                logger.debug(f'[{tree_id}] Notifying tree of upserted node: {snp.sn.spid}, parent_guid={snp.parent_guid}')
                dispatcher.send(signal=Signal.NODE_UPSERTED, sender=tree_id, sn=snp.sn, parent_guid=snp.parent_guid)

    def _on_node_removed(self, sender: str, node: Node):
        for tree_id, tree_meta in self._display_tree_dict.items():
            subtree_sn_list = self._to_subtree_sn_list(node, tree_meta.root_sn.spid, tree_meta.filter_state)
            if SUPER_DEBUG:
                logger.debug(f'Removed node {node.device_uid}:{node.uid} resolved to {len(subtree_sn_list)} SPIDs in {tree_id}')

            for snp in subtree_sn_list:
                logger.debug(f'[{tree_id}] Notifying tree of removed node: {snp.sn.spid}')
                dispatcher.send(signal=Signal.NODE_REMOVED, sender=tree_id, sn=snp.sn, parent_guid=snp.parent_guid)

    def _on_merge_requested(self, sender: str):
        logger.info(f'Received signal: {Signal.COMPLETE_MERGE.name} for tree "{sender}"')

        meta = self.get_active_display_tree_meta(sender)
        if not meta:
            raise RuntimeError(f'Could not find merge tree: {sender}')
        if not meta.change_tree:
            raise RuntimeError(f'Could not find change tree for: {sender}')

        try:
            op_list = meta.change_tree.get_ops()
            logger.debug(f'Sending {len(op_list)} ops from tree "{sender}" to cacheman be enqueued')
            self.backend.cacheman.enqueue_op_list(op_list=op_list)

            self._cancel_diff_mode()
        except Exception as err:
            logger.exception(f'[{sender}] Failed to merge {len(meta.change_tree.get_ops())} operations')
            self.backend.report_error(sender=ID_GLOBAL_CACHE, msg=f'Failed to merge {len(meta.change_tree.get_ops())} operations',
                                      secondary_msg=f'{repr(err)}')

    def _on_exit_diff_mode_requested(self, sender: str):
        logger.info(f'Received signal: {Signal.EXIT_DIFF_MODE.name} for tree "{sender}"')
        self._cancel_diff_mode()

    def _cancel_diff_mode(self, tree_already_cancelled: Optional[DisplayTree] = None):
        if tree_already_cancelled and tree_already_cancelled.tree_id == ID_LEFT_TREE:
            left_tree = tree_already_cancelled
        else:
            request = DisplayTreeRequest(tree_id=ID_LEFT_DIFF_TREE, return_async=False, tree_display_mode=TreeDisplayMode.ONE_TREE_ALL_ITEMS)
            left_tree = self.request_display_tree(request, propogate_diff_tree_cancellation=False)

        if tree_already_cancelled and tree_already_cancelled.tree_id == ID_RIGHT_TREE:
            right_tree = tree_already_cancelled
        else:
            request = DisplayTreeRequest(tree_id=ID_RIGHT_DIFF_TREE, return_async=False, tree_display_mode=TreeDisplayMode.ONE_TREE_ALL_ITEMS)
            right_tree = self.request_display_tree(request, propogate_diff_tree_cancellation=False)

        logger.debug(f'Sending signal: {Signal.DIFF_TREES_CANCELLED.name}')
        dispatcher.send(signal=Signal.DIFF_TREES_CANCELLED, sender=ID_GLOBAL_CACHE, tree_left=left_tree, tree_right=right_tree)

    def _on_gdrive_whole_tree_reloaded(self, sender: str, device_uid: UID):
        # If GDrive cache was reloaded, our previous selection was almost certainly invalid. Just reset all open GDrive trees to GDrive root.
        logger.info(f'Received signal: "{Signal.GDRIVE_RELOADED.name}" with device_uid={device_uid}')

        tree_id_list: List[str] = []
        for tree_meta in self._display_tree_dict.values():
            if tree_meta.root_sn.spid.device_uid == device_uid:
                tree_id_list.append(tree_meta.tree_id)

        gdrive_root_spid = NodeIdentifierFactory.get_root_constant_gdrive_spid(device_uid)
        for tree_id in tree_id_list:
            logger.info(f'[{tree_id}] Resetting subtree path to GDrive root')
            request = DisplayTreeRequest(tree_id, spid=gdrive_root_spid, return_async=True)
            self.request_display_tree(request)

    def _deregister_display_tree(self, sender: str):
        logger.debug(f'[{sender}] Received signal: "{Signal.DEREGISTER_DISPLAY_TREE.name}"')
        display_tree = self._display_tree_dict.pop(sender, None)
        if display_tree:
            logger.debug(f'[{sender}] Display tree deregistered in backend')
        else:
            logger.debug(f'[{sender}] Could not deregister display tree in backend: it was not found')

        # Also stop live capture, if any
        if self._is_live_monitoring_enabled and self._live_monitor:
            self._live_monitor.stop_capture(sender)

    # Public methods
    # ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼

    def register_change_tree(self, change_tree: ChangeTree, src_tree_id: TreeID) -> DisplayTree:
        """Stores the given ChangeTree in the in-memory dict, and returns a DisplayTree which can be sent to clients.
        src_tree_id is a reference to the DisplayTree on which the ChangeTree was based"""
        logger.info(f'Registering ChangeTree: {change_tree.tree_id} (src_tree_id: {src_tree_id})')
        if SUPER_DEBUG:
            change_tree.print_tree_contents_debug()
            change_tree.print_op_structs_debug()

        filter_state = FilterState.from_config(self.backend, change_tree.tree_id, change_tree.get_root_sn())

        meta = ActiveDisplayTreeMeta(self.backend, change_tree.state, filter_state)
        meta.change_tree = change_tree
        meta.src_tree_id = src_tree_id
        self._display_tree_dict[change_tree.tree_id] = meta

        # I suppose we could return the full tree for the thick client, but let's try to sync its behavior of the thin client instead:
        return change_tree.state.to_display_tree(self.backend)

    # TODO: make this wayyyy less complicated by just making each tree_id represent a set of persisted configs. Minimize DisplayTreeRequest
    def request_display_tree(self, request: DisplayTreeRequest, propogate_diff_tree_cancellation: bool = True) -> Optional[DisplayTreeUiState]:
        """
        Gets the following into memory (if not already):
        1. Root SPID
        2. Root node (if exists)
        3. Previous filter state

        Then:
        1. Start or update (or stop) live capture of the affected subtree
        2. Return tree meta:
           a. If async==true, send via DISPLAY_TREE_CHANGED signal
           b. Else return the tree directly.

        Note: this does not actually load the tree's nodes beyond the root. To do that, the FE must call backend.start_subtree_load().

        See _cancel_diff_mode() for why propogate_diff_tree_cancellation is used
        """
        sender_tree_id = request.tree_id
        spid = request.spid
        logger.debug(f'[{sender_tree_id}] Got request to load display tree (user_path={request.user_path}, spid={spid}, '
                     f'device_uid={request.device_uid}, is_startup={request.is_startup}, tree_display_mode={request.tree_display_mode})')

        root_path_persister = None

        display_tree_meta: ActiveDisplayTreeMeta = self.get_active_display_tree_meta(sender_tree_id)

        # Build RootPathMeta object from params. If neither SPID nor user_path supplied, read from config
        if request.user_path:
            root_path_meta: RootPathMeta = self._resolve_root_meta_from_path(request.user_path, request.device_uid)
        elif spid:
            assert isinstance(spid, SinglePathNodeIdentifier), f'Expected SinglePathNodeIdentifier but got {type(spid)}'
            # params -> root_path_meta
            spid.normalize_paths()
            root_path_meta = RootPathMeta(spid, True)
        elif request.is_startup:
            # root_path_meta -> params
            root_path_persister = RootPathConfigPersister(backend=self.backend, tree_id=sender_tree_id)
            root_path_meta = root_path_persister.read_from_config()
        elif display_tree_meta:
            root_path_meta = RootPathMeta(display_tree_meta.root_sn.spid, display_tree_meta.root_exists, display_tree_meta.offending_path)
        else:
            raise RuntimeError(f'Invalid args supplied to get_display_tree_ui_state()! (tree_id={sender_tree_id})')

        logger.debug(f'[{sender_tree_id}] Got {root_path_meta}')

        spid = root_path_meta.root_spid
        if not spid:
            raise RuntimeError(f"Root identifier is not valid for: '{sender_tree_id}'")

        is_cancelling_diff = False

        response_tree_id = sender_tree_id
        if display_tree_meta:
            if display_tree_meta.state.tree_display_mode == TreeDisplayMode.CHANGES_ONE_TREE_PER_CATEGORY:
                if request.tree_display_mode == TreeDisplayMode.ONE_TREE_ALL_ITEMS:
                    logger.info(f'[{sender_tree_id}] Looks like we are exiting diff mode: switching back to tree_id={display_tree_meta.src_tree_id}')

                    # Exiting diff mode -> look up prev tree
                    assert display_tree_meta.src_tree_id, f'Expected not-null src_tree_id for {display_tree_meta.tree_id}'
                    response_tree_id = display_tree_meta.src_tree_id
                    display_tree_meta = self.get_active_display_tree_meta(response_tree_id)

                    if self._display_tree_dict.pop(sender_tree_id):
                        logger.debug(f'Discarded meta for tree: {sender_tree_id}')

                else:
                    # ChangeDisplayTrees are already loaded, and live capture should not apply
                    logger.warning(f'request_display_tree(): this is a ChangeDisplayTrees. Did you mean to call this method?')
                    return self._return_display_tree_ui_state(sender_tree_id, display_tree_meta, request.return_async)

            elif display_tree_meta.root_sn.spid == root_path_meta.root_spid:
                # Requested the existing tree and root? Just return that:
                logger.debug(f'Display tree already registered with given root; returning existing')
                return self._return_display_tree_ui_state(sender_tree_id, display_tree_meta, request.return_async)

            if display_tree_meta.root_path_config_persister:
                # If we started from a persister, continue persisting:
                root_path_persister = display_tree_meta.root_path_config_persister

        # Try to retrieve the root node from the cache:
        try:
            node: Optional[Node] = self.backend.cacheman.read_single_node(spid)
            logger.debug(f'[{sender_tree_id}] Read display tree root node: {node}')
        except RuntimeError:
            logger.error(f'Could not retrieve root node while loading tree (will try to recover): {spid}')
            node = None

        if spid.tree_type == TreeType.LOCAL_DISK:
            if node:
                if spid.node_uid != node.uid:
                    logger.warning(f'UID requested ({spid.node_uid}) does not match UID from cache ({node.uid}); will use value from cache')
                spid = node.node_identifier
                root_path_meta.root_spid = spid

            root_path_meta.root_exists = os.path.exists(spid.get_single_path())
        elif spid.tree_type == TreeType.GDRIVE:
            root_path_meta.root_exists = node is not None
        else:
            raise RuntimeError(f'Unrecognized tree type: {spid.tree_type}')

        root_path_meta.offending_path = None
        
        if not node:
            node = NonexistentDirNode(node_identifier=spid, name=os.path.basename(spid.get_single_path()))

        # Now that we have the root, we have all the info needed to assemble the ActiveDisplayTreeMeta from the RootPathMeta.
        root_sn = SPIDNodePair(spid, node)

        state = DisplayTreeUiState(response_tree_id, root_sn, root_path_meta.root_exists, root_path_meta.offending_path,
                                   TreeDisplayMode.ONE_TREE_ALL_ITEMS, False)
        if self.backend.cacheman.is_manual_load_required(root_sn.spid, request.is_startup):
            state.needs_manual_load = True

        if display_tree_meta:
            # reuse and update existing
            display_tree_meta.state = state
            display_tree_meta.filter_state.update_root_sn(display_tree_meta.state.root_sn)
            assert display_tree_meta.state.tree_id == response_tree_id, f'TreeID "{response_tree_id}" != {display_tree_meta.state.tree_id}'

        else:
            logger.debug(f'[{sender_tree_id}] Reading FilterCriteria from app_config')
            filter_state = FilterState.from_config(self.backend, sender_tree_id, root_sn)

            display_tree_meta = ActiveDisplayTreeMeta(self.backend, state, filter_state)

            # Store in dict here:
            self._display_tree_dict[response_tree_id] = display_tree_meta

        if root_path_persister:
            # Write updates to app_config if applicable
            root_path_persister.write_to_config(root_path_meta)

            # Retain the persister for next time:
            display_tree_meta.root_path_config_persister = root_path_persister

        # Update monitoring state
        if self._is_live_monitoring_enabled:
            if display_tree_meta.root_exists:
                self._live_monitor.start_or_update_capture(display_tree_meta.root_sn.spid, response_tree_id)
            else:
                self._live_monitor.stop_capture(response_tree_id)
        else:
            logger.debug(f'[{sender_tree_id}] Live monitoring is disabled: will not capture')

        if is_cancelling_diff and propogate_diff_tree_cancellation:
            self._cancel_diff_mode(display_tree_meta.state.to_display_tree(self.backend))

        return self._return_display_tree_ui_state(sender_tree_id, display_tree_meta, request.return_async)

    def _return_display_tree_ui_state(self, sender_tree_id: TreeID, display_tree_meta, return_async: bool) -> Optional[DisplayTreeUiState]:
        state = display_tree_meta.state
        assert state.tree_id and state.root_sn and state.root_sn.spid, f'Bad DisplayTreeUiState: {state}'

        if return_async:
            # notify clients asynchronously
            tree = state.to_display_tree(self.backend)
            logger.debug(f'[{sender_tree_id}] Firing signal: {Signal.DISPLAY_TREE_CHANGED.name}')
            dispatcher.send(Signal.DISPLAY_TREE_CHANGED, sender=sender_tree_id, tree=tree)
            return None
        else:
            logger.debug(f'[{sender_tree_id}] Returning display tree synchronously because return_async=False: {state}')
            return state

    def _resolve_root_meta_from_path(self, full_path: str, device_uid: UID) -> RootPathMeta:
        """Resolves the given path into either a local file, a set of Google Drive matches, or generates a GDriveItemNotFoundError,
        and returns a tuple of both"""
        logger.debug(f'resolve_root_from_path() called with path="{full_path}"')
        if not device_uid:
            raise RuntimeError('No device_uid provided!')

        tree_type = self.backend.cacheman.get_tree_type_for_device_uid(device_uid)

        try:
            # Assume the user means the local disk (for now). In the future, maybe we can add support for some kind of server name syntax
            full_path = file_util.normalize_path(full_path)
            if tree_type == TreeType.GDRIVE:
                # Need to wait until all caches are loaded:
                self.backend.cacheman.wait_for_startup_done()
                # this will load the GDrive master tree if needed:
                identifier_list = self.backend.cacheman.get_gdrive_identifier_list_for_full_path_list(device_uid, [full_path],
                                                                                                      error_if_not_found=True)
            else:  # LocalNode
                if not os.path.exists(full_path):
                    raise FileNotFoundError(errno.ENOENT, os.strerror(errno.ENOENT), full_path)
                uid = self.backend.cacheman.get_uid_for_local_path(full_path)
                identifier_list = [LocalNodeIdentifier(uid=uid, device_uid=device_uid, full_path=full_path)]

            assert len(identifier_list) > 0, f'Got no identifiers for path but no error was raised: {full_path}'
            logger.debug(f'resolve_root_from_path(): got identifier_list={identifier_list}"')
            if len(identifier_list) > 1:
                # Create the appropriate
                candidate_list = []
                for identifier in identifier_list:
                    if full_path in identifier.get_path_list():
                        candidate_list.append(identifier)
                if len(candidate_list) != 1:
                    raise RuntimeError(f'Serious error: found multiple identifiers with same path ({full_path}): {candidate_list}')
                new_root_spid: SinglePathNodeIdentifier = candidate_list[0]
            else:
                new_root_spid = identifier_list[0]

            # TODO: this is really ugly code, hastily written, hastily maintained. Clean up!
            if len(new_root_spid.get_path_list()) > 0:
                # must have single path
                tree_type = new_root_spid.tree_type
                new_root_spid = self.backend.node_identifier_factory.for_values(uid=new_root_spid.node_uid, device_uid=new_root_spid.device_uid,
                                                                                tree_type=tree_type, path_list=full_path, must_be_single_path=True)

            root_path_meta = RootPathMeta(new_root_spid, root_exists=True)
        except GDriveItemNotFoundError as ginf:
            root_path_meta = RootPathMeta(ginf.node_identifier, root_exists=False)
            root_path_meta.offending_path = ginf.offending_path
        except FileNotFoundError as fnf:
            root = self.backend.node_identifier_factory.for_values(device_uid=device_uid, path_list=full_path, uid=NULL_UID, must_be_single_path=True)
            root_path_meta = RootPathMeta(root, root_exists=False)
        except CacheNotLoadedError as cnlf:
            root = self.backend.node_identifier_factory.for_values(device_uid=device_uid, path_list=full_path, uid=NULL_UID, must_be_single_path=True)
            root_path_meta = RootPathMeta(root, root_exists=False)

        logger.debug(f'resolve_root_from_path(): returning new_root={root_path_meta}"')
        return root_path_meta

    def get_active_display_tree_meta(self, tree_id: TreeID) -> ActiveDisplayTreeMeta:
        return self._display_tree_dict.get(tree_id, None)

    def get_filter_criteria(self, tree_id: TreeID) -> Optional[FilterCriteria]:
        meta = self.get_active_display_tree_meta(tree_id)
        if not meta:
            logger.error(f'get_filter_criteria(): no ActiveDisplayTree found for tree_id "{tree_id}"')
            return None
        return meta.filter_state.filter

    def update_filter_criteria(self, tree_id: TreeID, filter_criteria: FilterCriteria):
        meta = self.get_active_display_tree_meta(tree_id)
        if not meta:
            raise RuntimeError(f'update_filter_criteria(): no ActiveDisplayTree found for tree_id "{tree_id}"')

        # replace FilterState for the given tree
        meta.filter_state = FilterState(filter_criteria, meta.root_sn)
        # write to disk
        meta.filter_state.write_to_config(self.backend, tree_id)

    # Expanded & selected row state tracking
    # ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼

    def get_rows_of_interest(self, tree_id: TreeID) -> RowsOfInterest:
        logger.debug(f'[{tree_id}] Getting rows of interest')

        meta = self.get_active_display_tree_meta(tree_id)
        if not meta:
            raise RuntimeError(f'get_rows_of_interest(): DisplayTree not registered: {tree_id}')

        rows_of_interest = RowsOfInterest()
        rows_of_interest.expanded = meta.expanded_row_set
        rows_of_interest.selected = meta.selected_row_set
        logger.debug(f'[{tree_id}] get_rows_of_interest(): returning {len(meta.expanded_row_set)} expanded & {len(meta.selected_row_set)} selected')
        if SUPER_DEBUG:
            logger.debug(f'[{tree_id}] get_rows_of_interest(): Returning: expanded={meta.expanded_row_set}, selected={meta.selected_row_set}')

        return rows_of_interest

    def load_rows_of_interest(self, tree_id: TreeID):
        logger.debug(f'[{tree_id}] Loading rows of interest')

        meta = self.get_active_display_tree_meta(tree_id)
        if not meta:
            raise RuntimeError(f'load_rows_of_interest(): DisplayTree not registered: {tree_id}')

        # NOTE: the purge process will actually end up populating the expanded_row_set in the display_tree_meta, but we will just overwrite it
        expanded_row_set = self._load_expanded_rows_from_config(meta.tree_id)
        selected_row_set = self._load_selected_rows_from_config(meta.tree_id)
        rows_of_interest = self._purge_dead_rows(expanded_row_set, selected_row_set, meta)
        meta.expanded_row_set = rows_of_interest.expanded
        meta.selected_row_set = rows_of_interest.selected

    def set_selected_rows(self, tree_id: TreeID, selected: Set[GUID]):
        display_tree_meta: ActiveDisplayTreeMeta = self.get_active_display_tree_meta(tree_id)
        if not display_tree_meta:
            raise RuntimeError(f'Tree not found in memory: {tree_id}')

        logger.debug(f'[{tree_id}] Storing selection: {selected}')
        display_tree_meta.selected_row_set = selected
        # TODO: use a timer for this
        self._save_selected_rows_to_config(display_tree_meta)

    def add_expanded_row(self, guid: GUID, tree_id: TreeID):
        """AKA expanding a row on the frontend"""
        display_tree_meta: ActiveDisplayTreeMeta = self.get_active_display_tree_meta(tree_id)
        if not display_tree_meta:
            raise RuntimeError(f'Tree not found in memory: {tree_id}')

        if display_tree_meta.root_sn.spid.guid == guid:
            logger.debug(f'[{tree_id}] add_expanded_row(): ignoring root: {guid}')
            return

        logger.debug(f'[{tree_id}] Adding row to expanded_row_set: {guid}')
        display_tree_meta.expanded_row_set.add(guid)
        # TODO: use a timer for this. Also write selection to file
        self._save_expanded_rows_to_config(display_tree_meta)

    def remove_expanded_row(self, row_guid: GUID, tree_id: TreeID):
        """AKA collapsing a row on the frontend"""
        # TODO: remove descendants
        display_tree_meta: ActiveDisplayTreeMeta = self.get_active_display_tree_meta(tree_id)
        if not display_tree_meta:
            raise RuntimeError(f'Tree not found in memory: {tree_id}')

        if display_tree_meta.root_sn.spid.guid == row_guid:
            logger.debug(f'[{tree_id}] remove_expanded_row(): ignoring root: {row_guid}')
            return

        try:
            logger.debug(f'[{tree_id}] Removing row from expanded_row_set: {row_guid}')
            display_tree_meta.expanded_row_set.remove(row_guid)
        except Exception as err:
            self.backend.report_exception(sender=tree_id, msg=f'Failed to remove expanded row {row_guid}', error=err)
            return

        # TODO: use a timer for this. Also write selection to file
        self._save_expanded_rows_to_config(display_tree_meta)

    def _load_expanded_rows_from_config(self, tree_id: TreeID) -> Set[str]:
        """Loads the Set of expanded rows from config file"""
        logger.debug(f'[{tree_id}] Loading expanded rows from app_config')
        try:
            expanded_row_set: Set[str] = set()
            expanded_rows_str: str = self.backend.get_config(ActiveTreeManager._make_expanded_rows_config_key(tree_id), default_val='',
                                                             required=False)
            if expanded_rows_str:
                for guid in expanded_rows_str.split(CONFIG_DELIMITER):
                    expanded_row_set.add(guid)
            return expanded_row_set
        except RuntimeError:
            logger.exception(f'[{tree_id}] Failed to load expanded rows from app_config')

    def _save_selected_rows_to_config(self, display_tree_meta: ActiveDisplayTreeMeta):
        selected_rows_str: str = CONFIG_DELIMITER.join(str(guid) for guid in display_tree_meta.selected_row_set)
        self.backend.put_config(ActiveTreeManager._make_selected_rows_config_key(display_tree_meta.tree_id), selected_rows_str)

    @staticmethod
    def _make_expanded_rows_config_key(tree_id: TreeID) -> str:
        return f'ui_state.{tree_id}.expanded_rows'

    def _load_selected_rows_from_config(self, tree_id: TreeID) -> Set[GUID]:
        """Loads the Set of selected rows from app_config file"""
        logger.debug(f'[{tree_id}] Loading selected rows from app_config')
        try:
            selected_row_set: Set[GUID] = set()
            selected_rows_unparsed: str = self.backend.get_config(ActiveTreeManager._make_selected_rows_config_key(tree_id), default_val='',
                                                                  required=False)
            if selected_rows_unparsed:
                for guid in selected_rows_unparsed.split(CONFIG_DELIMITER):
                    selected_row_set.add(guid)
            return selected_row_set
        except RuntimeError as err:
            self.backend.report_exception(sender=tree_id, msg=f'Failed to load expanded rows from app_config', error=err)

    def _save_expanded_rows_to_config(self, display_tree_meta: ActiveDisplayTreeMeta):
        expanded_rows_str: str = CONFIG_DELIMITER.join(str(uid) for uid in display_tree_meta.expanded_row_set)
        self.backend.put_config(ActiveTreeManager._make_expanded_rows_config_key(display_tree_meta.tree_id), expanded_rows_str)

    @staticmethod
    def _make_selected_rows_config_key(tree_id: TreeID) -> str:
        return f'ui_state.{tree_id}.selected_rows'

    def _purge_dead_rows(self, expanded_cached: Set[GUID], selected_cached: Set[GUID], display_tree_meta: ActiveDisplayTreeMeta) -> RowsOfInterest:
        verified = RowsOfInterest()

        if not display_tree_meta.root_exists:
            verified.expanded = expanded_cached
            verified.selected = selected_cached
            return verified

        stopwatch = Stopwatch()

        processing_queue: Deque[SPIDNodePair] = deque()

        for sn in self.backend.get_child_list(parent_spid=display_tree_meta.state.root_sn.spid, tree_id=display_tree_meta.tree_id):
            processing_queue.append(sn)

        while len(processing_queue) > 0:
            sn: SPIDNodePair = processing_queue.popleft()
            guid = sn.spid.guid
            if guid in selected_cached:
                verified.selected.add(guid)

            if guid in expanded_cached:
                verified.expanded.add(guid)
                for sn in self.backend.get_child_list(parent_spid=sn.spid, tree_id=display_tree_meta.tree_id):
                    processing_queue.append(sn)

        logger.debug(f'[{display_tree_meta.tree_id}] {stopwatch} Verified {len(verified.expanded)} of {len(expanded_cached)} expanded rows '
                     f'and {len(verified.selected)} of {len(selected_cached)} selected')
        return verified
