import collections
import errno
import logging
import os
import threading
from typing import Callable, Dict, List, Optional, Set

from pydispatch import dispatcher

from backend.display_tree.active_tree_meta import ActiveDisplayTreeMeta
from backend.display_tree.change_tree import ChangeTree
from backend.display_tree.filter_state import FilterState
from backend.display_tree.root_path_config import RootPathConfigPersister
from backend.realtime.live_monitor import LiveMonitor
from constants import GDRIVE_ROOT_UID, LOCAL_ROOT_UID, NULL_UID, STATS_REFRESH_HOLDOFF_TIME_MS, TreeDisplayMode, \
    TreeID, TreeLoadState, TreeType
from logging_constants import DIFF_DEBUG_ENABLED, SUPER_DEBUG_ENABLED, TRACE_ENABLED
from error import CacheNotLoadedError, GDriveNodePathNotFoundError
from model.display_tree.build_struct import DisplayTreeRequest
from model.display_tree.display_tree import DisplayTree, DisplayTreeUiState
from model.display_tree.filter_criteria import FilterCriteria
from model.node.node import Node, NonexistentDirNode, SPIDNodePair
from model.node_identifier import GUID, LocalNodeIdentifier, NodeIdentifier, SinglePathNodeIdentifier
from model.node_identifier_factory import NodeIdentifierFactory
from model.uid import UID
from model.user_op import Batch
from signal_constants import ID_GLOBAL_CACHE, ID_LEFT_DIFF_TREE, ID_LEFT_TREE, ID_RIGHT_DIFF_TREE, ID_RIGHT_TREE, Signal
from util import file_util
from util.has_lifecycle import HasLifecycle
from util.holdoff_timer import HoldOffTimer
from util.root_path_meta import RootPathMeta

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
        self._display_tree_dict_lock = threading.Lock()
        """Need to use this to lock putters and iterators. Looks like getters should be fine though..."""

        self._is_live_monitoring_enabled = backend.get_config('cache.monitoring.live_monitoring_enabled')

        # FIXME: this is a bad solution. Having a single global timer like this can result in stats never getting refreshed if there is frequent
        # updates going on anywhere
        self._stats_refresh_timer = HoldOffTimer(holdoff_time_ms=STATS_REFRESH_HOLDOFF_TIME_MS, task_func=self._process_queued_stats)
        self._tree_stats_refresh_queue_dict: Dict[TreeID, Set[GUID]] = {}
        self._stat_dict_lock = threading.Lock()

        # simple-as-can-be hook for
        self.on_deregister_tree_hook: Optional[Callable] = None

    def start(self):
        logger.debug('[ActiveTreeManager] Startup started')
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
        self.connect_dispatch_listener(signal=Signal.SUBTREE_NODES_CHANGED_IN_CACHE, receiver=self._on_subtree_nodes_changed_in_cache)

        self._live_monitor.start()
        logger.debug('[ActiveTreeManager] Startup done')

    def shutdown(self):
        logger.debug('[ActiveTreeManager] Shutdown started')
        HasLifecycle.shutdown(self)

        # Do this after destroying controllers, for a more orderly shutdown:
        try:
            if self._live_monitor:
                self._live_monitor.shutdown()
                self._live_monitor = None
        except (AttributeError, NameError):
            pass

        logger.debug('[ActiveTreeManager] Shutdown done')

    # SignalDispatcher callbacks
    # ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼

    def _get_filtered_sn(self, node: Node, full_path: str, tree_meta: ActiveDisplayTreeMeta) -> Optional[SPIDNodePair]:
        filter_state: FilterState = tree_meta.filter_state

        sn = SPIDNodePair(self.backend.cacheman.make_spid_for(node_uid=node.uid, device_uid=node.device_uid, full_path=full_path), node)

        parent_sn: SPIDNodePair = self.backend.cacheman.get_parent_for_sn(sn)
        if not parent_sn:
            if sn.spid.tree_type == TreeType.LOCAL_DISK and sn.spid.node_uid == LOCAL_ROOT_UID:
                # tree root does not have a parent
                return None
            if sn.spid.tree_type == TreeType.GDRIVE and sn.spid.node_uid == GDRIVE_ROOT_UID:
                # tree root does not have a parent
                return None

            # this really shouldn't happen otherwise...
            logger.warning(f'[{tree_meta.tree_id}] No parent found in cacheman for: {sn.spid}. (tree_type={sn.spid.tree_type}). '
                           f'Will discard notification!')
            return None

        sn.spid.parent_guid = parent_sn.spid.guid
        assert sn.spid.parent_guid != sn.spid.guid, f'Parent GUID ({sn.spid.parent_guid}) should not be the same as: {sn.spid}'
        # Will need to refresh stats for parent, even if the node is filtered out:
        self._enqueue_stat_refresh_for_dir(sn.spid.parent_guid, tree_meta.tree_id)

        if filter_state.has_criteria() and not filter_state.matches(sn):
            if TRACE_ENABLED:
                logger.debug(f'[{tree_meta.tree_id}] Node is excluded by user filter criteria; will discard notification for {sn.spid}')
            return None

        # FIXME: almost certainly a race condition here. Low-priority high-effort: user can work around for now
        if sn.spid.parent_guid != tree_meta.root_sn.spid.guid and sn.spid.parent_guid not in tree_meta.expanded_row_set:
            if SUPER_DEBUG_ENABLED:
                logger.debug(f'[{tree_meta.tree_id}] Parent {sn.spid.parent_guid} is not expanded in FE; will discard notification for {sn.spid}')
            return None

        return sn

    def _enqueue_stat_refresh_for_dir(self, dir_guid: GUID, tree_id: TreeID):
        with self._stat_dict_lock:
            guid_set = self._tree_stats_refresh_queue_dict.get(tree_id, None)
            if not guid_set:
                guid_set: Set[GUID] = set()
                self._tree_stats_refresh_queue_dict[tree_id] = guid_set
            is_new: bool = dir_guid not in guid_set
            if is_new:
                guid_set.add(dir_guid)

        if SUPER_DEBUG_ENABLED:
            if is_new:
                logger.debug(f'[{tree_id}] Added dir {dir_guid} to stats queue; giving the stats refresh timer a kick')
            else:
                logger.debug(f'[{tree_id}] Dir {dir_guid} already present in stats queue')

        if is_new:
            self._stats_refresh_timer.start_or_delay()

    def _process_queued_stats(self):
        with self._stat_dict_lock:
            # For each display tree in the dict, need to regenerate stats for the given GUIDs and their descendants AND direct ancestors.
            # To simplify things and avoid possible errors, let's just regenerate the stats for the entire tree and see how that performs.
            for tree_id, guid_set in self._tree_stats_refresh_queue_dict.items():
                logger.debug(f'Recomputed stats for tree_id: {tree_id}')

                meta: ActiveDisplayTreeMeta = self.get_active_display_tree_meta(tree_id)
                if meta:
                    if meta.load_state == TreeLoadState.COMPLETELY_LOADED:
                        # Regenerate all the stats (+ status msg) and store the updates in the tree_meta:
                        self.backend.cacheman.repopulate_dir_stats_for_tree(meta)

                        # Push out the updates to all of the affected clients:
                        dispatcher.send(signal=Signal.STATS_UPDATED, sender=tree_id, status_msg=meta.summary_msg,
                                        dir_stats_dict_by_guid=meta.dir_stats_unfiltered_by_guid,
                                        dir_stats_dict_by_uid=meta.dir_stats_unfiltered_by_uid)
                    else:
                        logger.debug(f'Will skip regeneration of DirStats for tree_id "{tree_id}": tree is not in '
                                     f'{TreeLoadState.COMPLETELY_LOADED.name} state (actual state={meta.load_state})')
                else:
                    logger.debug(f'Will skip regeneration of DirStats for tree_id "{tree_id}": tree no longer active!')

            self._tree_stats_refresh_queue_dict.clear()

    def _is_node_in_gdrive_subtree(self, node: Node, subtree_root_spid: SinglePathNodeIdentifier) -> bool:
        ancestor_list = [node]
        while True:
            new_ancestor_list = []
            for ancestor in ancestor_list:
                if ancestor.uid == subtree_root_spid.node_uid:
                    return True

                for parent_node in self.backend.cacheman.get_parent_list_for_node(ancestor):
                    new_ancestor_list.append(parent_node)

            ancestor_list = new_ancestor_list

            if not ancestor_list:
                return False

    def _to_subtree_sn_list(self, node: Node, tree_meta: ActiveDisplayTreeMeta) -> List[SPIDNodePair]:
        subtree_root_spid: SinglePathNodeIdentifier = tree_meta.root_sn.spid

        if node.device_uid != subtree_root_spid.device_uid:
            return []

        # LocalDisk: easy: check path
        if node.tree_type == TreeType.LOCAL_DISK and node.node_identifier.has_path_in_subtree(subtree_root_spid.get_single_path()):
            sn = self._get_filtered_sn(node, node.get_single_path(), tree_meta)
            if sn:
                return [sn]
            else:
                return []

        # GDrive: laborious
        if self._is_node_in_gdrive_subtree(node, subtree_root_spid):
            # OK, yes: we are in subtree. Now just create SN:
            subtree_root_path = subtree_root_spid.get_single_path()
            return_list = []
            found = False  # use boolean for sanity check
            assert node.get_path_list(), f'Node has no paths: {node}'
            for path in node.get_path_list():
                if path.startswith(subtree_root_path):
                    found = True
                    sn = self._get_filtered_sn(node, path, tree_meta)
                    if sn:
                        return_list.append(sn)

            if not found:
                raise RuntimeError(f'Internal error: we should have found node {node} in subtree {subtree_root_spid}')
            return return_list

        return []

    def _on_node_upserted(self, sender: str, node: Node):
        with self._display_tree_dict_lock:
            for tree_id, tree_meta in self._display_tree_dict.items():
                if not tree_meta.is_first_order():
                    if TRACE_ENABLED:
                        logger.debug(f'[{tree_id}] Tree is not first-order; ignoring upserted node {node.device_uid}:{node.uid}')
                    continue
                subtree_sn_list = self._to_subtree_sn_list(node, tree_meta)
                if TRACE_ENABLED:
                    logger.debug(f'[{tree_id}] Upserted node {node.device_uid}:{node.uid} resolved to {len(subtree_sn_list)} SPIDs')

                for sn in subtree_sn_list:
                    if SUPER_DEBUG_ENABLED:
                        logger.debug(f'[{tree_id}] Notifying tree of upserted node {sn} parent_guid={sn.spid.parent_guid} icon={sn.node.get_icon()}')
                    dispatcher.send(signal=Signal.NODE_UPSERTED, sender=tree_id, sn=sn)

    def _on_node_removed(self, sender: str, node: Node):
        with self._display_tree_dict_lock:
            for tree_id, tree_meta in self._display_tree_dict.items():
                if not tree_meta.is_first_order():
                    if TRACE_ENABLED:
                        logger.debug(f'[{tree_id}] Tree is not first-order; ignoring removed node {node.device_uid}:{node.uid}')
                    continue
                subtree_sn_list = self._to_subtree_sn_list(node, tree_meta)
                if TRACE_ENABLED:
                    logger.debug(f'Removed node {node.device_uid}:{node.uid} resolved to {len(subtree_sn_list)} SPIDs in {tree_id}')

                for sn in subtree_sn_list:
                    if SUPER_DEBUG_ENABLED:
                        logger.debug(f'[{tree_id}] Notifying tree of removed node {sn.spid} parent_guid={sn.spid.parent_guid}')
                    dispatcher.send(signal=Signal.NODE_REMOVED, sender=tree_id, sn=sn)

    def _get_intersecting_spid_for_subtree_root(self, tree_meta, subtree_root: NodeIdentifier) -> Optional[SinglePathNodeIdentifier]:
        if subtree_root.is_spid():
            assert isinstance(subtree_root, SinglePathNodeIdentifier)
            return subtree_root
        else:
            tree_root_path: str = tree_meta.root_sn.spid.get_single_path()
            for path in subtree_root.get_path_list():
                if path.startswith(tree_root_path) or tree_root_path.startswith(path):
                    logger.debug(f'Looks like paths intersect: tree_subroot="{tree_root_path}", update_subroot="{path}"')

                    return self.backend.cacheman.make_spid_for(node_uid=subtree_root.node_uid, device_uid=subtree_root.device_uid, full_path=path)

        logger.debug(f'Looks like tree_root ("{tree_root_path}") does not intersect with update_subroot ("{subtree_root.get_path_list()}")')
        return None

    def _on_subtree_nodes_changed_in_cache(self, sender: str, subtree_root: NodeIdentifier, upserted_node_list: List[Node],
                                           removed_node_list: List[Node]):

        if not upserted_node_list and not removed_node_list:
            if SUPER_DEBUG_ENABLED:
                logger.debug(f'Ignoring batch update at {subtree_root}: batch contains no nodes')
            return

        with self._display_tree_dict_lock:
            for tree_id, tree_meta in self._display_tree_dict.items():
                if tree_meta.root_sn.spid.device_uid == subtree_root.device_uid:
                    subtree_root_spid = self._get_intersecting_spid_for_subtree_root(tree_meta, subtree_root)
                    if not subtree_root_spid:
                        continue
                    assert isinstance(subtree_root_spid, SinglePathNodeIdentifier), f'Not a SPID: {subtree_root_spid}'

                    upserted_sn_list = []
                    removed_sn_list = []

                    if SUPER_DEBUG_ENABLED:
                        logger.debug(f'[{tree_id}] Examining {len(upserted_node_list)} upserts & {len(removed_node_list)} removes at {subtree_root_spid}')

                    # Just do the easiest and least-error prone thing for now:
                    for node in upserted_node_list:
                        upserted_sn_list = upserted_sn_list + self._to_subtree_sn_list(node, tree_meta)

                    for node in removed_node_list:
                        removed_sn_list = removed_sn_list + self._to_subtree_sn_list(node, tree_meta)

                    if upserted_sn_list or removed_sn_list:
                        if SUPER_DEBUG_ENABLED:
                            upserts = ', '.join([f'(guid={sn.spid.guid} parent={sn.spid.parent_guid})' for sn in upserted_sn_list])
                            removes = ', '.join([f'(guid={sn.spid.guid} parent={sn.spid.parent_guid})' for sn in removed_sn_list])
                            logger.debug(f'[{tree_id}] Notifying tree of batch update at {subtree_root_spid} '
                                         f'with upserts={upserts} & removes={removes}')
                        dispatcher.send(signal=Signal.SUBTREE_NODES_CHANGED, sender=tree_id, subtree_root_spid=subtree_root_spid,
                                        upserted_sn_list=upserted_sn_list, removed_sn_list=removed_sn_list)
                    elif SUPER_DEBUG_ENABLED:
                        logger.debug(f'[{tree_id}] Ignoring batch update at {subtree_root_spid}; it does not apply to this tree')

    def _on_merge_requested(self, sender: str):
        logger.info(f'Received signal: {Signal.COMPLETE_MERGE.name} for tree "{sender}"')

        meta = self.get_active_display_tree_meta(sender)
        if not meta:
            raise RuntimeError(f'Could not find merge tree: {sender}')
        if not meta.change_tree:
            raise RuntimeError(f'Could not find change tree for: {sender}')

        try:
            op_list = meta.change_tree.get_op_list()
            if op_list:
                batch = Batch(batch_uid=op_list[0].batch_uid, op_list=op_list)
                logger.debug(f'Sending new batch (uid={batch.batch_uid}) with {len(op_list)} ops from tree "{sender}" to cacheman be enqueued')
                self.backend.cacheman.enqueue_op_batch(batch=batch)

            self._cancel_diff_mode()
        except Exception as err:
            self.backend.report_exception(sender=ID_GLOBAL_CACHE, msg=f'Failed to merge {len(meta.change_tree.get_op_list())} operations',
                                          error=err)

    def _on_exit_diff_mode_requested(self, sender: str):
        logger.info(f'Received signal: {Signal.EXIT_DIFF_MODE.name} for tree "{sender}"')
        try:
            self._cancel_diff_mode()
        except Exception as err:
            self.backend.report_exception(sender=ID_GLOBAL_CACHE, msg=f'Failed to exit diff mode', error=err)

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
        logger.info(f'Received signal: "{Signal.GDRIVE_RELOADED.name}" from {sender} with device_uid={device_uid}')

        try:
            tree_id_list: List[str] = []
            with self._display_tree_dict_lock:
                for tree_meta in self._display_tree_dict.values():
                    if tree_meta.root_sn.spid.device_uid == device_uid:
                        tree_id_list.append(tree_meta.tree_id)

            gdrive_root_spid = NodeIdentifierFactory.get_root_constant_gdrive_spid(device_uid)
            for tree_id in tree_id_list:
                logger.info(f'[{tree_id}] Resetting subtree path to GDrive root')
                request = DisplayTreeRequest(tree_id, spid=gdrive_root_spid, return_async=True)
                self.request_display_tree(request)
        except Exception as err:
            self.backend.report_exception(sender=ID_GLOBAL_CACHE, msg=f'Error occurred while relaoding Google Drive trees', error=err)

    def _deregister_display_tree(self, sender: str):
        logger.debug(f'[{sender}] Received signal: "{Signal.DEREGISTER_DISPLAY_TREE.name}"')

        if self.on_deregister_tree_hook:
            self.on_deregister_tree_hook()

        with self._display_tree_dict_lock:
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
        if DIFF_DEBUG_ENABLED:
            change_tree.print_tree_contents_debug()
            change_tree.print_op_structs_debug()

        filter_state = FilterState.from_config(self.backend, change_tree.tree_id, change_tree.get_root_sn())

        meta = ActiveDisplayTreeMeta(self.backend, change_tree.state, filter_state)
        meta.change_tree = change_tree
        meta.src_tree_id = src_tree_id
        with self._display_tree_dict_lock:
            self._display_tree_dict[change_tree.tree_id] = meta

        # I suppose we could return the full tree for the thick client, but let's try to sync its behavior of the thin client instead:
        return change_tree.state.to_display_tree(self.backend)

    # TODO: make this wayyyy less complicated by just making each tree_id represent a set of persisted configs. Minimize DisplayTreeRequest
    def request_display_tree(self, request: DisplayTreeRequest, propogate_diff_tree_cancellation: bool = True) -> Optional[DisplayTree]:
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

        Note: this does not actually load the tree's nodes beyond the root node. To do that, the FE must call backend.start_subtree_load().

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

                    with self._display_tree_dict_lock:
                        if self._display_tree_dict.pop(sender_tree_id):
                            logger.debug(f'Discarded meta for tree: {sender_tree_id}')

                else:
                    # ChangeDisplayTrees are already loaded, and live capture should not apply
                    logger.warning(f'request_display_tree(): this is a ChangeDisplayTrees. Did you mean to call this method?')
                    return self._return_display_tree(sender_tree_id, display_tree_meta, request.return_async)

            elif display_tree_meta.root_sn.spid == root_path_meta.root_spid and display_tree_meta.root_exists == root_path_meta.root_exists:
                # Requested the existing tree and root? Just return that. (note that we make an exception if root existence has changed)
                logger.debug(f'Display tree already registered with given root; returning existing')
                return self._return_display_tree(sender_tree_id, display_tree_meta, request.return_async)

            if display_tree_meta.root_path_config_persister:
                # If we started from a persister, continue persisting:
                root_path_persister = display_tree_meta.root_path_config_persister

        # Ensure cache is at least init'd for the root node, so that read_node_for_spid() doesn't error out:
        self.backend.cacheman.get_cache_info_for_subtree(subtree_root=spid, create_if_not_found=True)
        # Try to retrieve the root node from the cache:
        try:
            # FIXME: if the GDrive cache hasn't been loaded, this falsely reports that subtree does not exist!
            node: Optional[Node] = self.backend.cacheman.read_node_for_spid(spid)
            if node:
                logger.debug(f'[{sender_tree_id}] Read DisplayTree root node: {node}')
            else:
                logger.debug(f'[{sender_tree_id}] DisplayTree root node not found in cache')
        except RuntimeError as e:
            logger.warning(f'[{sender_tree_id}] Could not retrieve DisplayTree root node (will try to recover): {spid} (error: {e})')
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
            logger.debug(f'[{sender_tree_id}] Creating NonexistentDirNode because node was null')
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
            filter_state = FilterState.from_config(self.backend, sender_tree_id, root_sn)
            display_tree_meta = ActiveDisplayTreeMeta(self.backend, state, filter_state)

            # Store in dict here:
            with self._display_tree_dict_lock:
                self._display_tree_dict[response_tree_id] = display_tree_meta

        if root_path_persister:
            # Write updates to app_config if applicable
            root_path_persister.write_to_config(root_path_meta)

            # Retain the persister for next time:
            display_tree_meta.root_path_config_persister = root_path_persister

        if is_cancelling_diff and propogate_diff_tree_cancellation:
            self._cancel_diff_mode(display_tree_meta.state.to_display_tree(self.backend))

        return self._return_display_tree(sender_tree_id, display_tree_meta, request.return_async)

    def _return_display_tree(self, sender_tree_id: TreeID, display_tree_meta, return_async: bool) -> Optional[DisplayTree]:
        state: DisplayTreeUiState = display_tree_meta.state
        assert state.tree_id and state.root_sn and state.root_sn.spid, f'Bad DisplayTreeUiState: {state}'
        tree = state.to_display_tree(self.backend)

        if return_async:
            # notify clients asynchronously
            logger.debug(f'[{sender_tree_id}] Firing signal: {Signal.DISPLAY_TREE_CHANGED.name}')
            dispatcher.send(Signal.DISPLAY_TREE_CHANGED, sender=sender_tree_id, tree=tree)
            return None
        else:
            logger.debug(f'[{sender_tree_id}] Returning display tree synchronously because return_async=False: {state}')
            return tree

    def _resolve_root_meta_from_path(self, full_path: str, device_uid: UID) -> RootPathMeta:
        """Resolves the given path into either a local file, a set of Google Drive matches, or generates a GDriveNodePathNotFoundError,
        and returns a tuple of both"""
        logger.debug(f'resolve_root_from_path() called with device_uid={device_uid}, path="{full_path}"')

        if not device_uid:
            raise RuntimeError('No device_uid provided!')

        tree_type = self.backend.cacheman.get_tree_type_for_device_uid(device_uid)

        try:
            # Assume the user means the local disk (for now). In the future, maybe we can add support for some kind of server name syntax
            full_path = file_util.normalize_path(full_path)
            if tree_type == TreeType.GDRIVE:
                # Need to wait until all caches are loaded:
                # self.backend.cacheman.wait_for_startup_done()
                # this will load the GDrive master tree if needed:
                # TODO: we don't want to have to load the entire GDrive tree!
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
                new_root_spid = self.backend.node_identifier_factory.build_spid(node_uid=new_root_spid.node_uid, device_uid=new_root_spid.device_uid,
                                                                                single_path=full_path)

            root_path_meta = RootPathMeta(new_root_spid, root_exists=True)
        except GDriveNodePathNotFoundError as ginf:
            root_path_meta = RootPathMeta(ginf.node_identifier, root_exists=False)
            root_path_meta.offending_path = ginf.offending_path
        except FileNotFoundError:
            root = self.backend.node_identifier_factory.build_spid(device_uid=device_uid, single_path=full_path, node_uid=NULL_UID)
            root_path_meta = RootPathMeta(root, root_exists=False)
        except CacheNotLoadedError:
            root = self.backend.node_identifier_factory.build_spid(device_uid=device_uid, path_list=full_path, node_uid=NULL_UID)
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

    def update_live_capture(self, root_exists: bool, root_spid: SinglePathNodeIdentifier, tree_id: TreeID):
        if self._is_live_monitoring_enabled:
            if root_exists:
                self._live_monitor.start_or_update_capture(root_spid, tree_id)
            else:
                self._live_monitor.stop_capture(tree_id)
        else:
            logger.debug(f'[{tree_id}] Live monitoring is disabled: will not capture')
