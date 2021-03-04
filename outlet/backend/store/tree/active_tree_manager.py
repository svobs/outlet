import errno
import logging
import os
from collections import deque
from typing import Deque, Dict, List, Optional, Set

from pydispatch import dispatcher

from backend.store.tree.change_tree import ChangeTree
from backend.store.tree.filter_state import FilterState
from constants import CONFIG_DELIMITER, GDRIVE_ROOT_UID, NULL_UID, SUPER_DEBUG, TREE_TYPE_GDRIVE, TREE_TYPE_LOCAL_DISK, TreeDisplayMode
from error import CacheNotLoadedError, GDriveItemNotFoundError
from model.display_tree.build_struct import DisplayTreeRequest, RowsOfInterest
from model.display_tree.display_tree import DisplayTree, DisplayTreeUiState
from backend.store.gdrive.gdrive_whole_tree import GDriveWholeTree
from model.display_tree.filter_criteria import FilterCriteria
from model.node.node import Node, SPIDNodePair
from model.node_identifier import LocalNodeIdentifier, NodeIdentifier, SinglePathNodeIdentifier
from model.node_identifier_factory import NodeIdentifierFactory
from backend.realtime.live_monitor import LiveMonitor
from backend.store.tree.active_tree_meta import ActiveDisplayTreeMeta
from model.uid import UID
from signal_constants import Signal
from backend.store.tree.root_path_config import RootPathConfigPersister
from util import file_util
from util.ensure import ensure_uid
from util.has_lifecycle import HasLifecycle
from util.root_path_meta import RootPathMeta
from util.stopwatch_sec import Stopwatch

logger = logging.getLogger(__name__)


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

        self._display_tree_dict: Dict[str, ActiveDisplayTreeMeta] = {}
        """Keeps track of which display trees are currently being used in the UI"""

        self._is_live_capture_enabled = backend.get_config('cache.live_capture_enabled')

    def start(self):
        gdrive_live_monitor_enabled = self._is_live_capture_enabled and self._live_monitor.enable_gdrive_polling_thread
        if not gdrive_live_monitor_enabled and not self.backend.cacheman.sync_from_gdrive_on_cache_load:
            logger.warning(f'GDrive: live monitoring is disabled AND sync on cache load is disabled: GDrive cache will not be updated!')

        HasLifecycle.start(self)
        self.connect_dispatch_listener(signal=Signal.DEREGISTER_DISPLAY_TREE, receiver=self._deregister_display_tree)
        self.connect_dispatch_listener(signal=Signal.GDRIVE_RELOADED, receiver=self._on_gdrive_whole_tree_reloaded)
        self.connect_dispatch_listener(signal=Signal.COMPLETE_MERGE, receiver=self._on_merge_requested)

        self._live_monitor.start()

    def shutdown(self):
        logger.debug('ActiveTreeManager.shutdown() entered')
        HasLifecycle.shutdown(self)

        # Do this after destroying controllers, for a more orderly shutdown:
        try:
            if self._live_monitor:
                self._live_monitor.shutdown()
                self._live_monitor = None
        except NameError:
            pass

    # SignalDispatcher callbacks
    # ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼

    def _on_merge_requested(self, sender: str):
        logger.info(f'Received signal: {Signal.COMPLETE_MERGE.name} for tree "{sender}"')

        meta = self.get_active_display_tree_meta(sender)
        if not meta:
            raise RuntimeError(f'Could not find merge tree: {sender}')
        if not meta.change_tree:
            raise RuntimeError(f'Could not find change tree for: {sender}')

        op_list = meta.change_tree.get_ops()
        logger.debug(f'Sending {len(op_list)} ops from tree "{sender}" to cacheman be enqueued')
        self.backend.cacheman.enqueue_op_list(op_list=op_list)

    def _on_gdrive_whole_tree_reloaded(self, sender: str):
        # If GDrive cache was reloaded, our previous selection was almost certainly invalid. Just reset all open GDrive trees to GDrive root.
        logger.info(f'Received signal: "{Signal.GDRIVE_RELOADED.name}"')

        tree_id_list: List[str] = []
        for tree_meta in self._display_tree_dict.values():
            if tree_meta.root_sn.spid.tree_type == TREE_TYPE_GDRIVE:
                tree_id_list.append(tree_meta.tree_id)

        gdrive_root_spid = NodeIdentifierFactory.get_root_constant_gdrive_spid()
        for tree_id in tree_id_list:
            logger.info(f'[{tree_id}] Resetting subtree path to GDrive root')
            request = DisplayTreeRequest(tree_id, spid=gdrive_root_spid, return_async=True)
            self.request_display_tree_ui_state(request)

    def _deregister_display_tree(self, sender: str):
        logger.debug(f'[{sender}] Received signal: "{Signal.DEREGISTER_DISPLAY_TREE.name}"')
        display_tree = self._display_tree_dict.pop(sender, None)
        if display_tree:
            logger.debug(f'[{sender}] Display tree de-registered in backend')
        else:
            logger.debug(f'[{sender}] Could not deregister display tree in backend: it was not found')

        # Also stop live capture, if any
        if self._is_live_capture_enabled and self._live_monitor:
            self._live_monitor.stop_capture(sender)

    # Public methods
    # ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼

    def register_change_tree(self, change_display_tree: ChangeTree, src_tree_id: str):
        logger.info(f'Registering ChangeTree: {change_display_tree.tree_id} (src_tree_id: {src_tree_id})')
        if SUPER_DEBUG:
            change_display_tree.print_tree_contents_debug()
            change_display_tree.print_op_structs_debug()

        filter_state = FilterState.from_config(self.backend, change_display_tree.tree_id)

        meta = ActiveDisplayTreeMeta(self.backend, change_display_tree.state, filter_state)
        meta.change_tree = change_display_tree
        meta.src_tree_id = src_tree_id
        self._display_tree_dict[change_display_tree.tree_id] = meta

        # I suppose we could return the full tree for the thick client, but let's try to sync its behavior of the thin client instead:
        tree_stub = DisplayTree(self.backend, change_display_tree.state)

        # TODO: this is janky and is gonna break
        if src_tree_id:
            sender = src_tree_id
        else:
            sender = change_display_tree.tree_id

        logger.debug(f'Sending signal {Signal.DISPLAY_TREE_CHANGED.name} for tree_id={sender}')
        dispatcher.send(Signal.DISPLAY_TREE_CHANGED, sender=sender, tree=tree_stub)

    def get_active_display_tree_meta(self, tree_id) -> ActiveDisplayTreeMeta:
        return self._display_tree_dict.get(tree_id, None)

    def get_filter_criteria(self, tree_id: str) -> Optional[FilterCriteria]:
        meta = self.get_active_display_tree_meta(tree_id)
        if not meta:
            logger.error(f'get_filter_criteria(): no ActiveDisplayTree found for tree_id "{tree_id}"')
            return None
        return meta.filter_state.filter

    def update_filter_criteria(self, tree_id: str, filter_criteria: FilterCriteria):
        meta = self.get_active_display_tree_meta(tree_id)
        if not meta:
            raise RuntimeError(f'update_filter_criteria(): no ActiveDisplayTree found for tree_id "{tree_id}"')

        # replace FilterState for the given tree
        meta.filter_state = FilterState(filter_criteria, meta.root_sn)
        # write to disk
        meta.filter_state.write_to_config(self.backend, tree_id)

    # TODO: make this wayyyy less complicated by just making each tree_id represent a set of persisted configs. Minimize DisplayTreeRequest
    def request_display_tree_ui_state(self, request: DisplayTreeRequest) -> Optional[DisplayTreeUiState]:
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

        Note: this does not actually load the tree's nodes beyond the root. To do that, the FE must send the
        """
        sender_tree_id = request.tree_id
        spid = request.spid
        logger.debug(f'[{sender_tree_id}] Got request to load display tree (user_path="{request.user_path}", spid={spid}, '
                     f'is_startup={request.is_startup}, tree_display_mode={request.tree_display_mode}')

        root_path_persister = None

        display_tree_meta: ActiveDisplayTreeMeta = self.get_active_display_tree_meta(sender_tree_id)

        # Build RootPathMeta object from params. If neither SPID nor user_path supplied, read from config
        if request.user_path:
            root_path_meta: RootPathMeta = self._resolve_root_meta_from_path(request.user_path)
            spid = root_path_meta.root_spid
        elif spid:
            assert isinstance(spid, SinglePathNodeIdentifier), f'Expected SinglePathNodeIdentifier but got {type(spid)}'
            # params -> root_path_meta
            spid.normalize_paths()
            root_path_meta = RootPathMeta(spid, True)
        elif request.is_startup:
            # root_path_meta -> params
            root_path_persister = RootPathConfigPersister(backend=self.backend, tree_id=sender_tree_id)
            root_path_meta = root_path_persister.read_from_config()
            spid = root_path_meta.root_spid
            if not spid:
                raise RuntimeError(f"Unable to read valid root from config for: '{sender_tree_id}'")
        elif display_tree_meta:
            root_path_meta = RootPathMeta(display_tree_meta.root_sn.spid, display_tree_meta.root_exists, display_tree_meta.offending_path)
            spid = root_path_meta.root_spid
        else:
            raise RuntimeError(f'Invalid args supplied to get_display_tree_ui_state()! (tree_id={sender_tree_id})')

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
                    logger.warning(f'request_display_tree_ui_state(): this is a CategoryDisplayTree. Did you mean to call this method?')
                    return self._return_display_tree_ui_state(sender_tree_id, display_tree_meta, request.return_async)

            elif display_tree_meta.root_sn.spid == root_path_meta.root_spid:
                # Requested the existing tree and root? Just return that:
                logger.debug(f'Display tree already registered with given root; returning existing')
                return self._return_display_tree_ui_state(sender_tree_id, display_tree_meta, request.return_async)

            if display_tree_meta.root_path_config_persister:
                # If we started from a persister, continue persisting:
                root_path_persister = display_tree_meta.root_path_config_persister

        # Try to retrieve the root node from the cache:
        if spid.tree_type == TREE_TYPE_LOCAL_DISK:
            # TODO: hit memory cache instead if available
            node: Node = self.backend.cacheman.read_single_node_from_disk_for_local_path(spid.get_single_path())
            if node:
                if spid.uid != node.uid:
                    logger.warning(f'UID requested ({spid.uid}) does not match UID from cache ({node.uid}); will use value from cache')
                spid = node.node_identifier
                root_path_meta.root_spid = spid

            if os.path.exists(spid.get_single_path()):
                # Override in case something changed since the last shutdown
                root_path_meta.root_exists = True
            else:
                root_path_meta.root_exists = False
            root_path_meta.offending_path = None
        elif spid.tree_type == TREE_TYPE_GDRIVE:
            if spid.uid == GDRIVE_ROOT_UID:
                node: Node = GDriveWholeTree.get_super_root()
            else:
                # TODO: hit memory cache instead if available
                node: Node = self.backend.cacheman.read_single_node_from_disk_for_uid(spid.uid, TREE_TYPE_GDRIVE)
            root_path_meta.root_exists = node is not None
            root_path_meta.offending_path = None
        else:
            raise RuntimeError(f'Unrecognized tree type: {spid.tree_type}')

        # Now that we have the root, we have all the info needed to assemble the ActiveDisplayTreeMeta from the RootPathMeta.
        root_sn = SPIDNodePair(spid, node)

        state = DisplayTreeUiState(response_tree_id, root_sn, root_path_meta.root_exists, root_path_meta.offending_path,
                                   TreeDisplayMode.ONE_TREE_ALL_ITEMS, False)
        if self.backend.cacheman.is_manual_load_required(root_sn.spid, request.is_startup):
            state.needs_manual_load = True

        if display_tree_meta:
            # reuse and update existing
            display_tree_meta.state = state
            assert display_tree_meta.state.tree_id == response_tree_id, f'TreeID "{response_tree_id}" != {display_tree_meta.state.tree_id}'
        else:
            logger.debug(f'[{sender_tree_id}] Reading FilterCriteria from config')
            filter_state = FilterState.from_config(self.backend, sender_tree_id, root_sn)

            display_tree_meta = ActiveDisplayTreeMeta(self.backend, state, filter_state)

            # Store in dict here:
            self._display_tree_dict[response_tree_id] = display_tree_meta

        if root_path_persister:
            # Write updates to config if applicable
            root_path_persister.write_to_config(root_path_meta)

            # Retain the persister for next time:
            display_tree_meta.root_path_config_persister = root_path_persister

        # Update monitoring state
        if self._is_live_capture_enabled:
            if display_tree_meta.root_exists:
                self._live_monitor.start_or_update_capture(display_tree_meta.root_sn.spid, response_tree_id)
            else:
                self._live_monitor.stop_capture(response_tree_id)

        return self._return_display_tree_ui_state(sender_tree_id, display_tree_meta, request.return_async)

    def _return_display_tree_ui_state(self, sender_tree_id, display_tree_meta, return_async: bool) -> Optional[DisplayTreeUiState]:
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

    def _resolve_root_meta_from_path(self, full_path: str) -> RootPathMeta:
        """Resolves the given path into either a local file, a set of Google Drive matches, or generates a GDriveItemNotFoundError,
        and returns a tuple of both"""
        logger.debug(f'resolve_root_from_path() called with path="{full_path}"')
        try:
            full_path = file_util.normalize_path(full_path)
            node_identifier: NodeIdentifier = self.backend.node_identifier_factory.for_values(path_list=full_path)
            if node_identifier.tree_type == TREE_TYPE_GDRIVE:
                # Need to wait until all caches are loaded:
                self.backend.cacheman.wait_for_startup_done()
                # this will load the GDrive master tree if needed:
                identifier_list = self.backend.cacheman.get_gdrive_identifier_list_for_full_path_list(
                    node_identifier.get_path_list(), error_if_not_found=True)
            else:  # LocalNode
                if not os.path.exists(full_path):
                    raise FileNotFoundError(errno.ENOENT, os.strerror(errno.ENOENT), full_path)
                uid = self.backend.cacheman.get_uid_for_local_path(full_path)
                identifier_list = [LocalNodeIdentifier(uid=uid, path_list=full_path)]

            assert len(identifier_list) > 0, f'Got no identifiers for path but no error was raised: {full_path}'
            logger.debug(f'resolve_root_from_path(): got identifier_list={identifier_list}"')
            if len(identifier_list) > 1:
                # Create the appropriate
                candidate_list = []
                for identifier in identifier_list:
                    if identifier.tree_type == TREE_TYPE_GDRIVE:
                        path_to_find = NodeIdentifierFactory.strip_gdrive(full_path)
                    else:
                        path_to_find = full_path

                    if path_to_find in identifier.get_path_list():
                        candidate_list.append(identifier)
                if len(candidate_list) != 1:
                    raise RuntimeError(f'Serious error: found multiple identifiers with same path ({full_path}): {candidate_list}')
                new_root_spid: SinglePathNodeIdentifier = candidate_list[0]
            else:
                new_root_spid = identifier_list[0]

            if len(new_root_spid.get_path_list()) > 0:
                # must have single path
                if new_root_spid.tree_type == TREE_TYPE_GDRIVE:
                    full_path = NodeIdentifierFactory.strip_gdrive(full_path)
                new_root_spid = SinglePathNodeIdentifier(uid=new_root_spid.uid, path_list=full_path, tree_type=new_root_spid.tree_type)

            root_path_meta = RootPathMeta(new_root_spid, root_exists=True)
        except GDriveItemNotFoundError as ginf:
            root_path_meta = RootPathMeta(ginf.node_identifier, root_exists=False)
            root_path_meta.offending_path = ginf.offending_path
        except FileNotFoundError as fnf:
            root = self.backend.node_identifier_factory.for_values(path_list=full_path, must_be_single_path=True)
            root_path_meta = RootPathMeta(root, root_exists=False)
        except CacheNotLoadedError as cnlf:
            root = self.backend.node_identifier_factory.for_values(path_list=full_path, uid=NULL_UID, must_be_single_path=True)
            root_path_meta = RootPathMeta(root, root_exists=False)

        logger.debug(f'resolve_root_from_path(): returning new_root={root_path_meta}"')
        return root_path_meta

    # Expanded & selected row state tracking
    # ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼

    def get_rows_of_interest(self, tree_id: str) -> RowsOfInterest:
        logger.debug(f'[{tree_id}] Getting rows of interest')

        meta = self.get_active_display_tree_meta(tree_id)
        if not meta:
            raise RuntimeError(f'get_rows_of_interest(): DisplayTree not registered: {tree_id}')

        rows_of_interest = RowsOfInterest()
        rows_of_interest.expanded = meta.expanded_rows
        rows_of_interest.selected = meta.selected_rows
        logger.debug(f'[{tree_id}] get_rows_of_interest(): returning {meta.expanded_rows} expanded & {meta.selected_rows} selected')
        return rows_of_interest

    def load_rows_of_interest(self, tree_id: str):
        logger.debug(f'[{tree_id}] Loading rows of interest')

        meta = self.get_active_display_tree_meta(tree_id)
        if not meta:
            raise RuntimeError(f'load_rows_of_interest(): DisplayTree not registered: {tree_id}')

        # NOTE: the purge process will actually end up populating the expanded_rows in the display_tree_meta, but we will just overwrite it
        expanded_rows = self._load_expanded_rows_from_config(meta.tree_id)
        selected_rows = self._load_selected_rows_from_config(meta.tree_id)
        rows_of_interest = self._purge_dead_rows(expanded_rows, selected_rows, meta)
        meta.expanded_rows = rows_of_interest.expanded
        meta.selected_rows = rows_of_interest.selected

    def set_selected_rows(self, tree_id: str, selected: Set[UID]):
        display_tree_meta: ActiveDisplayTreeMeta = self.get_active_display_tree_meta(tree_id)
        if not display_tree_meta:
            raise RuntimeError(f'Tree not found in memory: {tree_id}')

        logger.debug(f'[{tree_id}] Storing selection: {selected}')
        display_tree_meta.selected_rows = selected
        # TODO: use a timer for this
        self._save_selected_rows_to_config(display_tree_meta)

    def add_expanded_row(self, row_uid: UID, tree_id: str):
        """AKA expanding a row on the frontend"""
        display_tree_meta: ActiveDisplayTreeMeta = self.get_active_display_tree_meta(tree_id)
        if not display_tree_meta:
            raise RuntimeError(f'Tree not found in memory: {tree_id}')

        if display_tree_meta.root_sn.spid.uid == row_uid:
            # ignore root
            return

        display_tree_meta.expanded_rows.add(row_uid)
        # TODO: use a timer for this. Also write selection to file
        self._save_expanded_rows_to_config(display_tree_meta)

    def remove_expanded_row(self, row_uid: UID, tree_id: str):
        """AKA collapsing a row on the frontend"""
        # TODO: remove descendants
        display_tree_meta: ActiveDisplayTreeMeta = self.get_active_display_tree_meta(tree_id)
        if not display_tree_meta:
            raise RuntimeError(f'Tree not found in memory: {tree_id}')

        display_tree_meta.expanded_rows.remove(row_uid)
        # TODO: use a timer for this. Also write selection to file
        self._save_expanded_rows_to_config(display_tree_meta)

    def _load_expanded_rows_from_config(self, tree_id: str) -> Set[UID]:
        """Loads the Set of expanded rows from config file"""
        logger.debug(f'[{tree_id}] Loading expanded rows from config')
        try:
            expanded_rows: Set[UID] = set()
            expanded_rows_str: Optional[str] = self.backend.get_config(ActiveTreeManager._make_expanded_rows_config_key(tree_id))
            if expanded_rows_str:
                for uid in expanded_rows_str.split(CONFIG_DELIMITER):
                    expanded_rows.add(ensure_uid(uid))
            return expanded_rows
        except RuntimeError:
            logger.exception(f'[{tree_id}] Failed to load expanded rows from config')

    def _save_selected_rows_to_config(self, display_tree_meta: ActiveDisplayTreeMeta):
        selected_rows_str: str = CONFIG_DELIMITER.join(str(uid) for uid in display_tree_meta.selected_rows)
        self.backend.put_config(ActiveTreeManager._make_selected_rows_config_key(display_tree_meta.tree_id), selected_rows_str)

    @staticmethod
    def _make_expanded_rows_config_key(tree_id: str) -> str:
        return f'ui_state.{tree_id}.expanded_rows'

    def _load_selected_rows_from_config(self, tree_id: str) -> Set[UID]:
        """Loads the Set of selected rows from config file"""
        logger.debug(f'[{tree_id}] Loading selected rows from config')
        try:
            selected_rows: Set[UID] = set()
            selected_rows_str: Optional[str] = self.backend.get_config(ActiveTreeManager._make_selected_rows_config_key(tree_id))
            if selected_rows_str:
                for uid in selected_rows_str.split(CONFIG_DELIMITER):
                    selected_rows.add(ensure_uid(uid))
            return selected_rows
        except RuntimeError:
            logger.exception(f'[{tree_id}] Failed to load expanded rows from config')

    def _save_expanded_rows_to_config(self, display_tree_meta: ActiveDisplayTreeMeta):
        expanded_rows_str: str = CONFIG_DELIMITER.join(str(uid) for uid in display_tree_meta.expanded_rows)
        self.backend.put_config(ActiveTreeManager._make_expanded_rows_config_key(display_tree_meta.tree_id), expanded_rows_str)

    @staticmethod
    def _make_selected_rows_config_key(tree_id: str) -> str:
        return f'ui_state.{tree_id}.selected_rows'

    def _purge_dead_rows(self, expanded_cached: Set[UID], selected_cached: Set[UID], display_tree_meta: ActiveDisplayTreeMeta) -> RowsOfInterest:
        verified = RowsOfInterest()

        if not display_tree_meta.root_exists:
            verified.expanded = expanded_cached
            verified.selected = selected_cached
            return verified

        stopwatch = Stopwatch()

        processing_queue: Deque[Node] = deque()

        for node in self.backend.get_child_list(parent_uid=display_tree_meta.state.root_sn.node.uid, tree_id=display_tree_meta.tree_id):
            processing_queue.append(node)

        while len(processing_queue) > 0:
            node: Node = processing_queue.popleft()
            if node.uid in selected_cached:
                verified.selected.add(node.uid)

            if node.uid in expanded_cached:
                verified.expanded.add(node.uid)
                for node in self.backend.get_child_list(parent_uid=node.uid, tree_id=display_tree_meta.tree_id):
                    processing_queue.append(node)

        logger.debug(f'[{display_tree_meta.tree_id}] {stopwatch} Verified {len(verified.expanded)} of {len(expanded_cached)} expanded rows '
                     f'and {len(verified.selected)} of {len(selected_cached)} selected')
        return verified
