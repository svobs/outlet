import errno
import logging
import os
from typing import Dict, List, Optional

from pydispatch import dispatcher

from constants import GDRIVE_ROOT_UID, SUPER_DEBUG, TREE_TYPE_GDRIVE, TREE_TYPE_LOCAL_DISK, TreeDisplayMode
from error import CacheNotLoadedError, GDriveItemNotFoundError
from model.display_tree.build_struct import DisplayTreeRequest
from store.tree.change_display_tree import ChangeDisplayTree
from model.display_tree.display_tree import DisplayTree, DisplayTreeUiState
from store.gdrive.gdrive_whole_tree import GDriveWholeTree
from model.node.node import Node, SPIDNodePair
from model.node_identifier import LocalNodeIdentifier, NodeIdentifier, SinglePathNodeIdentifier
from model.node_identifier_factory import NodeIdentifierFactory
from realtime.live_monitor import LiveMonitor
from store.tree.active_tree_meta import ActiveDisplayTreeMeta
from signal_constants import Signal
from store.tree.root_path_config import RootPathConfigPersister
from util import file_util
from util.has_lifecycle import HasLifecycle
from util.root_path_meta import RootPathMeta

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

        self._is_live_capture_enabled = backend.config.get('cache.live_capture_enabled')

    def start(self):
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

        gdrive_root_spid = NodeIdentifierFactory.get_gdrive_root_constant_single_path_identifier()
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

    def register_change_tree(self, change_display_tree: ChangeDisplayTree, src_tree_id: str):
        logger.info(f'Registering ChangeDisplayTree: {change_display_tree.tree_id} (src_tree_id: {src_tree_id})')
        if SUPER_DEBUG:
            change_display_tree.print_tree_contents_debug()
            change_display_tree.print_op_structs_debug()

        meta = ActiveDisplayTreeMeta(self.backend, change_display_tree.state)
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

    def request_display_tree_ui_state(self, request: DisplayTreeRequest) -> Optional[DisplayTreeUiState]:
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
                    display_tree_meta: ActiveDisplayTreeMeta = self.get_active_display_tree_meta(response_tree_id)

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
            display_tree_meta = ActiveDisplayTreeMeta(self.backend, state)
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
            logger.debug(f'[{sender_tree_id}] Firing signal: {Signal.DISPLAY_TREE_CHANGED}')
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
