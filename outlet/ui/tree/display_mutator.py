import collections
import logging
import os
import threading
from datetime import datetime
from typing import Deque, Iterable, List, Optional

import humanfriendly
from pydispatch import dispatcher

from constants import STATS_REFRESH_HOLDOFF_TIME_MS, IconId, LARGE_NUMBER_OF_CHILDREN, SUPER_DEBUG
from error import GDriveItemNotFoundError
from model.display_tree.display_tree import DisplayTree
from model.node.container_node import CategoryNode, ContainerNode
from model.node.ephemeral_node import EmptyNode, LoadingNode
from model.node.node import Node, SPIDNodePair
from model.node_identifier import SinglePathNodeIdentifier
from model.uid import UID
from ui.signal import Signal
from ui.tree.filter_criteria import FilterCriteria
from util.has_lifecycle import HasLifecycle
from util.holdoff_timer import HoldOffTimer

from util.root_path_meta import RootPathMeta

import gi
gi.require_version("Gtk", "3.0")
from gi.repository import GLib, Gtk
from gi.repository.Gtk import TreeIter

logger = logging.getLogger(__name__)


class DisplayMutator(HasLifecycle):
    """
    ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
    CLASS DisplayMutator

    CacheManager --> DisplayTree --> DisplayMutator --> DisplayStore (TreeModel)
    TODO: when does the number of display nodes start to slow down? -> add config for live node maximum
    ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼
    """
    def __init__(self, config, controller=None):
        HasLifecycle.__init__(self)
        self.con = controller
        self.use_empty_nodes = True
        self._lock = threading.Lock()
        self._stats_refresh_timer = HoldOffTimer(holdoff_time_ms=STATS_REFRESH_HOLDOFF_TIME_MS, task_func=self._request_subtree_stats_refresh)
        """Stats for the entire subtree are all connected to each other, so this is a big task. This timer allows us to throttle its frequency"""

        self._enable_expand_state_listeners = True
        """When true, listen and automatically update the display when a node is expanded or contracted.
        Needs to be set to false when modifying the model with certain operations, because leaving enabled would
        result in an infinite loop"""

        self._enable_node_signals = False
        """Set this initially to False because we have no data, and there are too many ways to end up blocking."""

        self._is_shutdown = False

    def start(self):
        """Do post-wiring stuff like connect listeners."""
        HasLifecycle.start(self)

        self.use_empty_nodes = self.con.config.get('display.diff_tree.use_empty_nodes')
        self._connect_node_listeners()
        logger.debug(f'[{self.con.tree_id}] DisplayMutator started')

    def _connect_node_listeners(self):
        tree_id = self.con.tree_id

        if self.con.treeview_meta.lazy_load:
            self.connect_dispatch_listener(signal=Signal.NODE_EXPANSION_TOGGLED, receiver=self._on_node_expansion_toggled)

        self.connect_dispatch_listener(signal=Signal.REFRESH_SUBTREE_STATS_DONE, receiver=self._on_refresh_stats_done)
        """This signal comes from the cacheman after it has finished updating all the nodes in the subtree,
        notfiying us that we can now refresh our display from it"""

        self.connect_dispatch_listener(signal=Signal.NODE_UPSERTED, receiver=self._on_node_upserted_in_cache)
        self.connect_dispatch_listener(signal=Signal.NODE_REMOVED, receiver=self._on_node_removed_from_cache)
        self.connect_dispatch_listener(signal=Signal.NODE_MOVED, receiver=self._on_node_moved_in_cache)

        # FIXME: figure out why the 'sender' arg fails when relayed from gRPC!
        self.connect_dispatch_listener(signal=Signal.LOAD_SUBTREE_DONE, receiver=self._populate_ui_tree_async)
        self.connect_dispatch_listener(signal=Signal.FILTER_UI_TREE, receiver=self._on_filter_ui_tree_requested)
        self.connect_dispatch_listener(signal=Signal.EXPAND_ALL, receiver=self._on_expand_all_requested)
        self.connect_dispatch_listener(signal=Signal.EXPAND_AND_SELECT_NODE, receiver=self._expand_and_select_node)

        logger.debug(f'[{tree_id}] DisplayMutator listeners connected')

    def shutdown(self):
        self._is_shutdown = True
        HasLifecycle.shutdown(self)
        self.con = None

    def _expand_and_select_node(self, sender: str, nid: SinglePathNodeIdentifier):
        if sender == self.con.tree_id:
            logger.debug(f'[{self.con.tree_id}] Got signal: "{Signal.EXPAND_AND_SELECT_NODE.name}"')
            self.expand_and_select_node(nid)

    def _on_expand_all_requested(self, sender, tree_path):
        if sender == self.con.tree_id:
            self.expand_all(tree_path)

    def _on_filter_ui_tree_requested(self, sender: str, filter_criteria: FilterCriteria):
        if sender == self.con.tree_id:
            self.filter_tree(filter_criteria)

    def _populate_ui_tree_async(self, sender):
        """Just populates the tree with nodes. Executed asyncly via Signal.LOAD_SUBTREE_DONE"""
        if sender == self.con.tree_id:
            logger.debug(f'[{self.con.tree_id}] Got signal: "{Signal.LOAD_SUBTREE_DONE.name}". Sending signal "{Signal.ENQUEUE_UI_TASK.name}"')
            dispatcher.send(signal=Signal.ENQUEUE_UI_TASK, sender=sender, task_func=self.populate_root)

    def _populate_recursively(self, parent_iter, node: Node, node_count: int = 0) -> int:
        # Do a DFS of the change tree and populate the UI tree along the way
        if node.is_dir():
            parent_iter = self._append_dir_node(parent_iter=parent_iter, node=node)

            for child in self.con.get_tree().get_children(node, self.con.treeview_meta.filter_criteria):
                node_count = self._populate_recursively(parent_iter, child, node_count)
        else:
            self._append_file_node(parent_iter, node)

        node_count += 1
        return node_count

    def _populate_and_restore_expanded_state(self, parent_iter, node: Node, node_count: int, to_expand: List[UID]) -> int:
        # Do a DFS of the change tree and populate the UI tree along the way
        if node.is_dir():
            is_expand = False

            if type(node) == CategoryNode and self.con.treeview_meta.is_display_persisted and self.con.treeview_meta.is_category_node_expanded(node):
                logger.debug(f'[{self.con.tree_id}] Category node {node.name} is expanded"')
                is_expand = True
            elif node.uid in self.con.display_store.expanded_rows:
                if SUPER_DEBUG:
                    logger.debug(f'[{self.con.tree_id}] Found UID {node.uid} in expanded_rows"')
                is_expand = True

            child_list = self.con.get_tree().get_children(node, self.con.treeview_meta.filter_criteria)

            if is_expand:
                # Append all child nodes and recurse to possibly expand more:
                parent_iter = self._append_dir_node(parent_iter=parent_iter, node=node)

                if SUPER_DEBUG:
                    logger.debug(f'[{self.con.tree_id}] Row will be expanded: {node.uid} ("{node.name}")')
                to_expand.append(node.uid)

                for child in child_list:
                    node_count = self._populate_and_restore_expanded_state(parent_iter, child, node_count, to_expand)

            else:
                if SUPER_DEBUG:
                    logger.debug(f'[{self.con.tree_id}] Node {node.uid} ("{node.name}") is not expanded')
                self._append_dir_node_and_loading_child(parent_iter, node)
        else:
            self._append_file_node(parent_iter, node)

        node_count += 1
        return node_count

    def _expand_row_without_event_firing(self, tree_path, expand_all):
        assert self.con.treeview_meta.lazy_load
        tree_path = self.con.display_store.ensure_tree_path(tree_path)

        self._enable_expand_state_listeners = False
        try:
            self.con.tree_view.expand_row(path=tree_path, open_all=expand_all)
        finally:
            self._enable_expand_state_listeners = True

    def select_uid(self, uid):
        tree_iter = self.con.display_store.find_uid_in_tree(uid, None)
        if not tree_iter:
            logger.info(f'[{self.con.tree_id}] Could not select node: could not find node in tree for UID {uid}')
            return
        tree_view_selection: Gtk.TreeSelection = self.con.tree_view.get_selection()
        # tree_view_selection.unselect_all()
        tree_view_selection.select_iter(tree_iter)
        tree_path = self.con.display_store.model.get_path(tree_iter)
        self.con.tree_view.scroll_to_cell(path=tree_path, column=None, use_align=True, row_align=0.5, col_align=0)

    def expand_and_select_node(self, selection: SinglePathNodeIdentifier):
        assert isinstance(selection, SinglePathNodeIdentifier), f'Expected instance of SinglePathNodeIdentifier but got: {type(selection)}'

        def do_in_ui():
            with self._lock:
                ancestor_list: Iterable[Node] = self.con.get_tree().get_ancestor_list(selection)

                # Expand all ancestors one by one:
                tree_iter = None
                for ancestor in ancestor_list:
                    tree_iter = self.con.display_store.find_uid_in_tree(ancestor.uid, tree_iter)
                    if not tree_iter:
                        logger.error(f'[{self.con.tree_id}] Could not expand ancestor node: could not find node in tree for: {ancestor}')
                        return
                    tree_path = self.con.display_store.model.get_path(tree_iter)
                    if not self.con.tree_view.row_expanded(tree_path):
                        self._expand_subtree(tree_path, expand_all=False)

                # Now select target node:
                self.select_uid(selection.uid)

        GLib.idle_add(do_in_ui)

    def expand_all(self, tree_path):
        def expand_me(tree_iter):
            self._expand_subtree(tree_iter, expand_all=True)

        with self._lock:
            self.con.display_store.do_for_self_and_descendants(tree_path=tree_path, action_func=expand_me)

    def _expand_subtree(self, tree_iter: Gtk.TreeIter, expand_all: bool):
        # convert tree_iter to path:
        tree_path = self.con.display_store.ensure_tree_path(tree_iter)

        if not self.con.treeview_meta.lazy_load:
            # We loaded all at once? -> we are already populated. just expand UI:
            self.con.tree_view.expand_row(path=tree_path, open_all=expand_all)
            return

        if self.con.tree_view.row_expanded(tree_path):
            return

        node = self.con.display_store.get_node_data(tree_path)
        parent_iter = self.con.display_store.model.get_iter(tree_path)
        self.con.display_store.remove_loading_node(parent_iter)
        children: List[Node] = self.con.get_tree().get_children(node, self.con.treeview_meta.filter_criteria)

        if expand_all:
            # populate all descendants
            node_count = 0
            for child in children:
                node_count = self._populate_recursively(parent_iter, child, node_count)
            logger.debug(f'[{self.con.tree_id}] Populated {node_count} nodes')
        else:
            # populate only children
            self._append_children(children=children, parent_iter=parent_iter)

        self._expand_row_without_event_firing(tree_path=tree_path, expand_all=expand_all)

    def filter_tree(self, filter_criteria: FilterCriteria):
        if filter_criteria:
            self.con.treeview_meta.filter_criteria = filter_criteria
        else:
            self.con.treeview_meta.filter_criteria = None

        self.populate_root()

    def populate_root(self):
        """START HERE.
        More like "repopulate" - clears model before populating.
        Draws from the undelying data store as needed, to populate the display store."""
        logger.debug(f'[{self.con.tree_id}] Entered populate_root(): expanded_rows={self.con.display_store.expanded_rows}')

        # FIXME: jesus this is nasty
        # This may be a long task
        try:
            # Lock this so that node-upserted and node-removed callbacks don't interfere
            self._enable_node_signals = False
            with self._lock:
                top_level_node_list: List[Node] = self.con.get_tree().get_children_for_root(self.con.treeview_meta.filter_criteria)
            logger.debug(f'[{self.con.tree_id}] populate_root(): got {len(top_level_node_list)} top_level_node_list for root')
        except GDriveItemNotFoundError as err:
            # Not found: signal error to UI and cancel
            logger.warning(f'[{self.con.tree_id}] Could not populate root: GDrive node not found: {self.con.get_tree().get_root_identifier()}')
            new_root_meta = RootPathMeta(self.con.get_tree().get_root_identifier(), root_exists=False)
            new_root_meta.offending_path = err.offending_path
            logger.debug(f'[{self.con.tree_id}] Sending signal: "{Signal.ROOT_PATH_UPDATED}" with new_root_meta={new_root_meta}')
            dispatcher.send(signal=Signal.ROOT_PATH_UPDATED, sender=self.con.tree_id, new_root_meta=new_root_meta, err=err)
        finally:
            self._enable_node_signals = True

        def update_ui():
            with self._lock:
                # retain selection (if any)
                prev_selection: List[Node] = self.con.display_store.get_multiple_selection()

                # Wipe out existing items:
                root_iter = self.con.display_store.clear_model()
                node_count = 0

                if self.con.treeview_meta.filter_criteria and self.con.treeview_meta.filter_criteria.has_criteria()\
                        and not self.con.treeview_meta.filter_criteria.show_subtrees_of_matches:
                    # not lazy: just one big list
                    if len(top_level_node_list) > LARGE_NUMBER_OF_CHILDREN:
                        logger.error(f'[{self.con.tree_id}] Too many top-level nodes to display! Count = {len(top_level_node_list)}')
                        self._append_empty_child(root_iter, f'ERROR: too many items to display ({len(top_level_node_list):n})', IconId.ICON_ALERT)
                    else:
                        logger.debug(f'[{self.con.tree_id}] Populating {len(top_level_node_list)} linear list of nodes for filter criteria')
                        for node in top_level_node_list:
                            self._append_file_node(root_iter, node)

                        logger.debug(f'[{self.con.tree_id}] Done populating linear list of nodes')

                elif self.con.treeview_meta.lazy_load:
                    # Recursively add child nodes for dir nodes which need expanding. We can only expand after we have nodes, due to GTK3 limitation
                    to_expand: List[UID] = []
                    for node in top_level_node_list:
                        self._populate_and_restore_expanded_state(root_iter, node, node_count, to_expand)

                    for uid in to_expand:
                        logger.debug(f'[{self.con.tree_id}] Expanding: {uid}')
                        tree_iter = self.con.display_store.find_uid_in_tree(uid)
                        self._expand_row_without_event_firing(tree_path=tree_iter, expand_all=False)

                    logger.debug(f'[{self.con.tree_id}] Populated {node_count} nodes and expanded {len(to_expand)} dir nodes')

                else:
                    # NOT lazy: load all at once, expand all
                    for node in top_level_node_list:
                        node_count = self._populate_recursively(None, node, node_count)

                    logger.debug(f'[{self.con.tree_id}] Populated {node_count} nodes')

                    # Expand all dirs:
                    assert not self.con.treeview_meta.lazy_load
                    self.con.tree_view.expand_all()

                for prev_node in prev_selection:
                    self.select_uid(prev_node.uid)

            dispatcher.send(signal=Signal.POPULATE_UI_TREE_DONE, sender=self.con.tree_id)

        GLib.idle_add(update_ui)

        self._request_subtree_stats_refresh()

    def get_checked_rows_as_list(self) -> List[SPIDNodePair]:
        """Returns a list which contains the DisplayNodes of the items which are currently checked by the user
        (including collapsed rows). This will be a subset of the DisplayTree which was used to
        populate this tree. Includes file nodes only, with the exception of GDrive FolderToAdd."""
        # subtree root will be the same as the current subtree's
        checked_items: List[SPIDNodePair] = []

        # Algorithm:
        # Iterate over display nodes. Start with top-level nodes.
        # - Add each checked row to DFS queue (whitelist). It and all of its descendants will be added
        # - Ignore each unchecked row
        # - Each inconsistent row needs to be drilled down into.
        whitelist: Deque[SPIDNodePair] = collections.deque()
        secondary_screening: Deque[SPIDNodePair] = collections.deque()

        with self._lock:
            assert self.con.treeview_meta.has_checkboxes
            child_list: Iterable[SPIDNodePair] = self.con.get_tree().get_child_sn_list_for_root()
            for child_sn in child_list:
                if self.con.display_store.checked_rows.get(child_sn.node.identifier, None):
                    whitelist.append(child_sn)
                elif self.con.display_store.inconsistent_rows.get(child_sn.node.identifier, None):
                    secondary_screening.append(child_sn)

            while len(secondary_screening) > 0:
                parent: SPIDNodePair = secondary_screening.popleft()
                assert parent.node.is_dir(), f'Expected a dir-type node: {parent}'

                if not parent.node.is_display_only() and not parent.node.is_live():
                    # Even an inconsistent FolderToAdd must be included as a checked item:
                    checked_items.append(parent)

                for child_sn in self.con.get_tree().get_child_sn_list(parent):
                    if self.con.display_store.checked_rows.get(child_sn.node.identifier, None):
                        whitelist.append(child_sn)
                    elif self.con.display_store.inconsistent_rows.get(child_sn.node.identifier, None):
                        secondary_screening.append(child_sn)

            while len(whitelist) > 0:
                chosen_sn: SPIDNodePair = whitelist.popleft()
                # all files and all non-existent dirs must be added
                if not parent.node.is_display_only() and not chosen_sn.node.is_dir() or not chosen_sn.node.is_live():
                    checked_items.append(chosen_sn)

                # drill down into all descendants of nodes in the whitelist
                for child_sn in self.con.get_tree().get_child_sn_list(chosen_sn):
                    whitelist.append(child_sn)

            return checked_items

    # LISTENERS begin
    # ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼

    def _on_node_expansion_toggled(self, sender: str, parent_iter: Gtk.TreeIter, parent_path, node: Node, is_expanded: bool) -> None:
        # Callback for Signal.NODE_EXPANSION_TOGGLED:
        if sender != self.con.tree_id:
            return

        assert self.con.treeview_meta.lazy_load
        logger.debug(f'[{self.con.tree_id}] Node expansion toggled to {is_expanded} for {node}"')

        # Still want to keep track of which nodes are expanded:
        if is_expanded:
            logger.debug(f'[{self.con.tree_id}] Added UID {node.uid} to expanded_rows"')
            self.con.display_store.expanded_rows.add(node.uid)
        else:
            def remove_from_expanded_rows(tree_iter: Gtk.TreeIter):
                n = self.con.display_store.get_node_data(tree_iter)
                if n.is_dir():
                    self.con.display_store.expanded_rows.discard(n.uid)
                    logger.debug(f'[{self.con.tree_id}] Removed UID {n.uid} from expanded_rows')

            self.con.display_store.do_for_self_and_descendants(parent_path, remove_from_expanded_rows)

        # TODO: use a timer for this. Make into backend API. Also write selection to file
        self.con.display_store.save_expanded_rows_to_config()

        if not self._enable_expand_state_listeners or not self._enable_node_signals:
            if SUPER_DEBUG:
                logger.debug(f'[{self.con.tree_id}] Ignoring signal "{Signal.NODE_EXPANSION_TOGGLED.name}": listeners disabled')
            return

        def expand_or_contract():
            with self._lock:
                # Add children for node:
                if is_expanded:
                    self.con.display_store.remove_loading_node(parent_iter)

                    children = self.con.get_tree().get_children(node, self.con.treeview_meta.filter_criteria)
                    self._append_children(children=children, parent_iter=parent_iter)

                    # Need to call this because removing the Loading node leaves the parent with no children,
                    # and due to a deficiency in GTK this causes the parent to become collapsed again.
                    # So we must wait until after the children have been added, and then disable listeners from firing
                    # and set it to expanded again.
                    self._expand_row_without_event_firing(tree_path=parent_path, expand_all=False)
                else:
                    # Collapsed:
                    self.con.display_store.remove_all_children(parent_iter)
                    # Always have at least a dummy node:
                    logger.debug(f'Collapsing tree: adding loading node')
                    self._append_loading_child(parent_iter)

                logger.debug(f'[{self.con.tree_id}] Displayed rows count: {len(self.con.display_store.displayed_rows)}')
                dispatcher.send(signal=Signal.NODE_EXPANSION_DONE, sender=self.con.tree_id)
        GLib.idle_add(expand_or_contract)

    def _update_or_append(self, node: Node, parent_iter, child_iter):
        if child_iter:
            # Node is update:
            logger.debug(f'[{self.con.tree_id}] Node already exists in tree (uid={node.uid}): doing an update instead')
            display_vals: list = self.generate_display_cols(parent_iter, node)
            for col, val in enumerate(display_vals):
                self.con.display_store.model.set_value(child_iter, col, val)
        else:
            logger.debug(f'[{self.con.tree_id}] Appending new node (uid={node.uid}, is_dir={node.is_dir()})')
            # Node is new
            if node.is_dir():
                self._append_dir_node_and_loading_child(parent_iter, node)
            else:
                self._append_file_node(parent_iter, node)

    def _on_node_upserted_in_cache(self, sender: str, node: Node) -> None:
        if SUPER_DEBUG:
            logger.debug(f'[{self.con.tree_id}] Entered _on_node_upserted_in_cache(): sender={sender}, node={node}')
        assert node is not None

        if not self._enable_node_signals:
            if SUPER_DEBUG:
                logger.debug(f'[{self.con.tree_id}] Ignoring signal "{Signal.NODE_UPSERTED.name}": node listeners disabled')
            return

        # Possibly long-running op to load lazy tree. Also has a nasty lock. Do this outside the UI thread.
        tree: DisplayTree = self.con.get_tree()

        def update_ui():
            with self._lock:
                parent_uid_list: List[UID] = node.get_parent_uids()

                # Often we want to refresh the stats, even if the node is not displayed, because it can affect other parts of the tree:
                needs_refresh = True

                if parent_uid_list:
                    if logger.isEnabledFor(logging.DEBUG):
                        logger.debug(f'[{self.con.tree_id}] Received signal {Signal.NODE_UPSERTED.name} for node {node.node_identifier} '
                                     f'with parents {parent_uid_list}')

                    for parent_uid in parent_uid_list:
                        if SUPER_DEBUG:
                            logger.debug(f'[{self.con.tree_id}] Examining parent {parent_uid} for displayed node {node.node_identifier}')

                        if self.con.get_tree().get_root_identifier().uid == parent_uid:
                            logger.debug(f'[{self.con.tree_id}] Node is topmost level: {node.node_identifier}')
                            parent_iter = None
                            child_iter = self.con.display_store.find_uid_in_children(node.uid, parent_iter)
                            self._update_or_append(node, parent_iter, child_iter)
                        else:
                            # Node is not topmost.
                            logger.debug(f'[{self.con.tree_id}] Node is not topmost: {node.node_identifier}')
                            parent_iter = self.con.display_store.find_uid_in_tree(target_uid=parent_uid)
                            if parent_iter:
                                parent_tree_path = self.con.display_store.model.get_path(parent_iter)
                                if self.con.tree_view.row_expanded(parent_tree_path):
                                    # Parent is present and expanded. Now check whether the "upserted node" already exists:
                                    child_iter = self.con.display_store.find_uid_in_children(node.uid, parent_iter)
                                    self._update_or_append(node, parent_iter, child_iter)
                                else:
                                    # Parent present but not expanded. Make sure it has a loading node (which allows child toggle):
                                    if self.con.display_store.model.iter_has_child(parent_iter):
                                        logger.debug(f'[{self.con.tree_id}] Will not upsert node {node.uid}: Parent is not expanded: {parent_uid}')
                                    else:
                                        # May have added a child to formerly childless parent: add loading node
                                        logger.debug(f'[{self.con.tree_id}] Parent ({parent_uid}) is not expanded; adding loading node')
                                        self._append_loading_child(parent_iter)
                            else:
                                # Not even parent is displayed. Probably an ancestor isn't expanded. Just skip
                                assert parent_uid not in self.con.display_store.displayed_rows, \
                                    f'DisplayedRows ({self.con.display_store.displayed_rows}) contains UID ({parent_uid})!'
                                logger.debug(f'[{self.con.tree_id}] Will not upsert node: Could not find parent node in display tree: {parent_uid}')

                else:
                    # No parent found in tree
                    if node.uid in self.con.display_store.displayed_rows:
                        logger.debug(f'[{self.con.tree_id}] Received signal {Signal.NODE_UPSERTED.name} for node {node.node_identifier} '
                                     f'but its parent is no longer in the tree; removing node from display store: {node.uid}')
                        self.con.display_store.remove_node(node.uid)
                    elif tree.is_path_in_subtree(node.get_path_list()):
                        # At least in subtree? If so, refresh stats to reflect change
                        logger.debug(f'[{self.con.tree_id}] Received signal {Signal.NODE_UPSERTED.name} for node {node.node_identifier}')
                    else:
                        needs_refresh = False
                        if logger.isEnabledFor(logging.DEBUG):
                            logger.debug(f'[{self.con.tree_id}] Ignoring signal {Signal.NODE_UPSERTED.name} for node {node.node_identifier}')

                if needs_refresh:
                    self._stats_refresh_timer.start_or_delay()

        GLib.idle_add(update_ui)

    def _on_node_removed_from_cache(self, sender: str, node: Node):
        if SUPER_DEBUG:
            logger.debug(f'[{self.con.tree_id}] Entered _on_node_removed_from_cache(): sender={sender}, node={node}')

        if not self._enable_node_signals:
            if SUPER_DEBUG:
                logger.debug(f'[{self.con.tree_id}] Ignoring signal "{Signal.NODE_REMOVED.name}": node listeners disabled')
            return

        assert node

        def update_ui():
            with self._lock:
                displayed_item = self.con.display_store.displayed_rows.get(node.uid, None)

                if displayed_item:
                    if logger.isEnabledFor(logging.DEBUG):
                        logger.debug(f'[{self.con.tree_id}] Received signal {Signal.NODE_REMOVED.name} for displayed node {node.node_identifier}')

                    stats_refresh_needed = True

                    logger.debug(f'[{self.con.tree_id}] Removing node from display store: {displayed_item.uid}')
                    self.con.display_store.remove_node(node.uid)
                    logger.debug(f'[{self.con.tree_id}] Node removed: {displayed_item.uid}')
                elif self.con.get_tree().is_path_in_subtree(node.get_path_list()):
                    if logger.isEnabledFor(logging.DEBUG):
                        logger.debug(f'[{self.con.tree_id}] Received signal {Signal.NODE_REMOVED.name} for node {node.node_identifier}')

                    stats_refresh_needed = True
                else:
                    if logger.isEnabledFor(logging.DEBUG):
                        logger.debug(f'[{self.con.tree_id}] Ignoring signal {Signal.NODE_REMOVED.name} for node {node.node_identifier}')
                    stats_refresh_needed = False

                if stats_refresh_needed:
                    self._stats_refresh_timer.start_or_delay()

        GLib.idle_add(update_ui)

    def _on_node_moved_in_cache(self, sender: str, src_node: Node, dst_node: Node):
        if not self._enable_node_signals:
            if SUPER_DEBUG:
                logger.debug(f'[{self.con.tree_id}] Ignoring signal "{Signal.NODE_MOVED.name}": node listeners disabled')
            return
            
        self._on_node_removed_from_cache(sender, src_node)
        self._on_node_upserted_in_cache(sender, dst_node)

    def _request_subtree_stats_refresh(self):
        # Requests the cacheman to recalculate stats for this subtree. Sends Signal.REFRESH_SUBTREE_STATS_DONE when done
        logger.debug(f'[{self.con.tree_id}] Requesting subtree stats refresh')
        self.con.app.backend.enqueue_refresh_subtree_stats_task(root_uid=self.con.get_tree().root_uid, tree_id=self.con.tree_id)

    def _on_refresh_stats_done(self, sender: str):
        """Should be called after the parent tree has had its stats refreshed. This will update all the displayed nodes
        with the current values from the cache."""
        if sender != self.con.tree_id:
            return
        logger.debug(f'[{self.con.tree_id}] Got signal: "{Signal.REFRESH_SUBTREE_STATS_DONE.name}"')

        def redraw_displayed_node(tree_iter):
            if self._is_shutdown:
                # try to prevent junk errors during shutdown
                return

            ds = self.con.display_store
            node: Node = ds.get_node_data(tree_iter)
            if not node:
                par_iter = ds.model.iter_parent(tree_iter)
                par_node: Node = ds.get_node_data(par_iter)
                logger.error(f'[{self.con.tree_id}] No node for child of {par_node}')
                return
            assert node, f'For tree_id="{sender} and row={self.con.display_store.model[tree_iter]}'
            if node.is_ephemereal():
                return

            if SUPER_DEBUG:
                logger.debug(f'[{self.con.tree_id}] Redrawing stats for node: {node}; tree_path="{ds.model.get_path(tree_iter)}"; '
                             f'size={node.get_size_bytes()} etc={node.get_etc()}')
            ds.model[tree_iter][self.con.treeview_meta.col_num_size] = _format_size_bytes(node)
            ds.model[tree_iter][self.con.treeview_meta.col_num_etc] = node.get_etc()
            ds.model[tree_iter][self.con.treeview_meta.col_num_data] = node

            redraw_displayed_node.nodes_redrawn += 1

        redraw_displayed_node.nodes_redrawn = 0

        def do_in_ui():
            logger.debug(f'[{self.con.tree_id}] Redrawing display tree stats in UI')
            with self._lock:
                if not self._is_shutdown:
                    self.con.display_store.recurse_over_tree(action_func=redraw_displayed_node)
                    logger.debug(f'[{self.con.tree_id}] Done redrawing stats in UI (for {redraw_displayed_node.nodes_redrawn} nodes): '
                                 f'sending signal "{Signal.REFRESH_SUBTREE_STATS_COMPLETELY_DONE}"')
                    # currently this is only used for functional tests
                    dispatcher.send(signal=Signal.REFRESH_SUBTREE_STATS_COMPLETELY_DONE, sender=self.con.tree_id)

        GLib.idle_add(do_in_ui)

    # ▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲
    # LISTENERS end

    def _append_dir_node_and_loading_child(self, parent_iter, node_data: Node):
        dir_node_iter = self._append_dir_node(parent_iter, node_data)
        self._append_loading_child(dir_node_iter)
        return dir_node_iter

    def _append_children(self, children: List[Node], parent_iter: Gtk.TreeIter):
        if children:
            logger.debug(f'[{self.con.tree_id}] Appending {len(children)} child display nodes')
            if len(children) > LARGE_NUMBER_OF_CHILDREN:
                logger.error(f'[{self.con.tree_id}] Too many children to display! Count = {len(children)}')
                self._append_empty_child(parent_iter, f'ERROR: too many items to display ({len(children):n})', IconId.ICON_ALERT)
                return
            # Append all underneath tree_iter
            for child in children:
                if child.is_dir():
                    self._append_dir_node_and_loading_child(parent_iter, child)
                else:
                    self._append_file_node(parent_iter, child)

        elif self.use_empty_nodes:
            self._append_empty_child(parent_iter, '(empty)')

    # Search for "TREE_VIEW_COLUMNS":

    def _append_empty_child(self, parent_node_iter, node_name, icon: IconId = IconId.NONE):
        row_values = []
        if self.con.treeview_meta.has_checkboxes:
            row_values.append(False)  # Checked
            row_values.append(False)  # Inconsistent
        row_values.append(icon)  # Icon
        row_values.append(node_name)  # Name
        if not self.con.treeview_meta.use_dir_tree:
            row_values.append(None)  # Directory
        row_values.append(None)  # Size
        row_values.append(None)  # Etc
        row_values.append(None)  # Modify Date
        row_values.append(None)  # Meta Changed Date / Created Date
        row_values.append(EmptyNode())

        return self.con.display_store.append_node(parent_node_iter, row_values)

    def _append_loading_child(self, parent_node_iter):
        row_values = []
        if self.con.treeview_meta.has_checkboxes:
            row_values.append(False)  # Checked
            row_values.append(False)  # Inconsistent
        row_values.append(None)  # Icon
        row_values.append('Loading...')  # Name
        if not self.con.treeview_meta.use_dir_tree:
            row_values.append(None)  # Directory
        row_values.append(None)  # Size
        row_values.append(None)  # Etc
        row_values.append(None)  # Modify Date
        row_values.append(None)  # Meta Changed Date / Created Date
        row_values.append(LoadingNode())

        return self.con.display_store.append_node(parent_node_iter, row_values)

    def generate_display_cols(self, parent_iter, node: Node):
        """Serializes a node into a list of strings which tell the TreeView how to populate each of the row's columns"""
        row_values = []

        self._add_checked_columns(parent_iter, node, row_values)

        # Icon: can vary based on pending actions

        icon = node.get_icon()
        if icon:
            icon = str(icon.value)
        row_values.append(icon)

        # Name
        node_name = node.name
        row_values.append(node_name)  # Name

        # Directory
        if not self.con.treeview_meta.use_dir_tree:
            directory, name = os.path.split(node.get_single_path())
            row_values.append(directory)  # Directory

        # Size Bytes
        row_values.append(_format_size_bytes(node))  # Size

        # etc
        row_values.append(node.get_etc())  # Etc

        # Modify TS
        try:
            if not node.modify_ts:
                row_values.append(None)
            else:
                modify_datetime = datetime.fromtimestamp(node.modify_ts / 1000)
                modify_formatted = modify_datetime.strftime(self.con.treeview_meta.datetime_format)
                row_values.append(modify_formatted)
        except AttributeError:
            row_values.append(None)

        # Change TS
        try:
            if not node.change_ts:
                row_values.append(None)
            else:
                change_datetime = datetime.fromtimestamp(node.change_ts / 1000)
                change_time = change_datetime.strftime(self.con.treeview_meta.datetime_format)
                row_values.append(change_time)
        except AttributeError:
            row_values.append(None)

        # Data (hidden)
        row_values.append(node)  # Data

        return row_values

    def _append_dir_node(self, parent_iter, node: Node) -> TreeIter:
        row_values = self.generate_display_cols(parent_iter, node)
        return self.con.display_store.append_node(parent_iter, row_values)

    def _append_file_node(self, parent_iter, node: Node):
        row_values = self.generate_display_cols(parent_iter, node)
        return self.con.display_store.append_node(parent_iter, row_values)

    def _add_checked_columns(self, parent_iter, node: Node, row_values: List):
        """Populates the checkbox and sets its state for a newly added row"""
        if self.con.treeview_meta.has_checkboxes and not node.is_ephemereal():
            if parent_iter:
                parent_checked = self.con.display_store.is_node_checked(parent_iter)
                if parent_checked:
                    row_values.append(True)  # Checked
                    row_values.append(False)  # Inconsistent
                    return
                parent_inconsistent = self.con.display_store.is_inconsistent(parent_iter)
                if not parent_inconsistent:
                    row_values.append(False)  # Checked
                    row_values.append(False)  # Inconsistent
                    return
                # Otherwise: inconsistent. Look up individual values below:
            row_id = node.identifier
            checked = self.con.display_store.checked_rows.get(row_id, None)
            inconsistent = self.con.display_store.inconsistent_rows.get(row_id, None)
            row_values.append(checked)  # Checked
            row_values.append(inconsistent)  # Inconsistent


def _format_size_bytes(node: Node):
    # remember that 0 and None mean different things here:
    if node.get_size_bytes() is None:
        return None
    else:
        return humanfriendly.format_size(node.get_size_bytes())
