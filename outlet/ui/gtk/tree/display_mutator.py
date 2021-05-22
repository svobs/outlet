import collections
import logging
import os
import threading
from datetime import datetime
from typing import Deque, Dict, Iterable, List, Set, Union

import humanfriendly
from pydispatch import dispatcher

from constants import IconId, MAX_NUMBER_DISPLAYABLE_CHILD_NODES, STATS_REFRESH_HOLDOFF_TIME_MS, SUPER_DEBUG, TreeDisplayMode
from error import ResultsExceededError
from global_actions import GlobalActions
from model.display_tree.build_struct import RowsOfInterest
from model.display_tree.filter_criteria import FilterCriteria
from model.node.container_node import CategoryNode
from model.node.directory_stats import DirectoryStats
from model.node.ephemeral_node import EmptyNode, LoadingNode
from model.node.node import SPIDNodePair
from model.node_identifier import GUID, SinglePathNodeIdentifier
from model.uid import UID
from signal_constants import Signal
from util.has_lifecycle import HasLifecycle
from util.holdoff_timer import HoldOffTimer

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
    def __init__(self, controller=None):
        HasLifecycle.__init__(self)
        self.con = controller
        self.use_empty_nodes = True  # default
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

        self.use_empty_nodes = self.con.backend.get_config('display.treeview.use_empty_nodes')
        self._connect_node_listeners()
        logger.debug(f'[{self.con.tree_id}] DisplayMutator started')

    def _connect_node_listeners(self):
        if self.con.treeview_meta.lazy_load:
            self.connect_dispatch_listener(signal=Signal.NODE_EXPANSION_TOGGLED, receiver=self._on_node_expansion_toggled)

        self.connect_dispatch_listener(signal=Signal.REFRESH_SUBTREE_STATS_DONE, receiver=self._on_refresh_stats_done)
        """This signal comes from the cacheman after it has finished updating all the nodes in the subtree,
        notfiying us that we can now refresh our display from it"""

        # Do not receive update notifications when displaying change trees.
        # (the need is not great enough to justify the effort - at present)
        if self.con.treeview_meta.tree_display_mode != TreeDisplayMode.CHANGES_ONE_TREE_PER_CATEGORY:
            self.connect_dispatch_listener(signal=Signal.NODE_UPSERTED, receiver=self._on_node_upserted)
            self.connect_dispatch_listener(signal=Signal.NODE_REMOVED, receiver=self._on_node_removed)

        # TODO: The 'sender' arg fails when relayed from gRPC! Dump PyDispatcher and replace with homegrown code
        self.connect_dispatch_listener(signal=Signal.LOAD_SUBTREE_DONE, receiver=self._on_load_subtree_done)
        self.connect_dispatch_listener(signal=Signal.FILTER_UI_TREE, receiver=self._on_filter_ui_tree_requested)
        self.connect_dispatch_listener(signal=Signal.EXPAND_ALL, receiver=self._on_expand_all_requested)
        self.connect_dispatch_listener(signal=Signal.EXPAND_AND_SELECT_NODE, receiver=self._expand_and_select_node)

        logger.debug(f'[{self.con.tree_id}] DisplayMutator listeners connected')

    def shutdown(self):
        self._is_shutdown = True
        HasLifecycle.shutdown(self)
        self.con = None

    def _expand_and_select_node(self, sender: str, spid: SinglePathNodeIdentifier):
        if sender == self.con.tree_id:
            logger.debug(f'[{self.con.tree_id}] Got signal: "{Signal.EXPAND_AND_SELECT_NODE.name}"')
            self.expand_and_select_node(spid)

    def _on_expand_all_requested(self, sender, tree_path):
        if sender == self.con.tree_id:
            self.expand_all(tree_path)

    def _on_filter_ui_tree_requested(self, sender: str, filter_criteria: FilterCriteria):
        if sender != self.con.tree_id:
            return

        try:
            # Update backend
            self.con.backend.update_filter_criteria(self.con.tree_id, filter_criteria)
            # Now that backend has returned, repopulate tree
            self.populate_root()
        except RuntimeError as err:
            msg = f'[{self.con.tree_id}] Failed to repopulate the tree using filter_criteria {filter_criteria}'
            logger.exception(msg)
            GlobalActions.display_error_in_ui(msg, repr(err))

    def _on_load_subtree_done(self, sender):
        """Just populates the tree with nodes. Executed asyncly via Signal.LOAD_SUBTREE_DONE"""
        if sender == self.con.tree_id:
            logger.debug(f'[{self.con.tree_id}] Got signal: "{Signal.LOAD_SUBTREE_DONE.name}". Sending signal "{Signal.ENQUEUE_UI_TASK.name}"')
            dispatcher.send(signal=Signal.ENQUEUE_UI_TASK, sender=sender, task_func=self.populate_root)

    def _populate_and_expand_recursively(self, parent_iter, sn: SPIDNodePair, node_count: int = 0) -> int:
        # Do a DFS of the change tree and populate the UI tree along the way
        if sn.node.is_dir():
            parent_iter = self._append_dir_node(parent_iter, sn)

            for child in self.con.get_tree().get_child_list_for_spid(sn.spid, is_expanding_parent=True):
                node_count = self._populate_and_expand_recursively(parent_iter, child, node_count)
        else:
            self._append_file_node(parent_iter, sn)

        node_count += 1
        return node_count

    def _populate_and_restore_expanded_state(self, parent_iter, sn: SPIDNodePair, node_count: int, expanded_row_guid_set: Set[GUID]) -> int:
        if SUPER_DEBUG:
            logger.debug(f'[{self.con.tree_id}] Populating node {sn.spid}')

        node = sn.node
        guid = sn.spid.guid
        # Do a DFS of the change tree and populate the UI tree along the way
        if node.is_dir():
            is_expand = False

            if type(node) == CategoryNode and self.con.treeview_meta.is_display_persisted and self.con.treeview_meta.is_category_node_expanded(node):
                cat_guid = sn.spid.guid
                expanded_row_guid_set.add(cat_guid)  # add to expanded set for later expansion
                logger.debug(f'[{self.con.tree_id}] Category node {sn.node.name} ({cat_guid}) is expanded')
                is_expand = True
            elif guid in expanded_row_guid_set:
                if SUPER_DEBUG:
                    logger.debug(f'[{self.con.tree_id}] Found GUID "{guid}" in expanded_row_set"')
                is_expand = True

            if is_expand:
                # Append all child nodes and recurse to possibly expand more:
                parent_iter = self._append_dir_node(parent_iter=parent_iter, sn=sn)

                if SUPER_DEBUG:
                    logger.debug(f'[{self.con.tree_id}] Row will be expanded: {guid} ("{node.name}")')

                child_list = self.con.get_tree().get_child_list_for_spid(sn.spid)
                for child in child_list:
                    node_count = self._populate_and_restore_expanded_state(parent_iter, child, node_count, expanded_row_guid_set)

            else:
                if SUPER_DEBUG:
                    logger.debug(f'[{self.con.tree_id}] Node is not expanded: {sn.spid}')
                self._append_dir_node_and_loading_child(parent_iter, sn)
        else:
            self._append_file_node(parent_iter, sn)

        node_count += 1
        return node_count

    def _expand_row_without_event_firing(self, tree_path, expand_all):
        assert self.con.treeview_meta.lazy_load
        assert tree_path, 'tree_path is empty!'
        tree_path = self.con.display_store.ensure_tree_path(tree_path)

        self._enable_expand_state_listeners = False
        try:
            self.con.tree_view.expand_row(path=tree_path, open_all=expand_all)
        finally:
            self._enable_expand_state_listeners = True

    def select_guid(self, guid: GUID):
        tree_iter = self.con.display_store.find_guid_in_tree(guid, None)
        if not tree_iter:
            logger.info(f'[{self.con.tree_id}] Could not select node: could not find node in tree for GUID {guid}')
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
                ancestor_sn_list: Iterable[SPIDNodePair] = self.con.get_tree().get_ancestor_list(selection)

                # Expand all ancestors one by one:
                tree_iter = None
                for ancestor_sn in ancestor_sn_list:
                    tree_iter = self.con.display_store.find_guid_in_tree(ancestor_sn.spid.guid, tree_iter)
                    if not tree_iter:
                        logger.error(f'[{self.con.tree_id}] Could not expand ancestor node: could not find node in tree for: {ancestor_sn.spid.guid}')
                        return
                    tree_path = self.con.display_store.model.get_path(tree_iter)
                    if not self.con.tree_view.row_expanded(tree_path):
                        self._expand_subtree(tree_path, expand_all=False)

                # Now select target node:
                self.select_guid(selection.guid)

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
            # already expanded
            return

        sn: SPIDNodePair = self.con.display_store.get_node_data(tree_path)
        parent_iter = self.con.display_store.model.get_iter(tree_path)
        self.con.display_store.remove_loading_node(parent_iter)
        child_sn_list: List[SPIDNodePair] = self.con.get_tree().get_child_list_for_spid(sn.spid, is_expanding_parent=True)

        if expand_all:
            # populate all descendants
            node_count = 0
            for child_sn in child_sn_list:
                node_count = self._populate_and_expand_recursively(parent_iter, child_sn, node_count)
            logger.debug(f'[{self.con.tree_id}] Populated {node_count} nodes')
        else:
            # populate only children
            self._append_child_list(child_list=child_sn_list, parent_iter=parent_iter)

        self._expand_row_without_event_firing(tree_path=tree_path, expand_all=expand_all)

    def populate_root(self):
        """START HERE.
        More like "repopulate" - clears model before populating.
        Draws from the undelying data store as needed, to populate the display store."""

        rows: RowsOfInterest = self.con.backend.get_rows_of_interest(self.con.tree_id)

        logger.debug(f'[{self.con.tree_id}] Entered populate_root(): lazy={self.con.treeview_meta.lazy_load}'
                     f' expanded_row_set={rows.expanded} selected_row_set={rows.selected}')

        too_many_results: bool = False
        count_results: int = 0

        # This may be a long task
        try:
            # Lock this so that node-upserted and node-removed callbacks don't interfere
            self._enable_node_signals = False
            try:
                with self._lock:
                    top_level_sn_list: List[SPIDNodePair] = self.con.get_tree().get_child_list_for_root()
                logger.debug(f'[{self.con.tree_id}] populate_root(): got {len(top_level_sn_list)} top-level nodes for root')
            except ResultsExceededError as err:
                too_many_results = True
                count_results = err.actual_count

        finally:
            self._enable_node_signals = True

        def _update_ui():
            with self._lock:
                # Wipe out existing items:
                logger.debug(f'[{self.con.tree_id}] Clearing model')
                root_iter = self.con.display_store.clear_model()
                node_count = 0

                if too_many_results:
                    logger.error(f'[{self.con.tree_id}] Too many top-level nodes to display! Count = {count_results}')
                    self._append_empty_child(root_iter, f'ERROR: too many items to display ({count_results:n})', IconId.ICON_ALERT)

                elif self.con.treeview_meta.lazy_load:
                    logger.debug(f'[{self.con.tree_id}] Populating via lazy load')
                    # Recursively add child nodes for dir nodes which need expanding. We can only expand after we have nodes, due to GTK3 limitation
                    for sn in top_level_sn_list:
                        self._populate_and_restore_expanded_state(root_iter, sn, node_count, rows.expanded)

                    for guid in rows.expanded:
                        logger.debug(f'[{self.con.tree_id}] Expanding: {guid}')
                        try:
                            tree_iter = self.con.display_store.find_guid_in_tree(guid)
                            if tree_iter:
                                self._expand_row_without_event_firing(tree_path=tree_iter, expand_all=False)
                            else:
                                logger.info(f'[{self.con.tree_id}] Could not expand row because it was not found: {guid}')
                        except RuntimeError as e:
                            # non-fatal error
                            logger.error(f'[{self.con.tree_id}] Failed to expand row: {guid}: {e}')

                    logger.debug(f'[{self.con.tree_id}] Populated {node_count} nodes and expanded {len(rows.expanded)} dir nodes')

                else:
                    logger.debug(f'[{self.con.tree_id}] Populating recursively')
                    # NOT lazy: load all at once, expand all
                    for sn in top_level_sn_list:
                        node_count = self._populate_and_expand_recursively(None, sn, node_count)

                    logger.debug(f'[{self.con.tree_id}] Populated {node_count} nodes')

                    # Expand all dirs:
                    assert not self.con.treeview_meta.lazy_load
                    self.con.tree_view.expand_all()

                if rows.selected:
                    logger.debug(f'[{self.con.tree_id}] Attempting to restore {len(rows.selected)} previously selected rows')
                try:
                    for prev_guid in rows.selected:
                        self.select_guid(prev_guid)
                except RuntimeError:
                    logger.exception(f'[{self.con.tree_id}] Failed to restore selection')

            logger.debug(f'[{self.con.tree_id}] Sending signal {Signal.POPULATE_UI_TREE_DONE.name}')
            dispatcher.send(signal=Signal.POPULATE_UI_TREE_DONE, sender=self.con.tree_id)

        GLib.idle_add(_update_ui)

        self._request_subtree_stats_refresh()

    def generate_checked_row_list(self) -> List[SPIDNodePair]:
        """Returns a list which contains the nodes of the items which are currently checked by the user
        (including any rows which may not be visible in the UI due to being collapsed). This will be a subset of the ChangeDisplayTree currently
        being displayed. Includes file nodes only, with the exception of MKDIR nodes.

        This method assumes that we are a ChangeTree and thus returns instances of GUID. We don't try to do any fancy logic to filter out
        CategoryNodes, existing directories, or other non-change nodes; we'll let the backend handle that. We simply return each of the GUIDs which
        have check boxes in the GUI (1-to-1 in count)

        Algorithm:
            Iterate over display nodes. Start with top-level nodes.
          - If row is checked, add it to checked_queue.
            Each node added to checked_queue will be added to the returned list along with all its descendants.
          - If row is unchecked, ignore it.
          - If row is inconsistent, add it to mixed_queue. Each of its descendants will be examined and have these rules applied recursively.
        """
        checked_row_list: List[SPIDNodePair] = []

        checked_queue: Deque[SPIDNodePair] = collections.deque()
        mixed_queue: Deque[SPIDNodePair] = collections.deque()

        with self._lock:
            assert self.con.treeview_meta.has_checkboxes, f'Tree does not have checkboxes. Is this a ChangeTree? {self.con.get_tree()}'

            mixed_queue.append(self.con.get_tree().get_root_sn())

            while len(mixed_queue) > 0:
                inconsistent_dir_sn: SPIDNodePair = mixed_queue.popleft()
                # only dir nodes can be inconsistent
                assert inconsistent_dir_sn.node.is_dir(), f'Expected a dir-type node: {inconsistent_dir_sn}'

                # We will iterate through the master cache, which is necessary since we may have implicitly checked nodes which are not in UI.
                # Check each child of an inconsistent dir for checked or inconsistent status.
                for child_sn in self.con.get_tree().get_child_list_for_spid(inconsistent_dir_sn.spid):
                    if child_sn.spid.guid in self.con.display_store.checked_guid_set:
                        checked_queue.append(child_sn)
                    elif child_sn.spid.guid in self.con.display_store.inconsistent_guid_set:
                        mixed_queue.append(child_sn)

            # Whitelist contains nothing but trees full of checked items
            while len(checked_queue) > 0:
                chosen_sn: SPIDNodePair = checked_queue.popleft()
                # Add all checked stuff:
                checked_row_list.append(chosen_sn)

                # Drill down and add all descendants of nodes in the checked_queue:
                if chosen_sn.node.is_dir:
                    for child_sn in self.con.get_tree().get_child_list_for_spid(chosen_sn.spid):
                        checked_queue.append(child_sn)

            return checked_row_list

    # LISTENERS begin
    # ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼

    def _on_node_expansion_toggled(self, sender: str, parent_iter: Gtk.TreeIter, parent_path, sn: SPIDNodePair, is_expanded: bool) -> None:
        # Callback for Signal.NODE_EXPANSION_TOGGLED:
        if sender != self.con.tree_id:
            return

        assert self.con.treeview_meta.lazy_load
        logger.debug(f'[{self.con.tree_id}] Node expansion toggled to {is_expanded} for {sn.spid}"')

        if not self._enable_expand_state_listeners or not self._enable_node_signals:
            if SUPER_DEBUG:
                logger.debug(f'[{self.con.tree_id}] Ignoring signal "{Signal.NODE_EXPANSION_TOGGLED.name}": listeners disabled')
            return

        def expand_or_contract():
            with self._lock:
                # Add children for node:
                if is_expanded:
                    self.con.display_store.remove_loading_node(parent_iter)

                    # This will also add the node to the backend set of expanded nodes:
                    child_list = self.con.get_tree().get_child_list_for_spid(sn.spid)
                    self._append_child_list(child_list=child_list, parent_iter=parent_iter)

                    # Need to call this because removing the Loading node leaves the parent with no children,
                    # and due to a deficiency in GTK this causes the parent to become collapsed again.
                    # So we must wait until after the children have been added, and then disable listeners from firing
                    # and set it to expanded again.
                    self._expand_row_without_event_firing(tree_path=parent_path, expand_all=False)
                else:
                    # Report the collapsed row to the backend, which is tracking expanded nodes:
                    self.con.backend.remove_expanded_row(row_uid=sn.spid.guid, tree_id=self.con.tree_id)

                    # Collapsed:
                    self.con.display_store.remove_all_children(parent_iter)
                    # Always have at least a dummy node:
                    logger.debug(f'Collapsing tree: adding loading node')
                    self._append_loading_child(parent_iter)

                logger.debug(f'[{self.con.tree_id}] Displayed rows count: {len(self.con.display_store.displayed_guid_dict)}')
                dispatcher.send(signal=Signal.NODE_EXPANSION_DONE, sender=self.con.tree_id)
        GLib.idle_add(expand_or_contract)

    def _update_or_append(self, sn: SPIDNodePair, parent_iter, child_iter):
        if child_iter:
            # Node is update:
            logger.debug(f'[{self.con.tree_id}] Node already exists in tree (guid={sn.spid.guid}): doing an update instead')
            display_vals: list = self.generate_display_cols(parent_iter, sn)
            for col, val in enumerate(display_vals):
                self.con.display_store.model.set_value(child_iter, col, val)
        else:
            logger.debug(f'[{self.con.tree_id}] Appending new node (guid={sn.spid.guid}, is_dir={sn.node.is_dir()})')
            # Node is new
            if sn.node.is_dir():
                self._append_dir_node_and_loading_child(parent_iter, sn)
            else:
                self._append_file_node(parent_iter, sn)

    def _on_node_upserted(self, sender: str, sn: SPIDNodePair, parent_guid: GUID) -> None:
        if sender != self.con.tree_id:
            return

        if not self._enable_node_signals:
            if SUPER_DEBUG:
                logger.debug(f'[{self.con.tree_id}] Ignoring signal "{Signal.NODE_UPSERTED.name}": node listeners disabled')
            return

        guid = sn.spid.guid

        def update_ui():
            with self._lock:
                if logger.isEnabledFor(logging.DEBUG):
                    logger.debug(f'[{self.con.tree_id}] Received signal {Signal.NODE_UPSERTED.name} for {sn.spid}, parent={parent_guid}')

                if SUPER_DEBUG:
                    logger.debug(f'[{self.con.tree_id}] Examining parent {parent_guid} for displayed node {sn.node.node_identifier}')

                if self.con.get_tree().get_root_spid().guid == parent_guid:
                    logger.debug(f'[{self.con.tree_id}] Node is topmost level: {sn.node.node_identifier}')
                    child_iter = self.con.display_store.find_guid_in_children(sn.spid.guid, None)
                    self._update_or_append(sn, None, child_iter)
                else:
                    # Node is not topmost.
                    logger.debug(f'[{self.con.tree_id}] Node is not topmost: {sn.spid}')
                    parent_iter = self.con.display_store.find_guid_in_tree(target_guid=parent_guid)
                    if parent_iter:
                        parent_tree_path = self.con.display_store.model.get_path(parent_iter)
                        if self.con.tree_view.row_expanded(parent_tree_path):
                            # Parent is present and expanded. Now check whether the "upserted node" already exists:
                            child_iter = self.con.display_store.find_guid_in_children(guid, parent_iter)
                            self._update_or_append(sn, parent_iter, child_iter)
                        else:
                            # Parent present but not expanded. Make sure it has a loading node (which allows child toggle):
                            if self.con.display_store.model.iter_has_child(parent_iter):
                                logger.debug(f'[{self.con.tree_id}] Will not upsert node {guid}: Parent is not expanded: {parent_guid}')
                            else:
                                # May have added a child to formerly childless parent: add loading node
                                logger.debug(f'[{self.con.tree_id}] Parent ({parent_guid}) is not expanded; adding loading node')
                                self._append_loading_child(parent_iter)
                    else:
                        # Not even parent is displayed. Probably an ancestor isn't expanded. Just skip
                        assert parent_guid not in self.con.display_store.displayed_guid_dict, \
                            f'DisplayedRows ({self.con.display_store.displayed_guid_dict}) contains GUID ({parent_guid})!'
                        logger.debug(f'[{self.con.tree_id}] Will not upsert node: Could not find parent GUID in display tree: {parent_guid}')

                        # sanity check
                        if guid in self.con.display_store.displayed_guid_dict:
                            logger.warning(f'[{self.con.tree_id}] Received signal {Signal.NODE_UPSERTED.name} for node {sn.spid} '
                                           f'but its parent is no longer in the tree; removing node from display store: {guid}')
                            self.con.display_store.remove_node(guid)

                # If we received the update, it is somewhere in our subtree (even if invisible) and thus affects our stats:
                self._stats_refresh_timer.start_or_delay()

        GLib.idle_add(update_ui)

    def _on_node_removed(self, sender: str, sn: SPIDNodePair, parent_guid: GUID):
        # Note: parent_guid is not used for Linux version but is needed for Mac
        if sender != self.con.tree_id:
            return

        if not self._enable_node_signals:
            if SUPER_DEBUG:
                logger.debug(f'[{self.con.tree_id}] Ignoring signal "{Signal.NODE_REMOVED.name}": node listeners disabled')
            return

        guid = sn.spid.guid

        def update_ui():
            try:
                with self._lock:
                    displayed_item = self.con.display_store.displayed_guid_dict.get(guid, None)

                    if displayed_item:
                        if logger.isEnabledFor(logging.DEBUG):
                            logger.debug(f'[{self.con.tree_id}] Received signal {Signal.NODE_REMOVED.name} for displayed node {sn.spid}')

                        stats_refresh_needed = True

                        logger.debug(f'[{self.con.tree_id}] Removing node from display store: {guid}')
                        self.con.display_store.remove_node(guid)
                        logger.debug(f'[{self.con.tree_id}] Node removed: {guid}')
                    elif self.con.get_tree().is_path_in_subtree(sn.spid.get_single_path()):
                        # not visible, but stats still need refresh
                        if logger.isEnabledFor(logging.DEBUG):
                            logger.debug(f'[{self.con.tree_id}] Received signal {Signal.NODE_REMOVED.name} for node {sn.spid}')

                        stats_refresh_needed = True
                    else:
                        if logger.isEnabledFor(logging.DEBUG):
                            logger.debug(f'[{self.con.tree_id}] Ignoring signal {Signal.NODE_REMOVED.name} for node {sn.spid}')
                        stats_refresh_needed = False

                    if stats_refresh_needed:
                        self._stats_refresh_timer.start_or_delay()
            except RuntimeError:
                logger.exception(f'While removing node {guid} ("{sn.node.name}") from UI')

        GLib.idle_add(update_ui)

    def _request_subtree_stats_refresh(self):
        # Requests the cacheman to recalculate stats for this subtree. Sends Signal.REFRESH_SUBTREE_STATS_DONE when done
        logger.debug(f'[{self.con.tree_id}] Requesting subtree stats refresh')
        self.con.app.backend.enqueue_refresh_subtree_stats_task(root_uid=self.con.get_tree().root_uid, tree_id=self.con.tree_id)

    def _on_refresh_stats_done(self, sender: str, status_msg: str, dir_stats_dict: Dict[Union[UID, GUID], DirectoryStats], key_is_uid: bool):
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
            sn: SPIDNodePair = ds.get_node_data(tree_iter)
            if not sn:
                par_iter = ds.model.iter_parent(tree_iter)
                par_sn: SPIDNodePair = ds.get_node_data(par_iter)
                logger.error(f'[{self.con.tree_id}] No node for child of {par_sn.spid}')
                return
            assert sn, f'For tree_id="{sender} and row={self.con.display_store.model[tree_iter]}'
            if sn.node.is_ephemereal() or not sn.node.is_dir():
                return

            if key_is_uid:
                key = sn.node.uid
            else:
                key = sn.spid.guid
            dir_stats_for_node = dir_stats_dict.get(key, None)
            if dir_stats_for_node:
                if SUPER_DEBUG:
                    logger.debug(f'[{self.con.tree_id}] Redrawing stats for node: {sn.spid}; tree_path="{ds.model.get_path(tree_iter)}"; '
                                 f'size={dir_stats_for_node.get_size_bytes()} etc={dir_stats_for_node.get_etc()}')
                ds.model[tree_iter][self.con.treeview_meta.col_num_size] = _format_size_bytes(dir_stats_for_node)
                ds.model[tree_iter][self.con.treeview_meta.col_num_etc] = dir_stats_for_node.get_etc()

            redraw_displayed_node.nodes_redrawn += 1

        redraw_displayed_node.nodes_redrawn = 0

        def do_in_ui():
            logger.debug(f'[{self.con.tree_id}] Redrawing display tree stats in UI')
            if status_msg is None:
                logger.error(f'Status msg is None!')
            else:
                self.con.status_bar.set_label(status_msg)
            with self._lock:
                if not self._is_shutdown:
                    self.con.display_store.recurse_over_tree(action_func=redraw_displayed_node)
                    logger.debug(f'[{self.con.tree_id}] Done redrawing stats in UI (for {redraw_displayed_node.nodes_redrawn} nodes): '
                                 f'sending signal "{Signal.REFRESH_SUBTREE_STATS_COMPLETELY_DONE.name}"')
                    # currently this is only used for functional tests
                    dispatcher.send(signal=Signal.REFRESH_SUBTREE_STATS_COMPLETELY_DONE, sender=self.con.tree_id)

        GLib.idle_add(do_in_ui)

    # ▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲
    # LISTENERS end

    def _append_dir_node_and_loading_child(self, parent_iter, sn: SPIDNodePair):
        dir_node_iter = self._append_dir_node(parent_iter, sn)
        self._append_loading_child(dir_node_iter)
        return dir_node_iter

    def _append_child_list(self, child_list: List[SPIDNodePair], parent_iter: Gtk.TreeIter):
        if child_list:
            logger.debug(f'[{self.con.tree_id}] Appending {len(child_list)} child display nodes')
            if len(child_list) > MAX_NUMBER_DISPLAYABLE_CHILD_NODES:
                logger.error(f'[{self.con.tree_id}] Too many children to display! Count = {len(child_list)}')
                self._append_empty_child(parent_iter, f'ERROR: too many items to display ({len(child_list):n})', IconId.ICON_ALERT)
                return
            # Append all underneath tree_iter
            for child_sn in child_list:
                if child_sn.node.is_dir():
                    self._append_dir_node_and_loading_child(parent_iter, child_sn)
                else:
                    self._append_file_node(parent_iter, child_sn)

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
        node = EmptyNode()
        sn = SPIDNodePair(node.node_identifier, node)
        row_values.append(sn)

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
        loading_node = LoadingNode()
        loading_sn = SPIDNodePair(loading_node.node_identifier, loading_node)
        row_values.append(loading_sn)

        return self.con.display_store.append_node(parent_node_iter, row_values)

    def generate_display_cols(self, parent_iter, sn: SPIDNodePair):
        """Serializes a node into a list of strings which tell the TreeView how to populate each of the row's columns"""
        row_values = []

        self._add_checked_columns(parent_iter, sn, row_values)

        # Icon: can vary based on pending actions

        icon = sn.node.get_icon()
        if icon:
            icon = str(icon.value)
        row_values.append(icon)

        # Name
        node_name = sn.node.name
        row_values.append(node_name)  # Name

        # Directory
        if not self.con.treeview_meta.use_dir_tree:
            directory, name = os.path.split(sn.spid.get_single_path())
            row_values.append(directory)  # Directory

        # Size Bytes
        row_values.append(_format_size_bytes(sn.node))  # Size

        # etc
        row_values.append(sn.node.get_etc())  # Etc

        # Modify TS
        try:
            if not sn.node.modify_ts:
                row_values.append(None)
            else:
                modify_datetime = datetime.fromtimestamp(sn.node.modify_ts / 1000)
                modify_formatted = modify_datetime.strftime(self.con.treeview_meta.datetime_format)
                row_values.append(modify_formatted)
        except AttributeError:
            row_values.append(None)

        # Change TS
        try:
            if not sn.node.change_ts:
                row_values.append(None)
            else:
                change_datetime = datetime.fromtimestamp(sn.node.change_ts / 1000)
                change_time = change_datetime.strftime(self.con.treeview_meta.datetime_format)
                row_values.append(change_time)
        except AttributeError:
            row_values.append(None)

        # Data (hidden)
        row_values.append(sn)  # Data

        return row_values

    def _append_dir_node(self, parent_iter, sn: SPIDNodePair) -> TreeIter:
        row_values = self.generate_display_cols(parent_iter, sn)
        return self.con.display_store.append_node(parent_iter, row_values)

    def _append_file_node(self, parent_iter, sn: SPIDNodePair):
        row_values = self.generate_display_cols(parent_iter, sn)
        return self.con.display_store.append_node(parent_iter, row_values)

    def _add_checked_columns(self, parent_iter, sn: SPIDNodePair, row_values: List):
        """Populates the checkbox and sets its state for a newly added row"""
        if self.con.treeview_meta.has_checkboxes and not sn.node.is_ephemereal():
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
            guid = sn.spid.guid
            # This will automatically set checked state for nodes which are duplicated in the GUI
            is_checked: bool = guid in self.con.display_store.checked_guid_set
            is_inconsistent: bool = guid in self.con.display_store.inconsistent_guid_set
            row_values.append(is_checked)  # Checked
            row_values.append(is_inconsistent)  # Inconsistent


def _format_size_bytes(node):
    # remember that 0 and None mean different things here:
    if not node or node.get_size_bytes() is None:
        return None
    else:
        return humanfriendly.format_size(node.get_size_bytes())
