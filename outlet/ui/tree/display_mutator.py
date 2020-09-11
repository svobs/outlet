import collections
import logging
import os
import threading
import humanfriendly
from pydispatch import dispatcher
from datetime import datetime
from typing import Deque, Iterable, List, Optional

from pydispatch.errors import DispatcherKeyError

from constants import HOLDOFF_TIME_MS, LARGE_NUMBER_OF_CHILDREN
from index.error import GDriveItemNotFoundError
from model.op import Op
from util.holdoff_timer import HoldOffTimer
from model.node.container_node import CategoryNode
from model.node.display_node import DisplayNode
from model.node.ephemeral_node import EmptyNode, LoadingNode
from model.node_identifier import NodeIdentifier
from ui import actions

import gi
gi.require_version("Gtk", "3.0")
from gi.repository import GLib, Gtk
from gi.repository.Gtk import TreeIter

logger = logging.getLogger(__name__)


# CLASS DisplayMutator
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼

class DisplayMutator:
    """
    Cache Manager --> TreeBuilder --> DisplayTree --> DisplayMutator --> DisplayStore (TreeModel)
    TODO: when does the number of display nodes start to slow down? -> add config for live node maximum
    """
    def __init__(self, config, controller=None):
        self.con = controller
        self.use_empty_nodes = True
        self._lock = threading.Lock()
        self._stats_refresh_timer = HoldOffTimer(holdoff_time_ms=HOLDOFF_TIME_MS, task_func=self._refresh_subtree_stats)
        """Stats for the entire subtree are all connected to each other, so this is a big task. This timer allows us to throttle its frequency"""

        self._enable_state_listeners = False
        """When true, listen and automatically update the display when a node is added, expanded, etc.
        Needs to be set to false when modifying the model with certain operations, because leaving enabled would
        result in an infinite loop"""

    def __del__(self):
        try:
            if self.con and self.con.treeview_meta:
                dispatcher.disconnect(signal=actions.NODE_EXPANSION_TOGGLED, receiver=self._on_node_expansion_toggled,
                                      sender=self.con.treeview_meta.tree_id)
        except DispatcherKeyError:
            pass

        try:
            dispatcher.disconnect(signal=actions.REFRESH_SUBTREE_STATS_DONE, receiver=self._on_subtree_stats_updated)
        except DispatcherKeyError:
            pass

        try:
            dispatcher.disconnect(signal=actions.NODE_UPSERTED, receiver=self._on_node_upserted_in_cache)
        except DispatcherKeyError:
            pass

        try:
            dispatcher.disconnect(signal=actions.NODE_REMOVED, receiver=self._on_node_removed_from_cache)
        except DispatcherKeyError:
            pass

    def init(self):
        """Do post-wiring stuff like connect listeners."""
        logger.debug(f'[{self.con.tree_id}] DisplayMutator init')
        self.use_empty_nodes = self.con.config.get('display.diff_tree.use_empty_nodes')

        if self.con.treeview_meta.lazy_load:
            dispatcher.connect(signal=actions.NODE_EXPANSION_TOGGLED, receiver=self._on_node_expansion_toggled,
                               sender=self.con.treeview_meta.tree_id)

        dispatcher.connect(signal=actions.REFRESH_SUBTREE_STATS_DONE, receiver=self._on_subtree_stats_updated, sender=self.con.treeview_meta.tree_id)
        """This signal comes from the cacheman after it has finished updating all the nodes in the subtree,
        notfiying us that we can now refresh our display from it"""

        if self.con.treeview_meta.can_modify_tree:
            dispatcher.connect(signal=actions.NODE_UPSERTED, receiver=self._on_node_upserted_in_cache)
            dispatcher.connect(signal=actions.NODE_REMOVED, receiver=self._on_node_removed_from_cache)

    def _populate_recursively(self, parent_iter, node: DisplayNode, node_count: int = 0) -> int:
        # Do a DFS of the change tree and populate the UI tree along the way
        if node.is_dir():
            parent_iter = self._append_dir_node(parent_iter=parent_iter, node=node)

            for child in self.con.lazy_tree.get_children(node):
                node_count = self._populate_recursively(parent_iter, child, node_count)
        else:
            self._append_file_node(parent_iter, node)

        node_count += 1
        return node_count

    def expand_and_select_node(self, selection: NodeIdentifier):
        def do_in_ui():
            with self._lock:
                item = self.con.parent_win.application.cache_manager.get_node_for_uid(selection.uid, selection.tree_type)
                ancestor_list: Iterable[DisplayNode] = self.con.get_tree().get_ancestors(item)

                tree_iter = None
                for ancestor in ancestor_list:
                    if tree_iter is None:
                        tree_iter = self.con.display_store.find_uid_in_top_level(target_uid=ancestor.uid)
                    else:
                        tree_iter = self.con.display_store.find_uid_in_children(target_uid=ancestor.uid, parent_iter=tree_iter)
                    if not tree_iter:
                        logger.error(f'[{self.con.tree_id}] Could not expand ancestor node: could not find node in tree for: {ancestor}')
                        return
                    tree_path = self.con.display_store.model.get_path(tree_iter)
                    if not self.con.tree_view.row_expanded(tree_path):
                        self._expand_subtree(tree_path, expand_all=False)

                tree_iter = self.con.display_store.find_uid_in_children(target_uid=selection.uid, parent_iter=tree_iter)
                if not tree_iter:
                    logger.error(f'[{self.con.tree_id}] Could not expand node: could not find node in tree for: {selection}')
                    return
                tree_view_selection: Gtk.TreeSelection = self.con.tree_view.get_selection()
                # tree_view_selection.unselect_all()
                tree_view_selection.select_iter(tree_iter)
                tree_path = self.con.display_store.model.get_path(tree_iter)
                self.con.tree_view.scroll_to_cell(path=tree_path, column=None, use_align=True, row_align=0.5, col_align=0)

        GLib.idle_add(do_in_ui)

    def expand_all(self, tree_path):
        def expand_me(tree_iter):
            path = self.con.display_store.model.get_path(tree_iter)
            self._expand_subtree(path, expand_all=True)

        with self._lock:
            if not self.con.tree_view.row_expanded(tree_path):
                self._expand_subtree(tree_path, expand_all=True)
            else:
                self.con.display_store.do_for_descendants(tree_path=tree_path, action_func=expand_me)

    def _expand_subtree(self, tree_path: Gtk.TreePath, expand_all: bool):
        if not self.con.treeview_meta.lazy_load:
            # no need to populate. just expand:
            self.con.tree_view.expand_row(path=tree_path, open_all=expand_all)
            return

        if self.con.tree_view.row_expanded(tree_path):
            return

        self._enable_state_listeners = False
        try:
            node = self.con.display_store.get_node_data(tree_path)
            parent_iter = self.con.display_store.model.get_iter(tree_path)
            self.con.display_store.remove_loading_node(parent_iter)
            children: List[DisplayNode] = self.con.lazy_tree.get_children(node)

            if expand_all:
                node_count = 0
                for child in children:
                    node_count = self._populate_recursively(parent_iter, child, node_count)
                logger.debug(f'[{self.con.tree_id}] Populated {node_count} nodes')
            else:
                self._append_children(children=children, parent_iter=parent_iter)

            self.con.tree_view.expand_row(path=tree_path, open_all=expand_all)
        finally:
            self._enable_state_listeners = True

    def populate_root(self):
        """START HERE.
        More like "repopulate" - clears model before populating.
        Draws from the undelying data store as needed, to populate the display store."""

        # This may be a long task
        try:
            children: List[DisplayNode] = self.con.lazy_tree.get_children_for_root()
        except GDriveItemNotFoundError as err:
            # Not found: signal error to UI and cancel
            logger.warning(f'[{self.con.tree_id}] Could not populate root: GDrive node not found: {self.con.lazy_tree.get_root_identifier()}')
            logger.debug(f'[{self.con.tree_id}] Sending signal: "{actions.ROOT_PATH_UPDATED}" with new_root='
                         f'{self.con.lazy_tree.get_root_identifier()}, err={err}')
            dispatcher.send(signal=actions.ROOT_PATH_UPDATED, sender=self.con.tree_id, new_root=self.con.lazy_tree.get_root_identifier(), err=err)
            return

        def update_ui():
            self._enable_state_listeners = False
            try:
                with self._lock:
                    # Wipe out existing items:
                    root_iter = self.con.display_store.clear_model()

                    if self.con.treeview_meta.lazy_load:
                        # Just append for the root level. Beyond that, we will only load more nodes when
                        # the expanded state is toggled
                        self._append_children(children=children, parent_iter=root_iter)
                    else:
                        node_count = 0
                        for ch in children:
                            node_count = self._populate_recursively(None, ch, node_count)

                        logger.debug(f'[{self.con.tree_id}] Populated {node_count} nodes')

                        self.con.tree_view.expand_all()
            finally:
                self._enable_state_listeners = True

            with self._lock:
                # This should fire expanded state listener to populate nodes as needed:
                if self.con.treeview_meta.is_display_persisted:
                    self._set_expand_states_from_config()

            dispatcher.send(signal=actions.LOAD_UI_TREE_DONE, sender=self.con.tree_id)

        GLib.idle_add(update_ui)

        # Show tree summary. This will probably just display 'Loading...' until REFRESH_SUBTREE_STATS is done processing
        actions.set_status(sender=self.con.tree_id, status_msg=self.con.get_tree().get_summary())

        logger.debug(f'[{self.con.tree_id}] Sending signal "{actions.REFRESH_SUBTREE_STATS}"')
        dispatcher.send(signal=actions.REFRESH_SUBTREE_STATS, sender=self.con.tree_id)

    def get_checked_rows_as_list(self) -> List[DisplayNode]:
        """Returns a list which contains the DisplayNodes of the items which are currently checked by the user
        (including collapsed rows). This will be a subset of the DisplayTree which was used to
        populate this tree. Includes file nodes only, with the exception of GDrive FolderToAdd."""
        assert self.con.treeview_meta.has_checkboxes
        # subtree root will be the same as the current subtree's
        checked_items: List[DisplayNode] = []

        # Algorithm:
        # Iterate over display nodes. Start with top-level nodes.
        # - Add each checked row to DFS queue (whitelist). It and all of its descendants will be added
        # - Ignore each unchecked row
        # - Each inconsistent row needs to be drilled down into.
        whitelist: Deque[DisplayNode] = collections.deque()
        secondary_screening: Deque[DisplayNode] = collections.deque()

        children: Iterable[DisplayNode] = self.con.lazy_tree.get_children_for_root()
        for child in children:

            if self.con.display_store.checked_rows.get(child.identifier, None):
                whitelist.append(child)
            elif self.con.display_store.inconsistent_rows.get(child.identifier, None):
                secondary_screening.append(child)

        while len(secondary_screening) > 0:
            parent: DisplayNode = secondary_screening.popleft()
            assert parent.is_dir(), f'Expected a dir-type node: {parent}'

            if not parent.exists():
                # Even an inconsistent FolderToAdd must be included as a checked item:
                checked_items.append(parent)

            children: Iterable[DisplayNode] = self.con.lazy_tree.get_children(parent)

            for child in children:
                if self.con.display_store.checked_rows.get(child.identifier, None):
                    whitelist.append(child)
                elif self.con.display_store.inconsistent_rows.get(child.identifier, None):
                    secondary_screening.append(child)

        while len(whitelist) > 0:
            chosen_node: DisplayNode = whitelist.popleft()
            # non-existent directory must be added
            if not chosen_node.is_dir() or not chosen_node.exists():
                checked_items.append(chosen_node)

            children: Iterable[DisplayNode] = self.con.lazy_tree.get_children(chosen_node)
            for child in children:
                whitelist.append(child)

        return checked_items

    # LISTENERS begin
    # ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼

    def _on_node_expansion_toggled(self, sender: str, parent_iter: Gtk.TreeIter, parent_path, node: DisplayNode, is_expanded: bool):
        # Callback for actions.NODE_EXPANSION_TOGGLED:
        logger.debug(f'[{self.con.tree_id}] Node expansion toggled to {is_expanded} for {node}"')

        if not self._enable_state_listeners:
            logger.debug(f'[{self.con.tree_id}] Ignoring signal "{actions.NODE_EXPANSION_TOGGLED}: listeners disabled"')
            return

        def expand_or_contract():
            with self._lock:
                # Add children for node:
                if is_expanded:
                    self.con.display_store.remove_loading_node(parent_iter)

                    children = self.con.lazy_tree.get_children(node)
                    self._append_children(children=children, parent_iter=parent_iter)

                    # Need to call this because removing the Loading node leaves the parent with no children,
                    # and due to a deficiency in GTK this causes the parent to become collapsed again.
                    # So we must wait until after the children have been added, and then disable listeners from firing
                    # and set it to expanded again.
                    self._enable_state_listeners = False
                    self.con.tree_view.expand_row(path=parent_path, open_all=False)
                    self._enable_state_listeners = True
                else:
                    # Collapsed:
                    self.con.display_store.remove_all_children(parent_iter)
                    # Always have at least a dummy node:
                    logger.debug(f'Collapsing tree: adding loading node')
                    self._append_loading_child(parent_iter)

                logger.debug(f'[{self.con.tree_id}] Displayed rows count: {len(self.con.display_store.displayed_rows)}')
                dispatcher.send(signal=actions.NODE_EXPANSION_DONE, sender=self.con.tree_id)
        GLib.idle_add(expand_or_contract)

    def _on_node_upserted_in_cache(self, sender: str, node: DisplayNode):
        assert node is not None
        if not self._enable_state_listeners:
            # TODO: is this still necessary here now that we use a lock?
            logger.debug(f'[{self.con.tree_id}] Ignoring signal "{actions.NODE_UPSERTED}: listeners disabled"')
            return

        def update_ui():
            with self._lock:
                # TODO: this can be optimized to search only the paths of the ancestors
                parent = self.con.get_tree().get_parent_for_node(node)

                if logger.isEnabledFor(logging.DEBUG):
                    if parent:
                        text = 'Received'
                    else:
                        text = 'Ignoring'
                    logger.debug(f'[{self.con.tree_id}] {text} signal {actions.NODE_UPSERTED} with node {node}')

                if not parent:
                    # logger.debug(f'[{self.con.tree_id}] No parent for node: {node}')
                    return

                parent_uid = parent.uid

                existing_node: Optional[DisplayNode] = None

                try:
                    if self.con.get_tree().root_uid == parent_uid:
                        # Top level? Special case. There will be no parent iter
                        parent_iter = None
                        child_iter = self.con.display_store.find_uid_in_top_level(node.uid)
                        if child_iter:
                            existing_node = self.con.display_store.get_node_data(child_iter)
                    else:
                        parent_iter = self.con.display_store.find_uid_in_tree(target_uid=parent_uid)
                        if not parent_iter:
                            # Probably an ancestor isn't expanded. Just skip
                            assert parent_uid not in self.con.display_store.displayed_rows, \
                                f'DisplayedRows ({self.con.display_store.displayed_rows}) contains UID ({parent_uid})!'
                            logger.debug(f'[{self.con.tree_id}] Will not add/update node: Could not find parent node in display tree: {parent}')
                            return
                        parent_path = self.con.display_store.model.get_path(parent_iter)
                        if not self.con.tree_view.row_expanded(parent_path):
                            logger.debug(f'[{self.con.tree_id}] Will not add/update node {node.uid}: Parent is not expanded: {parent.uid}')
                            return

                        # Check whether the "added node" already exists:
                        child_iter = self.con.display_store.find_uid_in_children(node.uid, parent_iter)
                        if child_iter:
                            existing_node = self.con.display_store.get_node_data(child_iter)

                    if existing_node:
                        logger.debug(f'[{self.con.tree_id}] Node already exists in tree (uid={node.uid}): doing an update instead')
                        display_vals: list = self.generate_display_cols(parent_iter, node)
                        for col, val in enumerate(display_vals):
                            self.con.display_store.model.set_value(child_iter, col, val)
                    else:
                        # New node
                        if node.is_dir():
                            self._append_dir_node_and_loading_child(parent_iter, node)
                        else:
                            self._append_file_node(parent_iter, node)
                finally:
                    # We want to refresh the stats, even if the node is not displayed (see return statements above)
                    self._stats_refresh_timer.start_or_delay()

        GLib.idle_add(update_ui)

    def _on_node_removed_from_cache(self, sender: str, node: DisplayNode):
        if not self._enable_state_listeners:
            # TODO: is this still necessary here now that we use a lock?
            logger.debug(f'[{self.con.tree_id}] Ignoring signal "{actions.NODE_REMOVED}: listeners disabled"')
            return

        def update_ui():
            with self._lock:
                displayed_item = self.con.display_store.displayed_rows.get(node.uid, None)
                if logger.isEnabledFor(logging.DEBUG):
                    if displayed_item:
                        text = 'Received'
                    else:
                        text = 'Ignoring'
                    logger.debug(f'[{self.con.tree_id}] {text} signal {actions.NODE_REMOVED} with node {node.node_identifier}')

                if not displayed_item:
                    return

                self.con.display_store.remove_from_lists(node.uid)

                # TODO: this can be optimized to search only the paths of the ancestors
                tree_iter = self.con.display_store.find_uid_in_tree(target_uid=displayed_item.uid)
                if not tree_iter:
                    raise RuntimeError(f'[{self.con.tree_id}] Cannot remove node: Could not find node in display tree: {displayed_item}')

                logger.debug(f'[{self.con.tree_id}] Removing node from display store: {displayed_item.uid}')
                self.con.display_store.model.remove(tree_iter)
                logger.debug(f'[{self.con.tree_id}] Node removed: {displayed_item.uid}')

                self._stats_refresh_timer.start_or_delay()

        GLib.idle_add(update_ui)

    def _refresh_subtree_stats(self):
        # Requests the cacheman to recalculate stats for this subtree. Sends actions.REFRESH_SUBTREE_STATS_DONE when done
        logger.debug(f'[{self.con.tree_id}] Sending signal: "{actions.REFRESH_SUBTREE_STATS}"')
        dispatcher.send(signal=actions.REFRESH_SUBTREE_STATS, sender=self.con.tree_id)

    def _on_subtree_stats_updated(self, sender: str):
        """Should be called after the parent tree has had its stats refreshed. This will update all the displayed nodes
        with the current values from the cache."""
        logger.debug(f'[{self.con.tree_id}] Got signal: "{actions.REFRESH_SUBTREE_STATS_DONE}"')

        def redraw_displayed_node(tree_iter):
            if self.con.app.shutdown:
                # try to prevent junk errors during shutdown
                return

            ds = self.con.display_store
            node: DisplayNode = ds.get_node_data(tree_iter)
            if not node:
                par_iter = ds.model.iter_parent(tree_iter)
                par_node: DisplayNode = ds.get_node_data(par_iter)
                logger.error(f'[{self.con.tree_id}] No node for child of {par_node}')
                return
            assert node, f'For tree_id="{sender} and row={self.con.display_store.model[tree_iter]}'
            if node.is_ephemereal():
                return

            assert id(self.con.app.cache_manager.get_node_for_uid(node.uid, node.get_tree_type())) == id(node)
            # TODO: remove the below commented-out code when it's clear this is stable
            # Need to get fresh node from cacheman. This is crucial for just-updated nodes (e.g. from completed operations)
            # updated_node = self.con.app.cache_manager.get_node_for_uid(node.uid, node.get_tree_type())
            # if not updated_node:
            #     raise RuntimeError(f'Could not find up-to-date node in memcache: {node}')
            # logger.debug(f'NodeID={hex(id(node))}; UpdatedNodeID={hex(id(updated_node))} EQ={id(node) == id(updated_node)}')
            # node = updated_node

            logger.debug(f'Redrawing stats for node: {node}; path={ds.model.get_path(tree_iter)}; size={node.get_size_bytes()} etc={node.get_etc()}')
            ds.model[tree_iter][self.con.treeview_meta.col_num_size] = _format_size_bytes(node)
            ds.model[tree_iter][self.con.treeview_meta.col_num_etc] = node.get_etc()
            ds.model[tree_iter][self.con.treeview_meta.col_num_data] = node

        def do_in_ui():
            with self._lock:
                if not self.con.app.shutdown:
                    self.con.display_store.recurse_over_tree(action_func=redraw_displayed_node)
                    logger.debug(f'[{self.con.tree_id}] Completely done redrawing display tree stats in UI')
                    # currently this is only used for functional tests
                    dispatcher.send(signal=actions.REFRESH_SUBTREE_STATS_COMPLETELY_DONE, sender=self.con.tree_id)

        logger.debug(f'[{self.con.tree_id}] Redrawing display tree stats in UI')
        GLib.idle_add(do_in_ui)
        # Refresh summary also:
        dispatcher.send(signal=actions.SET_STATUS, sender=self.con.tree_id, status_msg=self.con.get_tree().get_summary())

    # ▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲
    # LISTENERS end

    def _set_expand_states_from_config(self):
        # Loop over top level. Find the category nodes and expand them appropriately
        tree_iter = self.con.display_store.model.get_iter_first()
        while tree_iter is not None:
            node_data = self.con.display_store.get_node_data(tree_iter)
            if type(node_data) == CategoryNode:
                is_expand = self.con.treeview_meta.is_category_node_expanded(node_data)
                if is_expand:
                    logger.debug(f'[{self.con.tree_id}] Expanding row: {node_data.name} in tree {self.con.tree_id}')
                    tree_path = self.con.display_store.model.get_path(tree_iter)
                    self.con.tree_view.expand_row(path=tree_path, open_all=False)

            tree_iter = self.con.display_store.model.iter_next(tree_iter)

        logger.debug(f'[{self.con.tree_id}] Displayed rows count: {len(self.con.display_store.displayed_rows)}')

    def _append_dir_node_and_loading_child(self, parent_iter, node_data: DisplayNode):
        dir_node_iter = self._append_dir_node(parent_iter, node_data)
        self._append_loading_child(dir_node_iter)
        return dir_node_iter

    def _append_children(self, children: List[DisplayNode], parent_iter: Gtk.TreeIter):
        if children:
            logger.debug(f'[{self.con.tree_id}] Appending {len(children)} child display nodes')
            if len(children) > LARGE_NUMBER_OF_CHILDREN:
                logger.error(f'[{self.con.tree_id}] Too many children to display! Count = {len(children)}')
                self._append_empty_child(parent_iter, f'ERROR: too many items to display ({len(children):n})')
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

    def _append_empty_child(self, parent_node_iter, node_name):
        row_values = []
        if self.con.treeview_meta.has_checkboxes:
            row_values.append(False)  # Checked
            row_values.append(False)  # Inconsistent
        row_values.append(None)  # Icon
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

    def _get_icon_for_node(self, node: DisplayNode) -> str:
        op: Op = self.con.app.cache_manager.get_last_pending_op_for_node(node.uid)
        if op and not op.is_completed():
            logger.debug(f'[{self.con.tree_id}] Found pending op for node {node.uid}: {op.op_type.name}')
            icon = op.get_icon_for_node(node.uid)
        else:
            icon = node.get_icon()
        # logger.debug(f'[{self.con.tree_id}] Got icon "{icon}" for node {node}')
        return icon

    def generate_display_cols(self, parent_iter, node: DisplayNode):
        """Serializes a node into a list of strings which tell the TreeView how to populate each of the row's columns"""
        row_values = []

        self._add_checked_columns(parent_iter, node, row_values)

        # Icon: can vary based on pending actions

        row_values.append(self._get_icon_for_node(node))

        # Name
        node_name = node.name
        row_values.append(node_name)  # Name

        # Directory
        if not self.con.treeview_meta.use_dir_tree:
            directory, name = os.path.split(node.full_path)
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

    def _append_dir_node(self, parent_iter, node: DisplayNode) -> TreeIter:
        row_values = self.generate_display_cols(parent_iter, node)
        return self.con.display_store.append_node(parent_iter, row_values)

    def _append_file_node(self, parent_iter, node: DisplayNode):
        row_values = self.generate_display_cols(parent_iter, node)
        return self.con.display_store.append_node(parent_iter, row_values)

    def _add_checked_columns(self, parent_iter, node: DisplayNode, row_values: List):
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


def _format_size_bytes(node: DisplayNode):
    if not node.get_size_bytes():
        return None
    else:
        return humanfriendly.format_size(node.get_size_bytes())
