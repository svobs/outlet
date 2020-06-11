import collections
import logging
import os
from datetime import datetime
from typing import Deque, Iterable, List, Optional

import gi

from constants import LARGE_NUMBER_OF_CHILDREN

gi.require_version("Gtk", "3.0")
from gi.repository import GLib
from gi.repository.Gtk import TreeIter
from pydispatch import dispatcher
import humanfriendly

from model.display_node import CategoryNode, DisplayNode, EmptyNode, LoadingNode
from model.planning_node import FileToMove
from ui import actions


logger = logging.getLogger(__name__)

"""
#    CLASS LazyDisplayStrategy
# SubtreeSnapshot --> TreeBuilder --> treelib.Tree  --> LazyDisplayStrategy --> DisplayStore (TreeModel)
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
"""


class LazyDisplayStrategy:
    """
    TODO: when does the number of display nodes start to slow down? -> add config for live node maximum
    """
    def __init__(self, config, controller=None):
        self.con = controller
        self.use_empty_nodes = True

        self._enable_state_listeners = False
        """When true, listen and automatically update the display when a node is added, expanded, etc.
        Needs to be set to false when modifying the model with certain operations, because leaving enabled would
        result in an infinite loop"""

    def init(self):
        """Do post-wiring stuff like connect listeners."""
        self.use_empty_nodes = self.con.config.get('display.diff_tree.use_empty_nodes')

        if self.con.treeview_meta.lazy_load:
            dispatcher.connect(signal=actions.NODE_EXPANSION_TOGGLED, receiver=self._on_node_expansion_toggled,
                               sender=self.con.treeview_meta.tree_id)

        dispatcher.connect(signal=actions.NODE_ADDED_OR_UPDATED, receiver=self._on_node_added_or_updated_in_cache)
        dispatcher.connect(signal=actions.NODE_REMOVED, receiver=self._on_node_removed_from_cache)
        dispatcher.connect(signal=actions.REFRESH_ALL_NODE_STATS, receiver=self._on_refresh_all_node_stats)

    def populate_recursively(self, parent_iter, node: DisplayNode):
        node_count = self._populate_recursively(parent_iter, node)
        logger.debug(f'[{self.con.tree_id}] Populated {node_count} nodes')

    def _populate_recursively(self, parent_iter, node: DisplayNode, node_count: int = 0) -> int:
        total_expanded = 0
        # Do a DFS of the change tree and populate the UI tree along the way
        if node.is_dir():
            parent_iter = self._append_dir_node(parent_iter=parent_iter, node=node)

            for child in self.con.tree_builder.get_children(node):
                node_count = self._populate_recursively(parent_iter, child, node_count)
        else:
            self._append_file_node(parent_iter, node)

        node_count += 1
        return node_count

    def expand_all(self, tree_path):
        def expand_me(tree_iter):
            path = self.con.display_store.model.get_path(tree_iter)
            self.con.display_strategy.expand_subtree(path)

        if not self.con.tree_view.row_expanded(tree_path):
            self.con.display_strategy.expand_subtree(tree_path)
        else:
            self.con.display_store.do_for_descendants(tree_path=tree_path, action_func=expand_me)

    def expand_subtree(self, tree_path):
        if not self.con.treeview_meta.lazy_load:
            # no need to populate. just expand:
            self.con.tree_view.expand_row(path=tree_path, open_all=True)
            return

        if self.con.tree_view.row_expanded(tree_path):
            return

        self._enable_state_listeners = False

        node = self.con.display_store.get_node_data(tree_path)
        parent_iter = self.con.display_store.model.get_iter(tree_path)
        # Remove loading node:
        self.con.display_store.remove_first_child(parent_iter)

        children: List[DisplayNode] = self.con.tree_builder.get_children(node)
        for child in children:
            self.populate_recursively(parent_iter, child)

        self.con.tree_view.expand_row(path=tree_path, open_all=True)

        self._enable_state_listeners = True

    def populate_root(self):
        """Draws from the undelying data store as needed, to populate the display store."""

        self._enable_state_listeners = False

        # This may be a long task
        children: List[DisplayNode] = self.con.tree_builder.get_children_for_root()

        def update_ui():
            # Wipe out existing items:
            root_iter = self.con.display_store.clear_model()

            if self.con.treeview_meta.lazy_load:
                # Just append for the root level. Beyond that, we will only load more nodes when
                # the expanded state is toggled
                self._append_children(children=children, parent_iter=root_iter)
            else:
                for ch in children:
                    self.populate_recursively(None, ch)

                self.con.tree_view.expand_all()

            self._enable_state_listeners = True

            # This should fire expanded state listener to populate nodes as needed:
            if self.con.treeview_meta.is_display_persisted:
                self._set_expand_states_from_config()

        GLib.idle_add(update_ui)

        # Show tree summary:
        actions.set_status(sender=self.con.treeview_meta.tree_id,
                           status_msg=self.con.get_tree().get_summary())

    def get_checked_rows_as_list(self) -> List[DisplayNode]:
        """Returns a list which contains the DisplayNodes of the items which are currently checked by the user
        (including collapsed rows). This will be a subset of the SubtreeSnapshot which was used to
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

        children: Iterable[DisplayNode] = self.con.tree_builder.get_children_for_root()
        for child in children:

            if self.con.display_store.checked_rows.get(child.identifier, None):
                whitelist.append(child)
            elif self.con.display_store.inconsistent_rows.get(child.identifier, None):
                secondary_screening.append(child)

        while len(secondary_screening) > 0:
            parent: DisplayNode = secondary_screening.popleft()
            assert parent.is_dir(), f'Expected a dir-type node: {parent}'

            if not parent.is_just_fluff():
                # Even an inconsistent FolderToAdd must be included as a checked item:
                checked_items.append(parent)

            children: Iterable[DisplayNode] = self.con.tree_builder.get_children(parent)

            for child in children:
                if self.con.display_store.checked_rows.get(child.identifier, None):
                    whitelist.append(child)
                elif self.con.display_store.inconsistent_rows.get(child.identifier, None):
                    secondary_screening.append(child)

        while len(whitelist) > 0:
            chosen_node: DisplayNode = whitelist.popleft()
            if not chosen_node.is_dir() or not chosen_node.is_just_fluff():
                checked_items.append(chosen_node)

            children: Iterable[DisplayNode] = self.con.tree_builder.get_children(chosen_node)
            for child in children:
                whitelist.append(child)

        return checked_items

    # LISTENERS begin
    # ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼

    def _on_node_expansion_toggled(self, sender: str, parent_iter, node_data: DisplayNode, is_expanded: bool):
        # Callback for actions.NODE_EXPANSION_TOGGLED:
        logger.debug(f'[{self.con.tree_id}] Node expansion toggled to {is_expanded} for {node_data}"')

        if not self._enable_state_listeners:
            logger.debug('Auto-populate disabled')
            return

        def expand_or_contract():
            # Add children for node:
            if is_expanded:
                children = self.con.tree_builder.get_children(node_data)
                self._append_children(children=children, parent_iter=parent_iter)
                # Remove Loading node:
                self.con.display_store.remove_first_child(parent_iter)
            else:
                # Collapsed:
                self.con.display_store.remove_all_children(parent_iter)
                # Always have at least a dummy node:
                self._append_loading_child(parent_iter)

            logger.debug(f'Displayed rows count: {len(self.con.display_store.displayed_rows)}')
        GLib.idle_add(expand_or_contract)

    def _on_node_added_or_updated_in_cache(self, sender: str, node: DisplayNode):
        assert node is not None
        if not self._enable_state_listeners:
            return

        logger.debug(f'[{self.con.tree_id}] Received signal {actions.NODE_ADDED_OR_UPDATED} with node {node.node_identifier}')

        # TODO: this can be optimized to search only the paths of the ancestors
        parent = self.con.get_tree().get_parent_for_item(node)
        if not parent:
            logger.debug(f'[{self.con.tree_id}] Ignoring added node because its parent does not appear to be in this tree')
            return

        parent_uid = parent.uid
        parent_iter = self.con.display_store.find_in_tree(target_uid=parent_uid)
        if not parent_iter:
            # FIXME: this breaks for root-level nodes
            raise RuntimeError(f'[{self.con.tree_id}] Cannot add node: Could not find parent node in display tree: {parent}')

        # Check whether the "added node" already exists:
        child_iter = self.con.display_store.find_in_children(node.uid, parent_iter)
        if child_iter:
            existing_node: DisplayNode = self.con.display_store.get_node_data(child_iter)
            if existing_node:
                logger.debug(f'[{self.con.tree_id}] Node already exists in tree (uid={node.uid}): doing an update instead')
                display_vals: list = self.generate_display_cols(parent_iter, node)
                for col, val in enumerate(display_vals):
                    self.con.display_store.model.set_value(child_iter, col, val)
                return

        if node.is_dir():
            self._append_dir_node(parent_iter, node)
        else:
            self._append_file_node(parent_iter, node)

    def _on_node_removed_from_cache(self, sender: str, node: DisplayNode):
        if not self._enable_state_listeners:
            return

        logger.debug(f'[{self.con.tree_id}] Received signal {actions.NODE_REMOVED} with node {node.node_identifier}')
        if not self.con.display_store.displayed_rows.get(node.uid, None):
            return

        # TODO: this can be optimized to search only the paths of the ancestors
        tree_iter = self.con.display_store.find_in_tree(target_uid=node.uid)
        if not tree_iter:
            raise RuntimeError(f'Cannot remove node: Could not find node in display tree: {node}')

        self.con.display_store.model.remove(tree_iter)

    def _on_refresh_all_node_stats(self, sender: str):
        def refresh_node(tree_iter):
            ds = self.con.display_store
            data: DisplayNode = ds.get_node_data(tree_iter)
            assert data, f'For tree_id="{sender} and row={self.con.display_store.model[tree_iter]}'
            ds.model[tree_iter][self.con.treeview_meta.col_num_size] = _format_size_bytes(data)
            ds.model[tree_iter][self.con.treeview_meta.col_num_etc] = data.etc

        self.con.display_store.recurse_over_tree(action_func=refresh_node)

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
                    tree_path = self.con.display_store.model.get_path(tree_iter)
                    logger.debug(f'[{self.con.tree_id}] Expanding row: {node_data.name} in tree {self.con.tree_id}')
                    self.con.tree_view.expand_row(path=tree_path, open_all=True)
                    # FIXME! Open-all not respected!

            tree_iter = self.con.display_store.model.iter_next(tree_iter)

        logger.debug(f'Displayed rows count: {len(self.con.display_store.displayed_rows)}')

    def _append_dir_node_and_empty_child(self, parent_iter, node_data: DisplayNode):
        dir_node_iter = self._append_dir_node(parent_iter, node_data)
        self._append_loading_child(dir_node_iter)
        return dir_node_iter

    def _append_children(self, children: List[DisplayNode], parent_iter):
        if children:
            logger.debug(f'[{self.con.tree_id}] Appending {len(children)} child display nodes')
            if len(children) > LARGE_NUMBER_OF_CHILDREN:
                logger.error(f'[{self.con.tree_id}] Too many children to display! Count = {len(children)}')
                self._append_empty_child(parent_iter, f'ERROR: too many items to display ({len(children):n})')
                return
            # Append all underneath tree_iter
            for child in children:
                if child.is_dir():
                    self._append_dir_node_and_empty_child(parent_iter, child)
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

    def generate_display_cols(self, parent_iter, node: DisplayNode):
        row_values = []

        self._add_checked_columns(parent_iter, node, row_values)

        # Icon
        row_values.append(node.get_icon())

        # Name
        # TODO: find more elegant solution
        if isinstance(node, FileToMove):
            node_name = f'{node.original_full_path} -> "{node.name}"'
        else:
            node_name = node.name
        row_values.append(node_name)  # Name

        # Directory
        if not self.con.treeview_meta.use_dir_tree:
            directory, name = os.path.split(node.full_path)
            row_values.append(directory)  # Directory

        # Size Bytes

        row_values.append(_format_size_bytes(node))  # Size

        # etc
        row_values.append(node.etc)  # Etc

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
        if self.con.treeview_meta.has_checkboxes and node.has_path():
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
    if not node.size_bytes:
        return None
    else:
        return humanfriendly.format_size(node.size_bytes)
