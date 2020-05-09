import logging
import os
from datetime import datetime
from queue import Queue
from typing import List, Optional

import gi

from constants import LARGE_NUMBER_OF_CHILDREN, TreeDisplayMode
from model.display_id import Identifier
from model.subtree_snapshot import SubtreeSnapshot

gi.require_version("Gtk", "3.0")
from gi.repository import GLib
from gi.repository.Gtk import TreeIter
from pydispatch import dispatcher
import humanfriendly

from model.display_node import CategoryNode, DisplayNode, EmptyNode, LoadingNode
from model.planning_node import FileToMove
from ui import actions


logger = logging.getLogger(__name__)

#    CLASS LazyDisplayStrategy
# ⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟


class LazyDisplayStrategy:
    """
    - Need to create a store which can keep track of whether each parent has all children. If not we
    will have to make a request to retrieve all nodes with 'X' as parent and update the store before
    returning

    - GoogRemote >= GoogDiskStores >= GoogInMemoryStore >= DisplayStore

    - GoogDiskCache should try to download all dirs & files ASAP. But in the meantime, download level by level

    - Every time you expand a node, you should call to sync it from the GoogStore.
    - Every time you retrieve new data from G, you must perform sanity checks on it and proactively

    TODO: TBD: when does the number of display nodes start to slow down? -> add config for live node maximum
    """
    def __init__(self, config, controller=None):
        self.con = controller
        self.use_empty_nodes = True

    def populate_root(self):
        """Draws from the undelying data store as needed, to populate the display store."""

        # This may be a long task
        children = self.con.tree_builder.get_children_for_root()

        def update_ui():
            # Wipe out existing items:
            root_iter = self.con.display_store.clear_model()

            self._append_children(children=children, parent_iter=root_iter, parent_uid=None)

            # This should fire expanded state listener to populate nodes as needed:
            if self.con.treeview_meta.is_display_persisted:
                self._set_expand_states_from_config()

        GLib.idle_add(update_ui)

        # Show tree summary:
        actions.set_status(sender=self.con.treeview_meta.tree_id,
                           status_msg=self.con.get_tree().get_summary())

    def init(self):
        """Do post-wiring stuff like connect listeners."""
        self.use_empty_nodes = self.con.config.get('display.diff_tree.use_empty_nodes')

        dispatcher.connect(signal=actions.NODE_EXPANSION_TOGGLED, receiver=self._on_node_expansion_toggled, sender=self.con.treeview_meta.tree_id)

    def get_checked_rows_as_tree(self) -> SubtreeSnapshot:
        """Returns a SubtreeSnapshot which contains the DisplayNodes of the rows which are currently
        checked by the user (including collapsed rows). This will be a subset of the SubtreeSnapshot which was used to
        populate this tree. Includes file nodes only; does not include directory nodes."""
        assert self.con.treeview_meta.has_checkboxes
        # subtree root will be the same as the current subtree's
        subtree: SubtreeSnapshot = self.con.get_tree().create_empty_subtree(self.con.get_tree().root_node)

        # Algorithm:
        # Iterate over display nodes. Start with top-level nodes.
        # - Add each checked row to DFS queue. It and all of its descendants will be added.
        # - Ignore each unchecked row
        # - Each inconsistent row needs to be drilled down into.
        whitelist = Queue()
        secondary_screening = Queue()

        children = self.con.tree_builder.get_children_for_root()
        for child in children:
            if self.con.display_store.checked_rows.get(child.uid, None):
                whitelist.put(child)
            elif self.con.display_store.inconsistent_rows.get(child.uid, None):
                secondary_screening.put(child)

        while not secondary_screening.empty():
            parent: DisplayNode = secondary_screening.get()
            children = self.con.tree_builder.get_children(parent)
            for child in children:
                if self.con.display_store.checked_rows.get(child.uid, None):
                    whitelist.put(child)
                elif self.con.display_store.inconsistent_rows.get(child.uid, None):
                    secondary_screening.put(child)

        while not whitelist.empty():
            chosen_node: DisplayNode = whitelist.get()
            if not chosen_node.is_dir():
                subtree.add_item(chosen_node)
            children = self.con.tree_builder.get_children(chosen_node)
            for child in children:
                whitelist.put(child)

        return subtree

    # LISTENERS begin
    # ⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟

    def _on_node_expansion_toggled(self, sender: str, parent_iter, node_data: DisplayNode, is_expanded: bool):
        # Callback for actions.NODE_EXPANSION_TOGGLED:
        logger.debug(f'Node expansion toggled to {is_expanded} for cat={node_data.category} id="{node_data.uid}" tree_id={sender}')

        if not self.con.treeview_meta.lazy_load:
            return

        def expand_or_contract():
            # Add children for node:
            if is_expanded:
                children = self.con.tree_builder.get_children(node_data.identifier)
                self._append_children(children=children, parent_iter=parent_iter, parent_uid=node_data.uid)
                # Remove Loading node:
                self.con.display_store.remove_first_child(parent_iter)
            else:
                # Collapsed:
                self.con.display_store.remove_all_children(parent_iter)
                # Always have at least a dummy node:
                self._append_loading_child(parent_iter)
        GLib.idle_add(expand_or_contract)

    # ⮝⮝⮝⮝⮝⮝⮝⮝⮝⮝⮝⮝⮝⮝⮝⮝⮝⮝⮝⮝⮝⮝⮝⮝⮝⮝⮝⮝⮝⮝⮝⮝⮝⮝⮝⮝⮝⮝⮝⮝⮝⮝⮝
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
                    logger.info(f'Expanding row: {node_data.name} in tree {self.con.tree_id}')
                    self.con.tree_view.expand_row(path=tree_path, open_all=True)
                    # FIXME! Open-all not respected!

            tree_iter = self.con.display_store.model.iter_next(tree_iter)

    def _append_dir_node_and_empty_child(self, parent_iter, parent_uid: Optional[str], node_data: DisplayNode):
        dir_node_iter = self._append_dir_node(parent_iter, parent_uid, node_data)
        self._append_loading_child(dir_node_iter)
        return dir_node_iter

    def _append_children(self, children: List[DisplayNode], parent_iter, parent_uid: Optional[str]):
        if children:
            logger.debug(f'Appending {len(children)} child display nodes')
            if len(children) > LARGE_NUMBER_OF_CHILDREN:
                logger.error(f'Too many children to display! Count = {len(children)}')
                self._append_empty_child(parent_iter, f'ERROR: too many items to display ({len(children):n})')
                return
            # Append all underneath tree_iter
            for child in children:
                if child.is_dir():
                    self._append_dir_node_and_empty_child(parent_iter, parent_uid, child)
                else:
                    self._append_file_node(parent_iter, parent_uid, child)
        elif self.use_empty_nodes:
            self._append_empty_child(parent_iter, '(empty)')

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
        row_values.append(None)  # Modify Date
        row_values.append(None)  # Created Date
        row_values.append(EmptyNode())

        return self.con.display_store.model.append(parent_node_iter, row_values)

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
        row_values.append(None)  # Modify Date
        row_values.append(None)  # Created Date
        row_values.append(LoadingNode())

        return self.con.display_store.model.append(parent_node_iter, row_values)

    def _add_checked_columns(self, parent_uid: Optional[str], node_data: DisplayNode, row_values: List):
        if self.con.treeview_meta.has_checkboxes and node_data.has_path():
            if parent_uid:
                parent_checked = self.con.display_store.checked_rows.get(parent_uid, None)
                if parent_checked:
                    row_values.append(True)  # Checked
                    row_values.append(False)  # Inconsistent
                    return
                parent_inconsistent = self.con.display_store.inconsistent_rows.get(parent_uid, None)
                if not parent_inconsistent:
                    row_values.append(False)  # Checked
                    row_values.append(False)  # Inconsistent
                    return
                # Otherwise: inconsistent. Look up individual values below:
            row_id = node_data.identifier.uid
            checked = self.con.display_store.checked_rows.get(row_id, None)
            inconsistent = self.con.display_store.inconsistent_rows.get(row_id, None)
            row_values.append(checked)  # Checked
            row_values.append(inconsistent)  # Inconsistent

    def _append_dir_node(self, parent_iter, parent_uid: Optional[str], node_data: DisplayNode) -> TreeIter:
        """Appends a dir-type node to the model"""
        row_values = []

        self._add_checked_columns(parent_uid, node_data, row_values)

        # Icon
        row_values.append(node_data.get_icon())

        row_values.append(node_data.name)  # Name

        if not self.con.treeview_meta.use_dir_tree:
            row_values.append(None)  # Directory

        if not node_data.size_bytes:
            num_bytes_formatted = None
        else:
            num_bytes_formatted = humanfriendly.format_size(node_data.size_bytes)
        row_values.append(num_bytes_formatted)  # Size

        row_values.append(None)  # Modify Date

        if self.con.treeview_meta.show_change_ts:
            row_values.append(None)  # Changed (UNIX) / Created (GOOG) Date

        row_values.append(node_data)  # Data

        return self.con.display_store.model.append(parent_iter, row_values)

    def _append_file_node(self, parent_iter, parent_uid: Optional[str], node_data: DisplayNode):
        row_values = []

        # Checked State
        self._add_checked_columns(parent_uid, node_data, row_values)

        # Icon
        row_values.append(node_data.get_icon())

        # Name
        # TODO: find more elegant solution
        if isinstance(node_data, FileToMove):
            node_name = f'{node_data.original_full_path} -> "{node_data.name}"'
        else:
            node_name = node_data.name
        row_values.append(node_name)  # Name

        # Directory
        if not self.con.treeview_meta.use_dir_tree:
            directory, name = os.path.split(node_data.full_path)
            row_values.append(directory)  # Directory

        # Size Bytes
        if not node_data.size_bytes:
            num_bytes_formatted = None
        else:
            num_bytes_formatted = humanfriendly.format_size(node_data.size_bytes)
        row_values.append(num_bytes_formatted)  # Size

        # Modify TS
        if node_data.modify_ts is None:
            row_values.append(None)
        else:
            modify_datetime = datetime.fromtimestamp(node_data.modify_ts / 1000)
            modify_formatted = modify_datetime.strftime(self.con.treeview_meta.datetime_format)
            row_values.append(modify_formatted)

        # Change TS
        if self.con.treeview_meta.show_change_ts:
            try:
                change_datetime = datetime.fromtimestamp(node_data.change_ts / 1000)
                change_time = change_datetime.strftime(self.con.treeview_meta.datetime_format)
                row_values.append(change_time)
            except AttributeError:
                row_values.append(None)

        # Data (hidden)
        row_values.append(node_data)  # Data

        return self.con.display_store.model.append(parent_iter, row_values)
