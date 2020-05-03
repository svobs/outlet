import logging
import os
from datetime import datetime

import gi

from model.display_id import DisplayId

gi.require_version("Gtk", "3.0")
from gi.repository import GLib
from gi.repository.Gtk import TreeIter
from pydispatch import dispatcher
import humanfriendly

from model.display_node import CategoryNode, DisplayNode, EmptyNode, LoadingNode
from model.planning_node import FileToMove
from ui import actions


logger = logging.getLogger(__name__)


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
    def __init__(self, controller=None):
        self.con = controller
        self.use_empty_nodes = True

    def populate_root(self):
        """Draws from the undelying data store as needed, to populate the display store."""
        # This may be a long task
        children = self.con.meta_store.get_children(parent_id=None)

        def update_ui():
            # Wipe out existing items:
            root_iter = self.con.display_store.clear_model()

            self._append_children(children, root_iter, None)

            # This should fire expanded state listener to populate nodes as needed:
            if self.con.treeview_meta.is_display_persisted:
                self._set_expand_states_from_config()

        GLib.idle_add(update_ui)

    def init(self):
        """Do post-wiring stuff like connect listeners."""
        self.use_empty_nodes = self.con.config.get('display.diff_tree.use_empty_nodes')

        dispatcher.connect(signal=actions.ROOT_PATH_UPDATED, receiver=self._on_root_path_updated, sender=self.con.meta_store.tree_id)
        dispatcher.connect(signal=actions.NODE_EXPANSION_TOGGLED, receiver=self._on_node_expansion_toggled, sender=self.con.meta_store.tree_id)

    def _on_root_path_updated(self, sender, new_root, tree_type):
        # Callback for actions.ROOT_PATH_UPDATED
        # Get a new metastore from the cache manager:
        cacheman = self.con.parent_win.application.cache_manager
        self.con.meta_store = cacheman.get_metastore_for_subtree(new_root, tree_type, self.con.meta_store.tree_id)

    def _on_node_expansion_toggled(self, sender, parent_iter, node_data, is_expanded):
        # Callback for actions.NODE_EXPANSION_TOGGLED:
        logger.debug(f'Node expansion toggled to {is_expanded} for cat={node_data.category} id="{node_data.display_id}" tree_id={sender}')

        if not self.con.meta_store.is_lazy():
            return

        def expand_or_contract():
            # Add children for node:
            if is_expanded:
                children = self.con.meta_store.get_children(node_data.display_id)
                self._append_children(children, parent_iter, node_data.display_id)
                # Remove Loading node:
                self.con.display_store.remove_first_child(parent_iter)
            else:
                # Collapsed:
                self.con.display_store.remove_all_children(parent_iter)
                # Always have at least a dummy node:
                self._append_loading_child(parent_iter)
        GLib.idle_add(expand_or_contract)

    def _set_expand_states_from_config(self):
        # Loop over top level. Find the category nodes and expand them appropriately
        tree_iter = self.con.display_store.model.get_iter_first()
        while tree_iter is not None:
            node_data = self.con.display_store.get_node_data(tree_iter)
            if type(node_data) == CategoryNode:
                is_expand = self.con.treeview_meta.is_category_node_expanded(node_data)
                if is_expand:
                    tree_path = self.con.display_store.model.get_path(tree_iter)
                    logger.info(f'Expanding row: {node_data.get_name()} in tree {self.con.tree_id}')
                    self.con.tree_view.expand_row(path=tree_path, open_all=True)
                    # FIXME! Open-all not respected!

            tree_iter = self.con.display_store.model.iter_next(tree_iter)

    def _append_dir_node_and_empty_child(self, parent_iter, parent_display_id, node_data):
        dir_node_iter = self._append_dir_node(parent_iter, parent_display_id, node_data)
        self._append_loading_child(dir_node_iter)
        return dir_node_iter

    def _append_children(self, children, parent_iter, parent_display_id: DisplayId):
        if children:
            logger.debug(f'Filling out display children: {len(children)}')
            # Append all underneath tree_iter
            for child in children:
                if child.is_dir():
                    self._append_dir_node_and_empty_child(parent_iter, parent_display_id, child)
                else:
                    self._append_file_node(parent_iter, parent_display_id, child)
        elif self.use_empty_nodes:
            self._append_empty_child(parent_iter)

    def _append_empty_child(self, parent_node_iter):
        row_values = []
        if self.con.treeview_meta.editable:
            row_values.append(False)  # Checked
            row_values.append(False)  # Inconsistent
        row_values.append(None)  # Icon
        row_values.append('(empty)')  # Name
        if not self.con.treeview_meta.use_dir_tree:
            row_values.append(None)  # Directory
        row_values.append(None)  # Size
        row_values.append(None)  # Modify Date
        row_values.append(None)  # Created Date
        row_values.append(EmptyNode())

        return self.con.display_store.model.append(parent_node_iter, row_values)

    def _append_loading_child(self, parent_node_iter):
        row_values = []
        if self.con.treeview_meta.editable:
            row_values.append(False)  # Checked
            row_values.append(False)  # Inconsistent
        row_values.append('folder')  # Icon
        row_values.append('Loading...')  # Name
        if not self.con.treeview_meta.use_dir_tree:
            row_values.append(None)  # Directory
        row_values.append(None)  # Size
        row_values.append(None)  # Modify Date
        row_values.append(None)  # Created Date
        row_values.append(LoadingNode())

        return self.con.display_store.model.append(parent_node_iter, row_values)

    def _add_checked_columns(self, parent_row_id: DisplayId, node_data, row_values):
        if self.con.treeview_meta.editable and node_data.has_path():
            if parent_row_id:
                parent_checked = self.con.display_store.selected_rows.get(parent_row_id.id_string, None)
                if parent_checked:
                    row_values.append(True)  # Checked
                    row_values.append(False)  # Inconsistent
                    return
                parent_inconsistent = self.con.display_store.inconsistent_rows.get(parent_row_id.id_string, None)
                if not parent_inconsistent:
                    row_values.append(False)  # Checked
                    row_values.append(False)  # Inconsistent
                    return
                # Otherwise: inconsistent. Look up individual values below:
            row_id = node_data.display_id.id_string
            checked = self.con.display_store.selected_rows.get(row_id, None)
            inconsistent = self.con.display_store.inconsistent_rows.get(row_id, None)
            row_values.append(checked)  # Checked
            row_values.append(inconsistent)  # Inconsistent

    def _append_dir_node(self, parent_iter, parent_display_id, node_data: DisplayNode) -> TreeIter:
        """Appends a dir-type node to the model"""
        row_values = []

        self._add_checked_columns(parent_display_id, node_data, row_values)

        # Icon
        row_values.append(node_data.get_icon())

        row_values.append(node_data.get_name())  # Name

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

    def _append_file_node(self, parent_iter, parent_display_id, node_data: DisplayNode):
        row_values = []

        # Checked State
        self._add_checked_columns(parent_display_id, node_data, row_values)

        # Icon
        row_values.append(node_data.get_icon())

        # Name
        # TODO: find more elegant solution
        if isinstance(node_data, FileToMove):
            node_name = f'{node_data.original_full_path} -> "{node_data.get_name()}"'
        else:
            node_name = node_data.get_name()
        row_values.append(node_name)  # Name

        # Directory
        if not self.con.treeview_meta.use_dir_tree:
            # TODO: need to generate full_path for GOOG files
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
            # TODO: TS GOOG vs LocalFS?
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
