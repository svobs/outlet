from gdrive.gdrive_model import NOT_TRASHED
from ui import actions
from ui.assets import ICON_GENERIC_DIR, ICON_GENERIC_FILE, ICON_TRASHED_DIR, ICON_TRASHED_FILE
import logging
from ui.tree.data_store import DisplayStrategy
from datetime import datetime

import humanfriendly

from model.display_model import CategoryNode, EmptyNode, LoadingNode

logger = logging.getLogger(__name__)


class LazyLoadStrategy(DisplayStrategy):
    """
    - Start by listing root nodes
    Phase 1: do not worry about scrolling
    - When a dir node is expanded, a call should be made to the data_store to retrieve its children, which may or may not be cached. But new display nodes will be created when it is expanded (i.e. lazily)
    - Need to create a store which can keep track of whether each parent has all children. If not we will have to make a request to retrieve all nodes with 'X' as parent and update the store before returning
    was last synced (for stats if nothing else)

    - GoogRemote >= GoogDiskCache >= GoogInMemoryCache >= DisplayNode

    - GoogDiskCache should try to download all dirs & files ASAP. But in the meantime, download level by level

    DisplayNode <- CatDisplayNode <- DirDisplayNode <- FileDisplayNode
    (the preceding line does not contain instantiated classes)

    - Every time you expand a node, you should call to sync it from the GoogStor.

    TODO: TBD: when does the number of display nodes start to slow down? -> add config for live node maximum
    -
    - Every time you retrieve new data from G, you must perform sanity checks on it and proactively correct them.
    - - Modify TS, MD5, create date, version, revision - any of these changes should be aggressively logged
    and their meta updated in the data_store,

    Google Drive Stor <- superset of Display Stor
    """

    def __init__(self, controller=None):
        super().__init__(controller)

    def init(self):
        actions.connect(actions.NODE_EXPANSION_TOGGLED, self._on_node_expansion_toggled)

    def _on_node_expansion_toggled(self, sender, parent_iter, node_data, is_expanded):
        # TODO: put this elsewhere
        if type(node_data) == CategoryNode:
            self.con.display_store.display_meta.set_category_node_expanded_state(node_data.category, is_expanded)

        # Add children for node:
        if is_expanded:
            children = self.con.data_store.get_children(node_data.id)
            if children:
                logger.debug(f'Filling out display children: {len(children)}')
                # Append all underneath tree_iter
                for child in children:
                    if child.is_dir():
                        self._append_dir_node_and_dummy_child(parent_iter, child)
                    else:
                        self._append_file_node(parent_iter, child)
                # Remove dummy node:
                self.con.display_store.remove_first_child(parent_iter)
            else:
                self._append_empty_child(parent_iter)
            # Remove Loading node:
            self.con.display_store.remove_first_child(parent_iter)
        else:
            # Collapsed:
            self.con.display_store.remove_all_children(parent_iter)
            # Always have at least a dummy node:
            self._append_loading_child(parent_iter)

    def _append_empty_child(self, parent_node_iter):
        row_values = []
        if self.con.display_store.display_meta.editable:
            row_values.append(False)  # Checked
            row_values.append(False)  # Inconsistent
        row_values.append(None)  # Icon
        row_values.append('(empty)')  # Name
        if not self.con.display_store.display_meta.use_dir_tree:
            row_values.append(None)  # Directory
        row_values.append(None)  # Size
        row_values.append(None)  # Modify Date
        row_values.append(None)  # Created Date
        row_values.append(EmptyNode())

        return self.con.display_store.model.append(parent_node_iter, row_values)

    def _append_loading_child(self, parent_node_iter):
        row_values = []
        if self.con.display_store.display_meta.editable:
            row_values.append(False)  # Checked
            row_values.append(False)  # Inconsistent
        row_values.append('folder')  # Icon
        row_values.append('Loading...')  # Name
        if not self.con.display_store.display_meta.use_dir_tree:
            row_values.append(None)  # Directory
        row_values.append(None)  # Size
        row_values.append(None)  # Modify Date
        row_values.append(None)  # Created Date
        row_values.append(LoadingNode())

        return self.con.display_store.model.append(parent_node_iter, row_values)

    def _append_dir_node_and_dummy_child(self, tree_iter, node_data):
        """Appends a dir or cat node to the model"""
        row_values = []
        if self.con.display_store.display_meta.editable:
            row_values.append(False)  # Checked
            row_values.append(False)  # Inconsistent
        icon_id = ICON_GENERIC_DIR
        try:
            if node_data.trashed != NOT_TRASHED:
                icon_id = ICON_TRASHED_DIR
        except AttributeError:
            pass
        row_values.append(icon_id)  # Icon
        row_values.append(node_data.name)  # Name
        if not self.con.display_store.display_meta.use_dir_tree:
            row_values.append(None)  # Directory
        row_values.append(None)  # Size
        row_values.append(None)  # Modify Date
        row_values.append(None)  # Created Date
        row_values.append(node_data)  # Data

        dir_node_iter = self.con.display_store.model.append(tree_iter, row_values)
        self._append_loading_child(dir_node_iter)
        return dir_node_iter

    def _append_file_node(self, tree_iter, node_data):
        row_values = []

        if self.con.display_store.display_meta.editable:
            row_values.append(False)  # Checked
            row_values.append(False)  # Inconsistent
        icon_id = ICON_GENERIC_FILE
        try:
            if node_data.trashed != NOT_TRASHED:
                icon_id = ICON_TRASHED_FILE
        except AttributeError:
            pass
        row_values.append(icon_id)  # Icon

        row_values.append(node_data.name)  # Name

        # TODO: dir tree required for lazy load
        # if not display_store.display_meta.use_dir_tree:
        #     directory, name = os.path.split(fmeta.full_path)
        #     row_values.append(directory)  # Directory

        # Size
        if node_data.size_bytes is None:
            row_values.append(None)
        else:
            num_bytes_str = humanfriendly.format_size(node_data.size_bytes)
            row_values.append(num_bytes_str)

        # Modified TS
        if node_data.modify_ts is None:
            row_values.append(None)
        else:
            modify_datetime = datetime.fromtimestamp(node_data.modify_ts / 1000)
            modify_formatted = modify_datetime.strftime(self.con.display_store.display_meta.datetime_format)
            row_values.append(modify_formatted)

        # Change TS
        if self.con.display_store.display_meta.show_change_ts:
            if node_data.create_ts is None:
                row_values.append(None)
            else:
                change_datetime = datetime.fromtimestamp(node_data.create_ts / 1000)
                change_time = change_datetime.strftime(self.con.display_store.display_meta.datetime_format)
                row_values.append(change_time)

        row_values.append(node_data)  # Data
        return self.con.display_store.model.append(tree_iter, row_values)

    def populate_root(self):
        children = self.con.data_store.get_children(parent_id=None)
        tree_iter = self.con.display_store.model.get_iter_first()
        # Append all underneath tree_iter
        for child in children:
            if child.is_dir():
                self._append_dir_node_and_dummy_child(tree_iter, child)
            else:
                self._append_file_node(tree_iter, child)
