import os
from datetime import datetime

import humanfriendly

import file_util
import logging
import subprocess
import ui.actions as actions
from ui.tree import tree_factory
from ui.tree.display_meta import TreeDisplayMeta
from fmeta.fmeta import FMeta, FMetaTree, Category
from fmeta.fmeta_tree_loader import TreeMetaScanner
from ui.diff_tree.dt_model import DirNode, CategoryNode

import gi
gi.require_version("Gtk", "3.0")
from gi.repository import GLib, Gtk, Gdk
from ui.tree.display_store import DisplayStore
from ui.progress_meter import ProgressMeter

logger = logging.getLogger(__name__)


class LazyTree:
    """
    - Start by listing root nodes
    Phase 1: do not worry about scrolling
    - When a dir node is expanded, a call should be made to the store to retrieve its children, which may or may not be cached. But new display nodes will be created when it is expanded (i.e. lazily)
    - Need to create a stor which can keep track of whether each parent has all children. If not we will have to make a request to retrieve all nodes with 'X' as parent and update the stor before returning
    was last synced (for stats if nothing else)
    - When a node is collpased, keep any display nodes which are hidden (TODO: yeah?)

    - GoogRemote >= GoogDiskCache >= GoogInMemoryCache >= DisplayNode

    - When a node is expanded, get from the stor.

    - GoogDiskCache should try to download all dirs & files ASAP. But in the meantime, download level by level

    DisplayNode <- CatDisplayNode <- DirDisplayNode <- FileDisplayNode
    (the preceding line does not contain instantiated classes)


    - Every time you expand a node, you should call to sync it from the GoogStor.

    TODO: TBD: when does the number of display nodes start to slow down? -> add config for live node maximum
    -
    - Every time you retrieve new data from G, you must perform sanity checks on it and proactively correct them.
    - - Modify TS, MD5, create date, version, revision - any of these changes should be aggressively logged
    and their meta updated in the store,

    Google Drive Stor <- superset of Display Stor


    """

    def __init__(self, store, parent_win, editable, is_display_persisted):
        # Should be a subclass of BaseDialog:
        self.parent_win = parent_win
        self.store = store

        def is_ignored_func(data_node):
            return data_node.category == Category.Ignored
        display_meta = TreeDisplayMeta(config=self.parent_win.config, tree_id=self.store.tree_id, editable=editable, is_display_persisted=is_display_persisted, is_ignored_func=is_ignored_func)

        self.display_store = DisplayStore(display_meta)

        self.treeview, self.status_bar, self.content_box = tree_factory.build_all(
            parent_win=parent_win, store=self.store, display_store=self.display_store)

        select = self.treeview.get_selection()
        select.set_mode(Gtk.SelectionMode.MULTIPLE)

        self.add_listeners()

    @property
    def tree_id(self):
        return self.store.tree_id

    @property
    def editable(self):
        return self.display_store.display_meta.editable

    @property
    def root_path(self):
        return self.store.get_root_path()

    def _set_status(self, status_msg):
        GLib.idle_add(lambda: self.status_bar.set_label(status_msg))

    def _append_dir_node(self, tree_iter, node_data):
        """Appends a dir or cat node to the model"""
        row_values = []
        if self.display_store.display_meta.editable:
            row_values.append(False)  # Checked
            row_values.append(False)  # Inconsistent
        row_values.append('folder')  # Icon
        row_values.append(node_data.name)  # Name
        if not self.display_store.display_meta.use_dir_tree:
            row_values.append(None)  # Directory
        num_bytes_str = humanfriendly.format_size(node_data.size_bytes)
        row_values.append(num_bytes_str)  # Size
        row_values.append(None)  # Modify Date
        if self.display_store.display_meta.show_change_ts:
            row_values.append(None)  # Modify Date
        row_values.append(node_data)  # Data

        return self.display_store.model.append(tree_iter, row_values)

    def _append_file_node(self, tree_iter, node):
        row_values = []

        if self.display_store.display_meta.editable:
            row_values.append(False)  # Checked
            row_values.append(False)  # Inconsistent
        row_values.append(Category.Updated.name)  # Icon

        row_values.append(node.name)  # Name

        # TODO: dir tree required for lazy load
        # if not display_store.display_meta.use_dir_tree:
        #     directory, name = os.path.split(fmeta.file_path)
        #     row_values.append(directory)  # Directory

        # Size
        if node.size_bytes is None:
            row_values.append(None)
        else:
            num_bytes_str = humanfriendly.format_size(node.size_bytes)
            row_values.append(num_bytes_str)

        # Modified TS
        if node.modify_ts is None:
            row_values.append(None)
        else:
            modify_datetime = datetime.fromtimestamp(node.modify_ts / 1000)
            modify_time = modify_datetime.strftime(self.display_store.display_meta.datetime_format)
            row_values.append(modify_time)

        # Change TS
        if self.display_store.display_meta.show_change_ts:
            if node.create_ts is None:
                row_values.append(None)
            else:
                change_datetime = datetime.fromtimestamp(node.create_ts / 1000)
                change_time = change_datetime.strftime(self.display_store.display_meta.datetime_format)
                row_values.append(change_time)

        row_values.append(node)  # Data
        return self.display_store.model.append(tree_iter, row_values)

    def populate_root(self):
        children = self.store.get_children(self.store.get_root_path())
        tree_iter = self.display_store.model.get_iter_first()
        for child in children:
            if child.is_dir():
                tree_iter = self._append_dir_node(tree_iter, child)
            else:
                self._append_file_node(tree_iter, child)

    # --- LISTENERS ---

    def add_listeners(self):
        actions.connect(actions.TOGGLE_UI_ENABLEMENT, self._on_enable_ui_toggled)

        # TODO: Holy shit this is unnecessarily complicated. Clean this up
        def on_progress_made(this, progress, total):
            self._set_status(f'Scanning file {progress} of {total}')

        self.progress_meter = ProgressMeter(on_progress_made, self.parent_win.config, self)

        actions.connect(actions.SET_TOTAL_PROGRESS, self._on_set_total_progress, self.store.tree_id)
        actions.connect(actions.PROGRESS_MADE, self._on_progress_made, self.store.tree_id)
        actions.connect(actions.SET_STATUS, self._on_set_status, self.store.tree_id)

        self.treeview.connect("row-activated", self._on_row_activated)
        self.treeview.connect('button-press-event', self._on_tree_button_press)
        self.treeview.connect('key-press-event', self._on_key_press)
        self.treeview.connect('row-expanded', self._on_toggle_row_expanded_state, True)
        self.treeview.connect('row-collapsed', self._on_toggle_row_expanded_state, False)

        # select.connect("changed", self._on_tree_selection_changed)

    # Remember, use member functions instead of lambdas, because PyDispatcher will remove refs
    def _on_set_status(self, sender, status_msg):
        self._set_status(status_msg)

    def _on_set_total_progress(self, sender, total):
        self.progress_meter.set_total(total)

    def _on_progress_made(self, sender, progress):
        self.progress_meter.add_progress(progress)

    def _on_enable_ui_toggled(self, sender, enable):
        # TODO!
        pass

    def _on_tree_selection_changed(self, selection):
        model, treeiter = selection.get_selected_rows()
        if treeiter is not None and len(treeiter) == 1:
            meta = self.display_store.get_node_data(treeiter)
            if isinstance(meta, FMeta):
                logger.debug(f'User selected cat="{meta.category.name}" sig="{meta.signature}" path="{meta.file_path}" prev_path="{meta.prev_path}"')
            else:
                logger.debug(f'User selected {self.display_store.get_node_name(treeiter)}')

    def _on_row_activated(self, tree_view, path, col):
        selection = self.treeview.get_selection()
        model, treeiter = selection.get_selected_rows()
        if not treeiter:
            logger.error('No selection!')
            return

        # if len(treeiter) == 1:
        # Single node

        # for selected_node in treeiter:
        # TODO: intelligent logic for multiple selected rows

        """Fired when an item is double-clicked or when an item is selected and Enter is pressed"""
        node_data = self.display_store.get_node_data(treeiter)
        if type(node_data) == CategoryNode:
            # Special handling for categories: toggle collapse state
            if tree_view.row_expanded(path):
                tree_view.collapse_row(path)
            else:
                tree_view.expand_row(path=path, open_all=False)
        elif type(node_data) == DirNode or type(node_data) == FMeta:
            if node_data.category == Category.Deleted:
                logger.debug(f'Cannot open a Deleted node: {node_data.file_path}')
            else:
                # TODO: ensure prev_path is filled out for all nodes!
                file_path = os.path.join(self.root_path, node_data.file_path)
                # if not os.path.exists(file_path):
                #     logger.debug(f'File not found: {file_path}')
                #     # File is an 'added' node or some such. Open the old one:
                #     file_path = os.path.join(self.root_path, node_data.prev_path)
                self.call_xdg_open(file_path)
        else:
            raise RuntimeError('Unexpected data element')

    def _on_toggle_row_expanded_state(self, tree_view, tree_path, col, is_expanded):
        node_data = self.display_store.get_node_data(tree_path)
        if type(node_data) == CategoryNode:
            self.display_store.display_meta.set_category_node_expanded_state(node_data.category, is_expanded)

    def _on_key_press(self, widget, event, user_data=None):
        """Fired when a key is pressed"""

        # Note: if the key sequence matches a Gnome keyboard shortcut, it will grab part
        # of the sequence and we will never get notified
        mods = []
        if (event.state & Gdk.ModifierType.CONTROL_MASK) == Gdk.ModifierType.CONTROL_MASK:
            mods.append('Ctrl')
        if (event.state & Gdk.ModifierType.SHIFT_MASK) == Gdk.ModifierType.SHIFT_MASK:
            mods.append('Shift')
        if (event.state & Gdk.ModifierType.META_MASK) == Gdk.ModifierType.META_MASK:
            mods.append('Meta')
        if (event.state & Gdk.ModifierType.SUPER_MASK) == Gdk.ModifierType.SUPER_MASK:
            mods.append('Super')
        if (event.state & Gdk.ModifierType.MOD1_MASK) == Gdk.ModifierType.MOD1_MASK:
            mods.append('Alt')
        logger.debug(f'Key pressed, mods: {Gdk.keyval_name(event.keyval)} ({event.keyval}), {" ".join(mods)}')

        if event.keyval == Gdk.KEY_Delete:
            logger.debug('DELETE key detected!')

            # Get the TreeView selected row(s)
            selection = self.treeview.get_selection()
            # get_selected_rows() returns a tuple
            # The first element is a ListStore
            # The second element is a list of tree paths
            # of all selected rows
            model, paths = selection.get_selected_rows()

            # Get the TreeIter instance for each path
            for tree_path in paths:
                # TODO: nothing at the moment
                pass
            return False
        else:
            return True

    def _on_tree_button_press(self, tree_view, event):
        """Used for displaying context menu on right click"""
        if event.button == 3:  # right click
            tree_path, col, cell_x, cell_y = tree_view.get_path_at_pos(int(event.x), int(event.y))
            # do something with the selected path
            node_data = self.display_store.get_node_data(tree_path)
            if type(node_data) == CategoryNode:
                logger.debug(f'User right-clicked on {self.display_store.get_node_name(tree_path)}')
            else:
                logger.debug(f'User right-clicked on {node_data.file_path}')

            # Display context menu:
            # TODO: disabled for now
            # context_menu = self.build_context_menu(tree_path, node_data)
            # context_menu.popup_at_pointer(event)
            # Suppress selection event:
            return True

    # --- END of LISTENERS ---

    # --- ACTIONS ---

    def expand_all(self, tree_path):
        # TODO
        pass

    # --- END ACTIONS ---

    # --- MODEL UTIL FUNCTIONS --
