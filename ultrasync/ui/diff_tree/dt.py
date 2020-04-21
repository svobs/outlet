import os
import file_util
import logging
import subprocess
import ui.actions as actions
from ui.tree import tree_factory
from ui.tree.display_meta import TreeDisplayMeta
import ui.assets
from fmeta.fmeta import FMeta, FMetaTree, Category
from fmeta.fmeta_tree_loader import TreeMetaScanner
from ui.root_dir_panel import RootDirPanel
from ui.diff_tree.dt_model import DirNode, CategoryNode

import gi
gi.require_version("Gtk", "3.0")
from gi.repository import GLib, Gtk, Gdk, GdkPixbuf
from ui.tree.display_store import DisplayStore
from ui.progress_meter import ProgressMeter

logger = logging.getLogger(__name__)


class DiffTree:
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

    def build_context_menu(self, tree_path: Gtk.TreePath, node_data):
        """Dynamic context menu (right-click on tree item)"""

        menu = Gtk.Menu()

        abs_path = self.get_abs_path(node_data)
        # Important: use abs_path here, otherwise file names for category nodes are not displayed properly
        parent_path, file_name = os.path.split(abs_path)

        is_category_node = type(node_data) == CategoryNode
        file_exists = os.path.exists(abs_path)

        item = Gtk.MenuItem(label='')
        label = item.get_child()
        label.set_markup(f'<i>{abs_path}</i>')
        item.set_sensitive(False)
        menu.append(item)

        item = Gtk.SeparatorMenuItem()
        menu.append(item)

        if file_exists:
            item = Gtk.MenuItem(label='Show in Nautilus')
            item.connect('activate', lambda menu_item, f: self.show_in_nautilus(f), abs_path)
            menu.append(item)
        else:
            item = Gtk.MenuItem(label='')
            label = item.get_child()
            label.set_markup(f'<i>Path not found</i>')
            item.set_sensitive(False)
            menu.append(item)

        if os.path.isdir(abs_path):
            item = Gtk.MenuItem(label=f'Expand all')
            item.connect('activate', lambda menu_item: self.expand_all(tree_path))
            menu.append(item)

            if not is_category_node and file_exists:
                item = Gtk.MenuItem(label=f'Delete tree "{file_name}"')
                item.connect('activate', lambda menu_item, abs_p: self.delete_dir_tree(abs_p, tree_path), abs_path)
                menu.append(item)
        elif file_exists:
            item = Gtk.MenuItem(label=f'Delete "{file_name}"')
            item.connect('activate', lambda menu_item, abs_p: self.delete_single_file(abs_p, tree_path), abs_path)
            menu.append(item)

        menu.show_all()
        return menu

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
                # Delete the actual file:
                node_data = self.display_store.get_node_data(tree_path)
                if node_data is not None:
                    abs_path = self.get_abs_path(node_data)
                    if not self.delete_dir_tree(subtree_root=abs_path, tree_path=tree_path):
                        # something went wrong if we got False. Stop.
                        break
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
            context_menu = self.build_context_menu(tree_path, node_data)
            context_menu.popup_at_pointer(event)
            # Suppress selection event:
            return True

    # --- END of LISTENERS ---

    # --- ACTIONS ---

    def delete_single_file(self, file_path: str, tree_path: Gtk.TreePath):
        """ Param file_path must be an absolute path"""
        if os.path.exists(file_path):
            try:
                logger.info(f'Deleting file: {file_path}')
                os.remove(file_path)
            except Exception as err:
                self.parent_win.show_error_msg(f'Error deleting file "{file_path}"', str(err))
                raise
            finally:
                self.resync_subtree(tree_path)
        else:
            self.parent_win.show_error_msg('Could not delete file', f'Not found: {file_path}')

    def expand_all(self, tree_path):
        # TODO
        pass

    def delete_dir_tree(self, subtree_root: str, tree_path: Gtk.TreePath):
        """
        Param subtree_root must be an absolute path.
        This will delete the files corresponding to the UI tree -
        which may NOT represent all the files in the corresponding filesystem tree!
        If a directory is found to be empty after we are done deleting files in it,
        we will delete the directory as well.
        """
        if not os.path.exists(subtree_root):
            self.parent_win.show_error_msg('Could not delete tree', f'Not found: {subtree_root}')
            return False
        logger.info(f'User chose to delete subtree: {subtree_root}')

        dir_count = 0

        try:
            root_path = self.root_path
            # We will populate this with files and directories we encounter
            # doing a DFS of the subtree root:
            path_list = []

            def add_to_list_func(t_iter):
                data_node = self.display_store.get_node_data(t_iter)
                p = os.path.join(root_path, data_node.file_path)
                path_list.append(p)
                if os.path.isdir(p):
                    add_to_list_func.dir_count += 1

            add_to_list_func.dir_count = 0

            self.display_store.do_for_self_and_descendants(tree_path, add_to_list_func)

            dir_count = add_to_list_func.dir_count
        except Exception as err:
            self.parent_win.show_error_msg(f'Error collecting file list for "{subtree_root}"', str(err))
            raise

        file_count = len(path_list) - dir_count
        msg = f'Are you sure you want to delete the {file_count} files in {subtree_root}?'
        is_confirmed = self.parent_win.show_question_dialog('Confirm subtree deletion',
                                                            secondary_msg=msg)
        if not is_confirmed:
            logger.debug('User cancelled delete')
            return

        try:
            logger.info(f'About to delete {file_count} files and up to {dir_count} dirs')
            # By going backwards, we iterate from the bottom to top of tree.
            # This guarantees that we examine the files before their parent dirs.
            for path_to_delete in path_list[-1::-1]:
                if os.path.isdir(path_to_delete):
                    if not os.listdir(path_to_delete):
                        logger.info(f'Deleting empty dir: {path_to_delete}')
                        os.rmdir(path_to_delete)
                else:
                    logger.info(f'Deleting file: {path_to_delete}')
                    os.remove(path_to_delete)

        except Exception as err:
            self.parent_win.show_error_msg(f'Error deleting tree "{subtree_root}"', str(err))
            raise
        finally:
            self.resync_subtree(tree_path)

    def call_xdg_open(self, file_path):
        if os.path.exists(file_path):
            logger.info(f'Calling xdg-open for: {file_path}')
            subprocess.check_call(["xdg-open", file_path])
        else:
            self.parent_win.show_error_msg(f'Cannot open file', f'File not found: {file_path}')

    def show_in_nautilus(self, file_path):
        if os.path.exists(file_path):
            logger.info(f'Opening in Nautilus: {file_path}')
            subprocess.check_call(["nautilus", "--browser", file_path])
        else:
            self.parent_win.show_error_msg('Cannot open file in Nautilus', f'File not found: {file_path}')

    # --- END ACTIONS ---

    # --- MODEL UTIL FUNCTIONS --

    def get_abs_path(self, node_data):
        """ Utility function: joins the two paths together into an absolute path and returns it"""
        return self.store.get_root_path() if not node_data.file_path else os.path.join(self.store.get_root_path(), node_data.file_path)

    def get_abs_file_path(self, tree_path: Gtk.TreePath):
        """ Utility function: get absolute file path from a TreePath """
        node_data = self.display_store.get_node_data(tree_path)
        assert node_data is not None
        return self.get_abs_path(node_data)

    def resync_subtree(self, tree_path):
        # Construct a FMetaTree from the UI nodes: this is the 'stale' subtree.
        stale_tree = self.get_subtree_as_tree(tree_path)
        fresh_tree = None
        # Master tree contains all FMeta in this widget
        master_tree = self.store.get_whole_tree()

        # If the path no longer exists at all, then it's simple: the entire stale_tree should be deleted.
        if os.path.exists(stale_tree.root_path):
            # But if there are still files present: use FMetaTreeLoader to re-scan subtree
            # and construct a FMetaTree from the 'fresh' data
            logger.debug(f'Scanning: {stale_tree.root_path}')
            scanner = TreeMetaScanner(root_path=stale_tree.root_path, stale_tree=stale_tree, tree_id=self.store.tree_id, track_changes=False)
            fresh_tree = scanner.scan()

        # TODO: files in different categories are showing up as 'added' in the scan
        # TODO: should just be removed then added below, but brainstorm how to optimize this

        for fmeta in stale_tree.get_all():
            # Anything left in the stale tree no longer exists. Delete it from master tree
            # NOTE: stale tree will contain old FMeta which is from the master tree, and
            # thus does need to have its file path adjusted.
            # This seems awfully fragile...
            old = master_tree.remove(file_path=fmeta.file_path, sig=fmeta.signature, ok_if_missing=False)
            if old:
                logger.debug(f'Deleted from master tree: sig={old.signature} path={old.file_path}')
            else:
                logger.warning(f'Could not delete "stale" from master (not found): sig={fmeta.signature} path={fmeta.file_path}')

        if fresh_tree:
            for fmeta in fresh_tree.get_all():
                # Anything in the fresh tree needs to be either added or updated in the master tree.
                # For the 'updated' case, remove the old FMeta from the file mapping and any old signatures.
                # Note: Need to adjust file path here, because these FMetas were created with a different root
                abs_path = os.path.join(fresh_tree.root_path, fmeta.file_path)
                fmeta.file_path = file_util.strip_root(abs_path, master_tree.root_path)
                old = master_tree.remove(file_path=fmeta.file_path, sig=fmeta.signature, remove_old_sig=True, ok_if_missing=True)
                if old:
                    logger.debug(f'Removed from master tree: sig={old.signature} path={old.file_path}')
                else:
                    logger.debug(f'Could not delete "fresh" from master (not found): sig={fmeta.signature} path={fmeta.file_path}')
                master_tree.add(fmeta)
                logger.debug(f'Added to master tree: sig={fmeta.signature} path={fmeta.file_path}')

        # 3. Then re-diff and re-populate

        # TODO: Need to introduce a signalling mechanism for the other tree
        logger.info('TODO: re-diff and re-populate!')

    def get_checked_rows_as_tree(self):
        """Returns a FMetaTree which contains the FMetas of the rows which are currently
        checked by the user. This will be a subset of the FMetaTree which was used to
        populate this tree."""
        assert self.editable

        tree_iter = self.display_store.model.get_iter_first()
        tree_path = self.display_store.model.get_path(tree_iter)
        return self.get_subtree_as_tree(tree_path, include_following_siblings=True, checked_only=True)

    def get_subtree_as_tree(self, tree_path, include_following_siblings=False, checked_only=False):
        """
        Constructs a new FMetaTree out of the data nodes of the subtree referenced
        by tree_path. NOTE: currently the FMeta objects are reused in the new tree,
        for efficiency.
        Args:
            tree_path: root of the subtree, as a GTK3 TreePath
            include_following_siblings: if False, include only the root node and its children
            (filtered by checked state if checked_only is True)
            checked_only: if True, include only rows which are checked
                          if False, include all rows in the subtree
        Returns:
            A new FMetaTree which consists of a subset of the current UI tree
        """
        subtree_root = self.get_abs_file_path(tree_path)
        subtree = FMetaTree(subtree_root)

        def action_func(t_iter):
            if not action_func.checked_only or self.display_store.is_node_checked(t_iter):
                data_node = self.display_store.get_node_data(t_iter)
                if isinstance(data_node, FMeta):
                    subtree.add(data_node)

        action_func.checked_only = checked_only

        if include_following_siblings:
            self.display_store.do_for_subtree_and_following_sibling_subtrees(tree_path, action_func)
        else:
            self.display_store.do_for_self_and_descendants(tree_path, action_func)

        return subtree
