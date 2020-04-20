import os
import file_util
import logging
import subprocess
import ui.actions as actions
from fmeta.fmeta import FMeta, FMetaTree, Category
from fmeta.fmeta_tree_loader import TreeMetaScanner
from ui.root_dir_panel import RootDirPanel
from ui.diff_tree.dt_model import DirNode, CategoryNode
import gi
gi.require_version("Gtk", "3.0")
from gi.repository import GLib, Gtk, Gdk, GdkPixbuf
from ui.progress_meter import ProgressMeter

logger = logging.getLogger(__name__)


def _build_icons(icon_size):
    icons = dict()
    icons['folder'] = GdkPixbuf.Pixbuf.new_from_file(file_util.get_resource_path(f'resources/Folder-icon-{icon_size}px.png'))
    icons[Category.Added.name] = GdkPixbuf.Pixbuf.new_from_file(file_util.get_resource_path(f'resources/Document-Add-icon-{icon_size}px.png'))
    icons[Category.Deleted.name] = GdkPixbuf.Pixbuf.new_from_file(file_util.get_resource_path(f'resources/Document-Delete-icon-{icon_size}px.png'))
    icons[Category.Moved.name] = GdkPixbuf.Pixbuf.new_from_file(file_util.get_resource_path(f'resources/Document-icon-{icon_size}px.png'))
    icons[Category.Updated.name] = GdkPixbuf.Pixbuf.new_from_file(file_util.get_resource_path(f'resources/Document-icon-{icon_size}px.png'))
    icons[Category.Ignored.name] = GdkPixbuf.Pixbuf.new_from_file(file_util.get_resource_path(f'resources/Document-icon-{icon_size}px.png'))
    return icons


def _build_info_bar():
    info_bar_container = Gtk.Box(spacing=6, orientation=Gtk.Orientation.HORIZONTAL)
    info_bar = Gtk.Label(label='')
    info_bar.set_justify(Gtk.Justification.LEFT)
    info_bar.set_line_wrap(True)
    info_bar_container.add(info_bar)
    return info_bar, info_bar_container


def _build_content_box(root_dir_panel, tree_view, status_bar_container):
    content_box = Gtk.Box(spacing=6, orientation=Gtk.Orientation.VERTICAL)

    content_box.pack_start(root_dir_panel, False, False, 5)

    # Tree will take up all the excess space
    tree_view.set_vexpand(True)
    tree_view.set_hexpand(False)
    tree_scroller = Gtk.ScrolledWindow()
    # No horizontal scrolling - only vertical
    tree_scroller.set_policy(Gtk.PolicyType.NEVER, Gtk.PolicyType.AUTOMATIC)
    tree_scroller.add(tree_view)
    # child, expand, fill, padding
    content_box.pack_start(tree_scroller, False, True, 5)

    content_box.pack_start(status_bar_container, False, True, 5)

    return content_box


class DiffTree:
    model: Gtk.TreeStore

    def __init__(self, store, parent_win, sizegroups=None):
        # Should be a subclass of BaseDialog:
        self.parent_win = parent_win
        self.sizegroups = sizegroups
        self.store = store
        """If true, create a node for each ancestor directory for the files.
           If false, create a second column which shows the parent path. """
        self.use_dir_tree = parent_win.config.get('display.diff_tree.use_dir_tree')

        self.show_change_ts = parent_win.config.get('display.diff_tree.show_change_ts')

        icon_size = parent_win.config.get('display.diff_tree.icon_size')
        self.datetime_format = parent_win.config.get('display.diff_tree.datetime_format')
        self.icons = _build_icons(icon_size=icon_size)

        col_count = 0
        col_types = []
        self.col_names = []
        if self.store.editable:
            self.col_num_checked = col_count
            self.col_names.append('Checked')
            col_types.append(bool)
            col_count += 1

            self.col_num_inconsistent = col_count
            self.col_names.append('Inconsistent')
            col_types.append(bool)
            col_count += 1
        self.col_num_icon = col_count
        self.col_names.append('Icon')
        col_types.append(str)
        col_count += 1

        self.col_num_name = col_count
        self.col_names.append('Name')
        col_types.append(str)
        col_count += 1

        if not self.use_dir_tree:
            self.col_num_directory = col_count
            self.col_names.append('Directory')
            col_types.append(str)
            col_count += 1

        self.col_num_size = col_count
        self.col_names.append('Size')
        col_types.append(str)
        col_count += 1

        self.col_num_modification_ts = col_count
        self.col_names.append('Modification Time')
        col_types.append(str)
        col_count += 1

        if self.show_change_ts:
            self.col_num_change_ts = col_count
            self.col_names.append('Meta Change Time')
            col_types.append(str)
            col_count += 1

        self.col_num_data = col_count
        self.col_names.append('Data')
        col_types.append(object)
        col_count += 1

        self.model = Gtk.TreeStore()
        self.model.set_column_types(col_types)

        self.root_dir_panel = RootDirPanel(self, self.store)

        self.treeview = self._build_treeview(self.model)

        self.status_bar, status_bar_container = _build_info_bar()
        self.content_box = _build_content_box(self.root_dir_panel.content_box, self.treeview, status_bar_container)
        if self.sizegroups is not None and self.sizegroups.get('tree_status') is not None:
            self.sizegroups['tree_status'].add_widget(status_bar_container)

        self.add_listeners()

    def _compare_fmeta(self, model, row1, row2, compare_field_func):
        """
        Comparison function, for use in model sort by column.
        """
        sort_column, _ = model.get_sort_column_id()
        fmeta1 = model[row1][self.col_num_data]
        fmeta2 = model[row2][self.col_num_data]
        if type(fmeta1) == FMeta and type(fmeta2) == FMeta:
            value1 = compare_field_func(fmeta1)
            value2 = compare_field_func(fmeta2)
            if value1 < value2:
                return -1
            elif value1 == value2:
                return 0
            else:
                return 1
        else:
            # This appears to achieve satisfactory behavior
            # (preserving previous column sort order for directories)
            return 0

    def _build_treeview(self, model):
        """ Builds the GTK3 treeview widget"""

        extra_indent = self.parent_win.config.get('display.diff_tree.extra_indent')
        row_height = self.parent_win.config.get('display.diff_tree.row_height')

        # TODO: detach from model while populating
        treeview = Gtk.TreeView(model=model)
        treeview.set_level_indentation(extra_indent)
        treeview.set_show_expanders(True)
        treeview.set_property('enable_grid_lines', True)
        treeview.set_property('enable_tree_lines', True)
        treeview.set_fixed_height_mode(True)
        treeview.set_vscroll_policy(Gtk.ScrollablePolicy.NATURAL)
        # Allow click+drag to select multiple items.
        # May want to disable if using drag+drop
        treeview.set_rubber_banding(True)

        # 0 Checkbox + Icon + Name
        # See: https://stackoverflow.com/questions/27745585/show-icon-or-color-in-gtk-treeview-tree
        px_column = Gtk.TreeViewColumn(self.col_names[self.col_num_name])
        px_column.set_sizing(Gtk.TreeViewColumnSizing.FIXED)

        if self.store.editable:
            renderer = Gtk.CellRendererToggle()
            renderer.connect("toggled", self._on_cell_checkbox_toggled)
            renderer.set_fixed_size(width=-1, height=row_height)
            px_column.pack_start(renderer, False)
            px_column.add_attribute(renderer, 'active', self.col_num_checked)
            px_column.add_attribute(renderer, 'inconsistent', self.col_num_inconsistent)

        px_renderer = Gtk.CellRendererPixbuf()
        px_renderer.set_fixed_size(width=-1, height=row_height)
        px_column.pack_start(px_renderer, False)

        str_renderer = Gtk.CellRendererText()
        str_renderer.set_fixed_height_from_font(1)
        str_renderer.set_fixed_size(width=-1, height=row_height)
        str_renderer.set_property('width-chars', 15)
        px_column.pack_start(str_renderer, False)

        # set data connector function/method
        px_column.set_cell_data_func(px_renderer, self.get_tree_cell_pixbuf)
        px_column.set_cell_data_func(str_renderer, self.get_tree_cell_text)
        px_column.set_min_width(50)
        px_column.set_expand(True)
        px_column.set_resizable(True)
        px_column.set_reorderable(True)
        px_column.set_sort_column_id(self.col_num_name)
        treeview.append_column(px_column)

        if not self.use_dir_tree:
            # DIRECTORY COLUMN
            renderer = Gtk.CellRendererText()
            renderer.set_fixed_height_from_font(1)
            renderer.set_fixed_size(width=-1, height=row_height)
            renderer.set_property('width-chars', 20)
            column = Gtk.TreeViewColumn(self.col_names[self.col_num_dir], renderer, text=self.col_num_dir)
            column.set_sort_column_id(self.col_num_dir)

            column.set_sizing(Gtk.TreeViewColumnSizing.FIXED)
            column.set_min_width(50)
            # column.set_max_width(300)
            column.set_expand(True)
            column.set_resizable(True)
            column.set_reorderable(True)
            column.set_fixed_height_from_font(1)
            treeview.append_column(column)

        # SIZE COLUMN
        renderer = Gtk.CellRendererText()
        renderer.set_fixed_size(width=-1, height=row_height)
        renderer.set_fixed_height_from_font(1)
        renderer.set_property('width-chars', 10)
        column = Gtk.TreeViewColumn(self.col_names[self.col_num_size], renderer, text=self.col_num_size)
        column.set_sort_column_id(self.col_num_size)

        column.set_sizing(Gtk.TreeViewColumnSizing.FIXED)
        #  column.set_fixed_width(50)
        column.set_min_width(50)
        # column.set_max_width(300)
        column.set_expand(False)
        column.set_resizable(True)
        column.set_reorderable(True)
        treeview.append_column(column)

        # Need the original file sizes (in bytes) here, not the formatted one
        model.set_sort_func(self.col_num_size, self._compare_fmeta, lambda f: f.size_bytes)

        # MODIFICATION TS COLUMN
        renderer = Gtk.CellRendererText()
        renderer.set_property('width-chars', 8)
        renderer.set_fixed_size(width=-1, height=row_height)
        renderer.set_fixed_height_from_font(1)
        column = Gtk.TreeViewColumn(self.col_names[self.col_num_modification_ts], renderer, text=self.col_num_modification_ts)
        column.set_sort_column_id(self.col_num_modification_ts)

        column.set_sizing(Gtk.TreeViewColumnSizing.FIXED)
        # column.set_fixed_width(50)
        column.set_min_width(50)
        # column.set_max_width(300)
        column.set_expand(False)
        column.set_resizable(True)
        column.set_reorderable(True)
        treeview.append_column(column)

        model.set_sort_func(self.col_num_modification_ts, self._compare_fmeta, lambda f: f.modify_ts)

        if self.show_change_ts:
            # METADATA CHANGE TS COLUMN
            renderer = Gtk.CellRendererText()
            renderer.set_property('width-chars', 8)
            renderer.set_fixed_size(width=-1, height=row_height)
            renderer.set_fixed_height_from_font(1)
            column = Gtk.TreeViewColumn(self.col_names[self.col_num_change_ts], renderer, text=self.col_num_change_ts)
            column.set_sort_column_id(self.col_num_change_ts)

            column.set_sizing(Gtk.TreeViewColumnSizing.FIXED)
            # column.set_fixed_width(50)
            column.set_min_width(50)
            # column.set_max_width(300)
            column.set_expand(False)
            column.set_resizable(True)
            column.set_reorderable(True)
            treeview.append_column(column)

            model.set_sort_func(self.col_num_change_ts, self._compare_fmeta, lambda f: f.change_ts)

        select = treeview.get_selection()
        select.set_mode(Gtk.SelectionMode.MULTIPLE)
        return treeview

    # For displaying icons
    def get_tree_cell_pixbuf(self, col, cell, model, iter, user_data):
        cell.set_property('pixbuf', self.icons[model.get_value(iter, self.col_num_icon)])

    # For displaying text next to icon
    def get_tree_cell_text(self, col, cell, model, iter, user_data):
        cell.set_property('text', model.get_value(iter, self.col_num_name))

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
            logger.info(f'Hello! Progress made: {progress}')
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
            meta = model[treeiter][self.col_num_data]
            if isinstance(meta, FMeta):
                logger.debug(f'User selected cat="{meta.category.name}" sig="{meta.signature}" path="{meta.file_path}" prev_path="{meta.prev_path}"')
            else:
                logger.debug(f'User selected {model[treeiter][self.col_num_name]}')

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
        node_data = self.model[path][self.col_num_data]
        xdg_open = False
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
                xdg_open = True
        else:
            raise RuntimeError('Unexpected data element')

        if xdg_open:
            self.call_xdg_open(file_path)

    def _on_cell_checkbox_toggled(self, widget, path):
        """Called when checkbox in treeview is toggled"""
        data_node = self.model[path][self.col_num_data]
        if data_node.category == Category.Ignored:
            logger.debug('Disallowing checkbox toggle because node is in IGNORED category')
            return
        # DOC: model[path][column] = not model[path][column]
        checked_value = not self.model[path][self.col_num_checked]
        logger.debug(f'Toggled {checked_value}: {self.model[path][self.col_num_name]}')
        self.model[path][self.col_num_checked] = checked_value
        self.model[path][self.col_num_inconsistent] = False

        # Update all of the node's children change to match its check state:
        def update_checked_state(t_iter):
            self.model[t_iter][self.col_num_checked] = checked_value
            self.model[t_iter][self.col_num_inconsistent] = False

        self.do_for_descendants(path, update_checked_state)

        # Now update its ancestors' states:
        tree_path = Gtk.TreePath.new_from_string(path)
        while True:
            # Go up the tree, one level per loop,
            # with each node updating itself based on its immediate children
            tree_path.up()
            if tree_path.get_depth() < 1:
                # Stop at root
                break
            else:
                tree_iter = self.model.get_iter(tree_path)
                has_checked = False
                has_unchecked = False
                has_inconsistent = False
                child_iter = self.model.iter_children(tree_iter)
                while child_iter is not None:
                    # Parent is inconsistent if any of its children do not match it...
                    if self.model[child_iter][self.col_num_checked]:
                        has_checked = True
                    else:
                        has_unchecked = True
                    # ...or if any of its children are inconsistent
                    has_inconsistent |= self.model[child_iter][self.col_num_inconsistent]
                    child_iter = self.model.iter_next(child_iter)
                self.model[tree_iter][self.col_num_inconsistent] = has_inconsistent or (has_checked and has_unchecked)
                self.model[tree_iter][self.col_num_checked] = has_checked and not has_unchecked and not has_inconsistent

    def _on_toggle_row_expanded_state(self, tree_view, tree_path, col, is_expanded):
        node_data = self.model[tree_path][self.col_num_data]
        if type(node_data) == CategoryNode:
            self.store.set_category_node_expanded_state(node_data.category, is_expanded)

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
                fmeta = self.model[tree_path][self.col_num_data]
                if fmeta is not None:
                    abs_path = self.get_abs_path(fmeta)
                    if not self.delete_dir_tree(subtree_root=abs_path, tree_path=tree_path):
                        # something went wrong if we got False. Stop.
                        break
            return False
        else:
            return True

    def _on_tree_button_press(self, tree_view, event):
        """Used for displaying context menu on right click"""
        if event.button == 3: # right click
            tree_path, col, cell_x, cell_y = tree_view.get_path_at_pos(int(event.x), int(event.y))
            # do something with the selected path
            node_data = self.model[tree_path][self.col_num_data]
            if type(node_data) == CategoryNode:
                logger.debug(f'User right-clicked on {self.model[tree_path][self.col_num_name]}')
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
                data_node = self.model[t_iter][self.col_num_data]
                p = os.path.join(root_path, data_node.file_path)
                path_list.append(p)
                if os.path.isdir(p):
                    add_to_list_func.dir_count += 1

            add_to_list_func.dir_count = 0

            self.do_for_self_and_descendants(tree_path, add_to_list_func)

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
        node_data = self.model[tree_path][self.col_num_data]
        assert node_data is not None
        return self.get_abs_path(node_data)

    def resync_subtree(self, tree_path):
        # Construct a FMetaTree from the UI nodes: this is the 'stale' subtree.
        stale_tree = self.get_subtree_as_tree(tree_path)
        fresh_tree = None
        # Master tree contains all FMeta in this widget
        master_tree = self.store.get_fmeta_tree()

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

    def recurse_over_tree(self, tree_iter, action_func):
        """
        Performs the action_func on the node at this tree_iter AND all of its following
        siblings, and all of their descendants
        """
        while tree_iter is not None:
            action_func(tree_iter)
            if self.model.iter_has_child(tree_iter):
                child_iter = self.model.iter_children(tree_iter)
                self.recurse_over_tree(child_iter, action_func)
            tree_iter = self.model.iter_next(tree_iter)

    def do_for_descendants(self, tree_path, action_func):
        tree_iter = self.model.get_iter(tree_path)
        child_iter = self.model.iter_children(tree_iter)
        if child_iter:
            self.recurse_over_tree(child_iter, action_func)

    def do_for_self_and_descendants(self, tree_path, action_func):
        tree_iter = self.model.get_iter(tree_path)
        action_func(tree_iter)

        child_iter = self.model.iter_children(tree_iter)
        if child_iter:
            self.recurse_over_tree(child_iter, action_func)

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
            # logger.debug(f'Node: {self.model[t_iter][self.col_num_name]} = {type(self.model[t_iter][self.col_num_data])} checked={self.model[t_iter][self.col_num_checked]}')
            if not action_func.checked_only or self.model[t_iter][self.col_num_checked]:
                data_node = self.model[t_iter][self.col_num_data]
                if isinstance(data_node, FMeta):
                    subtree.add(data_node)

        action_func.checked_only = checked_only

        tree_iter = self.model.get_iter(tree_path)
        if not include_following_siblings:
            # Execute on target node, then dive into its children:
            action_func(tree_iter)
            tree_iter = self.model.iter_children(tree_iter)

        self.recurse_over_tree(tree_iter, action_func)

        return subtree

    def get_checked_rows_as_tree(self):
        """Returns a FMetaTree which contains the FMetas of the rows which are currently
        checked by the user. This will be a subset of the FMetaTree which was used to
        populate this tree."""
        assert self.store.editable

        tree_iter = self.model.get_iter_first()
        tree_path = self.model.get_path(tree_iter)
        return self.get_subtree_as_tree(tree_path, include_following_siblings=True, checked_only=True)
