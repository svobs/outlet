import os
import shutil
from datetime import datetime
import humanfriendly
import file_util
from fmeta.fmeta import FMeta, DirNode, CategoryNode, FMetaTree, Category
from treelib import Node, Tree
from widget.root_dir_panel import RootDirPanel
import gi
gi.require_version("Gtk", "3.0")
from gi.repository import GLib, Gtk, Gdk, GdkPixbuf
import subprocess

count = 0


class DiffTree:
    EXTRA_INDENTATION_LEVEL = 0
    model: Gtk.TreeStore

    def __init__(self, parent_win, root_path, editable):
        # The source files
        """If true, create a node for each ancestor directory for the files.
           If false, create a second column which shows the parent path. """
        self.use_dir_tree = True
        self.parent_win = parent_win
        self.root_path = root_path
        """If false, hide checkboxes and tree root change button"""
        self.editable = editable

        col_count = 0
        col_types = []
        self.col_names = []
        if self.editable:
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

        self.col_num_modification_date = col_count
        self.col_names.append('Modification Date')
        col_types.append(str)
        col_count += 1

        self.col_num_data = col_count
        self.col_names.append('Data')
        col_types.append(object)
        col_count += 1

        self.model = Gtk.TreeStore()
        self.model.set_column_types(col_types)

        icon_size = 24
        self.icons = DiffTree._build_icons(icon_size)

        self.root_dir_panel = RootDirPanel(self)

        self.treeview: Gtk.Treeview
        self.treeview = self._build_treeview(self.model)

        self.status_bar, status_bar_container = DiffTree._build_info_bar()
        self.content_box = DiffTree._build_content_box(self.root_dir_panel.content_box, self.treeview, status_bar_container)

    @classmethod
    def _build_icons(cls, icon_size):
        icons = dict()
        icons['folder'] = GdkPixbuf.Pixbuf.new_from_file(file_util.get_resource_path(f'../resources/Folder-icon-{icon_size}px.png'))
        icons[Category.Added.name] = GdkPixbuf.Pixbuf.new_from_file(file_util.get_resource_path(f'../resources/Document-Add-icon-{icon_size}px.png'))
        icons[Category.Deleted.name] = GdkPixbuf.Pixbuf.new_from_file(file_util.get_resource_path(f'../resources/Document-Delete-icon-{icon_size}px.png'))
        icons[Category.Moved.name] = GdkPixbuf.Pixbuf.new_from_file(file_util.get_resource_path(f'../resources/Document-icon-{icon_size}px.png'))
        icons[Category.Updated.name] = GdkPixbuf.Pixbuf.new_from_file(file_util.get_resource_path(f'../resources/Document-icon-{icon_size}px.png'))
        icons[Category.Ignored.name] = GdkPixbuf.Pixbuf.new_from_file(file_util.get_resource_path(f'../resources/Document-icon-{icon_size}px.png'))
        return icons

    @classmethod
    def _build_content_box(cls, root_dir_panel, tree_view, status_bar_container):
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

    def _build_treeview(self, model):
        """ Builds the GTK3 treeview widget"""

        # TODO: detach from model while populating
        treeview = Gtk.TreeView(model=model)
        treeview.set_level_indentation(DiffTree.EXTRA_INDENTATION_LEVEL)
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

        if self.editable:
            renderer = Gtk.CellRendererToggle()
            renderer.connect("toggled", self.on_cell_toggled)
            px_column.pack_start(renderer, False)
            px_column.add_attribute(renderer, 'active', self.col_num_checked)
            px_column.add_attribute(renderer, 'inconsistent', self.col_num_inconsistent)

        px_renderer = Gtk.CellRendererPixbuf()
        px_column.pack_start(px_renderer, False)

        str_renderer = Gtk.CellRendererText()
        str_renderer.set_fixed_height_from_font(1)
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
            # 2 DIRECTORY
            renderer = Gtk.CellRendererText()
            renderer.set_fixed_height_from_font(1)
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

        # 3 SIZE
        renderer = Gtk.CellRendererText()
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

        def compare_file_size(model, row1, row2, user_data):
            sort_column, _ = model.get_sort_column_id()
            # Need the original file sizes (in bytes) here, not the formatted one
            fmeta1 = model[row1][self.col_num_data]
            fmeta2 = model[row2][self.col_num_data]
            if type(fmeta1) == FMeta and type(fmeta2) == FMeta:
                value1 = fmeta1.size_bytes
                value2 = fmeta2.size_bytes
                if value1 < value2:
                    return -1
                elif value1 == value2:
                    return 0
                else:
                    return 1
            else:
                # This appears to achieve satisfactory behavior, comparing file name
                return 0

        model.set_sort_func(self.col_num_size, compare_file_size, None)

        # 4 MODIFICATION DATE
        renderer = Gtk.CellRendererText()
        renderer.set_property('width-chars', 8)
        renderer.set_fixed_height_from_font(1)
        column = Gtk.TreeViewColumn(self.col_names[self.col_num_modification_date], renderer, text=self.col_num_modification_date)
        column.set_sort_column_id(self.col_num_modification_date)

        column.set_sizing(Gtk.TreeViewColumnSizing.FIXED)
        #column.set_fixed_width(50)
        column.set_min_width(50)
        # column.set_max_width(300)
        column.set_expand(False)
        column.set_resizable(True)
        column.set_reorderable(True)
        treeview.append_column(column)

        def on_tree_selection_changed(selection):
            model, treeiter = selection.get_selected_rows()
            if treeiter is not None and len(treeiter) == 1:
                meta = model[treeiter][self.col_num_data]
                if isinstance(meta, FMeta):
                    print(f'User selected signature {meta.signature}')
                else:
                    print(f'User selected {model[treeiter][self.col_num_name]}')

        select = treeview.get_selection()
        select.set_mode(Gtk.SelectionMode.MULTIPLE)
        select.connect("changed", on_tree_selection_changed)
        treeview.connect("row-activated", self.on_tree_selection_activated)
        treeview.connect('button-press-event', self.on_tree_button_press)
        treeview.connect('key-press-event', self.on_key_press)

        return treeview

    def set_status(self, status_msg):
        #print(status_msg)
        self.status_bar.set_label(status_msg)

    @classmethod
    def _build_info_bar(cls):
        info_bar_container = Gtk.Box(spacing=6, orientation=Gtk.Orientation.HORIZONTAL)
        info_bar = Gtk.Label(label='')
        info_bar.set_justify(Gtk.Justification.LEFT)
        info_bar.set_line_wrap(True)
        info_bar_container.add(info_bar)
        return info_bar, info_bar_container

    def build_context_menu(self, tree_path: Gtk.TreePath, fmeta: FMeta):
        """Dynamic context menu (right-click on tree item)"""

        abs_path = self.get_abs_path(fmeta)
        # Important: use abs_path here, otherwise file names for category nodes are not displayed properly
        parent_path, file_name = os.path.split(abs_path)

        menu = Gtk.Menu()

        if not os.path.exists(abs_path):
            i1 = Gtk.MenuItem(label='')
            label = i1.get_child()
            label.set_markup(f'<i>File "{file_name}" not found</i>')
            i1.set_sensitive(False)
            menu.append(i1)
        else:
            i1 = Gtk.MenuItem(label='Show in Nautilus')
            i1.connect('activate', lambda menu_item, f: self.show_in_nautilus(f), abs_path)
            menu.append(i1)

            if os.path.isdir(file_name):
                i2 = Gtk.MenuItem(label=f'Delete tree "{file_name}"')
                i2.connect('activate', lambda menu_item, abs_p: self.delete_dir_tree(abs_p, tree_path), abs_path)
                menu.append(i2)
            else:
                i2 = Gtk.MenuItem(label=f'Delete "{file_name}"')
                i2.connect('activate', lambda menu_item, abs_p: self.delete_single_file(abs_p, tree_path), abs_path)
                menu.append(i2)

        menu.show_all()
        return menu

    def on_key_press(self, widget, event, user_data=None):
        """Fired when a key is pressed"""
        print("Key press on widget: ", widget)
        print("          Modifiers: ", event.state)
        print("      Key val, name: ", event.keyval, Gdk.keyval_name(event.keyval))

        # check the event modifiers (can also use SHIFTMASK, etc)
       # ctrl = (event.state & Gdk.ModifierType.CONTROL_MASK)

        if event.keyval == Gdk.KEY_Delete:
            print('DELETE key detected!')

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
                    file_path = self.get_abs_path(fmeta)
                    if not self.delete_dir_tree(file_path=file_path, tree_path=tree_path):
                        # something went wrong if we got False. Stop.
                        break
            return False
        else:
            return True

    def on_tree_button_press(self, tree_view, event):
        """Used for displaying context menu on right click"""
        if event.button == 3: # right click
            tree_path, col, cell_x, cell_y = tree_view.get_path_at_pos(int(event.x), int(event.y))
            # do something with the selected path
            fmeta = self.model[tree_path][self.col_num_data]
            if fmeta.file_path == '':
                # This is the category node
                print(f'User right-clicked on {self.model[tree_path][self.col_num_name]}')
            else:
                print(f'User right-clicked on {fmeta.file_path}')

            # Display context menu:
            context_menu = self.build_context_menu(tree_path, fmeta)
            context_menu.popup_at_pointer(event)
            # Suppress selection event:
            return True

    def get_abs_path(self, node_data):
        """ Utility function """
        return self.root_path if node_data.file_path == '' else os.path.join(self.root_path, node_data.file_path)

    def get_abs_file_path(self, tree_path: Gtk.TreePath):
        """ Utility function: get absolute file path from a TreePath """
        node_data = self.model[tree_path][self.col_num_data]
        assert node_data is not None
        return self.get_abs_path(node_data)

    def update_subtree(self, tree_path):
        # TODO: need to possibly add new FMeta if we find new files,
        # TODO: and also update the other tree if we discover it.
        # TODO: Need to introduce a callback mechanism for the other tree,
        # TODO: as well as a way to find a Node from a file path by walking the tree

        # Other
        file_path_subtree_root = self.get_abs_file_path(tree_path)
        #for root, dirs, files in os.walk(file_path_subtree_root, topdown=True):


        # TODO: not just delete!
        tree_iter = self.model.get_iter(tree_path)
        self.model.remove(tree_iter)

    def show_in_nautilus(self, file_path):
        if os.path.exists(file_path):
            print(f'Opening in Nautilus: {file_path}')
            subprocess.check_call(["nautilus", "--browser", file_path])
        else:
            self.parent_win.show_error_msg('Cannot open file in Nautilus', f'File not found: {file_path}')

    def delete_single_file(self, file_path: str, tree_path: Gtk.TreePath):
        """ Param file_path must be an absolute path"""
        if os.path.exists(file_path):
            try:
                print(f'Deleting file: {file_path}')
                os.remove(file_path)
            except Exception as err:
                self.parent_win.show_error_msg(f'Error deleting file "{file_path}"', err)
                raise
            finally:
                self.update_subtree(tree_path)
        else:
            self.parent_win.show_error_msg('Could not delete file', f'Not found: {file_path}')

    def delete_dir_tree(self, file_path: str, tree_path: Gtk.TreePath):
        """ Param file_path must be an absolute path"""
        if os.path.exists(file_path):
            try:
                print(f'Deleting dir tree: {file_path}')
                shutil.rmtree(file_path)
                return True
            except Exception as err:
                self.parent_win.show_error_msg(f'Error deleting directory tree "{file_path}"', err)
                raise
            finally:
                self.update_subtree(tree_path)
        else:
            self.parent_win.show_error_msg('Could not delete directory tree', f'Not found: {file_path}')
            return False

    def call_xdg_open(self, file_path):
        if os.path.exists(file_path):
            print(f'Calling xdg-open for: {file_path}')
            subprocess.check_call(["xdg-open", file_path])
        else:
            self.parent_win.show_error_msg(f'Cannot open file', f'File not found: {file_path}')

    def on_tree_selection_activated(self, tree_view, path, col):
        """Fired when an item is double-clicked or when an item is selected and Enter is pressed"""
        node_data = self.model[path][self.col_num_data]
        xdg_open = False
        if type(node_data) == CategoryNode:
            # Special handling for categories: toggle collapse state
            if tree_view.row_expanded(path):
                tree_view.collapse_row(path)
            else:
                tree_view.expand_row(path=path, open_all=False)
        elif type(node_data) == DirNode:
            file_path = os.path.join(self.root_path, node_data.file_path)
            xdg_open = True
        elif type(node_data) == FMeta:
            file_path = os.path.join(self.root_path, node_data.file_path)
            xdg_open = True
        else:
            raise RuntimeError('Unexpected data element')

        if xdg_open:
            self.call_xdg_open(file_path)

    def on_cell_toggled(self, widget, path):
        """Called when checkbox in treeview is toggled"""
        data_node = self.model[path][self.col_num_data]
        if data_node.category == Category.Ignored:
            print('Disallowing checkbox toggle because node is in IGNORED category')
            return
        # DOC: model[path][column] = not model[path][column]
        checked_value = not self.model[path][self.col_num_checked]
        print(f'Toggled {checked_value}: {self.model[path][self.col_num_name]}')
        self.model[path][self.col_num_checked] = checked_value
        self.model[path][self.col_num_inconsistent] = False

        tree_iter = self.model.get_iter(path)
        child_iter = self.model.iter_children(tree_iter)
        if child_iter:
            def action_func(t_iter):
                self.model[t_iter][self.col_num_checked] = checked_value
                self.model[t_iter][self.col_num_inconsistent] = False

            self.recurse_over_tree(child_iter, action_func)

        tree_path = Gtk.TreePath.new_from_string(path)
        while True:
            tree_path.up()
            if tree_path.get_depth() < 1:
                break
            else:
                tree_iter = self.model.get_iter(tree_path)
                parent_checked = self.model[tree_iter][self.col_num_checked]
                inconsistent = False
                child_iter = self.model.iter_children(tree_iter)
                while child_iter is not None:
                    # Parent is inconsistent if any of its children do not match it...
                    inconsistent |= (parent_checked != self.model[child_iter][self.col_num_checked])
                    # ...or if any of its children are inconsistent
                    inconsistent |= self.model[child_iter][self.col_num_inconsistent]
                    child_iter = self.model.iter_next(child_iter)
                self.model[tree_iter][self.col_num_inconsistent] = inconsistent

    def recurse_over_tree(self, tree_iter, action_func):
        """Performs the action_func on the node at this tree_iter AND all of its following
        siblings, and all of their descendants"""
        while tree_iter is not None:
            action_func(tree_iter)
            if self.model.iter_has_child(tree_iter):
                child_iter = self.model.iter_children(tree_iter)
                self.recurse_over_tree(child_iter, action_func)
            tree_iter = self.model.iter_next(tree_iter)

    # For displaying icons
    def get_tree_cell_pixbuf(self, col, cell, model, iter, user_data):
        cell.set_property('pixbuf', self.icons[model.get_value(iter, self.col_num_icon)])

    # For displaying text next to icon
    def get_tree_cell_text(self, col, cell, model, iter, user_data):
        cell.set_property('text', model.get_value(iter, self.col_num_name))

    @classmethod
    def _build_category_change_tree(cls, change_set, category):
        """
        Builds a tree out of the flat change set.
        Args:
            change_set: source tree for category
            cat_name: the category name

        Returns:
            change tree
        """
        # The change set in tree form
        change_tree = Tree()  # from treelib

        set_len = len(change_set)
        if set_len > 0:
            print(f'Building change trees for category {category.name} with {set_len} files...')

            root = change_tree.create_node(tag=f'{category.name} ({set_len} files)', identifier='', data=CategoryNode(category))   # root
            for fmeta in change_set:
                dirs_str, file_name = os.path.split(fmeta.file_path)
                # nid == Node ID == directory name
                nid = ''
                parent = root
                #print(f'Adding root file "{fmeta.file_path}" to dir "{parent.data.file_path}"')
                parent.data.add_meta(fmeta)
                if dirs_str != '':
                    directories = file_util.split_path(dirs_str)
                    for dir_name in directories:
                        nid = os.path.join(nid, dir_name)
                        child = change_tree.get_node(nid=nid)
                        if child is None:
                            #print(f'Creating dir: {nid}')
                            child = change_tree.create_node(tag=dir_name, identifier=nid, parent=parent, data=DirNode(nid, category))
                        parent = child
                        #print(f'Adding file "{fmeta.file_path}" to dir {parent.data.file_path}"')
                        parent.data.add_meta(fmeta)
                nid = os.path.join(nid, file_name)
                #print(f'Creating file: {nid}')
                change_tree.create_node(identifier=nid, tag=file_name, parent=parent, data=fmeta)

        return change_tree

    def _append_dir(self, tree_iter, dir_name, dmeta):
        row_values = []
        if self.editable:
            row_values.append(False)  # Checked
            row_values.append(False)  # Inconsistent
        row_values.append('folder')  # Icon
        row_values.append(dir_name)  # Name
        if not self.use_dir_tree:
            row_values.append(None)  # Directory
        num_bytes_str = humanfriendly.format_size(dmeta.size_bytes)
        row_values.append(num_bytes_str)  # Size
        row_values.append(None)  # Modify Date
        row_values.append(dmeta)  # Data
        return self.model.append(tree_iter, row_values)

    def _append_fmeta(self, tree_iter, file_name, fmeta: FMeta, category):
        row_values = []

        if self.editable:
            row_values.append(False)  # Checked
            row_values.append(False)  # Inconsistent
        row_values.append(category.name)  # Icon

        if category == Category.Moved:
            node_name = f'{file_name} <- "{fmeta.prev_path}"'
        else:
            node_name = file_name
        row_values.append(node_name)  # Name

        if not self.use_dir_tree:
            directory, name = os.path.split(fmeta.file_path)
            row_values.append(directory)  # Directory

        num_bytes_str = humanfriendly.format_size(fmeta.size_bytes)
        row_values.append(num_bytes_str)  # Size

        modify_datetime = datetime.fromtimestamp(fmeta.modify_ts)
        modify_time = modify_datetime.strftime("%Y-%m-%d %H:%M:%S")
        row_values.append(modify_time)  # Modify Date

        row_values.append(fmeta)  # Data
        return self.model.append(tree_iter, row_values)

    def _populate_category(self, category: Category, fmeta_list):
        change_tree = DiffTree._build_category_change_tree(fmeta_list, category)

        def append_recursively(tree_iter, node):
            # Do a DFS of the change tree and populate the UI tree along the way
            if isinstance(node.data, DirNode):
                # Is dir
                tree_iter = self._append_dir(tree_iter, node.tag, node.data)
                for child in change_tree.children(node.identifier):
                    append_recursively(tree_iter, child)
            else:
                self._append_fmeta(tree_iter, node.tag, node.data, category)

        def do_on_ui_thread():
            if change_tree.size(1) > 0:
                #print(f'Appending category: {category.name}')
                root = change_tree.get_node('')
                append_recursively(None, root)

                tree_iter = self.model.get_iter_first()
                while tree_iter is not None:
                    node_data = self.model[tree_iter][self.col_num_data]
                    if type(node_data) == CategoryNode and node_data.category != Category.Ignored:
                        tree_path = self.model.get_path(tree_iter)
                        self.treeview.expand_row(path=tree_path, open_all=True)
                    tree_iter = self.model.iter_next(tree_iter)

        GLib.idle_add(do_on_ui_thread)

    def rebuild_ui_tree(self, fmeta_tree: FMetaTree):
        """FMetaTree is needed for list of ignored files"""

        # Wipe out existing items:
        self.model.clear()

        self.root_path = fmeta_tree.root_path
        # TODO: excluded MOVED for quicker testing
        for category in [Category.Added, Category.Deleted, Category.Updated, Category.Ignored]:
            self._populate_category(category, fmeta_tree.get_for_cat(category))

    def get_selected_changes(self):
        """Returns a FMetaTree which contains the FMetas of the rows which are currently
        checked by the user. This will be a subset of the FMetaTree which was used to populate
        this tree."""
        assert self.editable
        selected_changes = FMetaTree(self.root_path)

        tree_iter = self.model.get_iter_first()

        def action_func(t_iter):
            if self.model[t_iter][self.col_num_checked]:
                data_node = self.model[t_iter][self.col_num_data]
                #print(f'Node: {self.model[t_iter][self.col_num_name]} = {type(data_node)}')
                if isinstance(data_node, FMeta):
                    selected_changes.add(data_node)

        self.recurse_over_tree(tree_iter, action_func)

        return selected_changes
