import os
from datetime import datetime
import humanfriendly
import file_util
from fmeta.fmeta import FMeta, DMeta, FMetaTree, Category
from treelib import Node, Tree
import gi
gi.require_version("Gtk", "3.0")
from gi.repository import GLib, Gtk, Gdk, GdkPixbuf
import subprocess

count = 0


class DiffTree:
    EXTRA_INDENTATION_LEVEL = 0
    model: Gtk.TreeStore

    def __init__(self, root_path):
        # The source files
        """If true, create a node for each ancestor directory for the files.
           If false, create a second column which shows the parent path. """
        self.use_dir_tree = True
        self.root_path = root_path

        self.col_num_checked = 0
        self.col_num_inconsistent = 1
        self.col_num_icon = 2
        self.col_num_name = 3
        if self.use_dir_tree:
            self.col_names = ['Checked', 'Inconsistent', 'Icon', 'Name', 'Size', 'Modification Date', 'Data']
            self.col_num_size = 4
            self.col_num_modification_date = 5
            self.col_num_data = 6
            self.model = Gtk.TreeStore(bool, bool, str, str, str, str, object)
        else:
            self.col_names = ['Checked', 'Inconsistent', 'Icon', 'Name', 'Directory', 'Size', 'Modification Date', 'Data']
            self.col_num_dir = 4
            self.col_num_size = 5
            self.col_num_modification_date = 6
            self.col_num_data = 7
            self.model = Gtk.TreeStore(bool, bool, str, str, str, str, str, object)

        icon_size = 24
        self.icons = DiffTree._build_icons(icon_size)

        self.treeview: Gtk.Treeview
        self.treeview = self._build_treeview(self.model)

    @classmethod
    def _build_icons(cls, icon_size):
        icons = dict()
        icons['folder'] = GdkPixbuf.Pixbuf.new_from_file(file_util.get_resource_path(f'../resources/Folder-icon-{icon_size}px.png'))
        icons[Category.ADDED.name] = GdkPixbuf.Pixbuf.new_from_file(file_util.get_resource_path(f'../resources/Document-Add-icon-{icon_size}px.png'))
        icons[Category.DELETED.name] = GdkPixbuf.Pixbuf.new_from_file(file_util.get_resource_path(f'../resources/Document-Delete-icon-{icon_size}px.png'))
        icons[Category.MOVED.name] = GdkPixbuf.Pixbuf.new_from_file(file_util.get_resource_path(f'../resources/Document-icon-{icon_size}px.png'))
        icons[Category.UPDATED.name] = GdkPixbuf.Pixbuf.new_from_file(file_util.get_resource_path(f'../resources/Document-icon-{icon_size}px.png'))
        return icons

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
            fmeta1 = self.fmeta_tree.sig_dict[model.get_value(row1, 0)]
            fmeta2 = self.fmeta_tree.sig_dict[model.get_value(row2, 0)]
            value1 = fmeta1.length
            value2 = fmeta2.length
            if value1 < value2:
                return -1
            elif value1 == value2:
                return 0
            else:
                return 1

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
        treeview.connect("row-activated", self.on_tree_selection_doubleclick)
        treeview.connect('button-press-event', self.on_tree_button_press)

        return treeview

    def build_context_menu(self, fmeta):
        menu = Gtk.Menu()

        def callback(source, directory):
            self.open_in_nautilus(directory)

        directory = os.path.join(self.root_path, fmeta.file_path)
        i1 = Gtk.MenuItem(label='Show in Nautilus')
        i1.connect('activate', callback, directory)
        menu.append(i1)

        menu.show_all()
        return menu

    def on_tree_button_press(self, tree_view, event):
        """Used for displaying context menu on right click"""
        if event.button == 3: # right click
            path, col, cell_x, cell_y = tree_view.get_path_at_pos(int(event.x), int(event.y))
            # do something with the selected path
            fmeta = self.model[path][self.col_num_data]
            if fmeta.file_path == '':
                # This is the category node
                print(f'User right-clicked on {self.model[path][self.col_num_name]}')
            else:
                print(f'User right-clicked on {fmeta.file_path}')

            # Display context menu:
            context_menu = self.build_context_menu(fmeta)
            context_menu.popup_at_pointer(event)
            # Suppress selection event:
            return True

    def show_error_msg(self, msg, secondary_msg=None):
        dialog = Gtk.MessageDialog(None, 0, Gtk.MessageType.ERROR, Gtk.ButtonsType.CANCEL, msg)
        if secondary_msg is None:
            print(f'ERROR: {msg}')
        else:
            print(f'ERROR: {msg}: {secondary_msg}')
            dialog.format_secondary_text(secondary_msg)

        def run_on_ui_thread():
            dialog.run()
            dialog.destroy()

        run_on_ui_thread()

    def on_question_clicked(self, msg, secondary_msg=None):
        dialog = Gtk.MessageDialog(self, 0, Gtk.MessageType.QUESTION,
                                   Gtk.ButtonsType.YES_NO, msg)
        if secondary_msg is None:
            print(f'Q: {msg}')
        else:
            print(f'Q: {msg}: {secondary_msg}')
            dialog.format_secondary_text(secondary_msg)
        response = dialog.run()
        if response == Gtk.ResponseType.YES:
            print("QUESTION dialog closed by clicking YES button")
        elif response == Gtk.ResponseType.NO:
            print("QUESTION dialog closed by clicking NO button")

        dialog.destroy()

    def open_in_nautilus(self, file_path):
        if os.path.exists(file_path):
            print(f'Opening in Nautilus: {file_path}')
            subprocess.check_call(["nautilus", "--browser", file_path])
        else:
            self.show_error_msg('Cannot open file in Nautilus', f'File not found: {file_path}')

    def call_xdg_open(self, file_path):
        if os.path.exists(file_path):
            print(f'Calling xdg-open for: {file_path}')
            subprocess.check_call(["xdg-open", file_path])
        else:
            self.show_error_msg(f'Cannot open file', f'File not found: {file_path}')

    def on_tree_selection_doubleclick(self, tree_view, path, col):
        fmeta = self.model[path][self.col_num_data]
        xdg_open = False
        if isinstance(fmeta, DMeta):
            if fmeta.file_path == '':
                # Special handling for categories: toggle collapse state
                if tree_view.row_expanded(path):
                    tree_view.collapse_row(path)
                else:
                    tree_view.expand_row(path=path, open_all=False)
            else:
                file_path = os.path.join(self.root_path, fmeta.file_path)
                xdg_open = True
        elif isinstance(fmeta, FMeta):
            file_path = os.path.join(self.root_path, fmeta.file_path)
            xdg_open = True
        else:
            raise RuntimeError('Unexpected data element')

        if xdg_open:
            self.call_xdg_open(file_path)

    def on_cell_toggled(self, widget, path):
        """Called when checkbox in treeview is toggled"""
        # DOC: model[path][column] = not model[path][column]
        checked_value = not self.model[path][self.col_num_checked]
        #print(f'Toggled -> {checked_value}')
        self.model[path][self.col_num_checked] = checked_value
        self.model[path][self.col_num_inconsistent] = False

        tree_iter = self.model.get_iter(path)
        child_iter = self.model.iter_children(tree_iter)
        if child_iter:
            def action_func(iter):
                self.model[iter][self.col_num_checked] = checked_value
                self.model[iter][self.col_num_inconsistent] = False

            self.change_value_recursively(child_iter, action_func)

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

    def change_value_recursively(self, tree_iter, action_func):
        """Performs the action_func on the node at this tree_iter AND all of its following
        siblings, and all of their descendants"""
        while tree_iter is not None:
            action_func(tree_iter)
            if self.model.iter_has_child(tree_iter):
                child_iter = self.model.iter_children(tree_iter)
                self.change_value_recursively(child_iter, action_func)
            tree_iter = self.model.iter_next(tree_iter)

    # For displaying icons
    def get_tree_cell_pixbuf(self, col, cell, model, iter, user_data):
        cell.set_property('pixbuf', self.icons[model.get_value(iter, self.col_num_icon)])

    # For displaying text next to icon
    def get_tree_cell_text(self, col, cell, model, iter, user_data):
        cell.set_property('text', model.get_value(iter, self.col_num_name))

    @classmethod
    def _build_category_change_tree(cls, change_set, cat_name):
        """
        Builds a tree out of the flat change set.
        Args:
            change_set: source tree for category
            cat_name: the category name

        Returns:
            change tree
        """
        # The change set in tree form
        change_tree = Tree() # from treelib

        set_len = len(change_set)
        if set_len > 0:
            print(f'Building change trees for category {cat_name} with {set_len} items...')

            root = change_tree.create_node(tag=f'{cat_name} ({set_len} items)', identifier='', data=DMeta(file_path=''))   # root
            for fmeta in change_set:
                dirs_str, file_name = os.path.split(fmeta.file_path)
                # nid == Node ID == directory name
                nid = ''
                parent = root
                parent.data.add_meta(fmeta)
                if dirs_str != '':
                    directories = DiffTree.split_path(dirs_str)
                    for dir_name in directories:
                        nid = os.path.join(nid, dir_name)
                        child = change_tree.get_node(nid=nid)
                        if child is None:
                            #print(f'Creating dir: {nid}')
                            child = change_tree.create_node(tag=dir_name, identifier=nid, parent=parent, data=DMeta(nid))
                        parent = child
                        parent.data.add_meta(fmeta)
                nid = os.path.join(nid, file_name)
                #print(f'Creating file: {nid}')
                change_tree.create_node(identifier=nid, tag=file_name, parent=parent, data=fmeta)

        return change_tree

    def __append_dir(self, tree_iter, dir_name, dmeta):
        num_bytes_str = humanfriendly.format_size(dmeta.total_size_bytes)
        if self.use_dir_tree:
            return self.model.append(tree_iter, [False, False, 'folder', dir_name, num_bytes_str, None, dmeta])
        else:
            return self.model.append(tree_iter, [False, False, 'folder', dir_name, num_bytes_str, None, None, dmeta])

    def _append_fmeta(self, tree_iter, file_name, fmeta: FMeta, cat_name):
        num_bytes_str = humanfriendly.format_size(fmeta.length)
        modify_datetime = datetime.fromtimestamp(fmeta.modify_ts)
        modify_time = modify_datetime.strftime("%Y-%m-%d %H:%M:%S")

        if self.use_dir_tree:
            return self.model.append(tree_iter, [False, False, cat_name, file_name, num_bytes_str, modify_time, fmeta])
        else:
            directory, name = os.path.split(fmeta.file_path)
            return self.model.append(tree_iter, [False, False, cat_name, file_name, directory, num_bytes_str, modify_time, fmeta])

    def _populate_category(self, cat_name, fmeta_list):
        change_tree = DiffTree._build_category_change_tree(fmeta_list, cat_name)

        def append_recursively(tree_iter, node):
            # Do a DFS of the change tree and populate the UI tree along the way
            if isinstance(node.data, DMeta):
                # Is dir
                tree_iter = self.__append_dir(tree_iter, node.tag, node.data)
                for child in change_tree.children(node.identifier):
                    append_recursively(tree_iter, child)
            else:
                self._append_fmeta(tree_iter, node.tag, node.data, cat_name)

        def do_on_ui_thread():
            if change_tree.size(1) > 0:
                #print(f'Appending category: {cat_name}')
                root = change_tree.get_node('')
                append_recursively(None, root)

            self.treeview.expand_all()

        GLib.idle_add(do_on_ui_thread)

    def rebuild_ui_tree(self, fmeta_tree: FMetaTree):
        """FMetaTree is needed for list of ignored files"""

        # Wipe out existing items:
        self.model.clear()

        self.root_path = fmeta_tree.root_path
        for category in [Category.ADDED, Category.DELETED, Category.MOVED, Category.UPDATED, Category.IGNORED]:
            self._populate_category(category.name, fmeta_tree.get_for_cat(category))
        self.model.clear()

    def get_selected_change_set(self):
        """Returns a ChangeSet which contains the FMetas of the rows which are currently
        checked by the user. This will be a subset of the ChangeSet which was used to populate
        this tree."""
        selected_changes = FMetaTree()

        # TODO

        return selected_changes

    @classmethod
    def split_path(cls, path):
        all_parts = []
        while 1:
            parts = os.path.split(path)
            if parts[0] == path:  # sentinel for absolute paths
                all_parts.insert(0, parts[0])
                break
            elif parts[1] == path: # sentinel for relative paths
                all_parts.insert(0, parts[1])
                break
            else:
                path = parts[0]
                all_parts.insert(0, parts[1])
        return all_parts
