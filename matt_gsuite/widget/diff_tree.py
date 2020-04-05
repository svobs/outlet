import os
from datetime import datetime
import humanfriendly
import file_util
from fmeta.fmeta import DMeta, FMetaSet, ChangeSet
from enum import Enum, auto
from treelib import Node, Tree
import gi
gi.require_version("Gtk", "3.0")
from gi.repository import GLib, Gtk, Gdk, GdkPixbuf
import subprocess

count = 0


class Category(Enum):
    ADDED = auto()
    UPDATED = auto()
    DELETED = auto()
    MOVED = auto()
    IGNORED = auto()


cat_names = {Category.ADDED: 'Added',
             Category.DELETED: 'Deleted',
             Category.UPDATED: 'Updated',
             Category.MOVED: "Moved",
             Category.IGNORED: 'Ignored'}


class DiffTree:
    EXTRA_INDENTATION_LEVEL = 0
    fmeta_set: FMetaSet
    model: Gtk.TreeStore

    def __init__(self):
        # The source files
        self.fmeta_set = None
        """If true, create a node for each ancestor directory for the files.
           If false, create a second column which shows the parent path. """
        self.use_dir_tree = True

        self.col_num_checked = 0
        self.col_num_inconsistent = 1
        self.col_num_icon = 2
        self.col_num_name = 3
        if self.use_dir_tree:
            self.col_names = ['Checked', 'Inconsistent', 'Icon', 'Name', 'Size', 'Modification Date', 'Signature']
            self.col_num_size = 4
            self.col_num_modification_date = 5
            self.col_num_signature = 6
            self.model = Gtk.TreeStore(bool, bool, str, str, str, str, str)
        else:
            self.col_names = ['Checked', 'Inconsistent', 'Icon', 'Name', 'Directory', 'Size', 'Modification Date', 'Signature']
            self.col_num_dir = 4
            self.col_num_size = 5
            self.col_num_modification_date = 6
            self.col_num_signature = 7
            self.model = Gtk.TreeStore(bool, bool, str, str, str, str, str, str)

        icon_size = 24
        self.icons = DiffTree._build_icons(icon_size)

        self.treeview = self._build_treeview(self.model)

    @classmethod
    def _build_icons(cls, icon_size):
        icons = dict()
        icons['folder'] = GdkPixbuf.Pixbuf.new_from_file(file_util.get_resource_path(f'../resources/Folder-icon-{icon_size}px.png'))
        icons[cat_names[Category.ADDED]] = GdkPixbuf.Pixbuf.new_from_file(file_util.get_resource_path(f'../resources/Document-Add-icon-{icon_size}px.png'))
        icons[cat_names[Category.DELETED]] = GdkPixbuf.Pixbuf.new_from_file(file_util.get_resource_path(f'../resources/Document-Delete-icon-{icon_size}px.png'))
        icons[cat_names[Category.MOVED]] = GdkPixbuf.Pixbuf.new_from_file(file_util.get_resource_path(f'../resources/Document-icon-{icon_size}px.png'))
        icons[cat_names[Category.UPDATED]] = GdkPixbuf.Pixbuf.new_from_file(file_util.get_resource_path(f'../resources/Document-icon-{icon_size}px.png'))
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
            fmeta1 = self.fmeta_set.sig_dict[model.get_value(row1, 0)]
            fmeta2 = self.fmeta_set.sig_dict[model.get_value(row2, 0)]
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
                print(f'You selected signature {model[treeiter][self.col_num_signature]}')

        select = treeview.get_selection()
        select.set_mode(Gtk.SelectionMode.MULTIPLE)
        select.connect("changed", on_tree_selection_changed)
        treeview.connect("row-activated", self.on_tree_selection_doubleclick)

        return treeview

    def on_tree_selection_doubleclick(self, tree_view, path, col):
        if self.use_dir_tree:
            file_path = self.model[path][self.col_num_name]
            # walk up the tree and piece together the path:
            while True:
                path.up()
                # skip the top level (category)
                if path.get_depth() < 2:
                    break
                parent = self.model[path][self.col_num_name]
                file_path = os.path.join(parent, file_path)

            file_path = os.path.join(self.fmeta_set.root_path, file_path)
        else:
            dir_path = self.model[path][self.col_num_dir]
            file_name = self.model[path][self.col_num_name]
            file_path = os.path.join(self.fmeta_set.root_path, dir_path, file_name)
        print(f'User double clicked on: {file_path}')
        subprocess.call(["xdg-open", file_path])

# Checkbox toggled
    def on_cell_toggled(self, widget, path):
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
    def __build_category_change_tree(cls, change_set, cat_name):
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

            root = change_tree.create_node(tag=f'{cat_name} ({set_len} items)', identifier='', data=DMeta())   # root
            for fmeta in change_set:
                dirs_str, file_name = os.path.split(fmeta.file_path)
                nid = ''
                parent = root
                parent.data.add_meta(fmeta)
                if dirs_str != '':
                    directories = DiffTree.split_path(dirs_str)
                    for dir_name in directories:
                        if nid != '':
                            nid += '/'
                        nid += dir_name
                        child = change_tree.get_node(nid=nid)
                        if child is None:
                            #print(f'Creating dir: {nid}')
                            child = change_tree.create_node(tag=dir_name, identifier=nid, parent=parent, data=DMeta())
                        parent = child
                        parent.data.add_meta(fmeta)
                if nid != '':
                    nid += '/'
                nid += file_name
                #print(f'Creating file: {nid}')
                change_tree.create_node(identifier=nid, tag=file_name, parent=parent, data=fmeta)

        return change_tree

    def __append_dir(self, tree_iter, dir_name, dmeta):
        num_bytes_str = humanfriendly.format_size(dmeta.total_size_bytes)
        if self.use_dir_tree:
            return self.model.append(tree_iter, [False, False, 'folder', dir_name, num_bytes_str, None, None])
        else:
            return self.model.append(tree_iter, [False, False, 'folder', dir_name, num_bytes_str, None, None, None])

    def _append_fmeta(self, tree_iter, file_name, fmeta, cat_name):
        num_bytes_str = humanfriendly.format_size(fmeta.length)
        modify_datetime = datetime.fromtimestamp(fmeta.modify_ts)
        modify_time = modify_datetime.strftime("%Y-%m-%d %H:%M:%S")

        if self.use_dir_tree:
            return self.model.append(tree_iter, [False, False, cat_name, file_name, num_bytes_str, modify_time, fmeta.signature])
        else:
            directory, name = os.path.split(fmeta.file_path)
            return self.model.append(tree_iter, [False, False, cat_name, file_name, directory, num_bytes_str, modify_time, fmeta.signature])

    def _populate_category(self, cat_name, change_set):
        change_tree = DiffTree.__build_category_change_tree(change_set, cat_name)

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

    def rebuild_ui_tree(self, change_set: ChangeSet):
        self._populate_category(cat_names[Category.ADDED], change_set.adds)
        self._populate_category(cat_names[Category.DELETED], change_set.dels)
        self._populate_category(cat_names[Category.MOVED], change_set.moves)
        self._populate_category(cat_names[Category.UPDATED], change_set.updates)
        self._populate_category(cat_names[Category.IGNORED], self.fmeta_set.ignored_files)

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
