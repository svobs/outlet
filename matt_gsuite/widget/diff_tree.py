import os
import humanfriendly
from fmeta.fmeta import FMetaSet
from gi.repository import GLib, Gtk
from enum import Enum, auto
from treelib import Node, Tree

count = 0


class ChangeSet:
    def __init__(self):
        self.adds = []
        self.updates = []
        self.dels = []
        self.unexpected = []


class Category(Enum):
    ADDED = auto()
    UNEXPECTED = auto()


cat_names = {Category.ADDED: 'Added', Category.UNEXPECTED: 'Unexpected'}


class DirTreeNode:
    def __init__(self, name, index_in_parent):
        self.name = name
        self.index_in_parent = index_in_parent
        self.children = {}


class DiffTree:
    def __init__(self):
        # The source files
        self.fmeta_set = FMetaSet()
        # The set of changes based on those files
        self.change_set = ChangeSet()

        self.use_dir_tree = True
        # only used if self.use_dir_tree==True.
        # This is a dict of categories, each of which stores a directory tree
        self.dir_tree_dict = {}


        if self.use_dir_tree:
            self.model = Gtk.TreeStore(str, str, str, str)
        else:
            self.model = Gtk.TreeStore(str, str, str, str, str)
        self.category_unexpected = None
        self.category_added = None
        self.category_removed = None

    #def build_gtk_tree(self):

        # The UI tree widget
        self.tree = Gtk.TreeView(model=self.model) #TODO: detach from model while populating
        #self.tree.set_level_indentation(20)
        self.tree.set_show_expanders(True)
        self.tree.set_property('enable_grid_lines', True)
        self.tree.set_property('enable_tree_lines', True)

        # 1 NAME
        renderer = Gtk.CellRendererText()
        # Set desired number of chars width
        renderer.set_property('width-chars', 15)
        column = Gtk.TreeViewColumn("Name", renderer, text=1)
        column.set_sort_column_id(1)

        column.set_sizing(Gtk.TreeViewColumnSizing.AUTOSIZE)
        #  column.set_fixed_width(50)
        column.set_min_width(50)
        # column.set_max_width(300)
        column.set_expand(True)
        column.set_resizable(True)
        column.set_reorderable(True)
        self.tree.append_column(column)

        if not self.use_dir_tree:
            # 2 DIRECTORY
            renderer = Gtk.CellRendererText()
            renderer.set_property('width-chars', 20)
            column = Gtk.TreeViewColumn("Directory", renderer, text=2)
            column.set_sort_column_id(2)

            column.set_sizing(Gtk.TreeViewColumnSizing.AUTOSIZE)
            column.set_min_width(50)
            # column.set_max_width(300)
            column.set_expand(True)
            column.set_resizable(True)
            column.set_reorderable(True)
            self.tree.append_column(column)

        # 3 SIZE
        renderer = Gtk.CellRendererText()
        renderer.set_property('width-chars', 10)
        column = Gtk.TreeViewColumn("Size", renderer, text=2)
        column.set_sort_column_id(2)

        column.set_sizing(Gtk.TreeViewColumnSizing.AUTOSIZE)
        #  column.set_fixed_width(50)
        column.set_min_width(50)
        # column.set_max_width(300)
        column.set_expand(False)
        column.set_resizable(True)
        column.set_reorderable(True)
        self.tree.append_column(column)

        def compare_file_size(model, row1, row2, user_data):
            sort_column, _ = model.get_sort_column_id()
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

        self.model.set_sort_func(2, compare_file_size, None)

        # 4 MODIFICATION DATE
        renderer = Gtk.CellRendererText()
        renderer.set_property('width-chars', 8)
        column = Gtk.TreeViewColumn("Modification Date", renderer, text=3)
        column.set_sort_column_id(3)

        column.set_sizing(Gtk.TreeViewColumnSizing.AUTOSIZE)
        #column.set_fixed_width(50)
        column.set_min_width(50)
        # column.set_max_width(300)
        column.set_expand(False)
        column.set_resizable(True)
        column.set_reorderable(True)
        self.tree.append_column(column)

        def on_tree_selection_changed(selection):
            model, treeiter = selection.get_selected_rows()
            if treeiter is not None and len(treeiter) > 0:
                print("You selected", model[treeiter][0])

        select = self.tree.get_selection()
        select.set_mode(Gtk.SelectionMode.MULTIPLE)
        select.connect("changed", on_tree_selection_changed)

    # Builds a tree out of the flat change set.
    @staticmethod
    def build_category_change_tree(change_set, cat_name):
        # The change set in tree form
        change_tree = Tree() # from treelib

        set_len = len(change_set)
        if set_len > 0:
            print(f'Building change trees for category {cat_name} with {set_len} items...')

            root = change_tree.create_node(tag=cat_name, identifier='')   # root
            for fmeta in change_set:
                dirs_str, file_name = os.path.split(fmeta.file_path)
                nid = ''
                parent = root
                if dirs_str != '':
                    directories = DiffTree.split_path(dirs_str)
                    for dir_name in directories:
                        if nid != '':
                            nid += '/'
                        nid += dir_name
                        child = change_tree.get_node(nid=nid)
                        if child is None:
                            #print(f'Creating dir: {nid}')
                            child = change_tree.create_node(tag=dir_name, identifier=nid, parent=parent)
                        parent = child
                if nid != '':
                    nid += '/'
                nid += file_name
                #print(f'Creating file: {nid}')
                change_tree.create_node(identifier=nid, tag=file_name, parent=parent, data=fmeta)

        return change_tree

    def append_dir(self, tree_iter, dir_name):
        if self.use_dir_tree:
            return self.model.append(tree_iter, [None, dir_name, None, None])
        else:
            return self.model.append(tree_iter, [None, dir_name, None, None, None])

    def append_fmeta(self, tree_iter, file_name, fmeta):
        num_bytes_str = humanfriendly.format_size(fmeta.length)
        modify_time = str(fmeta.modify_ts) # TODO
        if self.use_dir_tree:
            return self.model.append(tree_iter, [fmeta.signature, file_name, num_bytes_str, modify_time])
        else:
            directory, name = os.path.split(fmeta.file_path)
            return self.model.append(tree_iter, [fmeta.signature, file_name, directory, num_bytes_str, modify_time])

    def _populate_category(self, cat_name, change_set):
        change_tree = DiffTree.build_category_change_tree(change_set, cat_name)

        def append_recursively(tree_iter, node):
            # Do a DFS of the change tree and populate the UI tree along the way
            if node.data is None:
                # Is dir
                tree_iter = self.append_dir(tree_iter, node.tag)
                for child in change_tree.children(node.identifier):
                    append_recursively(tree_iter, child)
            else:
                self.append_fmeta(tree_iter, node.tag, node.data)

        def do_on_ui_thread():
            if change_tree.size(1) > 0:
                #print(f'Appending category: {cat_name}')
                root = change_tree.get_node('')
                append_recursively(None, root)

            # TODO: other categories
            self.tree.expand_all()

        GLib.idle_add(do_on_ui_thread)

    def rebuild_ui_tree(self):
        self._populate_category(cat_names[Category.ADDED], self.change_set.adds)
        self._populate_category(cat_names[Category.UNEXPECTED], self.change_set.unexpected)

    @staticmethod
    def split_path(path):
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
