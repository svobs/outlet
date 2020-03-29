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
    def build_category_change_tree(self, change_set, cat_name):
        # The change set in tree form
        change_tree = Tree() # from treelib

        if len(change_set) > 0:
            print(f'Building change trees for category {cat_name}...')

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

    def rebuild_ui_tree(self):
        cat_name = cat_names[Category.ADDED]
        change_tree = self.build_category_change_tree(self.change_set.adds, cat_name)

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
                print(f'Appending category: {cat_name}')
                root = change_tree.get_node('')
                append_recursively(None, root)

            # TODO: other categories
            self.tree.expand_all()

        GLib.idle_add(do_on_ui_thread)

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

    def _add_parent_dirs(self, dir_tree_node, path):
        tree_path_array = [dir_tree_node.index_in_parent, ]
        if path != '':
            print(f'Need to add parent dirs for "{path}"')

            directories = DiffTree.split_path(path)
            for dir_name in directories:
                child = dir_tree_node.children.get(dir_name, None)
                if child is None:
                    # Create
                    parent_tree_path = Gtk.TreePath(tree_path_array)
                    parent_tree_iter = self.model.get_iter(parent_tree_path)
                    self.model.append(parent_tree_iter, [dir_name, None, None, None])
                    print(f'AddedTree: {dir_name} at {str(parent_tree_path)}')
                    child = DirTreeNode(dir_name, len(dir_tree_node.children))
                    dir_tree_node.children[child.name] = child
                tree_path_array.append(child.index_in_parent)
        parent_tree_path = Gtk.TreePath(tree_path_array)
        return self.model.get_iter(parent_tree_path)

    def _add(self, category, fmeta):
        # create cat if not exist:
        cat_name = cat_names[category]
        category_node = self.dir_tree_dict.get(cat_name, None)
        if category_node is None:
            category_node = DirTreeNode(cat_name, len(self.dir_tree_dict))
            if self.use_dir_tree:
                print(f'Appending category: {cat_name}')
                self.model.append(None, [cat_name, None, None, None])
            else:
                self.model.append(None, [cat_name, None, None, None, None])
            self.dir_tree_dict[cat_name] = category_node
            print(f'AddedTree: {cat_name} at {category_node.index_in_parent}')

        directory, name = os.path.split(fmeta.file_path)
        num_bytes_str = humanfriendly.format_size(fmeta.length)
        modify_time = str(fmeta.modify_ts) # TODO
        if self.use_dir_tree:
            parent_dir_tree_iter = self._add_parent_dirs(category_node, directory)
            self.model.append(parent_dir_tree_iter, [fmeta.signature, name, num_bytes_str, modify_time])
        else:
            tree_path = Gtk.TreePath(category_node.index_in_parent)
            tree_iter = self.model.get_iter(tree_path)
            self.model.append(tree_iter, [fmeta.signature, name, directory, num_bytes_str, modify_time])
        self.fmeta_set.add(fmeta)

    def _add_category(self, name):
        return self.model.append(None, [None, name, None, None, None])

    def add_item(self, fmeta):
        self.change_set.added.append(fmeta)

    def add_unexpected_item(self, fmeta):
        self.change_set.unexpected.append(fmeta)

