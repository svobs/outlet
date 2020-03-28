import os
import humanfriendly
from fmeta.fmeta import FMetaSet
from gi.repository import GLib, Gtk


class DiffTree:
    def __init__(self):
        self.fmeta_set = FMetaSet()
        self.model = Gtk.TreeStore(str, str, str, str, str)
        self.category_unexpected = None
        self.category_added = None
        self.category_removed = None
        self.tree = Gtk.TreeView(model=self.model)
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

        # 2 DIRECTORY
        renderer = Gtk.CellRendererText()
        renderer.set_property('width-chars', 20)
        column = Gtk.TreeViewColumn("Directory", renderer, text=2)
        column.set_sort_column_id(2)

        column.set_sizing(Gtk.TreeViewColumnSizing.AUTOSIZE)
      #  column.set_fixed_width(50)
        column.set_min_width(50)
        # column.set_max_width(300)
        column.set_expand(True)
        column.set_resizable(True)
        column.set_reorderable(True)
        self.tree.append_column(column)

        # 3 SIZE
        renderer = Gtk.CellRendererText()
        renderer.set_property('width-chars', 10)
        column = Gtk.TreeViewColumn("Size", renderer, text=3)
        column.set_sort_column_id(3)

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

        self.model.set_sort_func(3, compare_file_size, None)

        # 4 MODIFICATION DATE
        renderer = Gtk.CellRendererText()
        renderer.set_property('width-chars', 8)
        column = Gtk.TreeViewColumn("Modification Date", renderer, text=4)
        column.set_sort_column_id(4)

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

    def _add(self, category, fmeta):
        directory, name = os.path.split(fmeta.file_path)
        num_bytes_str = humanfriendly.format_size(fmeta.length)
        self.model.append(category, [fmeta.signature, name, directory, num_bytes_str, fmeta.modify_ts])
        self.fmeta_set.add(fmeta)

    def _add_category(self, name):
        return self.model.append(None, [None, name, None, None, None])

    def add_item(self, fmeta):
        def do_on_ui_thread():
            if self.category_added is None:
                self.category_added = self._add_category('Added')
            self._add(self.category_added, fmeta)

        GLib.idle_add(do_on_ui_thread)

    def add_unexpected_item(self, fmeta):
        def do_on_ui_thread():
            if self.category_unexpected is None:
                self.category_unexpected = self._add_category('Unexpected')
            self._add(self.category_unexpected, fmeta)

        GLib.idle_add(do_on_ui_thread)

