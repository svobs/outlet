import os
import humanfriendly
from fmeta.fmeta import FMetaSet
from gi.repository import GLib, Gtk


class DiffTree:
    def __init__(self):
        self.fmeta_set = FMetaSet()
        self.store = Gtk.ListStore(str, str, str, str, str, str)

        self.tree = Gtk.TreeView(model=self.store)

        renderer = Gtk.CellRendererText()
        column = Gtk.TreeViewColumn("Name", renderer, text=1)
        column.set_sort_column_id(1)
        #column.set_expand(True)
        column.set_max_width(300)
        self.tree.append_column(column)

        renderer = Gtk.CellRendererText()
        column = Gtk.TreeViewColumn("Directory", renderer, text=2)
        column.set_sort_column_id(2)
        column.set_expand(True)
        column.set_resizable(True)
        column.set_min_width(50)
        #column.set_max_width(400)
        column.set_sizing(Gtk.TreeViewColumnSizing.AUTOSIZE)
        self.tree.append_column(column)

        renderer = Gtk.CellRendererText()
        column = Gtk.TreeViewColumn("Size", renderer, text=3)
        column.set_sort_column_id(3)
        column.set_expand(False)
        column.set_resizable(True)
        column.set_min_width(50)
        #column.set_max_width(100)
        column.set_sizing(Gtk.TreeViewColumnSizing.AUTOSIZE)
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

        self.store.set_sort_func(3, compare_file_size, None)

        renderer = Gtk.CellRendererText()
        column = Gtk.TreeViewColumn("Modification Date", renderer, text=4)
        column.set_sort_column_id(4)
        column.set_expand(False)
        column.set_resizable(True)
        column.set_min_width(50)
        #column.set_max_width(150)
        column.set_sizing(Gtk.TreeViewColumnSizing.AUTOSIZE)
        self.tree.append_column(column)

        renderer = Gtk.CellRendererText()
        column = Gtk.TreeViewColumn("Change Type", renderer, text=5)
        column.set_sort_column_id(5)
        column.set_expand(False)
        column.set_resizable(True)
        column.set_min_width(50)
        #column.set_max_width(50)
        column.set_sizing(Gtk.TreeViewColumnSizing.AUTOSIZE)
        self.tree.append_column(column)

        def on_tree_selection_changed(selection):
            model, treeiter = selection.get_selected_rows()
            if treeiter is not None and len(treeiter) > 0:
                print("You selected", model[treeiter][0])

        select = self.tree.get_selection()
        select.set_mode(Gtk.SelectionMode.MULTIPLE)
        select.connect("changed", on_tree_selection_changed)

    def add_item(self, fmeta, item_type):
        directory, name = os.path.split(fmeta.file_path)
        num_bytes_str = humanfriendly.format_size(fmeta.length)
        self.store.append([fmeta.signature, name, directory, num_bytes_str, fmeta.modify_ts, item_type])
        self.fmeta_set.add(fmeta)

    def add_unexpected_item(self, fmeta):
        self.add_item(fmeta, 'Unexpected')

