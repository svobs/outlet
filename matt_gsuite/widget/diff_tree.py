import os
from gi.repository import GLib, Gtk


class DiffTree:
    def __init__(self):
        self.store = Gtk.ListStore(str, str, int, str, int)

        self.tree = Gtk.TreeView(model=self.store)

        col_num = 0
        renderer = Gtk.CellRendererText()
        column = Gtk.TreeViewColumn("Name", renderer, text=0)
        self.tree.append_column(column)

        renderer = Gtk.CellRendererText()
        col_num += 1
        column = Gtk.TreeViewColumn("Directory", renderer, text=1)
        self.tree.append_column(column)

        renderer = Gtk.CellRendererText()
        col_num += 1
        column = Gtk.TreeViewColumn("Size", renderer, text=2)
        self.tree.append_column(column)

        renderer = Gtk.CellRendererText()
        col_num += 1
        column = Gtk.TreeViewColumn("Modification Date", renderer, text=3)
        self.tree.append_column(column)

        renderer = Gtk.CellRendererText()
        col_num += 1
        column = Gtk.TreeViewColumn("Change Type", renderer, text=4)
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
        self.store.append([name, directory, fmeta.length, fmeta.modify_ts, item_type])

    def add_unexpected_item(self, fmeta):
        self.add_item(fmeta, 3)

