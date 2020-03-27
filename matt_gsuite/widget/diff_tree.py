from gi.repository import GLib, Gtk


class DiffTree:
    def __init__(self):
        store = Gtk.ListStore(str, int, int)
        #treeiter = store.append(["The Art of Computer Programming", 123456, 1])
        #print(store[treeiter][2]) # Prints value of third column

        self.tree = Gtk.TreeView(model=store)

        renderer = Gtk.CellRendererText()
        column = Gtk.TreeViewColumn("Name", renderer, text=0)
        self.tree.append_column(column)

        renderer = Gtk.CellRendererText()
        column = Gtk.TreeViewColumn("Directory", renderer, text=0)
        self.tree.append_column(column)

        renderer = Gtk.CellRendererText()
        column = Gtk.TreeViewColumn("Size", renderer, text=1)
        self.tree.append_column(column)

        renderer = Gtk.CellRendererText()
        column = Gtk.TreeViewColumn("Mod Date", renderer, text=1)
        self.tree.append_column(column)

        renderer = Gtk.CellRendererText()
        column = Gtk.TreeViewColumn("Change Type", renderer, text=2)
        self.tree.append_column(column)

        def on_tree_selection_changed(selection):
            model, treeiter = selection.get_selected_rows()
            if treeiter is not None and len(treeiter) > 0:
                print("You selected", model[treeiter][0])

        select = self.tree.get_selection()
        select.set_mode(Gtk.SelectionMode.MULTIPLE)
        select.connect("changed", on_tree_selection_changed)

   # def add_item(self, sync_item):

