import logging.handlers
import gi
gi.require_version("Gtk", "3.0")
from gi.repository import Gtk


#class Dashboard:


logger = logging.getLogger(__name__)


class MyWindow(Gtk.Window):
    def __init__(self, tree):
        Gtk.Window.__init__(self, title="Hello World")

        self.button = Gtk.Button(label="Click Here")
        self.button.connect("clicked", self.on_button_clicked)
        #self.add(self.button)
        self.add(tree)

    def on_button_clicked(self, widget):
        print("Hello World")


def main():

    # Docs: https://python-gtk-3-tutorial.readthedocs.io/en/latest/treeview.html
    # Path, Length, status
    store = Gtk.ListStore(str, int, int)
    treeiter = store.append(["The Art of Computer Programming",
                            123456, 1])
    print(store[treeiter][2]) # Prints value of third column

    tree = Gtk.TreeView(model=store)
    renderer = Gtk.CellRendererText()
    column = Gtk.TreeViewColumn("Path", renderer, text=0)
    tree.append_column(column)

    renderer = Gtk.CellRendererText()
    column = Gtk.TreeViewColumn("Length", renderer, text=1)
    tree.append_column(column)

    renderer = Gtk.CellRendererText()
    column = Gtk.TreeViewColumn("Kind", renderer, text=2)
    tree.append_column(column)

    def on_tree_selection_changed(selection):
        model, treeiter = selection.get_selected_rows()
        if treeiter is not None and len(treeiter) > 0:
            print("You selected", model[treeiter][0])

    select = tree.get_selection()
    select.set_mode(Gtk.SelectionMode.MULTIPLE)
    select.connect("changed", on_tree_selection_changed)

    window = MyWindow(tree)
    window.connect("destroy", Gtk.main_quit)
    window.show_all()
    Gtk.main()


if __name__ == '__main__':
    main()
