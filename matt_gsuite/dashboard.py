import logging.handlers
import gi
gi.require_version("Gtk", "3.0")
from gi.repository import Gtk


#class Dashboard:


logger = logging.getLogger(__name__)


class MyWindow(Gtk.Window):
    def __init__(self, tree):
        Gtk.Window.__init__(self, title="Hello World")
        self.set_default_size(300, 500)
        self.set_border_width(10)
        self.content_box = Gtk.Box(spacing=6, orientation=Gtk.Orientation.VERTICAL)
        self.add(self.content_box)

        # Content:

        # Info bar (self.info_bar)
        info_bar_container = Gtk.Box(spacing=6, orientation=Gtk.Orientation.HORIZONTAL)
        self.content_box.add(info_bar_container)
        self.info_bar = Gtk.Label(label="Testing Matt x y z A B C")
        self.info_bar.set_justify(Gtk.Justification.LEFT)
        self.info_bar.set_line_wrap(True)
        info_bar_container.add(self.info_bar)

        self.button1 = Gtk.CheckButton(label="Filter1")
        self.button2 = Gtk.CheckButton(label="Filter2")
        self.checkbox_panel = Gtk.Box(spacing=6, orientation=Gtk.Orientation.HORIZONTAL)
        self.checkbox_panel.pack_start(self.button1, True, True, 0)
        self.checkbox_panel.pack_start(self.button2, True, True, 0)
        self.button1.set_sensitive(False)
        self.button2.set_sensitive(False) # disable

        #self.button1.connect("toggled", self.on_button_toggled, "3")
        self.content_box.add(self.checkbox_panel)

        # Tree will take up all the excess space
        tree.set_vexpand(True)
        self.content_box.add(tree)

        self.button = Gtk.Button(label="Click Here")
        self.button.connect("clicked", self.on_button_clicked)
        self.content_box.add(self.button)

    def on_button_clicked(self, widget):
        print("Hello World")


def main():

    # Docs: https://python-gtk-3-tutorial.readthedocs.io/en/latest/treeview.html
    # Path, Length, status
    store = Gtk.ListStore(str, int, int)
    treeiter = store.append(["The Art of Computer Programming",
                            123456, 1])
    #print(store[treeiter][2]) # Prints value of third column

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
