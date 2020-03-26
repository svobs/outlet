import logging.handlers
import gi
import threading
import time
gi.require_version("Gtk", "3.0")
from gi.repository import GLib, Gtk
from photo_report import DirDiffer
from progress_meter import ProgressMeter

DATABASE_FILE_PATH = './MattGSuite.db'
PHOTOS_DIR_PATH = r"/home/msvoboda/GoogleDrive/Media/Svoboda-Family/Svoboda Family Photos"

#class Dashboard:


logger = logging.getLogger(__name__)


def diff_task(main_window):
    logging.info("Scanning local file structure")
    main_window.info_bar.set_label('Scanning local file structure...')
    dir_differ = DirDiffer(DATABASE_FILE_PATH)
    sync_set = dir_differ.diff_full(PHOTOS_DIR_PATH, main_window.progress_meter)
    main_window.info_bar.set_label('Done with scan.')
    logging.info("Done with scan")


class MainWindow(Gtk.Window):
    def __init__(self, tree):
        Gtk.Window.__init__(self, title="Hello World")
        # Set minimum width and height
        self.set_size_request(600, 500)
        #self.set_default_size(500, 500)
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

        self.progress_meter = ProgressMeter(self.on_progress_made)

        self.button = Gtk.Button(label="Do Diff")
        self.button.connect("clicked", self.on_button_clicked)
        self.content_box.add(self.button)

    # Callback from DirDiffer:
    def on_progress_made(self, progress, total):
        def update_progress(progress, total):
            self.info_bar.set_label(f'Scanning file {progress} of {total}')
        GLib.idle_add(update_progress, progress, total)

    def on_button_clicked(self, widget):
        action = threading.Thread(target=diff_task, args=(self,))
        action.start()


def main():
    # Docs: https://python-gtk-3-tutorial.readthedocs.io/en/latest/treeview.html
    # Path, Length, status
    store = Gtk.ListStore(str, int, int)
    #treeiter = store.append(["The Art of Computer Programming", 123456, 1])
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

    window = MainWindow(tree)
    window.connect("destroy", Gtk.main_quit)
    window.show_all()
    Gtk.main()


if __name__ == '__main__':
    main()
