import logging.handlers
import gi
import threading
import os

gi.require_version("Gtk", "3.0")
from gi.repository import GLib, Gtk
from photo_report import DirDiffer
from widget.progress_meter import ProgressMeter
from widget.diff_tree import DiffTree

DATABASE_FILE_PATH = './MattGSuite.db'
PHOTOS_DIR_PATH = r"/home/msvoboda/GoogleDrive/Media/Svoboda-Family/Svoboda Family Photos"

#class Dashboard:


logger = logging.getLogger(__name__)


def diff_task(main_window):
    try:
        logging.info("Scanning local file structure")
        main_window.info_bar.set_label('Scanning local file structure...')

        sync_set = DirDiffer.scan_local_tree(PHOTOS_DIR_PATH, main_window.progress_meter, main_window.diff_tree_left)

        diff_result = DirDiffer.diff(PHOTOS_DIR_PATH, main_window.diff_tree_left, main_window.diff_tree_right)
        main_window.info_bar.set_label('Done with scan.')
        logging.info("Done with scan")
    except Exception as err:
        print('Diff task failed with exception')
        raise err


# Get absolute path from the given relative path (relative to this file's location)
def get_resource_path(rel_path):
    dir_of_py_file = os.path.dirname(__file__)
    rel_path_to_resource = os.path.join(dir_of_py_file, rel_path)
    abs_path_to_resource = os.path.abspath(rel_path_to_resource)
    return abs_path_to_resource


class DiffWindow(Gtk.Window):
    def __init__(self):
        Gtk.Window.__init__(self, title='UltrarSync')
        self.set_icon_from_file(get_resource_path("../resources/fslint_icon.png"))
        # Set minimum width and height
        self.set_size_request(900, 500)
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

        # Checkboxes:
        self.checkbox_panel = Gtk.Box(spacing=6, orientation=Gtk.Orientation.HORIZONTAL)
        # check for the following...
        self.button1 = Gtk.CheckButton(label="Empty dirs")
        self.checkbox_panel.pack_start(self.button1, True, True, 0)
        self.button1.set_sensitive(False)
        self.button2 = Gtk.CheckButton(label="Zero-length files")
        self.checkbox_panel.pack_start(self.button2, True, True, 0)
        self.button2.set_sensitive(False)
        self.button3= Gtk.CheckButton(label="Duplicate files")
        self.checkbox_panel.pack_start(self.button3, True, True, 0)
        self.button3.set_sensitive(False) # disable
        self.button4= Gtk.CheckButton(label="Unrecognized suffixes")
        self.checkbox_panel.pack_start(self.button4, True, True, 0)
        self.button4.set_sensitive(False) # disable
        self.button5= Gtk.CheckButton(label="Relative paths or file names differ")
        self.checkbox_panel.pack_start(self.button5, True, True, 0)
        self.button5.set_sensitive(False) # disable
        #self.button1.connect("toggled", self.on_button_toggled, "3")
        self.content_box.add(self.checkbox_panel)

        # Diff trees:
        self.diff_tree_panel = Gtk.Box(spacing=6, orientation=Gtk.Orientation.HORIZONTAL)
        self.content_box.add(self.diff_tree_panel)

        self.diff_tree_left = DiffTree()
        self.diff_tree_panel.pack_start(self.diff_tree_left.tree, True, True, 0)
        # Tree will take up all the excess space
        self.diff_tree_left.tree.set_vexpand(True)

        self.diff_tree_right = DiffTree()
        self.diff_tree_panel.pack_start(self.diff_tree_right.tree, True, True, 0)
        self.diff_tree_right.tree.set_vexpand(True)

        self.progress_meter = ProgressMeter(self.on_progress_made)

        self.button = Gtk.Button(label="Do Diff")
        self.button.connect("clicked", self.on_button_clicked)
        self.content_box.add(self.button)

        # TODO: create a 'Scan' button for each input source

        # TODO: allow merge for single file, or Merge All from left to right or right to left, or both

    # Callback from DirDiffer:
    def on_progress_made(self, progress, total):
        def update_progress(progress, total):
            self.info_bar.set_label(f'Scanning file {progress} of {total}')
        GLib.idle_add(update_progress, progress, total)

    def on_button_clicked(self, widget):
        action_thread = threading.Thread(target=diff_task, args=(self,))
        action_thread.daemon = True
        action_thread.start()


def main():
    # Docs: https://python-gtk-3-tutorial.readthedocs.io/en/latest/treeview.html
    # Path, Length, status

    window = DiffWindow()
    window.connect("destroy", Gtk.main_quit)
    window.show_all()
    Gtk.main()


if __name__ == '__main__':
    main()
