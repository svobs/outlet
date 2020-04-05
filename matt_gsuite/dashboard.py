import logging.handlers
import gi
import threading
import os
import file_util

from fmeta.fmeta_builder import FMetaScanner
from fmeta.fmeta_builder import FMetaLoader
from widget.progress_meter import ProgressMeter
from widget.diff_tree import DiffTree
import fmeta.fmeta_diff as fmeta_diff

gi.require_version("Gtk", "3.0")
from gi.repository import GLib, Gtk

LEFT_DB_PATH = '../test/Bigger/MattLeft.db'
RIGHT_DB_PATH = '../test/Bigger/MattRight.db'
LEFT_DIR_PATH = r"/home/msvoboda/GoogleDrive/Media/Svoboda-Family/Svoboda Family Photos"
RIGHT_DIR_PATH = r"/media/msvoboda/Thumb128G/Takeout/Google Photos"

logger = logging.getLogger(__name__)


def diff_task(main_window):
    try:
        left_db_loader = FMetaLoader(LEFT_DB_PATH)
        if left_db_loader.has_data():
            print(f'Loading Left data from DB: {LEFT_DB_PATH}')
            main_window.diff_tree_left.fmeta_set = left_db_loader.build_fmeta_from_db()
        else:
            logging.info(f"Scanning files in left tree: {main_window.diff_tree_left.root_path}")
            main_window.progress_meter = ProgressMeter(main_window.on_progress_made)
            main_window.info_bar.set_label(f'Scanning files in tree: {main_window.diff_tree_left.root_path}')
            FMetaScanner.scan_local_tree(main_window.diff_tree_left, main_window.progress_meter)
            left_db_loader.store_fmeta_to_db(main_window.diff_tree_left)

        right_db_loader = FMetaLoader(RIGHT_DB_PATH)
        if right_db_loader.has_data():
            print(f'Loading Right data from DB: {RIGHT_DB_PATH}')
            main_window.diff_tree_right.fmeta_set = right_db_loader.build_fmeta_from_db()
        else:
            logging.info(f"Scanning files in right tree: {main_window.diff_tree_right.root_path}")
            main_window.progress_meter = ProgressMeter(main_window.on_progress_made)
            main_window.info_bar.set_label(f'Scanning files in tree: {main_window.diff_tree_right.root_path}')
            FMetaScanner.scan_local_tree(main_window.diff_tree_right, main_window.progress_meter)
            right_db_loader.store_fmeta_to_db(main_window.diff_tree_right)

        main_window.info_bar.set_label('Diffing...')
        logging.info("Diffing...")

        fmeta_diff.diff(main_window.diff_tree_left, main_window.diff_tree_right, compare_paths_also=True, use_modify_times=False)

        def do_on_ui_thread():
            # TODO: put tree + statusbar into their own module
            main_window.diff_tree_left.rebuild_ui_tree()
            main_window.left_tree_statusbar.set_label(main_window.diff_tree_left.fmeta_set.get_summary())
            main_window.diff_tree_right.rebuild_ui_tree()
            main_window.right_tree_statusbar.set_label(main_window.diff_tree_right.fmeta_set.get_summary())

            # Replace diff btn with merge buttons
            main_window.merge_left_btn = Gtk.Button(label="<- Merge Left")
            main_window.merge_left_btn.connect("clicked", main_window.on_merge_left_btn_clicked)

            main_window.merge_both_btn = Gtk.Button(label="< Merge Both >")
            main_window.merge_both_btn.connect("clicked", main_window.on_merge_both_btn_clicked)

            main_window.merge_right_btn = Gtk.Button(label="Merge Right ->")
            main_window.merge_right_btn.connect("clicked", main_window.on_merge_right_btn_clicked)

            main_window.bottom_button_panel.remove(main_window.diff_action_btn)
            main_window.bottom_button_panel.pack_start(main_window.merge_left_btn, True, True, 0)
            main_window.bottom_button_panel.pack_start(main_window.merge_both_btn, True, True, 0)
            main_window.bottom_button_panel.pack_start(main_window.merge_right_btn, True, True, 0)
            main_window.merge_left_btn.show()
            main_window.merge_both_btn.show()
            main_window.merge_right_btn.show()
            main_window.info_bar.set_label('Done.')
            print('Done')
            logging.info('Done.')

        GLib.idle_add(do_on_ui_thread)
    except Exception as err:
        print('Diff task failed with exception')
        raise err


class DiffWindow(Gtk.Window):
    def __init__(self):
        Gtk.Window.__init__(self, title='UltrarSync')
        # program icon:
        self.set_icon_from_file(file_util.get_resource_path("../resources/fslint_icon.png"))
        # Set minimum width and height
        self.set_size_request(1400, 800)
      #  self.set_maximum_size(1800, 1200)
        #self.set_default_size(500, 500)
        self.set_border_width(10)
        self.content_box = Gtk.Box(spacing=6, orientation=Gtk.Orientation.VERTICAL)
        self.add(self.content_box)

        # Content:

        self.info_bar, info_bar_container = DiffWindow.build_info_bar()
        self.content_box.add(info_bar_container)

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
        self.diff_tree_left = DiffTree(LEFT_DIR_PATH)
        self.diff_tree_right = DiffTree(RIGHT_DIR_PATH)
        self.diff_tree_panel, self.left_tree_statusbar, self.right_tree_statusbar = DiffWindow.build_two_tree_panel(self.diff_tree_left.treeview, self.diff_tree_right.treeview)
        self.content_box.add(self.diff_tree_panel)

        # Bottom button panel:
        self.bottom_button_panel = Gtk.Box(spacing=6, orientation=Gtk.Orientation.HORIZONTAL)
        self.content_box.add(self.bottom_button_panel)

        self.diff_action_btn = Gtk.Button(label="Do Diff")
        self.diff_action_btn.connect("clicked", self.on_diff_button_clicked)
        self.bottom_button_panel.pack_start(self.diff_action_btn, True, True, 0)

        # TODO: create a 'Scan' button for each input source

    @staticmethod
    def build_two_tree_panel(tree_view_left, tree_view_right):
        diff_tree_panel = Gtk.Box(spacing=6, orientation=Gtk.Orientation.HORIZONTAL)
        # Each side is given 50% space
        diff_tree_panel.set_homogeneous(True)

        left_panel, left_tree_statusbar = DiffWindow.build_tree_view_side_panel(tree_view_left)
        diff_tree_panel.add(left_panel)

        right_panel, right_tree_statusbar = DiffWindow.build_tree_view_side_panel(tree_view_right)
        diff_tree_panel.add(right_panel)

        return diff_tree_panel, left_tree_statusbar, right_tree_statusbar

    @staticmethod
    def build_tree_view_side_panel(tree_view):
        side_panel = Gtk.Box(spacing=6, orientation=Gtk.Orientation.VERTICAL)

        # Tree will take up all the excess space
        tree_view.set_vexpand(True)
        tree_view.set_hexpand(False)
        tree_scroller = Gtk.ScrolledWindow()
        # No horizontal scrolling - only vertical
        tree_scroller.set_policy(Gtk.PolicyType.NEVER, Gtk.PolicyType.AUTOMATIC)
        tree_scroller.add(tree_view)
        # child, expand, fill, padding
        side_panel.pack_start(tree_scroller, False, True, 5)

        status_bar, status_bar_container = DiffWindow.build_info_bar()
        side_panel.pack_start(status_bar_container, False, True, 5)

        return side_panel, status_bar

    @staticmethod
    def build_info_bar():
        info_bar_container = Gtk.Box(spacing=6, orientation=Gtk.Orientation.HORIZONTAL)
        info_bar = Gtk.Label(label='')
        info_bar.set_justify(Gtk.Justification.LEFT)
        info_bar.set_line_wrap(True)
        info_bar_container.add(info_bar)
        return info_bar, info_bar_container

    # Callback from FMetaScanner:
    def on_progress_made(self, progress, total):
        def update_progress(progress, total):
            self.info_bar.set_label(f'Scanning file {progress} of {total}')
        GLib.idle_add(update_progress, progress, total)

    def on_diff_button_clicked(self, widget):
        action_thread = threading.Thread(target=diff_task, args=(self,))
        action_thread.daemon = True
        action_thread.start()

    def on_merge_left_btn_clicked(self, widget):
        print('MergeLeft btn clicked')
        # TODO: preview changes in UI pop-up
        file_util.apply_change_set(self.diff_tree_right.change_set, self.diff_tree_left.root_path, self.diff_tree_right.root_path)

    def on_merge_both_btn_clicked(self, widget):
        print('MergeBoth btn clicked')
        pass

    def on_merge_right_btn_clicked(self, widget):
        print('MergeRight btn clicked')
        pass


def main():
    # Docs: https://python-gtk-3-tutorial.readthedocs.io/en/latest/treeview.html
    # Path, Length, status

    window = DiffWindow()
    window.connect("destroy", Gtk.main_quit)
    window.show_all()
    Gtk.main()


if __name__ == '__main__':
    main()
