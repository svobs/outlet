import logging.handlers
import gi
import sys
import threading
import file_util
from stopwatch import Stopwatch

from fmeta.fmeta import FMetaTree
from fmeta.fmeta_builder import FMetaScanner, FMetaLoader
from widget.progress_meter import ProgressMeter
from widget.diff_tree import DiffTree
import fmeta.diff_content_first as diff_content_first

gi.require_version("Gtk", "3.0")
from gi.repository import GLib, Gtk, Gio, GObject

LEFT_DB_PATH = '../test/Bigger/MattLeft.db'
RIGHT_DB_PATH = '../test/Bigger/MattRight.db'
LEFT_DIR_PATH = r"/home/msvoboda/GoogleDrive/Media/Svoboda-Family/Svoboda Family Photos"
RIGHT_DIR_PATH = r"/media/msvoboda/Thumb128G/Takeout/Google Photos"

logger = logging.getLogger(__name__)


def diff_task(win):
    try:
        status_widget = win.diff_tree_left

        # Callback from FMetaScanner:
        def on_progress_made(progress, total):
            def update_progress(progress, total):
                status_widget.set_status(f'Scanning file {progress} of {total}')
            GLib.idle_add(update_progress, progress, total)

        stopwatch = Stopwatch()
        left_db_loader = FMetaLoader(LEFT_DB_PATH)
        left_fmeta_set: FMetaTree
        win.diff_tree_right.set_status('Waiting...')
        if left_db_loader.has_data():
            win.diff_tree_left.set_status(f'Loading Left data from DB: {LEFT_DB_PATH}')
            left_fmeta_set = left_db_loader.build_fmeta_set_from_db(LEFT_DIR_PATH)
        else:
            progress_meter = ProgressMeter(on_progress_made)
            win.diff_tree_left.set_status(f'Scanning files in tree: {win.diff_tree_left.root_path}')
            left_fmeta_set = FMetaScanner.scan_local_tree(LEFT_DIR_PATH, progress_meter)
            left_db_loader.store_fmeta_to_db(left_fmeta_set)
        stopwatch.stop()
        print(f'Left loaded in: {stopwatch}')

        stopwatch = Stopwatch()
        right_db_loader = FMetaLoader(RIGHT_DB_PATH)
        if right_db_loader.has_data():
            win.diff_tree_left.set_status(f'Loading Right data from DB: {RIGHT_DB_PATH}')
            right_fmeta_set = right_db_loader.build_fmeta_set_from_db(RIGHT_DIR_PATH)
        else:
            logging.info(f"Scanning files in right tree: {win.diff_tree_right.root_path}")

            status_widget = win.diff_tree_left
            progress_meter = ProgressMeter(on_progress_made)
            win.diff_tree_left.set_status(f'Scanning files in tree: {win.diff_tree_right.root_path}')
            right_fmeta_set = FMetaScanner.scan_local_tree(RIGHT_DIR_PATH, progress_meter)
            right_db_loader.store_fmeta_to_db(right_fmeta_set)
        stopwatch.stop()
        print(f'Right loaded in: {stopwatch}')

        logging.info("Diffing...")

        stopwatch = Stopwatch()
        diff_content_first.diff(left_fmeta_set, right_fmeta_set, compare_paths_also=True, use_modify_times=False)
        stopwatch.stop()
        print(f'Diff completed in: {stopwatch}')

        def do_on_ui_thread():
            win.diff_tree_left.rebuild_ui_tree(left_fmeta_set)
            win.diff_tree_right.rebuild_ui_tree(right_fmeta_set)

            # Replace diff btn with merge buttons
            win.merge_btn = Gtk.Button(label="Merge Selected...")
            win.merge_btn.connect("clicked", win.on_merge_btn_clicked)

            win.bottom_button_panel.remove(win.diff_action_btn)
            win.bottom_button_panel.pack_start(win.merge_btn, True, True, 0)
            win.merge_btn.show()
            print('Done')
            logging.info('Done.')

        GLib.idle_add(do_on_ui_thread)
    except Exception as err:
        print('Diff task failed with exception')
        raise


class MergePreviewDialog(Gtk.Dialog):

    def __init__(self, parent, fmeta_tree):
        Gtk.Dialog.__init__(self, "Confirm Merge", parent, 0)
        self.add_button(Gtk.STOCK_CANCEL, Gtk.ResponseType.CANCEL)
        self.add_button(Gtk.STOCK_OK, Gtk.ResponseType.OK)

        self.set_default_size(700, 700)

        label = Gtk.Label(label="The following changes will be made:")

        self.fmeta_tree = fmeta_tree
        # TODO: include DiffTree

        box = self.get_content_area()
        box.add(label)
        self.show_all()


class DiffWindow(Gtk.ApplicationWindow):
    def __init__(self, application):
        Gtk.Window.__init__(self, application=application)
        self.set_title('UltraSync')
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

# TODO
       # self.info_bar, info_bar_container = DiffWindow.build_info_bar()
    #    self.content_box.add(info_bar_container)

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

        diff_tree_panes = Gtk.HPaned()
        self.content_box.add(diff_tree_panes)

        # Diff Trees:
        self.diff_tree_left = DiffTree(parent_win=self, root_path=LEFT_DIR_PATH)
        diff_tree_panes.pack1(self.diff_tree_left.content_box, resize=True, shrink=False)
        self.diff_tree_right = DiffTree(parent_win=self, root_path=RIGHT_DIR_PATH)
        diff_tree_panes.pack2(self.diff_tree_right.content_box, resize=True, shrink=False)

        # Bottom button panel:
        self.bottom_button_panel = Gtk.Box(spacing=6, orientation=Gtk.Orientation.HORIZONTAL)
        self.content_box.add(self.bottom_button_panel)

        self.diff_action_btn = Gtk.Button(label="Diff (content-first)")
        self.diff_action_btn.connect("clicked", self.on_diff_button_clicked)
        self.bottom_button_panel.pack_start(self.diff_action_btn, True, True, 0)

      #  menubutton = Gtk.MenuButton()
      #  self.content_box.add(menubutton)
       # menumodel = Gio.Menu()
        #menubutton.set_menu_model(menumodel)
       # menumodel.append("New", "app.new")
      #  menumodel.append("Quit", "app.quit")

        # TODO: create a 'Scan' button for each input source

    def on_diff_button_clicked(self, widget):
        action_thread = threading.Thread(target=diff_task, args=(self,))
        action_thread.daemon = True
        action_thread.start()

    def on_merge_btn_clicked(self, widget):
        print('Merge btn clicked')

        left_selected_changes = self.diff_tree_left.get_selected_changes()
        print(f'Left changes: {left_selected_changes.get_summary()}')
        right_selected_changes = self.diff_tree_right.get_selected_changes()
        print(f'Right changes: {right_selected_changes.get_summary()}')

        merged_changes_tree = diff_content_first.merge_change_trees(left_selected_changes, right_selected_changes)

        # Preview changes in UI pop-up
        dialog = MergePreviewDialog(self, merged_changes_tree)
        response = dialog.run()

        if response == Gtk.ResponseType.OK:
            print("The OK button was clicked")
            file_util.apply_change_set(merged_changes_tree)
        elif response == Gtk.ResponseType.CANCEL:
            print("The Cancel button was clicked")

        dialog.destroy()


class MattApplication(Gtk.Application):
    """Main application.
    See: https://athenajc.gitbooks.io/python-gtk-3-api/content/gtk-group/gtkapplication.html"""
    def __init__(self):
        Gtk.Application.__init__(self)
        self.window = None

        self.add_main_option("test", ord("t"), GLib.OptionFlags.NONE,
                             GLib.OptionArg.NONE, "Command line test", None)

    def do_activate(self):
        # We only allow a single window and raise any existing ones
        if not self.window:
            # Windows are associated with the application
            # when the last one is closed the application shuts down
            self.window = DiffWindow(application=self)
            self.window.show_all()

        self.window.present()

    def do_command_line(self, command_line):
        options = command_line.get_options_dict()
        # convert GVariantDict -> GVariant -> dict
        options = options.end().unpack()

        if "test" in options:
            # This is printed on the main instance
            print("Test argument received: %s" % options["test"])

        self.activate()
        return 0

    def do_startup(self):
        Gtk.Application.do_startup(self)

        quit_action = Gio.SimpleAction.new("quit", None)
        quit_action.connect("activate", self.quit_callback)
        self.add_action(quit_action)
        # See: https://developer.gnome.org/gtk3/stable/gtk3-Keyboard-Accelerators.html#gtk-accelerator-parse
        self.set_accels_for_action('app.quit', 'q')

    def quit_callback(self, action, parameter):
        print("You chose Quit")
        self.quit()


def main():
    application = MattApplication()
    exit_status = application.run(sys.argv)
    sys.exit(exit_status)


if __name__ == '__main__':
    main()
