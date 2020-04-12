import logging.handlers
import gi
import sys
import threading
import file_util
from file_util import get_resource_path
from stopwatch import Stopwatch

from fmeta.fmeta import FMetaTree
from fmeta.fmeta_builder import FMetaDirScanner, FMetaDatabase
from widget.progress_meter import ProgressMeter
from widget.diff_tree import DiffTree
import fmeta.diff_content_first as diff_content_first

gi.require_version("Gtk", "3.0")
from gi.repository import GLib, Gtk, Gio, GObject

LEFT_DB_PATH = get_resource_path('test/BiDirMerge/Left.db')
RIGHT_DB_PATH = get_resource_path('test/BiDirMerge/Right.db')
LEFT_DIR_PATH = get_resource_path('test/BiDirMerge/Sen Mitsuji Left')
RIGHT_DIR_PATH = get_resource_path('test/BiDirMerge/Sen Mitsuji Right')

WINDOW_ICON_PATH = get_resource_path("resources/fslint_icon.png")
STAGING_DIR_PATH = get_resource_path("temp")

logger = logging.getLogger(__name__)


def scan_disk(diff_tree, root_path):
    # Callback from FMetaDirScanner:
    def on_progress_made(progress, total, tree):
        def update_progress(progress, total):
            tree.set_status(f'Scanning file {progress} of {total}')
        GLib.idle_add(update_progress, progress, total)

    progress_meter = ProgressMeter(lambda p, t: on_progress_made(p, t, diff_tree))
    status_msg = f'Scanning files in tree: {diff_tree.root_path}'
    print(status_msg)
    diff_tree.set_status(status_msg)
    dir_scanner = FMetaDirScanner(root_path=root_path, progress_meter=progress_meter)
    return dir_scanner.scan_local_tree()


def diff_task(win, enable_db_cache):
    try:

        stopwatch = Stopwatch()
        if enable_db_cache:
            left_db = FMetaDatabase(LEFT_DB_PATH)
            left_fmeta_tree: FMetaTree
            win.diff_tree_right.set_status('Waiting...')
            if left_db.has_data():
                win.diff_tree_left.set_status(f'Loading Left data from DB: {LEFT_DB_PATH}')
                left_fmeta_tree = left_db.load_fmeta_tree(LEFT_DIR_PATH)
            else:
                left_fmeta_tree = scan_disk(win.diff_tree_left, LEFT_DIR_PATH)
                left_db.save_fmeta_tree(left_fmeta_tree)
        else:
            left_fmeta_tree = scan_disk(win.diff_tree_left, LEFT_DIR_PATH)
        stopwatch.stop()
        print(f'Left loaded in: {stopwatch}')
        win.diff_tree_left.set_status(left_fmeta_tree.get_summary())

        stopwatch = Stopwatch()
        if enable_db_cache:
            right_db = FMetaDatabase(RIGHT_DB_PATH)
            if right_db.has_data():
                win.diff_tree_right.set_status(f'Loading Right data from DB: {RIGHT_DB_PATH}')
                right_fmeta_tree = right_db.load_fmeta_tree(RIGHT_DIR_PATH)
            else:
                right_fmeta_tree = scan_disk(win.diff_tree_right, RIGHT_DIR_PATH)
                right_db.save_fmeta_tree(right_fmeta_tree)
        else:
            right_fmeta_tree = scan_disk(win.diff_tree_right, RIGHT_DIR_PATH)
        stopwatch.stop()
        print(f'Right loaded in: {stopwatch}')
        win.diff_tree_right.set_status(right_fmeta_tree.get_summary())

        logging.info("Diffing...")

        stopwatch = Stopwatch()
        diff_content_first.diff(left_fmeta_tree, right_fmeta_tree, compare_paths_also=True, use_modify_times=False)
        stopwatch.stop()
        print(f'Diff completed in: {stopwatch}')

        def do_on_ui_thread():
            win.diff_tree_left.rebuild_ui_tree(left_fmeta_tree)
            win.diff_tree_right.rebuild_ui_tree(right_fmeta_tree)

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

        def do_on_ui_thread(err_msg):
            GLib.idle_add(lambda: win.show_error_msg('Diff task failed due to unexpected error', err_msg))
        do_on_ui_thread(repr(err))
        raise


class MergePreviewDialog(Gtk.Dialog):

    def __init__(self, parent, fmeta_tree):
        Gtk.Dialog.__init__(self, "Confirm Merge", parent, 0)
        self.add_button(Gtk.STOCK_CANCEL, Gtk.ResponseType.CANCEL)
        self.add_button(Gtk.STOCK_OK, Gtk.ResponseType.OK)

        self.set_default_size(700, 700)

        self.fmeta_tree = fmeta_tree

        box = self.get_content_area()
        self.content_box = Gtk.Box(spacing=6, orientation=Gtk.Orientation.VERTICAL)
        box.add(self.content_box)

        label = Gtk.Label(label="The following changes will be made:")
        self.content_box.add(label)

        self.diff_tree = DiffTree(parent_win=self, root_path=self.fmeta_tree.root_path, editable=False)
        self.content_box.pack_start(self.diff_tree.content_box, True, True, 0)

        self.diff_tree.rebuild_ui_tree(self.fmeta_tree)

        self.show_all()


class DiffWindow(Gtk.ApplicationWindow):
    def __init__(self, application):
        Gtk.Window.__init__(self, application=application)
        enable_db_cache = False

        self.set_title('UltraSync')
        # program icon:
        self.set_icon_from_file(WINDOW_ICON_PATH)
        # Set minimum width and height
        self.set_size_request(1400, 800)
        self.set_border_width(10)
        self.content_box = Gtk.Box(spacing=6, orientation=Gtk.Orientation.VERTICAL)
        self.add(self.content_box)

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

        self.sizegroups = {'root_paths': Gtk.SizeGroup(mode=Gtk.SizeGroupMode.VERTICAL),
                           'tree_status': Gtk.SizeGroup(mode=Gtk.SizeGroupMode.VERTICAL)}

        # Diff Trees:
        self.diff_tree_left = DiffTree(parent_win=self, root_path=LEFT_DIR_PATH, editable=True, sizegroups=self.sizegroups)
        diff_tree_panes.pack1(self.diff_tree_left.content_box, resize=True, shrink=False)
        self.diff_tree_right = DiffTree(parent_win=self, root_path=RIGHT_DIR_PATH, editable=True, sizegroups=self.sizegroups)
        diff_tree_panes.pack2(self.diff_tree_right.content_box, resize=True, shrink=False)

        # Bottom button panel:
        self.bottom_button_panel = Gtk.Box(spacing=6, orientation=Gtk.Orientation.HORIZONTAL)
        self.content_box.add(self.bottom_button_panel)

        self.diff_action_btn = Gtk.Button(label="Diff (content-first)")
        self.diff_action_btn.connect("clicked", self.on_diff_button_clicked, enable_db_cache)
        self.bottom_button_panel.pack_start(self.diff_action_btn, True, True, 0)

      #  menubutton = Gtk.MenuButton()
      #  self.content_box.add(menubutton)
       # menumodel = Gio.Menu()
        #menubutton.set_menu_model(menumodel)
       # menumodel.append("New", "app.new")
      #  menumodel.append("Quit", "app.quit")

        # TODO: create a 'Scan' button for each input source

    def on_diff_button_clicked(self, widget, enable_db_cache):
        action_thread = threading.Thread(target=diff_task, args=(self, enable_db_cache))
        action_thread.daemon = True
        action_thread.start()

    def on_merge_btn_clicked(self, widget):
        print('Merge btn clicked')

        left_selected_changes = self.diff_tree_left.get_selected_changes()
        print(f'Left changes: {left_selected_changes.get_summary()}')
        right_selected_changes = self.diff_tree_right.get_selected_changes()
        print(f'Right changes: {right_selected_changes.get_summary()}')
        if len(left_selected_changes.get_all()) == 0 and len(right_selected_changes.get_all()) == 0:
            self.show_error_msg('You must select change(s) first.')
            return

        merged_changes_tree, conflict_pairs = diff_content_first.merge_change_trees(left_selected_changes, right_selected_changes)
        if conflict_pairs is not None:
            # TODO: more informative error
            self.show_error_msg('Cannot merge', f'{len(conflict_pairs)} conflicts found')
            return

        print(f'Merged changes: {merged_changes_tree.get_summary()}')

        # Preview changes in UI pop-up
        dialog = MergePreviewDialog(self, merged_changes_tree)
        response = dialog.run()

        try:
            if response == Gtk.ResponseType.OK:
                print("The OK button was clicked")
                staging_dir = STAGING_DIR_PATH
                # TODO: clear dir after use
                file_util.apply_changes_atomically(changes=merged_changes_tree, staging_dir=staging_dir)
            elif response == Gtk.ResponseType.CANCEL:
                print("The Cancel button was clicked")
        except Exception as err:
            print('Diff task failed with exception')

            def do_on_ui_thread(err_msg):
                GLib.idle_add(lambda: self.show_error_msg('Diff task failed due to unexpected error', err_msg))
            do_on_ui_thread(repr(err))
            raise
        finally:
            dialog.destroy()

    def show_error_msg(self, msg, secondary_msg=None):
        dialog = Gtk.MessageDialog(parent=self, modal=True, message_type=Gtk.MessageType.ERROR, buttons=Gtk.ButtonsType.CANCEL, text=msg)
        if secondary_msg is None:
            print(f'ERROR: {msg}')
        else:
            print(f'ERROR: {msg}: {secondary_msg}')
            dialog.format_secondary_text(secondary_msg)

        def run_on_ui_thread():
            dialog.run()
            dialog.destroy()

        run_on_ui_thread()

    def on_question_clicked(self, msg, secondary_msg=None):
        dialog = Gtk.MessageDialog(parent=self, modal=True, message_type=Gtk.MessageType.QUESTION, buttons=Gtk.ButtonsType.YES_NO, text=msg)
        if secondary_msg is None:
            print(f'Q: {msg}')
        else:
            print(f'Q: {msg}: {secondary_msg}')
            dialog.format_secondary_text(secondary_msg)
        response = dialog.run()
        if response == Gtk.ResponseType.YES:
            print("QUESTION dialog closed by clicking YES button")
        elif response == Gtk.ResponseType.NO:
            print("QUESTION dialog closed by clicking NO button")

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
