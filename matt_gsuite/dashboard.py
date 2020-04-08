import logging.handlers
import gi
import sys
import threading
import file_util

from fmeta.fmeta import FMetaSet
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


def diff_task(main_window):
    try:
        left_db_loader = FMetaLoader(LEFT_DB_PATH)
        left_fmeta_set : FMetaSet
        if left_db_loader.has_data():
            print(f'Loading Left data from DB: {LEFT_DB_PATH}')
            left_fmeta_set = left_db_loader.build_fmeta_set_from_db(LEFT_DIR_PATH)
        else:
            logging.info(f"Scanning files in left tree: {main_window.diff_tree_left.root_path}")
            main_window.progress_meter = ProgressMeter(main_window.on_progress_made)
            main_window.info_bar.set_label(f'Scanning files in tree: {main_window.diff_tree_left.root_path}')
            left_fmeta_set = FMetaScanner.scan_local_tree(LEFT_DIR_PATH, main_window.progress_meter)
            left_db_loader.store_fmeta_to_db(left_fmeta_set)

        right_db_loader = FMetaLoader(RIGHT_DB_PATH)
        if right_db_loader.has_data():
            print(f'Loading Right data from DB: {RIGHT_DB_PATH}')
            right_fmeta_set = right_db_loader.build_fmeta_set_from_db(RIGHT_DIR_PATH)
        else:
            logging.info(f"Scanning files in right tree: {main_window.diff_tree_right.root_path}")
            main_window.progress_meter = ProgressMeter(main_window.on_progress_made)
            main_window.info_bar.set_label(f'Scanning files in tree: {main_window.diff_tree_right.root_path}')
            right_fmeta_set = FMetaScanner.scan_local_tree(RIGHT_DIR_PATH, main_window.progress_meter)
            right_db_loader.store_fmeta_to_db(right_fmeta_set)

        main_window.info_bar.set_label('Diffing...')
        logging.info("Diffing...")

        left_change_set, right_change_set = diff_content_first.diff(left_fmeta_set, right_fmeta_set, compare_paths_also=True, use_modify_times=False)

        def do_on_ui_thread():
            # TODO: put tree + statusbar into their own module
            main_window.diff_tree_left.rebuild_ui_tree(left_change_set, left_fmeta_set)
            main_window.left_tree_statusbar.set_label(left_fmeta_set.get_summary())
            main_window.diff_tree_right.rebuild_ui_tree(right_change_set, right_fmeta_set)
            main_window.right_tree_statusbar.set_label(right_fmeta_set.get_summary())

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
        raise


class DiffWindow(Gtk.ApplicationWindow):
    def __init__(self, application):
        Gtk.Window.__init__(self, application=application)
        self.set_title('UltrarSync')
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
        self.left_open_btn = Gtk.Button(label='Open left...')
        self.checkbox_panel.pack_start(self.left_open_btn, True, True, 0)
        self.right_open_btn = Gtk.Button(label='Open right...')
        self.checkbox_panel.pack_start(self.right_open_btn, True, True, 0)

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
        self.diff_tree_left = DiffTree()
        self.diff_tree_right = DiffTree()
        self.diff_tree_panel, self.left_tree_statusbar, self.right_tree_statusbar = DiffWindow.build_two_tree_panel(self.diff_tree_left.treeview, self.diff_tree_right.treeview)
        self.content_box.add(self.diff_tree_panel)

        # Bottom button panel:
        self.bottom_button_panel = Gtk.Box(spacing=6, orientation=Gtk.Orientation.HORIZONTAL)
        self.content_box.add(self.bottom_button_panel)

        self.diff_action_btn = Gtk.Button(label="Diff (content-first)")
        self.diff_action_btn.connect("clicked", self.on_diff_button_clicked)
        self.bottom_button_panel.pack_start(self.diff_action_btn, True, True, 0)

        # Open file action
        choose_left_root_action = Gio.SimpleAction.new("choose_root_left", None)
        choose_left_root_action.connect("activate", self.choose_left_root_callback)
        # TODO: how to pass args to action?
        self.add_action(choose_left_root_action)
        self.left_open_btn.set_action_name('win.choose_root_left')

        choose_right_root_action = Gio.SimpleAction.new("choose_root_right", None)
        choose_right_root_action.connect("activate", self.choose_right_root_callback)
        self.add_action(choose_right_root_action)
        self.right_open_btn.set_action_name('win.choose_root_right')


        menubutton = Gtk.MenuButton()
        self.content_box.add(menubutton)

        menumodel = Gio.Menu()
        menubutton.set_menu_model(menumodel)
        menumodel.append("New", "app.new")
        menumodel.append("Quit", "app.quit")

        # TODO: create a 'Scan' button for each input source

    def choose_left_root_callback(self, action, parameter):
        # create a filechooserdialog to open:
        # the arguments are: title of the window, parent_window, action,
        # (buttons, response)
        open_dialog = Gtk.FileChooserDialog("Pick a file", self,
                                            Gtk.FileChooserAction.OPEN,
                                            (Gtk.STOCK_CANCEL, Gtk.ResponseType.CANCEL,
                                             Gtk.STOCK_OPEN, Gtk.ResponseType.ACCEPT))

        # not only local files can be selected in the file selector
        open_dialog.set_local_only(False)
        # dialog always on top of the textview window
        open_dialog.set_modal(True)
        # connect the dialog with the callback function open_response_cb()
        open_dialog.connect("response", self.open_response_cb)
        # show the dialog
        open_dialog.show()

    # callback for open
    def choose_right_root_callback(self, action, parameter):
        # create a filechooserdialog to open:
        # the arguments are: title of the window, parent_window, action,
        # (buttons, response)
        open_dialog = Gtk.FileChooserDialog("Pick a file", self,
                                            Gtk.FileChooserAction.OPEN,
                                            (Gtk.STOCK_CANCEL, Gtk.ResponseType.CANCEL,
                                             Gtk.STOCK_OPEN, Gtk.ResponseType.ACCEPT))

        # not only local files can be selected in the file selector
        open_dialog.set_local_only(False)
        # dialog always on top of the textview window
        open_dialog.set_modal(True)
        # connect the dialog with the callback function open_response_cb()
        open_dialog.connect("response", self.open_response_cb)
        # show the dialog
        open_dialog.show()

    # callback function for the dialog open_dialog
    def open_response_cb(self, dialog, response_id):
        open_dialog = dialog
        # if response is "ACCEPT" (the button "Open" has been clicked)
        if response_id == Gtk.ResponseType.ACCEPT:
            # self.file is the file that we get from the FileChooserDialog
            self.file = open_dialog.get_file()
            # an empty string (provisionally)
            content = ""
            try:
                # load the content of the file into memory:
                # success is a boolean depending on the success of the operation
                # content is self-explanatory
                # etags is an entity tag (can be used to quickly determine if the
                # file has been modified from the version on the file system)
                [success, content, etags] = self.file.load_contents(None)
            except GObject.GError as e:
                print("Error: " + e.message)
            # set the content as the text into the buffer
            self.buffer.set_text(content, len(content))
            print("opened: " + open_dialog.get_filename())
        # if response is "CANCEL" (the button "Cancel" has been clicked)
        elif response_id == Gtk.ResponseType.CANCEL:
            print("cancelled: FileChooserAction.OPEN")
        # destroy the FileChooserDialog
        dialog.destroy()


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

        left_change_set = self.diff_tree_left.get_selected_change_set()
        right_change_set = self.diff_tree_right.get_selected_change_set()
        minimized_change_set_left, minimized_change_set_right = diff_content_first.simplify_change_sets(left_change_set, right_change_set)
        # TODO: preview changes in UI pop-up
        file_util.apply_change_set(minimized_change_set_left)
        file_util.apply_change_set(minimized_change_set_right)

    def on_merge_both_btn_clicked(self, widget):
        print('MergeBoth btn clicked')
        pass

    def on_merge_right_btn_clicked(self, widget):
        print('MergeRight btn clicked')
        pass


class MattApplication(Gtk.Application):
    """See: https://athenajc.gitbooks.io/python-gtk-3-api/content/gtk-group/gtkapplication.html"""
    def __init__(self):
        Gtk.Application.__init__(self)

    def do_activate(self):
        window = DiffWindow(self)
        window.show_all()

    def do_startup(self):
        Gtk.Application.do_startup(self)

        new_action = Gio.SimpleAction.new("new", None)
        new_action.connect("activate", self.new_callback)
        self.add_action(new_action)

        quit_action = Gio.SimpleAction.new("quit", None)
        quit_action.connect("activate", self.quit_callback)
        self.add_action(quit_action)
        # See: https://developer.gnome.org/gtk3/stable/gtk3-Keyboard-Accelerators.html#gtk-accelerator-parse
        self.set_accels_for_action('app.quit', 'q')

    def new_callback(self, action, parameter):
        print("You clicked New")

    def quit_callback(self, action, parameter):
        print("You clicked Quit")
        self.quit()


def main():
    application = MattApplication()
    exit_status = application.run(sys.argv)
    sys.exit(exit_status)


if __name__ == '__main__':
    main()
