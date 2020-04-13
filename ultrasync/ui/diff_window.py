import logging.handlers
import threading

from stopwatch import Stopwatch

import file_util
from file_util import get_resource_path, FMetaNoOp, FMetaError
from fmeta.fmeta import FMetaTree, Category
from fmeta.fmeta_builder import FMetaDirScanner, FMetaDatabase
from fmeta import diff_content_first
from ui.progress_meter import ProgressMeter
from ui.diff_tree import DiffTree
from ui.base_dialog import BaseDialog

import gi
gi.require_version("Gtk", "3.0")
from gi.repository import GLib, Gtk, Gio, GObject

# LEFT_DB_PATH = get_resource_path('test/BiDirMerge/Left.db')
# RIGHT_DB_PATH = get_resource_path('test/BiDirMerge/Right.db')
# LEFT_DIR_PATH = get_resource_path('test/BiDirMerge/Sen Mitsuji Left')
# RIGHT_DIR_PATH = get_resource_path('test/BiDirMerge/Sen Mitsuji Right')
LEFT_DB_PATH = get_resource_path('test/SvobodaLeft.db')
RIGHT_DB_PATH = get_resource_path('test/SvobodaRight.db')
LEFT_DIR_PATH = get_resource_path('/home/msvoboda/GoogleDrive/Media/Svoboda-Family/Svoboda Family Photos')
RIGHT_DIR_PATH = get_resource_path('/media/msvoboda/Thumb128G/Takeout/Google Photos')

WINDOW_ICON_PATH = get_resource_path("resources/fslint_icon.png")
STAGING_DIR_PATH = get_resource_path("temp")

logger = logging.getLogger(__name__)


def scan_disk(diff_tree):
    # Callback from FMetaDirScanner:
    def on_progress_made(progress, total, tree):
        tree.set_status(f'Scanning file {progress} of {total}')

    progress_meter = ProgressMeter(lambda p, t: on_progress_made(p, t, diff_tree))
    status_msg = f'Scanning files in tree: {diff_tree.root_path}'
    diff_tree.set_status(status_msg)
    dir_scanner = FMetaDirScanner(root_path=diff_tree.root_path, progress_meter=progress_meter)
    return dir_scanner.scan_local_tree()


class MergePreviewDialog(Gtk.Dialog, BaseDialog):

    def __init__(self, parent, fmeta_tree):
        Gtk.Dialog.__init__(self, "Confirm Merge", parent, 0)
        BaseDialog.__init__(self)
        self.add_button(Gtk.STOCK_CANCEL, Gtk.ResponseType.CANCEL)
        self.add_button(Gtk.STOCK_APPLY, Gtk.ResponseType.APPLY)

        self.set_default_size(700, 700)

        self.fmeta_tree = fmeta_tree

        box = self.get_content_area()
        self.content_box = Gtk.Box(spacing=6, orientation=Gtk.Orientation.VERTICAL)
        box.add(self.content_box)

        label = Gtk.Label(label="The following changes will be made:")
        self.content_box.add(label)

        self.diff_tree = DiffTree(parent_win=self, root_path=self.fmeta_tree.root_path, editable=False)
        self.diff_tree.set_status(fmeta_tree.get_summary())
        self.content_box.pack_start(self.diff_tree.content_box, True, True, 0)

        self.diff_tree.rebuild_ui_tree(self.fmeta_tree)

        self.connect("response", self.on_response)
        self.show_all()

    def on_response(self, dialog, response_id):
        print("response_id is", response_id)
        # destroy the widget (the dialog) when the function on_response() is called
        # (that is, when the button of the dialog has been clicked)

        try:
            if response_id == Gtk.ResponseType.APPLY:
                logger.debug("The APPLY button was clicked")
                self.on_apply_clicked()
            elif response_id == Gtk.ResponseType.CANCEL:
                logger.debug("The Cancel button was clicked")
        except FileNotFoundError as err:
            self.show_error_ui('File not found: ' + err.filename)
            raise
        except Exception as err:
            logger.exception(err)
            detail = f'{repr(err)}]'
            self.show_error_ui('Diff task failed due to unexpected error', detail)
            raise
        finally:
            dialog.destroy()

    def on_apply_clicked(self):
        staging_dir = STAGING_DIR_PATH
        # TODO: clear dir after use

        error_collection = []

        def on_progress_made(progress, total):
            self.diff_tree.set_status(f'Copied {progress} bytes of {total}')

        progress_meter = ProgressMeter(on_progress_made)
        file_util.apply_changes_atomically(tree=self.fmeta_tree, staging_dir=staging_dir,
                                           continue_on_error=True, error_collector=error_collection,
                                           progress_meter=progress_meter)
        if len(error_collection) > 0:
            # TODO: create a much better UI here
            noop_adds = 0
            noop_dels = 0
            noop_movs = 0
            err_adds = 0
            err_dels = 0
            err_movs = 0
            for err in error_collection:
                if err.fm.category == Category.Added:
                    if type(err) == FMetaError:
                        err_adds += 1
                    else:
                        noop_adds += 1
                elif err.fm.category == Category.Deleted:
                    if type(err) == FMetaError:
                        err_dels += 1
                    else:
                        noop_dels += 1
                elif err.fm.category == Category.Moved:
                    if type(err) == FMetaError:
                        err_movs += 1
                    else:
                        noop_movs += 1
            self.show_error_ui(f'{len(error_collection)} Errors occurred',
                               f'Collected the following errors while applying changes: adds={err_adds} dels={err_dels} movs={err_movs} noops={noop_adds+noop_dels+noop_movs}')


class DiffWindow(Gtk.ApplicationWindow, BaseDialog):
    def __init__(self, application):
        Gtk.Window.__init__(self, application=application)
        BaseDialog.__init__(self)
        self.enable_db_cache = True

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

        diff_action_btn = Gtk.Button(label="Diff (content-first)")
        diff_action_btn.connect("clicked", self.execute_diff_task)
        self.replace_bottom_button_panel(diff_action_btn)

      #  menubutton = Gtk.MenuButton()
      #  self.content_box.add(menubutton)
       # menumodel = Gio.Menu()
        #menubutton.set_menu_model(menumodel)
       # menumodel.append("New", "app.new")
      #  menumodel.append("Quit", "app.quit")

        # TODO: create a 'Scan' button for each input source

    def replace_bottom_button_panel(self, *buttons):
        for child in self.bottom_button_panel.get_children():
            self.bottom_button_panel.remove(child)

        for button in buttons:
            self.bottom_button_panel.pack_start(button, True, True, 0)
            button.show()

    def on_merge_btn_clicked(self, widget):
        logger.debug('Merge btn clicked')

        left_selected_changes = self.diff_tree_left.get_selected_changes()
        logger.info(f'Left changes: {left_selected_changes.get_summary()}')
        right_selected_changes = self.diff_tree_right.get_selected_changes()
        logger.info(f'Right changes: {right_selected_changes.get_summary()}')
        if len(left_selected_changes.get_all()) == 0 and len(right_selected_changes.get_all()) == 0:
            self.show_error_msg('You must select change(s) first.')
            return

        merged_changes_tree, conflict_pairs = diff_content_first.merge_change_trees(left_selected_changes, right_selected_changes)
        if conflict_pairs is not None:
            # TODO: more informative error
            self.show_error_msg('Cannot merge', f'{len(conflict_pairs)} conflicts found')
            return

        logger.info(f'Merged changes: {merged_changes_tree.get_summary()}')

        # Preview changes in UI pop-up
        dialog = MergePreviewDialog(self, merged_changes_tree)
        response_id = dialog.run()
        if response_id == Gtk.ResponseType.APPLY:
            # Refresh the diff trees:
            logger.debug('Refreshing the diff trees')
            self.execute_diff_task()

    def execute_diff_task(self, widget=None):
        action_thread = threading.Thread(target=self.diff_task)
        action_thread.daemon = True
        action_thread.start()

    def diff_task(self):
        try:
            stopwatch = Stopwatch()
            if self.enable_db_cache:
                left_db = FMetaDatabase(LEFT_DB_PATH)
                left_fmeta_tree: FMetaTree
                self.diff_tree_right.set_status('Waiting...')
                if left_db.has_data():
                    self.diff_tree_left.set_status(f'Loading Left data from DB: {LEFT_DB_PATH}')
                    left_fmeta_tree = left_db.load_fmeta_tree(self.diff_tree_left.root_path)
                else:
                    left_fmeta_tree = scan_disk(self.diff_tree_left)
                    left_db.save_fmeta_tree(left_fmeta_tree)
            else:
                left_fmeta_tree = scan_disk(self.diff_tree_left)
            stopwatch.stop()
            logger.info(f'Left loaded in: {stopwatch}')
            self.diff_tree_left.set_status(left_fmeta_tree.get_summary())

            stopwatch = Stopwatch()
            if self.enable_db_cache:
                right_db = FMetaDatabase(RIGHT_DB_PATH)
                if right_db.has_data():
                    self.diff_tree_right.set_status(f'Loading Right data from DB: {RIGHT_DB_PATH}')
                    right_fmeta_tree = right_db.load_fmeta_tree(self.diff_tree_right.root_path)
                else:
                    right_fmeta_tree = scan_disk(self.diff_tree_right)
                    right_db.save_fmeta_tree(right_fmeta_tree)
            else:
                right_fmeta_tree = scan_disk(self.diff_tree_right)
            stopwatch.stop()
            logger.info(f'Right loaded in: {stopwatch}')
            self.diff_tree_right.set_status(right_fmeta_tree.get_summary())

            logger.info("Diffing...")

            stopwatch = Stopwatch()
            diff_content_first.diff(left_fmeta_tree, right_fmeta_tree, compare_paths_also=True, use_modify_times=False)
            stopwatch.stop()
            logger.info(f'Diff completed in: {stopwatch}')

            def do_on_ui_thread():
                self.diff_tree_left.rebuild_ui_tree(left_fmeta_tree)
                self.diff_tree_right.rebuild_ui_tree(right_fmeta_tree)

                # Replace diff btn with merge buttons
                merge_btn = Gtk.Button(label="Merge Selected...")
                merge_btn.connect("clicked", self.on_merge_btn_clicked)

                self.replace_bottom_button_panel(merge_btn)
                logger.info('Done.')

            GLib.idle_add(do_on_ui_thread)
        except Exception as err:
            logger.exception('Diff task failed with exception')

            def do_on_ui_thread(err_msg):
                GLib.idle_add(lambda: self.show_error_msg('Diff task failed due to unexpected error', err_msg))
            do_on_ui_thread(repr(err))
            raise

