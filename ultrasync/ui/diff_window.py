import logging.handlers
import threading

import gi
gi.require_version("Gtk", "3.0")
from gi.repository import GLib, Gtk, Gio, GObject

from stopwatch import Stopwatch

from ui.merge_preview_dialog import MergePreviewDialog
import fmeta.fmeta_tree_cache as fmeta_tree_cache
from fmeta.fmeta_tree_source import FMetaTreeSource
from file_util import get_resource_path
from fmeta import diff_content_first
from ui.diff_tree import DiffTree
from ui.base_dialog import BaseDialog
import ui.diff_tree_populator as diff_tree_populator

WINDOW_ICON_PATH = get_resource_path("resources/fslint_icon.png")

logger = logging.getLogger(__name__)


class ConfigFileRootPathHandler:
    def __init__(self, config, config_entry):
        self.config = config
        self.config_entry = config_entry

    def get_root_path(self):
        return self.config.get(self.config_entry)

    def set_root_path(self, new_root_path):
        if self.get_root_path() != new_root_path:
            # Root changed.
            # TODO: wipe out UI and reload the whole damn thing
            logger.error('TODO! Need to implement wiping out the tree on root path change!')
            # TODO: fire signal to listeners
            self.config.write(transient_path=self.config_entry, value=new_root_path)


class DiffWindow(Gtk.ApplicationWindow, BaseDialog):
    def __init__(self, application):
        Gtk.Window.__init__(self, application=application)
        BaseDialog.__init__(self, application.config)
        # TODO: put in config file
        self.enable_file_scan = True
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
        self.button4.set_sensitive(False)  # disable
        self.button5= Gtk.CheckButton(label="Relative paths or file names differ")
        self.checkbox_panel.pack_start(self.button5, True, True, 0)
        self.button5.set_sensitive(False) # disable
        #self.button1.connect("toggled", self.on_button_toggled, "3")
        self.content_box.add(self.checkbox_panel)

        diff_tree_panes = Gtk.HPaned()
        self.content_box.add(diff_tree_panes)

        self.sizegroups = {'root_paths': Gtk.SizeGroup(mode=Gtk.SizeGroupMode.VERTICAL),
                           'tree_status': Gtk.SizeGroup(mode=Gtk.SizeGroupMode.VERTICAL)}

        # Diff Tree Left:
        root_path_handler_left = ConfigFileRootPathHandler(self.config, 'transient.left_tree.root_path')
        self.diff_tree_left = DiffTree(parent_win=self, root_path_handler=root_path_handler_left, editable=True, sizegroups=self.sizegroups)
        diff_tree_panes.pack1(self.diff_tree_left.content_box, resize=True, shrink=False)

        # Diff Tree Right:
        root_path_handler_right = ConfigFileRootPathHandler(self.config, 'transient.right_tree.root_path')
        self.diff_tree_right = DiffTree(parent_win=self, root_path_handler=root_path_handler_right, editable=True, sizegroups=self.sizegroups)
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

        left_selected_changes = self.diff_tree_left.get_checked_rows_as_tree()
        logger.info(f'Left changes: {left_selected_changes.get_summary()}')
        right_selected_changes = self.diff_tree_right.get_checked_rows_as_tree()
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

    # TODO: Encapsulate each FMetaTreeSource in a listener. Need each to subscribe to a signal emitted by DiffTrees whenever a root changes. Fire a 'needs-diff-recalculate' event appriately which will update the diff and then the trees
    # TODO: change DB path whenever root is changed
    def diff_task(self):
        try:
            self.diff_tree_right.set_status('Waiting...')

            # LEFT ---------------
            tree_id = 'left_tree'
            cache = fmeta_tree_cache.from_config(config=self.config, tree_id=tree_id)
            left_tree_source = FMetaTreeSource(tree_id, self.diff_tree_left.root_path, cache)
            left_fmeta_tree = left_tree_source.get_current_tree(status_receiver=self.diff_tree_left)

            # RIGHT --------------
            tree_id = 'right_tree'
            cache = fmeta_tree_cache.from_config(config=self.config, tree_id=tree_id)
            right_tree_source = FMetaTreeSource(tree_id, self.diff_tree_right.root_path, cache)
            right_fmeta_tree = right_tree_source.get_current_tree(status_receiver=self.diff_tree_right)

            logger.info("Diffing...")
            stopwatch = Stopwatch()
            diff_content_first.diff(left_fmeta_tree, right_fmeta_tree, compare_paths_also=True, use_modify_times=False)
            stopwatch.stop()
            logger.info(f'Diff completed in: {stopwatch}')

            diff_tree_populator.repopulate_diff_tree(self.diff_tree_left, left_fmeta_tree)
            diff_tree_populator.repopulate_diff_tree(self.diff_tree_right, right_fmeta_tree)

            def do_on_ui_thread():
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

