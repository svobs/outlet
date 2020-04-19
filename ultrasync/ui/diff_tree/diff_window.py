import logging.handlers
import threading
import os
import ui.actions as actions

import gi

from ui.diff_tree.dt_data_store import DtConfigFileStore

gi.require_version("Gtk", "3.0")
from gi.repository import GLib, Gtk, GObject

from stopwatch import Stopwatch

from gdrive.tree_builder import GDriveTreeBuilder
from ui.merge_preview_dialog import MergePreviewDialog
from file_util import get_resource_path
from fmeta import diff_content_first
from ui.diff_tree.dt_widget import DiffTree
from ui.base_dialog import BaseDialog
import ui.diff_tree.dt_populator as diff_tree_populator

WINDOW_ICON_PATH = get_resource_path("resources/fslint_icon.png")

logger = logging.getLogger(__name__)


class DiffWindow(Gtk.ApplicationWindow, BaseDialog):
    def __init__(self, application):
        Gtk.Window.__init__(self, application=application)
        BaseDialog.__init__(self, application.config)

        self.set_title('UltraSync')
        # program icon:
        self.set_icon_from_file(WINDOW_ICON_PATH)
        # Set minimum width and height
        self.set_size_request(1400, 800)
        self.set_border_width(10)
        self.content_box = Gtk.Box(spacing=6, orientation=Gtk.Orientation.VERTICAL)
        self.add(self.content_box)

        # Subscribe to signals:
        actions.connect(signal=actions.DO_DIFF, handler=self.on_diff_requested)
        actions.connect(signal=actions.DOWNLOAD_GDRIVE_META, handler=self.on_google_requested)
        actions.connect(actions.TOGGLE_UI_ENABLEMENT, self._on_enable_ui_toggled)

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
        store_left = DtConfigFileStore(config=self.config, tree_id='left_tree', editable=True)
        self.diff_tree_left = DiffTree(store=store_left, parent_win=self, sizegroups=self.sizegroups)
        diff_tree_panes.pack1(self.diff_tree_left.content_box, resize=True, shrink=False)

        # Diff Tree Right:
        store_right = DtConfigFileStore(config=self.config, tree_id='right_tree', editable=True)
        self.diff_tree_right = DiffTree(store=store_right, parent_win=self, sizegroups=self.sizegroups)
        diff_tree_panes.pack2(self.diff_tree_right.content_box, resize=True, shrink=False)

        # Bottom button panel:
        self.bottom_button_panel = Gtk.Box(spacing=6, orientation=Gtk.Orientation.HORIZONTAL)
        self.content_box.add(self.bottom_button_panel)

        diff_action_btn = Gtk.Button(label="Diff (content-first)")
        diff_action_btn.connect("clicked", self.on_diff_btn_clicked)

        gdrive_btn = Gtk.Button(label="Download Google Drive Meta")
        gdrive_btn.connect("clicked", self.on_goog_btn_clicked)

        self.replace_bottom_button_panel(diff_action_btn, gdrive_btn)

        # TODO: create a 'Scan' button for each input source

    def on_diff_btn_clicked(self, widget):
        logger.info('Diff btn clicked!')
        actions.send_signal(signal=actions.DO_DIFF, sender=self)

    def on_goog_btn_clicked(self, widget):
        logger.info('Goog btn clicked!')
        actions.send_signal(signal=actions.DOWNLOAD_GDRIVE_META, sender=self)

    def replace_bottom_button_panel(self, *buttons):
        for child in self.bottom_button_panel.get_children():
            self.bottom_button_panel.remove(child)

        for button in buttons:
            self.bottom_button_panel.pack_start(button, True, True, 0)
            button.show()

    def on_merge_btn_clicked(self, widget):
        logger.debug('Merge btn clicked')

        try:
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
                # Assume the dialog took care of applying the changes.
                # Refresh the diff trees:
                logger.debug('Refreshing the diff trees')
                actions.send_signal(actions.DO_DIFF, self)
        except Exception as err:
            self.show_error_ui('Merge preview failed due to unexpected error', repr(err))
            raise

    def _on_enable_ui_toggled(self, sender, enable):
        """Fired by TOGGLE_UI_ENABLEMENT"""
        for button in self.bottom_button_panel.get_children():
            button.set_sensitive(enable)

    def on_diff_requested(self, sender):
        actions.disable_ui(sender=sender)
        action_thread = threading.Thread(target=self.do_tree_diff)
        action_thread.daemon = True
        action_thread.start()

    def on_google_requested(self, sender):
        logger.info('Hello GOOOGLE WORLD!')
        actions.disable_ui(sender=sender)
        action_thread = threading.Thread(target=self.download_gdrive_meta)
        action_thread.daemon = True
        action_thread.start()

    def download_gdrive_meta(self):
        try:
            cache_path = get_resource_path('gdrive.db')
            tree_builder = GDriveTreeBuilder(config=self.config, cache_path=cache_path)
            tree_builder.build(invalidate_cache=False)
        finally:
            actions.enable_ui(sender=self)

    # TODO: change DB path whenever root is changed
    def do_tree_diff(self):
        try:
            if not os.path.exists(self.diff_tree_left.root_path) or not os.path.exists(self.diff_tree_right.root_path):
                logger.info('Skipping diff because one of the paths does not exist')
                return

            # TODO: disable all UI while loading
            self.diff_tree_right.set_status('Waiting...')

            # Load trees if not loaded - may be a long operation
            left_fmeta_tree = self.diff_tree_left.store.get_fmeta_tree()
            right_fmeta_tree = self.diff_tree_right.store.get_fmeta_tree()

            stopwatch_diff = Stopwatch()
            diff_content_first.diff(left_fmeta_tree, right_fmeta_tree, compare_paths_also=True, use_modify_times=False)
            logger.info(f'Diff completed in: {stopwatch_diff}')

            stopwatch_redraw = Stopwatch()

            diff_tree_populator.repopulate_diff_tree(self.diff_tree_left)
            diff_tree_populator.repopulate_diff_tree(self.diff_tree_right)

            def change_button_bar():
                # Replace diff btn with merge buttons
                merge_btn = Gtk.Button(label="Merge Selected...")
                merge_btn.connect("clicked", self.on_merge_btn_clicked)

                self.replace_bottom_button_panel(merge_btn)
                actions.enable_ui(sender=self)
                logger.debug(f'Redraw completed in: {stopwatch_redraw}')

            GLib.idle_add(change_button_bar)
        except Exception as err:
            self.show_error_ui('Diff task failed due to unexpected error', repr(err))
            actions.enable_ui(sender=self)
            raise

