import logging
import threading
import os
from stopwatch import Stopwatch
import ui.actions as actions
import ui.assets
from ui.diff_tree.dt_data_store import PersistentFMetaStore
from ui.gdrive_dir_selection_dialog import GDriveDirSelectionDialog
from ui.progress_bar_component import ProgressBarComponent

import gi
gi.require_version("Gtk", "3.0")
from gi.repository import GLib, Gtk


from gdrive.tree_builder import GDriveTreeLoader
from ui.merge_preview_dialog import MergePreviewDialog
from file_util import get_resource_path
from fmeta import diff_content_first
from ui.diff_tree.diff_tree_panel import DiffTreePanel
from ui.base_dialog import BaseDialog
import ui.diff_tree.dt_populator as diff_tree_populator

logger = logging.getLogger(__name__)


class DiffWindow(Gtk.ApplicationWindow, BaseDialog):
    def __init__(self, application):
        Gtk.Window.__init__(self, application=application)
        BaseDialog.__init__(self, application.config)

        self.set_title('UltraSync')
        # program icon:
        self.set_icon_from_file(ui.assets.WINDOW_ICON_PATH)
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

        store_left = PersistentFMetaStore(tree_id=actions.ID_LEFT_TREE, config=self.config)
        self.diff_tree_left = DiffTreePanel(data_store=store_left, parent_win=self, editable=True, is_display_persisted=True)
        diff_tree_panes.pack1(self.diff_tree_left.content_box, resize=True, shrink=False)

        # Diff Tree Right:
        store_right = PersistentFMetaStore(tree_id=actions.ID_RIGHT_TREE, config=self.config)
        self.diff_tree_right = DiffTreePanel(data_store=store_right, parent_win=self, editable=True, is_display_persisted=True)
        diff_tree_panes.pack2(self.diff_tree_right.content_box, resize=True, shrink=False)

        self.bottom_panel = Gtk.Box(spacing=6, orientation=Gtk.Orientation.HORIZONTAL)
        self.content_box.add(self.bottom_panel)
        # Bottom button panel:
        self.bottom_button_panel = Gtk.Box(spacing=6, orientation=Gtk.Orientation.HORIZONTAL)
        self.bottom_panel.add(self.bottom_button_panel)

        # Remember to hold a reference to this, for signals!
        listen_for = [actions.ID_LEFT_TREE, actions.ID_RIGHT_TREE, actions.ID_DIFF_WINDOW]
        self.proress_bar_component = ProgressBarComponent(self.config, listen_for)
        self.bottom_panel.pack_start(self.proress_bar_component.progressbar, True, True, 0)
        # Give progress bar exactly half of the window width:
        self.bottom_panel.set_homogeneous(True)

        def on_diff_btn_clicked(widget):
            logger.debug('Diff btn clicked!')
            actions.send_signal(signal=actions.DO_DIFF, sender=self.diff_tree_left.tree_id)
        diff_action_btn = Gtk.Button(label="Diff (content-first)")
        diff_action_btn.connect("clicked", on_diff_btn_clicked)

        def on_goog_btn_clicked(widget):
            logger.debug('DownloadGDrive btn clicked!')
            actions.send_signal(signal=actions.DOWNLOAD_GDRIVE_META, sender=actions.ID_DIFF_WINDOW)
        gdrive_btn = Gtk.Button(label="Download Google Drive Meta")
        gdrive_btn.connect("clicked", on_goog_btn_clicked)

        self.replace_bottom_button_panel(diff_action_btn, gdrive_btn)

        # Subscribe to signals:
        actions.connect(signal=actions.DO_DIFF, handler=self.on_diff_requested)
        actions.connect(signal=actions.DOWNLOAD_GDRIVE_META, handler=self.on_gdrive_requested)
        actions.connect(signal=actions.TOGGLE_UI_ENABLEMENT, handler=self.on_enable_ui_toggled)
        actions.connect(signal=actions.GDRIVE_DOWNLOAD_COMPLETE, handler=self.on_gdrive_download_complete)

    def replace_bottom_button_panel(self, *buttons):
        for child in self.bottom_button_panel.get_children():
            self.bottom_button_panel.remove(child)

        for button in buttons:
            self.bottom_button_panel.pack_start(button, False, False, 0)
            button.show()

    # --- ACTIONS ---

    def on_merge_preview_btn_clicked(self, widget):
        """
        1. Gets selected changes from both sides,
        2. merges into one change tree and raises an error for conflicts,
        3. or if successful displays the merge preview dialog with the summary.
        """
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
                actions.send_signal(signal=actions.DO_DIFF, sender=self)
        except Exception as err:
            self.show_error_ui('Merge preview failed due to unexpected error', repr(err))
            raise

    def on_enable_ui_toggled(self, sender, enable):
        """Callback for TOGGLE_UI_ENABLEMENT"""
        def toggle_ui():
            for button in self.bottom_button_panel.get_children():
                button.set_sensitive(enable)
        GLib.idle_add(toggle_ui)

    def on_gdrive_requested(self, sender):
        """Callback for signal DOWNLOAD_GDRIVE_META"""
        actions.disable_ui(sender=sender)
        action_thread = threading.Thread(target=self.download_gdrive_meta, args=(sender,))
        action_thread.daemon = True
        action_thread.start()

    def download_gdrive_meta(self, tree_id):
        try:
            cache_path = get_resource_path('gdrive.db')
            tree_builder = GDriveTreeLoader(config=self.config, cache_path=cache_path, tree_id=tree_id)
            meta = tree_builder.load_all(invalidate_cache=False)
            actions.get_dispatcher().send(signal=actions.GDRIVE_DOWNLOAD_COMPLETE, sender=tree_id, meta=meta)
        except Exception as err:
            self.show_error_ui('Downlaod from GDrive failed due to unexpected error', repr(err))
            logger.exception(err)
        finally:
            actions.enable_ui(sender=self)

    def on_gdrive_download_complete(self, sender, meta):
        """Callback for signal GDRIVE_DOWNLOAD_COMPLETE"""

        def open_dialog():
            try:
                # Preview changes in UI pop-up
                dialog = GDriveDirSelectionDialog(self, meta)
                response_id = dialog.run()
                if response_id == Gtk.ResponseType.OK:
                    logger.debug('User clicked OK!')

            except Exception as err:
                self.show_error_ui('GDriveDirSelectionDialog failed due to unexpected error', repr(err))
                raise

        GLib.idle_add(open_dialog)

    def on_diff_requested(self, sender):
        """Callback for signal DO_DIFF"""
        actions.disable_ui(sender=sender)
        action_thread = threading.Thread(target=self.do_tree_diff)
        action_thread.daemon = True
        action_thread.start()

    # TODO: change DB path whenever root is changed
    # TODO: disable all UI while loading (including tree)
    def do_tree_diff(self):
        try:
            if not os.path.exists(self.diff_tree_left.root_path) or not os.path.exists(self.diff_tree_right.root_path):
                logger.info('Skipping diff because one of the paths does not exist')
                actions.enable_ui(sender=self)
                return

            actions.set_status(sender=self.diff_tree_right.data_store.tree_id, status_msg='Waiting...')

            # Load trees if not loaded - may be a long operation
            left_fmeta_tree = self.diff_tree_left.data_store.get_whole_tree()
            right_fmeta_tree = self.diff_tree_right.data_store.get_whole_tree()

            logger.debug(f'Sending START_PROGRESS_INDETERMINATE for ID: {actions.ID_DIFF_WINDOW}')
            actions.get_dispatcher().send(actions.START_PROGRESS_INDETERMINATE, sender=actions.ID_DIFF_WINDOW)
            msg = 'Computing bidrectional content-first diff...'
            actions.get_dispatcher().send(actions.SET_PROGRESS_TEXT, sender=actions.ID_DIFF_WINDOW, msg=msg)

            stopwatch_diff = Stopwatch()
            diff_content_first.diff(left_fmeta_tree, right_fmeta_tree, compare_paths_also=True, use_modify_times=False)
            logger.info(f'Diff completed in: {stopwatch_diff}')

            actions.get_dispatcher().send(actions.SET_PROGRESS_TEXT, sender=actions.ID_DIFF_WINDOW, msg='Populating UI trees...')
            stopwatch_redraw = Stopwatch()
            diff_tree_populator.repopulate_diff_tree(self.diff_tree_left)
            diff_tree_populator.repopulate_diff_tree(self.diff_tree_right)

            def change_button_bar():
                # Replace diff btn with merge buttons
                merge_btn = Gtk.Button(label="Merge Selected...")
                merge_btn.connect("clicked", self.on_merge_preview_btn_clicked)

                self.replace_bottom_button_panel(merge_btn)

                logger.debug(f'Sending STOP_PROGRESS for ID: {actions.ID_DIFF_WINDOW}')
                actions.get_dispatcher().send(actions.STOP_PROGRESS, sender=actions.ID_DIFF_WINDOW)

                actions.enable_ui(sender=self)
                logger.debug(f'Redraw completed in: {stopwatch_redraw}')

            GLib.idle_add(change_button_bar)
        except Exception as err:
            actions.enable_ui(sender=self)
            self.show_error_ui('Diff task failed due to unexpected error', repr(err))
            logger.exception(err)

