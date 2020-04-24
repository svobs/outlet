import logging

import gi
gi.require_version("Gtk", "3.0")
from gi.repository import GLib, Gtk

from pydispatch import dispatcher

import ui.actions as actions
import ui.assets
from ui.diff_tree.fmeta_data_store import BulkLoadFMetaStore
from ui.progress_bar_component import ProgressBarComponent
from ui.tree import tree_factory

from ui.merge_preview_dialog import MergePreviewDialog
from fmeta import diff_content_first
from ui.base_dialog import BaseDialog

logger = logging.getLogger(__name__)


class DiffWindow(Gtk.ApplicationWindow, BaseDialog):
    def __init__(self, application):
        Gtk.Window.__init__(self, application=application)
        BaseDialog.__init__(self, application.config)

        self.application = application
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
        store_left = BulkLoadFMetaStore(tree_id=actions.ID_LEFT_TREE, config=self.config)
        self.tree_con_left = tree_factory.build_bulk_load_file_tree(parent_win=self, data_store=store_left)
        diff_tree_panes.pack1(self.tree_con_left.content_box, resize=True, shrink=False)

        # Diff Tree Right:
        store_right = BulkLoadFMetaStore(tree_id=actions.ID_RIGHT_TREE, config=self.config)
        self.tree_con_right = tree_factory.build_bulk_load_file_tree(parent_win=self, data_store=store_right)
        diff_tree_panes.pack2(self.tree_con_right.content_box, resize=True, shrink=False)

        self.bottom_panel = Gtk.Box(spacing=6, orientation=Gtk.Orientation.HORIZONTAL)
        self.content_box.add(self.bottom_panel)
        # Bottom button panel:
        self.bottom_button_panel = Gtk.Box(spacing=6, orientation=Gtk.Orientation.HORIZONTAL)
        self.bottom_panel.add(self.bottom_button_panel)

        listen_for = [actions.ID_LEFT_TREE, actions.ID_RIGHT_TREE, actions.ID_DIFF_WINDOW]
        # Remember to hold a reference to this, for signals!
        self.proress_bar_component = ProgressBarComponent(self.config, listen_for)
        self.bottom_panel.pack_start(self.proress_bar_component.progressbar, True, True, 0)
        # Give progress bar exactly half of the window width:
        self.bottom_panel.set_homogeneous(True)

        def on_diff_btn_clicked(widget):
            logger.debug('Diff btn clicked!')
            dispatcher.send(signal=actions.DO_DIFF, sender=actions.ID_DIFF_WINDOW,
                            tree_con_left=self.tree_con_left, tree_con_right=self.tree_con_right)
        diff_action_btn = Gtk.Button(label="Diff (content-first)")
        diff_action_btn.connect("clicked", on_diff_btn_clicked)

        def on_goog_btn_clicked(widget):
            logger.debug('DownloadGDrive btn clicked!')
            actions.send_signal(signal=actions.DOWNLOAD_GDRIVE_META, sender=actions.ID_DIFF_WINDOW)
        gdrive_btn = Gtk.Button(label="Download Google Drive Meta")
        gdrive_btn.connect("clicked", on_goog_btn_clicked)

        self.replace_bottom_button_panel(diff_action_btn, gdrive_btn)

        # Subscribe to signals:
        actions.connect(signal=actions.TOGGLE_UI_ENABLEMENT, handler=self.on_enable_ui_toggled)
        actions.connect(signal=actions.DIFF_DID_COMPLETE, handler=self.after_diff_completed)

    def replace_bottom_button_panel(self, *buttons):
        for child in self.bottom_button_panel.get_children():
            self.bottom_button_panel.remove(child)

        for button in buttons:
            self.bottom_button_panel.pack_start(button, False, False, 0)
            button.show()

    # --- ACTIONS ---

    def after_diff_completed(self, sender, stopwatch):
        """
        Callback for DIFF_DID_COMPLETE global action
        """
        def change_button_bar():
            # Replace diff btn with merge buttons
            merge_btn = Gtk.Button(label="Merge Selected...")
            merge_btn.connect("clicked", self.on_merge_preview_btn_clicked)

            # FIXME: this is causing the button bar to disappear. Fix layout!
            self.replace_bottom_button_panel(merge_btn)

            logger.debug(f'Sending STOP_PROGRESS for ID: {actions.ID_DIFF_WINDOW}')
            actions.get_dispatcher().send(actions.STOP_PROGRESS, sender=actions.ID_DIFF_WINDOW)

            actions.enable_ui(sender=self)
            logger.debug(f'Diff time + redraw: {stopwatch}')
        GLib.idle_add(change_button_bar)

    def on_merge_preview_btn_clicked(self, widget):
        """
        1. Gets selected changes from both sides,
        2. merges into one change tree and raises an error for conflicts,
        3. or if successful displays the merge preview dialog with the summary.
        """
        logger.debug('Merge btn clicked')

        try:
            left_selected_changes = self.tree_con_left.display_store.get_checked_rows_as_tree()
            logger.info(f'Left changes: {left_selected_changes.get_summary()}')
            right_selected_changes = self.tree_con_right.display_store.get_checked_rows_as_tree()
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

