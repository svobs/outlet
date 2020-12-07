import logging
from typing import List, Optional

import gi
from pydispatch import dispatcher
from pydispatch.dispatcher import Any

from ui.signal import ID_LEFT_TREE, ID_MERGE_TREE, ID_RIGHT_TREE, Signal
from constants import APP_NAME, H_PAD, IconId, TreeDisplayMode
from diff.diff_content_first import ContentFirstDiffer
from global_actions import GlobalActions
from model.display_tree.category import CategoryDisplayTree
from model.display_tree.display_tree import DisplayTree
from model.node.node import SPIDNodePair
from ui.dialog.base_dialog import BaseDialog
from ui.dialog.merge_preview_dialog import MergePreviewDialog
from ui.tree import tree_factory

gi.require_version("Gtk", "3.0")
from gi.repository import GLib, Gtk, Gdk

logger = logging.getLogger(__name__)


""" ▂ ▃ ▄ ▅ ▆ ▇ █ █ ▇ ▆ ▅ ▄ ▃ ▂
             TwoPanelWindow
"""


class TwoPanelWindow(Gtk.ApplicationWindow, BaseDialog):
    """🢄🢄🢄 2-panel window for comparing one file tree to another"""
    def __init__(self, app, win_id):
        Gtk.Window.__init__(self, application=app)
        BaseDialog.__init__(self, app)

        self.win_id = win_id
        self.set_title(APP_NAME)
        # program icon:
        self.set_icon_from_file(self.app.assets.get_path(IconId.ICON_WINDOW))
        # Set minimum width and height

        # Restore previous window location:
        self.x_loc_cfg_path = f'ui_state.{self.win_id}.x'
        self.y_loc_cfg_path = f'ui_state.{self.win_id}.y'
        self.x_loc = self.app.config.get(self.x_loc_cfg_path, 50)
        self.y_loc = self.app.config.get(self.y_loc_cfg_path, 50)
        self.move(x=self.x_loc, y=self.y_loc)

        self.set_hide_titlebar_when_maximized(True)

        # Restore previous width/height:
        self.width_cfg_path = f'ui_state.{self.win_id}.width'
        self.height_cfg_path = f'ui_state.{self.win_id}.height'

        width = self.app.config.get(self.width_cfg_path, 1200)
        height = self.app.config.get(self.height_cfg_path, 500)
        allocation = Gdk.Rectangle()
        allocation.width = width
        allocation.height = height
        self.size_allocate(allocation)
        # i.e. "minimum" window size allowed:
        self.set_size_request(1200, 500)

        self.set_border_width(H_PAD)
        self.content_box = Gtk.Box(spacing=0, orientation=Gtk.Orientation.VERTICAL)
        self.add(self.content_box)

        diff_tree_panes = Gtk.HPaned()
        self.content_box.add(diff_tree_panes)

        self.sizegroups = {'root_paths': Gtk.SizeGroup(mode=Gtk.SizeGroupMode.VERTICAL),
                           'filter_panel': Gtk.SizeGroup(mode=Gtk.SizeGroupMode.VERTICAL),
                           'tree_status': Gtk.SizeGroup(mode=Gtk.SizeGroupMode.VERTICAL)}

        # Diff Tree Left:
        tree = self.app.backend.create_display_tree_from_config(tree_id=ID_LEFT_TREE, is_startup=True)
        self.tree_con_left = tree_factory.build_editor_tree(parent_win=self, tree=tree)
        diff_tree_panes.pack1(self.tree_con_left.content_box, resize=True, shrink=False)

        # Diff Tree Right:
        tree = self.app.backend.create_display_tree_from_config(tree_id=ID_RIGHT_TREE, is_startup=True)
        self.tree_con_right = tree_factory.build_editor_tree(parent_win=self, tree=tree)
        diff_tree_panes.pack2(self.tree_con_right.content_box, resize=True, shrink=False)

        # Bottom panel: Bottom Button panel, toolbar and progress bar
        self.bottom_panel = Gtk.Box(spacing=H_PAD, orientation=Gtk.Orientation.HORIZONTAL)
        self.content_box.add(self.bottom_panel)

        # Bottom Button panel:
        self.bottom_button_panel = Gtk.Box(spacing=H_PAD, orientation=Gtk.Orientation.HORIZONTAL)
        self.bottom_panel.add(self.bottom_button_panel)

        # Toolbar
        self.pause_icon = Gtk.Image()
        self.pause_icon.set_from_file(self.app.assets.get_path(IconId.ICON_PAUSE))
        self.play_icon = Gtk.Image()
        self.play_icon.set_from_file(self.app.assets.get_path(IconId.ICON_PLAY))

        self.toolbar = Gtk.Toolbar()
        self.toolbar.set_style(Gtk.ToolbarStyle.ICONS)
        self.play_pause_btn = Gtk.ToolButton()

        self._is_playing = self.app.backend.get_op_execution_play_state()
        self._update_play_pause_btn(sender=self.win_id, is_enabled=self._is_playing)

        self.play_pause_btn.connect('clicked', self._on_play_pause_btn_clicked)
        self.toolbar.insert(self.play_pause_btn, -1)
        self.bottom_panel.pack_start(self.toolbar, expand=False, fill=True, padding=H_PAD)

        self.toolbar.show_all()

        filler = Gtk.Box(spacing=0, orientation=Gtk.Orientation.HORIZONTAL)
        self.bottom_panel.pack_start(filler, expand=True, fill=False, padding=H_PAD)

        # Progress bar: just disable for now - deal with this later
        # listen_for = [ID_LEFT_TREE, ID_RIGHT_TREE,
        #               self.win_id, ID_GLOBAL_CACHE, ID_COMMAND_EXECUTOR]
        # Remember to hold a reference to this, for signals!
        # self.proress_bar_component = ProgressBar(self.config, listen_for)
        # self.bottom_panel.pack_start(self.proress_bar_component.progressbar, True, False, 0)
        # Give progress bar exactly half of the window width:
        # self.bottom_panel.set_homogeneous(True)

        self._set_default_button_bar()

        # Subscribe to signals:
        dispatcher.connect(signal=Signal.TOGGLE_UI_ENABLEMENT, receiver=self._on_enable_ui_toggled)
        dispatcher.connect(signal=Signal.DIFF_TREES_DONE, receiver=self._after_diff_completed)
        dispatcher.connect(signal=Signal.DIFF_TREES_FAILED, receiver=self._after_diff_failed)

        # Need to add an extra listener to each tree, to reload when the other one's root changes
        # if displaying the results of a diff
        dispatcher.connect(signal=Signal.DISPLAY_TREE_CHANGED, receiver=self._on_display_tree_changed)
        dispatcher.connect(signal=Signal.EXIT_DIFF_MODE, receiver=self._on_merge_complete, sender=Any)
        dispatcher.connect(signal=Signal.OP_EXECUTION_PLAY_STATE_CHANGED, receiver=self._update_play_pause_btn)

        # Connect "resize" event. Lots of excess logic to determine approximately when the
        # window *stops* being resized, so we can persist the value semi-efficiently
        self._connect_resize_event()

        self.connect('destroy', self.shutdown)

        # Docs: https://developer.gnome.org/pygtk/stable/class-gdkdisplay.html
        display: Gdk.Display = self.get_display()
        logger.debug(f'Display has {display.get_n_monitors()} monitors:')
        for monitor_num in range(0, display.get_n_monitors()):
            monitor = display.get_monitor(monitor_num)
            size_rect: Gdk.Rectangle = monitor.get_geometry()
            logger.debug(f'        Monitor #{monitor_num} is {size_rect.width}x{size_rect.height}')

    def shutdown(self, arg=None):
        """Overrides Gtk.Window.close()"""
        logger.debug(f'TwoPanelWindow.shutdown() called')

        # Clean up:
        self.tree_con_left = None
        self.tree_con_right = None

        if self.app:
            # swap into local var to prevent infinite cycle
            app = self.app
            self.app = None
            app.quit()

    def replace_bottom_button_panel(self, *buttons):
        for child in self.bottom_button_panel.get_children():
            self.bottom_button_panel.remove(child)

        for button in buttons:
            self.bottom_button_panel.pack_start(button, False, False, 0)
            button.show()

    def _connect_resize_event(self):
        self._timer_id = None
        eid = self.connect('size-allocate', self._on_size_allocated)
        self._event_id_size_allocate = eid

    def _set_default_button_bar(self):
        def on_diff_btn_clicked(widget):
            logger.debug(f'Diff btn clicked! Sending signal: "{Signal.START_DIFF_TREES}"')
            # Disable button bar immediately:
            GlobalActions.disable_ui(sender=self.win_id)
            self.app.backend.start_diff_trees(tree_id_left=self.tree_con_left.tree_id, tree_id_right=self.tree_con_right.tree_id)
        diff_action_btn = Gtk.Button(label="Diff (content-first)")
        diff_action_btn.connect("clicked", on_diff_btn_clicked)

        def on_goog_btn_clicked(widget):
            logger.debug('DownloadGDrive btn clicked!')
            dispatcher.send(signal=Signal.DOWNLOAD_ALL_GDRIVE_META, sender=self.win_id)
        gdrive_btn = Gtk.Button(label="Download Google Drive Meta")
        gdrive_btn.connect("clicked", on_goog_btn_clicked)

        self.replace_bottom_button_panel(diff_action_btn, gdrive_btn)

    # GTK LISTENERS begin
    # ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼

    def _on_play_pause_btn_clicked(self, widget):
        if self._is_playing:
            logger.debug(f'Play/Pause btn clicked! Sending signal "{Signal.PAUSE_OP_EXECUTION}"')
            dispatcher.send(signal=Signal.PAUSE_OP_EXECUTION, sender=self.win_id)
        else:
            logger.debug(f'Play/Pause btn clicked! Sending signal "{Signal.RESUME_OP_EXECUTION}"')
            dispatcher.send(signal=Signal.RESUME_OP_EXECUTION, sender=self.win_id)

    def _on_size_allocated(self, widget, alloc):
        # logger.debug('EVENT: GTK "size-allocate" fired')

        # don't install a second timer
        if self._timer_id:
            return

        # remember new size
        self._remembered_size = alloc

        # disconnect the 'size-allocate' event
        self.disconnect(self._event_id_size_allocate)

        # create a 1000ms timer
        tid = GLib.timeout_add(interval=1000, function=self._on_size_timer)
        # ...and remember its id
        self._timer_id = tid

    def _on_size_timer(self):
        # current window size
        curr = self.get_allocated_size().allocation

        # was the size changed in the last 500ms?
        # NO changes anymore
        if self._remembered_size.equal(curr):  # == doesn't work here
            # logger.debug('RESIZING FINISHED')

            self.app.config.write(self.width_cfg_path, curr.width)
            self.app.config.write(self.height_cfg_path, curr.height)

            # Store position also
            x, y = self.get_position()
            if x != self.x_loc or y != self.y_loc:
                # logger.debug(f'Win position changed to {x}, {y}')
                self.app.config.write(self.x_loc_cfg_path, x)
                self.app.config.write(self.y_loc_cfg_path, y)
                self.x_loc = x
                self.y_loc = y

            # reconnect the 'size-allocate' event
            self._connect_resize_event()
            # stop the timer
            self._timer_id = None
            return False

        # YES size was changed
        # remember the new size for the next check after 1s
        self._remembered_size = curr
        # repeat timer
        return True

    def _generate_merge_tree(self) -> Optional[CategoryDisplayTree]:
        left_selected_changes: List[SPIDNodePair] = self.tree_con_left.get_checked_rows_as_list()
        right_selected_changes: List[SPIDNodePair] = self.tree_con_right.get_checked_rows_as_list()
        if len(left_selected_changes) == 0 and len(right_selected_changes) == 0:
            self.show_error_msg('You must select change(s) first.')
            return None

        left_sn = self.tree_con_left.get_tree().get_root_sn()
        right_sn = self.tree_con_right.get_tree().get_root_sn()
        differ = ContentFirstDiffer(left_sn, right_sn, self.app.backend)
        merged_changes_tree: CategoryDisplayTree = differ.merge_change_trees(left_selected_changes, right_selected_changes)

        conflict_pairs = []
        if conflict_pairs:
            # TODO: more informative error
            self.show_error_msg('Cannot merge', f'{len(conflict_pairs)} conflicts found')
            return None

        logger.info(f'Merged changes: {merged_changes_tree.get_summary()}')
        return merged_changes_tree

    def on_merge_preview_btn_clicked(self, widget):
        """
        1. Gets selected changes from both sides,
        2. merges into one change tree and raises an error for conflicts,
        3. or if successful displays the merge preview dialog with the summary.
        """
        logger.debug('Merge btn clicked')

        try:
            merged_changes_tree = self._generate_merge_tree()
            if merged_changes_tree:
                # Preview changes in UI pop-up
                dialog = MergePreviewDialog(self, merged_changes_tree)
                dialog.run()
        except Exception as err:
            self.show_error_ui('Merge preview failed due to unexpected error', repr(err))
            raise

    # SIGNAL LISTENERS begin
    # ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼

    def _on_enable_ui_toggled(self, sender, enable):
        """Callback for TOGGLE_UI_ENABLEMENT"""
        def toggle_ui():
            for button in self.bottom_button_panel.get_children():
                button.set_sensitive(enable)
        GLib.idle_add(toggle_ui)

    def _update_play_pause_btn(self, sender: str, is_enabled: bool):
        self._is_playing: bool = is_enabled
        logger.debug(f'Updating play/pause btn with is_playing={self._is_playing}')
        if self._is_playing:
            self.play_pause_btn.set_icon_widget(self.pause_icon)
            self.play_pause_btn.set_tooltip_text('Pause change operations')
        else:
            self.play_pause_btn.set_icon_widget(self.play_icon)
            self.play_pause_btn.set_tooltip_text('Resume change operations')
        self.toolbar.show_all()

    def _on_display_tree_changed(self, sender, tree: DisplayTree):
        logger.debug(f'Received signal: "{Signal.DISPLAY_TREE_CHANGED}"')

        if sender == ID_RIGHT_TREE and self.tree_con_left.tree_display_mode == TreeDisplayMode.CHANGES_ONE_TREE_PER_CATEGORY:
            # If displaying a diff and right root changed, reload left display
            # (note: right will update its own display)
            logger.debug(f'Detected that {ID_RIGHT_TREE} changed root. Reloading {ID_LEFT_TREE}')
            _reload_tree(self.tree_con_left)

        elif sender == ID_LEFT_TREE and self.tree_con_right.tree_display_mode == TreeDisplayMode.CHANGES_ONE_TREE_PER_CATEGORY:
            # Mirror of above:
            logger.debug(f'Detected that {ID_LEFT_TREE} changed root. Reloading {ID_RIGHT_TREE}')
            _reload_tree(self.tree_con_right)

        GLib.idle_add(self._set_default_button_bar)

    def _after_diff_failed(self, sender):
        logger.debug(f'Received signal: "{Signal.DIFF_TREES_FAILED}"')
        GLib.idle_add(self._set_default_button_bar)
        GlobalActions.enable_ui(sender=self.win_id)

    def _after_diff_completed(self, sender):
        logger.debug(f'Received signal: "{Signal.DIFF_TREES_DONE}"')

        def change_button_bar():
            # Replace diff btn with merge buttons
            merge_btn = Gtk.Button(label="Merge Selected...")
            merge_btn.connect("clicked", self.on_merge_preview_btn_clicked)

            def on_cancel_diff_btn_clicked(widget):
                dispatcher.send(signal=Signal.EXIT_DIFF_MODE, sender=ID_MERGE_TREE)

            cancel_diff_btn = Gtk.Button(label="Cancel Diff")
            cancel_diff_btn.connect("clicked", on_cancel_diff_btn_clicked)

            self.replace_bottom_button_panel(merge_btn, cancel_diff_btn)

            GlobalActions.enable_ui(sender=self.win_id)
        GLib.idle_add(change_button_bar)

    def _on_error_occurred(self, msg: str, secondary_msg: str = None):
        logger.debug(f'Received signal: "{Signal.ERROR_OCCURRED}"')
        self.show_error_ui(msg, secondary_msg)

    def _on_merge_complete(self):
        logger.debug('Received signal that merge completed. Reloading both trees')
        _reload_tree(self.tree_con_left)
        _reload_tree(self.tree_con_right)

        GLib.idle_add(self._set_default_button_bar)


def _reload_tree(tree_con):
    # set new_tree=None to trigger a reload of the existing tree
    tree_con.reload(new_tree=None, tree_display_mode=TreeDisplayMode.ONE_TREE_ALL_ITEMS, hide_checkboxes=True)
