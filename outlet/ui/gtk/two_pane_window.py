import logging
from typing import List

import gi
from pydispatch import dispatcher

from signal_constants import ID_LEFT_TREE, ID_MAIN_WINDOW, ID_MERGE_TREE, ID_RIGHT_TREE, Signal
from constants import APP_NAME, DEFAULT_MAIN_WIN_HEIGHT, DEFAULT_MAIN_WIN_WIDTH, DEFAULT_MAIN_WIN_X, DEFAULT_MAIN_WIN_Y, H_PAD, IconId, \
    TreeDisplayMode, WIN_SIZE_STORE_DELAY_MS
from global_actions import GlobalActions
from model.display_tree.display_tree import DisplayTree
from model.node.node import SPIDNodePair
from ui.gtk.dialog.base_dialog import BaseDialog
from ui.gtk.dialog.merge_preview_dialog import MergePreviewDialog
from ui.gtk.tree import tree_factory
from util.ensure import ensure_int

gi.require_version("Gtk", "3.0")
from gi.repository import GLib, Gtk, Gdk

logger = logging.getLogger(__name__)


class TwoPaneWindow(Gtk.ApplicationWindow, BaseDialog):
    """2-panel window for comparing one file tree to another"""
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
        self.x_loc = ensure_int(self.backend.get_config(self.x_loc_cfg_path, DEFAULT_MAIN_WIN_X))
        self.y_loc = ensure_int(self.backend.get_config(self.y_loc_cfg_path, DEFAULT_MAIN_WIN_Y))
        self.move(x=self.x_loc, y=self.y_loc)

        self.set_hide_titlebar_when_maximized(True)

        # Restore previous width/height:
        self.width_cfg_path = f'ui_state.{self.win_id}.width'
        self.height_cfg_path = f'ui_state.{self.win_id}.height'

        width = ensure_int(self.backend.get_config(self.width_cfg_path, DEFAULT_MAIN_WIN_WIDTH))
        height = ensure_int(self.backend.get_config(self.height_cfg_path, DEFAULT_MAIN_WIN_HEIGHT))
        allocation = Gdk.Rectangle()
        allocation.width = width
        allocation.height = height
        self.size_allocate(allocation)
        # i.e. "minimum" window size allowed:
        self.set_size_request(DEFAULT_MAIN_WIN_WIDTH, DEFAULT_MAIN_WIN_HEIGHT)

        self.set_border_width(H_PAD)
        self.content_box = Gtk.Box(spacing=0, orientation=Gtk.Orientation.VERTICAL)
        self.add(self.content_box)

        diff_tree_panes = Gtk.HPaned()
        self.content_box.add(diff_tree_panes)

        self.sizegroups = {'root_paths': Gtk.SizeGroup(mode=Gtk.SizeGroupMode.VERTICAL),
                           'filter_panel': Gtk.SizeGroup(mode=Gtk.SizeGroupMode.VERTICAL),
                           'tree_status': Gtk.SizeGroup(mode=Gtk.SizeGroupMode.VERTICAL)}

        # Diff Tree Left:
        tree = self.backend.create_display_tree_from_config(tree_id=ID_LEFT_TREE, is_startup=True)
        self.tree_con_left = tree_factory.build_editor_tree(parent_win=self, tree=tree)
        diff_tree_panes.pack1(self.tree_con_left.content_box, resize=True, shrink=False)

        # Diff Tree Right:
        tree = self.backend.create_display_tree_from_config(tree_id=ID_RIGHT_TREE, is_startup=True)
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

        self._is_playing = self.backend.get_op_execution_play_state()
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
        # self.proress_bar_component = ProgressBar(self.backend, listen_for)
        # self.bottom_panel.pack_start(self.proress_bar_component.progressbar, True, False, 0)
        # Give progress bar exactly half of the window width:
        # self.bottom_panel.set_homogeneous(True)

        self._set_default_button_bar()

        # Subscribe to signals:
        dispatcher.connect(signal=Signal.OP_EXECUTION_PLAY_STATE_CHANGED, receiver=self._update_play_pause_btn)
        dispatcher.connect(signal=Signal.TOGGLE_UI_ENABLEMENT, receiver=self._on_enable_ui_toggled)
        dispatcher.connect(signal=Signal.DIFF_TREES_DONE, receiver=self._after_diff_done)
        dispatcher.connect(signal=Signal.DIFF_TREES_FAILED, receiver=self._after_diff_failed)
        dispatcher.connect(signal=Signal.DIFF_TREES_CANCELLED, receiver=self._on_diff_exited)
        dispatcher.connect(signal=Signal.GENERATE_MERGE_TREE_DONE, receiver=self._after_merge_tree_generated)
        dispatcher.connect(signal=Signal.GENERATE_MERGE_TREE_FAILED, receiver=self._after_gen_merge_tree_failed)

        dispatcher.connect(signal=Signal.ERROR_OCCURRED, receiver=self._on_error_occurred)

        # Need to add an extra listener to each tree, to reload when the other one's root changes
        # if displaying the results of a diff
        dispatcher.connect(signal=Signal.DISPLAY_TREE_CHANGED, receiver=self._after_display_tree_changed_twopane)

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
        logger.debug(f'TwoPaneWindow.shutdown() called')

        dispatcher.send(Signal.SHUTDOWN_APP, sender=self.win_id)

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
        def _on_diff_btn_clicked(widget):
            logger.debug(f'Diff btn clicked! Sending diff request to BE')
            # Disable button bar immediately:
            GlobalActions.disable_ui(sender=self.win_id)
            self.backend.start_diff_trees(tree_id_left=self.tree_con_left.tree_id, tree_id_right=self.tree_con_right.tree_id)
            # We will be notified asynchronously when it is done/failed. If successful, the old tree_ids will be notified and supplied the new IDs
        diff_action_btn = Gtk.Button(label="Diff (content-first)")
        diff_action_btn.connect("clicked", _on_diff_btn_clicked)

        def on_goog_btn_clicked(widget):
            logger.debug(f'DownloadGDrive btn clicked! Sending signal: "{Signal.DOWNLOAD_ALL_GDRIVE_META.name}"')
            dispatcher.send(signal=Signal.DOWNLOAD_ALL_GDRIVE_META, sender=self.win_id)
        gdrive_btn = Gtk.Button(label="Download Google Drive Meta")
        gdrive_btn.connect("clicked", on_goog_btn_clicked)

        self.replace_bottom_button_panel(diff_action_btn, gdrive_btn)

    # GTK LISTENERS begin
    # ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼

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

        # create a timer
        tid = GLib.timeout_add(interval=WIN_SIZE_STORE_DELAY_MS, function=self._on_size_timer)
        # ...and remember its id
        self._timer_id = tid

    def _on_size_timer(self):
        # current window size
        curr = self.get_allocated_size().allocation

        # was the size changed in the last 500ms?
        # NO changes anymore
        if self._remembered_size.equal(curr):  # == doesn't work here
            # logger.debug('RESIZING FINISHED')

            self.backend.put_config(self.width_cfg_path, curr.width)
            self.backend.put_config(self.height_cfg_path, curr.height)

            # Store position also
            x, y = self.get_position()
            if x != self.x_loc or y != self.y_loc:
                # logger.debug(f'Win position changed to {x}, {y}')
                self.backend.put_config(self.x_loc_cfg_path, x)
                self.backend.put_config(self.y_loc_cfg_path, y)
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

    def _on_merge_preview_btn_clicked(self, widget):
        """
        1. Gets selected changes from both sides,
        2. merges into one change tree and raises an error for conflicts,
        3. or if successful displays the merge preview dialog with the summary.
        """
        logger.debug('Merge btn clicked')

        selected_changes_left: List[SPIDNodePair] = self.tree_con_left.generate_checked_row_list()
        selected_changes_right: List[SPIDNodePair] = self.tree_con_right.generate_checked_row_list()

        GlobalActions.disable_ui(sender=self.win_id)

        self.backend.generate_merge_tree(tree_id_left=self.tree_con_left.tree_id, tree_id_right=self.tree_con_right.tree_id,
                                         selected_changes_left=selected_changes_left, selected_changes_right=selected_changes_right)

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

    def _after_display_tree_changed_twopane(self, sender, tree: DisplayTree):
        # FIXME: put all this logic in the BE
        logger.debug(f'Received signal: "{Signal.DISPLAY_TREE_CHANGED.name}"')
        if tree.state.tree_display_mode != TreeDisplayMode.ONE_TREE_ALL_ITEMS:
            return

        if sender == self.tree_con_left.tree_id and self.tree_con_right.tree_display_mode == TreeDisplayMode.CHANGES_ONE_TREE_PER_CATEGORY:
            # If displaying a diff and right root changed, reload left display
            # (note: right will update its own display)
            logger.debug(f'Detected that {self.tree_con_right.tree_id} changed root. Reloading {self.tree_con_left.tree_id}')
            self._reload_tree(self.tree_con_left)

        elif sender == self.tree_con_right.tree_id and self.tree_con_left.tree_display_mode == TreeDisplayMode.CHANGES_ONE_TREE_PER_CATEGORY:
            # Mirror of above:
            logger.debug(f'Detected that {self.tree_con_left.tree_id} changed root. Reloading {self.tree_con_right.tree_id}')
            self._reload_tree(self.tree_con_right)
        else:
            # doesn't apply to us
            return

        GLib.idle_add(self._set_default_button_bar)

    def _reload_tree(self, tree_con):
        """Reload the given tree in regular mode. This will tell the backend to discard the diff information, and in turn the
        backend will provide us with our old tree_id"""
        new_tree = self.backend.create_existing_display_tree(tree_con.tree_id, TreeDisplayMode.ONE_TREE_ALL_ITEMS)
        tree_con.reload(new_tree)

    def _after_diff_failed(self, sender):
        logger.debug(f'Received signal: "{Signal.DIFF_TREES_FAILED.name}"')
        GLib.idle_add(self._set_default_button_bar)
        GlobalActions.enable_ui(sender=self.win_id)

    def _after_diff_done(self, sender, tree_left: DisplayTree, tree_right: DisplayTree):
        logger.debug(f'Received signal: "{Signal.DIFF_TREES_DONE.name}"')

        def change_button_bar():
            # Replace diff btn with merge buttons
            merge_btn = Gtk.Button(label="Merge Selected...")
            merge_btn.connect("clicked", self._on_merge_preview_btn_clicked)

            def on_cancel_diff_btn_clicked(widget):
                dispatcher.send(signal=Signal.EXIT_DIFF_MODE, sender=ID_MERGE_TREE)

            cancel_diff_btn = Gtk.Button(label="Cancel Diff")
            cancel_diff_btn.connect("clicked", on_cancel_diff_btn_clicked)

            self.replace_bottom_button_panel(merge_btn, cancel_diff_btn)

            GlobalActions.enable_ui(sender=self.win_id)
        GLib.idle_add(change_button_bar)

        self.tree_con_left.reload(tree_left)
        self.tree_con_right.reload(tree_right)

    def _on_diff_exited(self, sender, tree_left: DisplayTree, tree_right: DisplayTree):
        logger.debug(f'Received signal that we are exiting diff mode. Reloading both trees')

        GLib.idle_add(self._set_default_button_bar)

        self.tree_con_left.reload(tree_left)
        self.tree_con_right.reload(tree_right)

    def _after_merge_tree_generated(self, sender, tree: DisplayTree):
        logger.debug(f'Received signal: "{Signal.GENERATE_MERGE_TREE_DONE.name}"')

        def _show_merge_dialog():
            try:
                # Preview changes in UI pop-up
                dialog = MergePreviewDialog(self, tree)
                dialog.run()
            except Exception as err:
                self.show_error_ui('Merge preview failed due to unexpected error', repr(err))
                raise
            finally:
                GlobalActions.enable_ui(sender=self.win_id)

        GLib.idle_add(_show_merge_dialog)

    def _after_gen_merge_tree_failed(self, sender):
        logger.debug(f'Received signal: "{Signal.GENERATE_MERGE_TREE_FAILED.name}"')
        GlobalActions.enable_ui(sender=self.win_id)

    def _on_error_occurred(self, msg: str, secondary_msg: str = None):
        logger.debug(f'Received signal: "{Signal.ERROR_OCCURRED.name}"')
        self.show_error_ui(msg, secondary_msg)
