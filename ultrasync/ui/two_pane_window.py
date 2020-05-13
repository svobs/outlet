import logging
from typing import List

import gi

from constants import TreeDisplayMode
from model.display_id import Identifier
from model.display_node import DisplayNode

gi.require_version("Gtk", "3.0")
from gi.repository import GLib, Gtk, Gdk

from ui.tree.root_path_config import RootPathConfigPersister

from pydispatch import dispatcher

import ui.actions as actions
import ui.assets
from ui.comp.progress_bar import ProgressBar
from ui.tree import tree_factory

from ui.dialog.merge_preview_dialog import MergePreviewDialog
from diff import diff_content_first
from ui.dialog.base_dialog import BaseDialog

logger = logging.getLogger(__name__)


""" ▂ ▃ ▄ ▅ ▆ ▇ █ █ ▇ ▆ ▅ ▄ ▃ ▂
             TwoPanelWindow
"""


class TwoPanelWindow(Gtk.ApplicationWindow, BaseDialog):
    """🢄🢄🢄 2-panel window for comparing one file tree to another"""
    def __init__(self, application, win_id):
        Gtk.Window.__init__(self, application=application)
        BaseDialog.__init__(self, application)

        self.win_id = win_id
        self.set_title('UltraSync')
        # program icon:
        self.set_icon_from_file(ui.assets.WINDOW_ICON_PATH)
        # Set minimum width and height

        # Restore previous window location:
        self.x_loc_cfg_path = f'transient.{self.win_id}.x'
        self.y_loc_cfg_path = f'transient.{self.win_id}.y'
        self.x_loc = self.application.config.get(self.x_loc_cfg_path, 50)
        self.y_loc = self.application.config.get(self.y_loc_cfg_path, 50)
        self.move(x=self.x_loc, y=self.y_loc)

        self.set_hide_titlebar_when_maximized(True)

        # Restore previous width/height:
        self.width_cfg_path = f'transient.{self.win_id}.width'
        self.height_cfg_path = f'transient.{self.win_id}.height'

        width = self.application.config.get(self.width_cfg_path, 1200)
        height = self.application.config.get(self.height_cfg_path, 500)
        allocation = Gdk.Rectangle()
        allocation.width = width
        allocation.height = height
        self.size_allocate(allocation)
        # i.e. "minimum" window size allowed:
        self.set_size_request(1200, 500)

        self.set_border_width(10)
        self.content_box = Gtk.Box(spacing=6, orientation=Gtk.Orientation.VERTICAL)
        self.add(self.content_box)

        # Checkboxes: TODO
        # self.checkbox_panel = Gtk.Box(spacing=6, orientation=Gtk.Orientation.HORIZONTAL)
        #
        # # check for the following...
        # self.button1 = Gtk.CheckButton(label="Empty dirs")
        # self.checkbox_panel.pack_start(self.button1, True, True, 0)
        # self.button1.set_sensitive(False)
        # self.button2 = Gtk.CheckButton(label="Zero-length files")
        # self.checkbox_panel.pack_start(self.button2, True, True, 0)
        # self.button2.set_sensitive(False)
        # self.button3= Gtk.CheckButton(label="Duplicate files")
        # self.checkbox_panel.pack_start(self.button3, True, True, 0)
        # self.button3.set_sensitive(False) # disable
        # self.button4= Gtk.CheckButton(label="Unrecognized suffixes")
        # self.checkbox_panel.pack_start(self.button4, True, True, 0)
        # self.button4.set_sensitive(False)  # disable
        # self.button5= Gtk.CheckButton(label="Relative paths or file names differ")
        # self.checkbox_panel.pack_start(self.button5, True, True, 0)
        # self.button5.set_sensitive(False) # disable
        # self.content_box.add(self.checkbox_panel)

        diff_tree_panes = Gtk.HPaned()
        self.content_box.add(diff_tree_panes)

        self.sizegroups = {'root_paths': Gtk.SizeGroup(mode=Gtk.SizeGroupMode.VERTICAL),
                           'tree_status': Gtk.SizeGroup(mode=Gtk.SizeGroupMode.VERTICAL)}

        # Diff Tree Left:
        self.root_path_persister_left = RootPathConfigPersister(config=self.config, tree_id=actions.ID_LEFT_TREE)
        saved_root_left = self.root_path_persister_left.root_identifier
        self.tree_con_left = tree_factory.build_category_file_tree(parent_win=self, tree_id=actions.ID_LEFT_TREE, root=saved_root_left)
        diff_tree_panes.pack1(self.tree_con_left.content_box, resize=True, shrink=False)

        # Diff Tree Right:
        self.root_path_persister_right = RootPathConfigPersister(config=self.config, tree_id=actions.ID_RIGHT_TREE)
        saved_root_right = self.root_path_persister_right.root_identifier
        self.tree_con_right = tree_factory.build_category_file_tree(parent_win=self, tree_id=actions.ID_RIGHT_TREE, root=saved_root_right)
        diff_tree_panes.pack2(self.tree_con_right.content_box, resize=True, shrink=False)

        self.bottom_panel = Gtk.Box(spacing=6, orientation=Gtk.Orientation.HORIZONTAL)
        self.content_box.add(self.bottom_panel)
        # Bottom button panel:
        self.bottom_button_panel = Gtk.Box(spacing=6, orientation=Gtk.Orientation.HORIZONTAL)
        self.bottom_panel.add(self.bottom_button_panel)

        listen_for = [actions.ID_LEFT_TREE, actions.ID_RIGHT_TREE,
                      self.win_id, actions.ID_GLOBAL_CACHE]
        # Remember to hold a reference to this, for signals!
        self.proress_bar_component = ProgressBar(self.config, listen_for)
        self.bottom_panel.pack_start(self.proress_bar_component.progressbar, True, True, 0)
        # Give progress bar exactly half of the window width:
        self.bottom_panel.set_homogeneous(True)

        def on_diff_btn_clicked(widget):
            logger.debug('Diff btn clicked!')
            # Disable button bar immediately:
            self._on_enable_ui_toggled(sender=self.win_id, enable=False)
            dispatcher.send(signal=actions.START_DIFF_TREES, sender=self.win_id,
                            tree_con_left=self.tree_con_left, tree_con_right=self.tree_con_right)
        diff_action_btn = Gtk.Button(label="Diff (content-first)")
        diff_action_btn.connect("clicked", on_diff_btn_clicked)

        def on_goog_btn_clicked(widget):
            logger.debug('DownloadGDrive btn clicked!')
            actions.send_signal(signal=actions.DOWNLOAD_GDRIVE_META, sender=self.win_id)
        gdrive_btn = Gtk.Button(label="Download Google Drive Meta")
        gdrive_btn.connect("clicked", on_goog_btn_clicked)

        self.replace_bottom_button_panel(diff_action_btn, gdrive_btn)

        # Subscribe to signals:
        actions.connect(signal=actions.TOGGLE_UI_ENABLEMENT, handler=self._on_enable_ui_toggled)
        actions.connect(signal=actions.DIFF_TREES_DONE, handler=self._after_diff_completed)

        # Need to add an extra listener to each tree, to reload when the other one's root changes
        # if displaying the results of a diff
        dispatcher.connect(signal=actions.ROOT_PATH_UPDATED, receiver=self._on_root_path_updated, sender=actions.ID_LEFT_TREE)
        dispatcher.connect(signal=actions.ROOT_PATH_UPDATED, receiver=self._on_root_path_updated, sender=actions.ID_RIGHT_TREE)

        # Connect "resize" event. Lots of excess logic to determine approximately when the
        # window *stops* being resized, so we can persist the value semi-efficiently
        self._connect_resize_event()

        # Docs: https://developer.gnome.org/pygtk/stable/class-gdkdisplay.html
        display: Gdk.Display = self.get_display()
        logger.debug(f'Display has {display.get_n_screens()} screen(s)')
        for screen_num in range(0, display.get_n_screens()):
            screen = display.get_screen(screen_num)
            logger.debug(f'    Screen #{screen_num} has {screen.get_n_monitors()} monitors and is {screen.get_width()}x{screen.get_height()}')
            for monitor_num in range(0, screen.get_n_monitors()):
                size_rect: Gdk.Rectangle = screen.get_monitor_geometry(monitor_num)
                logger.debug(f'        Monitor #{monitor_num} is {size_rect.width}x{size_rect.height}')

        # Kick off cache load now that we have a progress bar
        actions.get_dispatcher().send(actions.LOAD_ALL_CACHES, sender=self.win_id)

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

    # GTK LISTENERS begin
    # ⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟

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

            self.application.config.write(self.width_cfg_path, curr.width)
            self.application.config.write(self.height_cfg_path, curr.height)

            # Store position also
            x, y = self.get_position()
            if x != self.x_loc or y != self.y_loc:
                # logger.debug(f'Win position changed to {x}, {y}')
                self.application.config.write(self.x_loc_cfg_path, x)
                self.application.config.write(self.y_loc_cfg_path, y)
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

    def on_merge_preview_btn_clicked(self, widget):
        """
        1. Gets selected changes from both sides,
        2. merges into one change tree and raises an error for conflicts,
        3. or if successful displays the merge preview dialog with the summary.
        """
        logger.debug('Merge btn clicked')

        try:
            left_selected_changes: List[DisplayNode] = self.tree_con_left.get_checked_rows_as_list()
            right_selected_changes: List[DisplayNode] = self.tree_con_right.get_checked_rows_as_list()
            if len(left_selected_changes) == 0 and len(right_selected_changes) == 0:
                self.show_error_msg('You must select change(s) first.')
                return

            merged_changes_tree = diff_content_first.merge_change_trees(self.tree_con_left.get_tree(),
                                                                        self.tree_con_right.get_tree(),
                                                                        left_selected_changes,
                                                                        right_selected_changes)

            conflict_pairs = []
            if conflict_pairs:
                # TODO: more informative error
                self.show_error_msg('Cannot merge', f'{len(conflict_pairs)} conflicts found')
                return

            logger.info(f'Merged changes: {merged_changes_tree.get_summary()}')

            # Preview changes in UI pop-up
            dialog = MergePreviewDialog(self, merged_changes_tree)
            response_id = dialog.run()
        except Exception as err:
            self.show_error_ui('Merge preview failed due to unexpected error', repr(err))
            raise

    # SIGNAL LISTENERS begin
    # ⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟

    def _on_enable_ui_toggled(self, sender, enable):
        """Callback for TOGGLE_UI_ENABLEMENT"""
        def toggle_ui():
            for button in self.bottom_button_panel.get_children():
                button.set_sensitive(enable)
        GLib.idle_add(toggle_ui)

    def _on_root_path_updated(self, sender, new_root: Identifier, err=None):
        logger.debug(f'Received signal: "{actions.ROOT_PATH_UPDATED}"')

        if sender == actions.ID_RIGHT_TREE and self.tree_con_left.tree_display_mode == TreeDisplayMode.CHANGES_ONE_TREE_PER_CATEGORY:
            # If displaying a diff and right root changed, reload left display
            # (note: right will update its own display)
            logger.debug(f'Detected that {actions.ID_RIGHT_TREE} changed root. Reloading {actions.ID_LEFT_TREE}')
            self.tree_con_left.reload()

        elif sender == actions.ID_LEFT_TREE and self.tree_con_right.tree_display_mode == TreeDisplayMode.CHANGES_ONE_TREE_PER_CATEGORY:
            # Mirror of above:
            logger.debug(f'Detected that {actions.ID_LEFT_TREE} changed root. Reloading {actions.ID_RIGHT_TREE}')
            self.tree_con_right.reload()

    def _after_diff_completed(self, sender, stopwatch):
        logger.debug(f'Received signal: "{actions.DIFF_TREES_DONE}"')

        def change_button_bar():
            # Replace diff btn with merge buttons
            merge_btn = Gtk.Button(label="Merge Selected...")
            merge_btn.connect("clicked", self.on_merge_preview_btn_clicked)

            # FIXME: this is causing the button bar to disappear. Fix layout!
            self.replace_bottom_button_panel(merge_btn)

            actions.enable_ui(sender=self)
            logger.debug(f'Diff time + redraw: {stopwatch}')
        GLib.idle_add(change_button_bar)
