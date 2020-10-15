import logging
from typing import List, Optional

import gi
from pydispatch.dispatcher import Any

from constants import APP_NAME, ICON_WINDOW, TreeDisplayMode
from diff.diff_content_first import ContentFirstDiffer
from model.node_identifier import NodeIdentifier
from model.node.display_node import DisplayNode
from ui.tree.category_display_tree import CategoryDisplayTree

from ui.tree.root_path_config import RootPathConfigPersister

from pydispatch import dispatcher

import ui.actions as actions
from ui.comp.progress_bar import ProgressBar
from ui.tree import tree_factory

from ui.dialog.merge_preview_dialog import MergePreviewDialog
from ui.dialog.base_dialog import BaseDialog

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
        self.set_icon_from_file(self.app.assets.get_path(ICON_WINDOW))
        # Set minimum width and height

        # Restore previous window location:
        self.x_loc_cfg_path = f'transient.{self.win_id}.x'
        self.y_loc_cfg_path = f'transient.{self.win_id}.y'
        self.x_loc = self.app.config.get(self.x_loc_cfg_path, 50)
        self.y_loc = self.app.config.get(self.y_loc_cfg_path, 50)
        self.move(x=self.x_loc, y=self.y_loc)

        self.set_hide_titlebar_when_maximized(True)

        # Restore previous width/height:
        self.width_cfg_path = f'transient.{self.win_id}.width'
        self.height_cfg_path = f'transient.{self.win_id}.height'

        width = self.app.config.get(self.width_cfg_path, 1200)
        height = self.app.config.get(self.height_cfg_path, 500)
        allocation = Gdk.Rectangle()
        allocation.width = width
        allocation.height = height
        self.size_allocate(allocation)
        # i.e. "minimum" window size allowed:
        self.set_size_request(1200, 500)

        self.set_border_width(10)
        self.content_box = Gtk.Box(spacing=6, orientation=Gtk.Orientation.VERTICAL)
        self.add(self.content_box)

        diff_tree_panes = Gtk.HPaned()
        self.content_box.add(diff_tree_panes)

        self.sizegroups = {'root_paths': Gtk.SizeGroup(mode=Gtk.SizeGroupMode.VERTICAL),
                           'tree_status': Gtk.SizeGroup(mode=Gtk.SizeGroupMode.VERTICAL)}

        # Diff Tree Left:
        self.root_path_persister_left = RootPathConfigPersister(app=self.app, tree_id=actions.ID_LEFT_TREE)
        saved_root_left = self.root_path_persister_left.root_identifier
        self.tree_con_left = tree_factory.build_editor_tree(parent_win=self, tree_id=actions.ID_LEFT_TREE, root=saved_root_left)
        diff_tree_panes.pack1(self.tree_con_left.content_box, resize=True, shrink=False)

        # Diff Tree Right:
        self.root_path_persister_right = RootPathConfigPersister(app=self.app, tree_id=actions.ID_RIGHT_TREE)
        saved_root_right = self.root_path_persister_right.root_identifier
        self.tree_con_right = tree_factory.build_editor_tree(parent_win=self, tree_id=actions.ID_RIGHT_TREE, root=saved_root_right)
        diff_tree_panes.pack2(self.tree_con_right.content_box, resize=True, shrink=False)

        # Bottom panel: buttons and progress bar
        self.bottom_panel = Gtk.Box(spacing=6, orientation=Gtk.Orientation.HORIZONTAL)
        self.content_box.add(self.bottom_panel)
        # Bottom button panel:
        self.bottom_button_panel = Gtk.Box(spacing=6, orientation=Gtk.Orientation.HORIZONTAL)
        self.bottom_panel.add(self.bottom_button_panel)

        listen_for = [actions.ID_LEFT_TREE, actions.ID_RIGHT_TREE,
                      self.win_id, actions.ID_GLOBAL_CACHE, actions.ID_COMMAND_EXECUTOR]
        # Remember to hold a reference to this, for signals!
        self.proress_bar_component = ProgressBar(self.config, listen_for)
        self.bottom_panel.pack_start(self.proress_bar_component.progressbar, True, True, 0)
        # Give progress bar exactly half of the window width:
        self.bottom_panel.set_homogeneous(True)

        self._set_default_button_bar()

        # Subscribe to signals:
        dispatcher.connect(signal=actions.TOGGLE_UI_ENABLEMENT, receiver=self._on_enable_ui_toggled)
        dispatcher.connect(signal=actions.DIFF_TREES_DONE, receiver=self._after_diff_completed)

        # Need to add an extra listener to each tree, to reload when the other one's root changes
        # if displaying the results of a diff
        dispatcher.connect(signal=actions.ROOT_PATH_UPDATED, receiver=self._on_root_path_updated, sender=actions.ID_LEFT_TREE)
        dispatcher.connect(signal=actions.ROOT_PATH_UPDATED, receiver=self._on_root_path_updated, sender=actions.ID_RIGHT_TREE)
        dispatcher.connect(signal=actions.EXIT_DIFF_MODE, receiver=self._on_merge_complete, sender=Any)

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
        if self.tree_con_left:
            self.tree_con_left.destroy()
            self.tree_con_left = None

        if self.tree_con_right:
            self.tree_con_right.destroy()
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
            logger.debug('Diff btn clicked!')
            # Disable button bar immediately:
            self._on_enable_ui_toggled(sender=self.win_id, enable=False)
            dispatcher.send(signal=actions.START_DIFF_TREES, sender=self.win_id,
                            tree_con_left=self.tree_con_left, tree_con_right=self.tree_con_right)
        diff_action_btn = Gtk.Button(label="Diff (content-first)")
        diff_action_btn.connect("clicked", on_diff_btn_clicked)

        def on_goog_btn_clicked(widget):
            logger.debug('DownloadGDrive btn clicked!')
            actions.send_signal(signal=actions.DOWNLOAD_ALL_GDRIVE_META, sender=self.win_id)
        gdrive_btn = Gtk.Button(label="Download Google Drive Meta")
        gdrive_btn.connect("clicked", on_goog_btn_clicked)

        self.replace_bottom_button_panel(diff_action_btn, gdrive_btn)

    # GTK LISTENERS begin
    # ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼

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
        left_selected_changes: List[DisplayNode] = self.tree_con_left.get_checked_rows_as_list()
        right_selected_changes: List[DisplayNode] = self.tree_con_right.get_checked_rows_as_list()
        if len(left_selected_changes) == 0 and len(right_selected_changes) == 0:
            self.show_error_msg('You must select change(s) first.')
            return None

        differ = ContentFirstDiffer(self.tree_con_left.get_tree(), self.tree_con_right.get_tree(), self.app)
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

    def _on_root_path_updated(self, sender, new_root: NodeIdentifier, err=None):
        logger.debug(f'Received signal: "{actions.ROOT_PATH_UPDATED}"')

        if sender == actions.ID_RIGHT_TREE and self.tree_con_left.tree_display_mode == TreeDisplayMode.CHANGES_ONE_TREE_PER_CATEGORY:
            # If displaying a diff and right root changed, reload left display
            # (note: right will update its own display)
            logger.debug(f'Detected that {actions.ID_RIGHT_TREE} changed root. Reloading {actions.ID_LEFT_TREE}')
            _reload_tree(self.tree_con_left)

        elif sender == actions.ID_LEFT_TREE and self.tree_con_right.tree_display_mode == TreeDisplayMode.CHANGES_ONE_TREE_PER_CATEGORY:
            # Mirror of above:
            logger.debug(f'Detected that {actions.ID_LEFT_TREE} changed root. Reloading {actions.ID_RIGHT_TREE}')
            _reload_tree(self.tree_con_right)

        GLib.idle_add(self._set_default_button_bar)

    def _after_diff_completed(self, sender, stopwatch):
        logger.debug(f'Received signal: "{actions.DIFF_TREES_DONE}"')

        def change_button_bar():
            # Replace diff btn with merge buttons
            merge_btn = Gtk.Button(label="Merge Selected...")
            merge_btn.connect("clicked", self.on_merge_preview_btn_clicked)

            def on_cancel_diff_btn_clicked(widget):
                dispatcher.send(signal=actions.EXIT_DIFF_MODE, sender=actions.ID_MERGE_TREE)

            cancel_diff_btn = Gtk.Button(label="Cancel Diff")
            cancel_diff_btn.connect("clicked", on_cancel_diff_btn_clicked)

            self.replace_bottom_button_panel(merge_btn, cancel_diff_btn)

            actions.enable_ui(sender=self)
            logger.debug(f'Diff time + redraw: {stopwatch}')
        GLib.idle_add(change_button_bar)

    def _on_merge_complete(self):
        logger.debug('Received signal that merge completed. Reloading both trees')
        _reload_tree(self.tree_con_left)
        _reload_tree(self.tree_con_right)

        GLib.idle_add(self._set_default_button_bar)


def _reload_tree(tree_con):
    # need to set root again to trigger a model wipe
    tree = tree_con.get_tree()
    root_identifier = tree.node_identifier
    tree_con.reload(new_root=root_identifier, tree_display_mode=TreeDisplayMode.ONE_TREE_ALL_ITEMS, hide_checkboxes=True)
