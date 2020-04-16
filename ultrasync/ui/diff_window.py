import logging.handlers
import threading

import gi
gi.require_version("Gtk", "3.0")
from gi.repository import GLib, Gtk, Gio, GObject

from stopwatch import Stopwatch

from ui.merge_preview_dialog import MergePreviewDialog
import fmeta.fmeta_tree_cache as fmeta_tree_cache
from fmeta.fmeta_tree_source import FMetaTreeLoader
from file_util import get_resource_path
from fmeta import diff_content_first
from ui.diff_tree import DiffTree
from ui.base_dialog import BaseDialog
import ui.diff_tree_populator as diff_tree_populator

WINDOW_ICON_PATH = get_resource_path("resources/fslint_icon.png")

SIGNAL_DO_DIFF = 'do-diff'

logger = logging.getLogger(__name__)


class ConfigFileDataSource:
    def __init__(self, tree_id, parent_win, status_receiver=None):
        self.tree_id = tree_id
        self.parent_win = parent_win
        self.tree = None
        self.cache = fmeta_tree_cache.from_config(config=self.parent_win.config, tree_id=self.tree_id)
        self.config_entry = f'transient.{self.tree_id}.root_path'
        self._root_path = self.parent_win.config.get(self.config_entry)
        # TODO: maybe this would be better done as an event receiver
        self.status_receiver = status_receiver

    def get_root_path(self):
        return self._root_path

    def set_root_path(self, new_root_path):
        if self.get_root_path() != new_root_path:
            # Root changed. Invalidate the current tree contents
            self.tree = None
            self.parent_win.config.write(transient_path=self.config_entry, value=new_root_path)
            self._root_path = new_root_path
            # Kick off the diff task. This will reload the tree as a side effect
            # since we set self.tree = None
            self.parent_win.emit(SIGNAL_DO_DIFF, 'from root path')

    def get_fmeta_tree(self):
        if self.tree is None:
            tree_loader = FMetaTreeLoader(self._root_path, self.cache)
            self.tree = tree_loader.get_current_tree(self.status_receiver)
        return self.tree


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

        # See: http://www.thepythontree.in/gtk3-python-custom-signals/
        GObject.signal_new(SIGNAL_DO_DIFF, self, GObject.SIGNAL_RUN_LAST, GObject.TYPE_PYOBJECT, (GObject.TYPE_PYOBJECT,))
        self.connect(SIGNAL_DO_DIFF, self.start_new_diff_daemon)

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
        source_left = ConfigFileDataSource(tree_id='left_tree', parent_win=self)
        self.diff_tree_left = DiffTree(parent_win=self, tree_id=source_left.tree_id, data_source=source_left, editable=True, sizegroups=self.sizegroups)
        diff_tree_panes.pack1(self.diff_tree_left.content_box, resize=True, shrink=False)

        # Diff Tree Right:
        source_right = ConfigFileDataSource(tree_id='right_tree', parent_win=self)
        self.diff_tree_right = DiffTree(parent_win=self, tree_id=source_right.tree_id, data_source=source_right, editable=True, sizegroups=self.sizegroups)
        diff_tree_panes.pack2(self.diff_tree_right.content_box, resize=True, shrink=False)

        # Bottom button panel:
        self.bottom_button_panel = Gtk.Box(spacing=6, orientation=Gtk.Orientation.HORIZONTAL)
        self.content_box.add(self.bottom_button_panel)

        diff_action_btn = Gtk.Button(label="Diff (content-first)")
        diff_action_btn.connect("clicked", lambda widget: self.emit(SIGNAL_DO_DIFF, 'blah'))
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
                self.emit(SIGNAL_DO_DIFF)
        except Exception as err:
            self.show_error_ui('Merge preview failed due to unexpected error', repr(err))
            raise

    def start_new_diff_daemon(self, window, arg):
        action_thread = threading.Thread(target=self.diff_task)
        action_thread.daemon = True
        action_thread.start()

    # TODO: change DB path whenever root is changed
    def diff_task(self):
        try:
            # TODO: disable all UI while loading
            self.diff_tree_right.set_status('Waiting...')

            # Load trees if not loaded - may be a long operation
            left_fmeta_tree = self.diff_tree_left.data_source.get_fmeta_tree()
            right_fmeta_tree = self.diff_tree_right.data_source.get_fmeta_tree()

            stopwatch_diff = Stopwatch()
            diff_content_first.diff(left_fmeta_tree, right_fmeta_tree, compare_paths_also=True, use_modify_times=False)
            stopwatch_diff.stop()
            logger.info(f'Diff completed in: {stopwatch_diff}')

            stopwatch_redraw = Stopwatch()

            # TODO: have each spawn thread on 'diff completed' signal
            diff_tree_populator.repopulate_diff_tree(self.diff_tree_left)
            diff_tree_populator.repopulate_diff_tree(self.diff_tree_right)

            def change_button_bar():
                # Replace diff btn with merge buttons
                merge_btn = Gtk.Button(label="Merge Selected...")
                merge_btn.connect("clicked", self.on_merge_btn_clicked)

                self.replace_bottom_button_panel(merge_btn)
                stopwatch_redraw.stop()
                logger.debug(f'Redraw completed in: {stopwatch_redraw}')

            GLib.idle_add(change_button_bar)
        except Exception as err:
            self.show_error_ui('Diff task failed due to unexpected error', repr(err))
            raise

