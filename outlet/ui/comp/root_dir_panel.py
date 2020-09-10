import errno
import logging
import os
from pydispatch import dispatcher

import gi

from util import file_util
from index.error import CacheNotLoadedError, GDriveItemNotFoundError
from ui.dialog.local_dir_chooser_dialog import LocalRootDirChooserDialog

from constants import BTN_GDRIVE, BTN_LOCAL_DISK_LINUX, GDRIVE_PATH_PREFIX, ICON_ALERT, ICON_REFRESH, NULL_UID, \
    TREE_TYPE_GDRIVE, \
    TREE_TYPE_LOCAL_DISK, TREE_TYPE_MIXED
from model.node_identifier import NodeIdentifier
from ui.dialog.base_dialog import BaseDialog
import ui.actions as actions

gi.require_version("Gtk", "3.0")
from gi.repository import Gtk, Gdk, GLib

logger = logging.getLogger(__name__)


#    CLASS RootDirPanel
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼


class RootDirPanel:
    def __init__(self, parent_win, controller, tree_id, current_root: NodeIdentifier, can_change_root, is_loaded):
        self.parent_win: BaseDialog = parent_win
        self.con = controller
        assert type(tree_id) == str
        self.tree_id = tree_id
        self.cache_manager = self.con.cache_manager
        self.content_box = Gtk.Box(spacing=6, orientation=Gtk.Orientation.HORIZONTAL)
        self.current_root: NodeIdentifier = current_root
        self.can_change_root = can_change_root
        self._ui_enabled = can_change_root
        """If editable, toggled via actions.TOGGLE_UI_ENABLEMENT. If not, always false"""

        if is_loaded or self.cache_manager.load_all_caches_on_startup or self.cache_manager.load_caches_for_displayed_trees_at_startup:
            # the actual load will be handled in TreeUiListeners:
            self.needs_load = False
        else:
            # Manual load:
            self.needs_load = True
            dispatcher.connect(signal=actions.LOAD_SUBTREE_STARTED, sender=self.tree_id, receiver=self._on_load_started)

        self.path_icon = Gtk.Image()
        self.refresh_icon = Gtk.Image()
        self.refresh_icon.set_from_file(self.parent_win.application.assets.get_path(ICON_REFRESH))
        if self.can_change_root:
            self.change_btn = Gtk.MenuButton()
            self.change_btn.set_image(image=self.path_icon)
            self.content_box.pack_start(self.change_btn, expand=False, fill=False, padding=5)
            self.change_btn.connect("clicked", self._on_change_btn_clicked)
            self.source_menu = self._build_source_menu()
            self.change_btn.set_popup(self.source_menu)

            def on_key_pressed(widget, event):
                if self._ui_enabled and event.keyval == Gdk.KEY_Escape and self.entry:
                    # cancel
                    logger.debug(f'Escape pressed! Cancelling root path entry box')
                    self._update_root_label(self.current_root, self.err)
                    return True
                return False

            # Connect Escape key listener to parent window so it can be heard everywhere
            self.key_press_event_eid = self.parent_win.connect('key-press-event', on_key_pressed)
        else:
            self.key_press_event_eid = None
            self.change_btn = None
            self.content_box.pack_start(self.path_icon, expand=False, fill=False, padding=0)

        # path_box contains alert_image_box (which may contain alert_image) and label_event_box (contains label)
        self.path_box = Gtk.Box(spacing=0, orientation=Gtk.Orientation.HORIZONTAL)
        self.content_box.pack_start(self.path_box, expand=True, fill=True, padding=0)

        self.alert_image_box = Gtk.Box(spacing=6, orientation=Gtk.Orientation.HORIZONTAL)
        self.path_box.pack_start(self.alert_image_box, expand=False, fill=False, padding=0)

        self.alert_image = Gtk.Image()
        self.alert_image.set_from_file(self.parent_win.application.assets.get_path(ICON_ALERT))

        self.entry_box_focus_eid = None
        self.entry = None
        self.label_event_box = None
        self.label = None
        self.toolbar = None
        self.refresh_button = None

        actions.connect(actions.TOGGLE_UI_ENABLEMENT, self._on_enable_ui_toggled)
        actions.connect(actions.ROOT_PATH_UPDATED, self._on_root_path_updated, self.tree_id)

        # Need to call this to do the initial UI draw:
        logger.debug(f'Building panel: {self.tree_id} with current root {self.current_root}')

        if self.current_root.tree_type == TREE_TYPE_LOCAL_DISK and not os.path.exists(self.current_root.full_path):
            self.err = FileNotFoundError(errno.ENOENT, os.strerror(errno.ENOENT), self.current_root.full_path)
        else:
            self.err = None

        GLib.idle_add(self._update_root_label, current_root, self.err)

    def __del__(self):
        if self.entry_box_focus_eid:
            if self.entry:
                self.entry.disconnect(self.entry_box_focus_eid)
                self.entry_box_focus_eid = None

        if self.key_press_event_eid:
            if self.parent_win:
                self.parent_win.disconnect(self.key_press_event_eid)
            self.key_press_event_eid = None

        self.cache_manager = None
        self.parent_win = None
        self.con = None

    def _on_root_text_entry_submitted(self, widget, tree_id):
        if self.entry and self.entry_box_focus_eid:
            self.entry.disconnect(self.entry_box_focus_eid)
            self.entry_box_focus_eid = None
        # Triggered when the user submits a root via the text entry box
        new_root_path: str = self.entry.get_text()
        logger.info(f'User entered root path: "{new_root_path}" for tree_id={tree_id}')
        new_root, err = self.cache_manager.resolve_root_from_path(new_root_path)

        if new_root == self.current_root:
            logger.debug('No change to root')
            self._update_root_label(self.current_root, err=self.err)
            return

        dispatcher.send(signal=actions.ROOT_PATH_UPDATED, sender=tree_id, new_root=new_root, err=err)

    def _on_change_btn_clicked(self, widget):
        if self._ui_enabled:
            self.source_menu.popup_at_widget(widget, Gdk.Gravity.SOUTH_WEST, Gdk.Gravity.NORTH_WEST, None)
        return True

    def _on_label_clicked(self, widget, event):
        """User clicked on the root label: toggle it to show the text entry box"""
        if not self._ui_enabled:
            logger.debug('Ignoring button press - UI is disabled')
            return False

        if not event.button == 1:
            # Left click only
            return False

        # Remove alert image if present; only show entry box
        if len(self.alert_image_box.get_children()) > 0:
            self.alert_image_box.remove(self.alert_image)

        self.entry = Gtk.Entry()

        path = self.current_root.full_path
        if self.current_root.tree_type == TREE_TYPE_GDRIVE:
            path = GDRIVE_PATH_PREFIX + path
        self.entry.set_text(path)
        self.entry.connect('activate', self._on_root_text_entry_submitted, self.tree_id)
        self.path_box.remove(self.label_event_box)
        if self.toolbar:
            self.path_box.remove(self.toolbar)
            self.toolbar = None
        self.path_box.pack_start(self.entry, expand=True, fill=True, padding=0)

        def cancel_edit(widget, event):
            if self.entry and self.entry_box_focus_eid:
                self.entry.disconnect(self.entry_box_focus_eid)
                self.entry_box_focus_eid = None
            logger.debug(f'Focus lost! Cancelling root path entry box')
            self._update_root_label(self.current_root, self.err)

        self.entry_box_focus_eid = self.entry.connect('focus-out-event', cancel_edit)
        self.entry.show()
        self.entry.grab_focus()
        return False

    def _on_enable_ui_toggled(self, sender, enable):
        # Callback for actions.TOGGLE_UI_ENABLEMENT
        if not self.can_change_root:
            self._ui_enabled = False
            return

        self._ui_enabled = enable
        # TODO: what if root text entry is showing?

        def change_button():
            self.change_btn.set_sensitive(enable)
        GLib.idle_add(change_button)

    def _update_root_label(self, new_root: NodeIdentifier, err=None):
        """Updates the UI to reflect the new root and tree type.
        Expected to be called from the UI thread.
        """
        if self.entry:
            if self.entry_box_focus_eid:
                self.entry.disconnect(self.entry_box_focus_eid)
                self.entry_box_focus_eid = None
            # Remove entry box (we were probably called by it to do cleanup in fact)
            self.path_box.remove(self.entry)
            self.entry = None
        elif self.label_event_box:
            # Remove label even if it's found. Simpler just to redraw the whole thing
            self.path_box.remove(self.label_event_box)
            self.label_event_box = None
        if self.toolbar:
            self.path_box.remove(self.toolbar)
            self.toolbar = None

        self.label_event_box = Gtk.EventBox()
        self.path_box.pack_start(self.label_event_box, expand=True, fill=True, padding=0)

        self.label = Gtk.Label(label='')
        self.label.set_justify(Gtk.Justification.LEFT)
        self.label.set_xalign(0)
        self.label.set_line_wrap(True)
        self.label_event_box.add(self.label)
        self.label_event_box.show()

        if self.needs_load:
            # Note: good example of toolbar here:
            # https://github.com/kyleuckert/LaserTOF/blob/master/labTOF_main.app/Contents/Resources/lib/python2.7/matplotlib/backends/backend_gtk3.py
            self.toolbar = Gtk.Toolbar()
            self.toolbar.set_style(Gtk.ToolbarStyle.ICONS)
            self.refresh_button = Gtk.ToolButton()
            self.refresh_button.set_icon_widget(self.refresh_icon)
            self.refresh_button.set_tooltip_text('Load meta for tree')
            self.refresh_button.connect('clicked', self._on_refresh_button_clicked)
            self.toolbar.insert(self.refresh_button, -1)
            self.path_box.pack_end(self.toolbar, expand=False, fill=True, padding=5)
            self.toolbar.show_all()

        if self.can_change_root:
            self.label_event_box.connect('button_press_event', self._on_label_clicked)

        if new_root.tree_type == TREE_TYPE_LOCAL_DISK:
            self.path_icon.set_from_file(self.parent_win.application.assets.get_path(BTN_LOCAL_DISK_LINUX))
        elif new_root.tree_type == TREE_TYPE_GDRIVE:
            self.path_icon.set_from_file(self.parent_win.application.assets.get_path(BTN_GDRIVE))
        elif new_root.tree_type == TREE_TYPE_MIXED:
            self.path_icon.set_from_file(self.parent_win.application.assets.get_path(BTN_LOCAL_DISK_LINUX))
        else:
            raise RuntimeError(f'Unrecognized tree type: {new_root.tree_type}')

        root_exists = not err and new_root.uid != NULL_UID
        if root_exists:
            pre = ''
            color = ''
            root_part_regular, root_part_bold = os.path.split(new_root.full_path)
            if len(self.alert_image_box.get_children()) > 0:
                self.alert_image_box.remove(self.alert_image)
        else:
            root_part_regular, root_part_bold = os.path.split(new_root.full_path)
            if err and isinstance(err, GDriveItemNotFoundError):
                root_part_regular = err.offending_path
                root_part_bold = file_util.strip_root(new_root.full_path, err.offending_path)
            if len(self.alert_image_box.get_children()) == 0:
                self.alert_image_box.pack_start(self.alert_image, expand=False, fill=False, padding=0)
            color = f"foreground='gray'"
            pre = f"<span foreground='red' size='small'>Not found:  </span>"
            self.alert_image.show()

        if root_part_regular != '/':
            root_part_regular = root_part_regular + '/'
        self._set_label_markup(pre, color, root_part_regular, root_part_bold)

    def _set_label_markup(self, pre, color, root_part_regular, root_part_bold):
        """Sets the content of the label only. Expected to be called from the UI thread"""
        root_part_regular = GLib.markup_escape_text(root_part_regular)
        root_part_bold = GLib.markup_escape_text(root_part_bold)
        self.label.set_markup(f"{pre}<span font_family='monospace' size='medium' {color}><i>{root_part_regular}\n<b>{root_part_bold}</b></i></span>")
        self.label.show()
        self.label_event_box.show()

    def _on_root_path_updated(self, sender, new_root: NodeIdentifier, err=None):
        """Callback for actions.ROOT_PATH_UPDATED"""
        logger.debug(f'[{sender}] Received signal "{actions.ROOT_PATH_UPDATED}": type={new_root.tree_type} path="{new_root.full_path}"')
        if not new_root or not new_root.full_path:
            raise RuntimeError(f'Root path cannot be empty! (tree_id={sender})')

        if self.current_root != new_root:
            self.current_root = new_root
            if not err and not self.cache_manager.reload_tree_on_root_path_update:
                self.needs_load = True

            # For markup options, see: https://developer.gnome.org/pygtk/stable/pango-markup-language.html
            GLib.idle_add(self._update_root_label, new_root, err)

    def _open_localdisk_root_chooser_dialog(self, menu_item):
        """Creates and displays a LocalRootDirChooserDialog.
        # the arguments are: title of the window, parent_window, action,
        # (buttons, response)"""
        logger.debug('Creating and displaying LocalRootDirChooserDialog')
        open_dialog = LocalRootDirChooserDialog(title="Pick a directory", parent_win=self.parent_win, tree_id=self.tree_id,
                                                current_dir=self.current_root.full_path)

        # show the dialog
        open_dialog.show()

    def _open_gdrive_root_chooser_dialog(self, menu_item):
        dispatcher.send(signal=actions.SHOW_GDRIVE_ROOT_DIALOG, sender=self.tree_id, current_selection=self.current_root)

    def _build_source_menu(self):
        source_menu = Gtk.Menu()
        item_select_local = Gtk.MenuItem(label="Local filesystem subtree...")
        item_select_local.connect('activate', self._open_localdisk_root_chooser_dialog)
        source_menu.append(item_select_local)
        item_gdrive = Gtk.MenuItem(label="Google Drive subtree...")
        source_menu.append(item_gdrive)
        item_gdrive.connect('activate', self._open_gdrive_root_chooser_dialog)
        source_menu.show_all()
        return source_menu

    def _on_refresh_button_clicked(self, widget):
        logger.debug('The Refresh button was clicked!')
        self.needs_load = False
        # Launch in a non-UI thread:
        dispatcher.send(signal=actions.LOAD_UI_TREE, sender=self.tree_id)
        # Hide Refresh button
        GLib.idle_add(self._update_root_label, self.current_root, self.err)

    def _on_load_started(self, sender):
        if self.needs_load:
            self.needs_load = False
            # Hide Refresh button
            GLib.idle_add(self._update_root_label, self.current_root, self.err)
