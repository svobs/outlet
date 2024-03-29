import logging
import os
from typing import Dict, List, Optional

from constants import GDRIVE_PATH_PREFIX, H_PAD, IconId, NULL_UID, TreeLoadState, TreeType
from model.device import Device
from model.disp_tree.display_tree import DisplayTree
from model.node_identifier import SinglePathNodeIdentifier
from model.uid import UID
from signal_constants import Signal
from fe.gtk.dialog.base_dialog import BaseDialog
from fe.gtk.dialog.gdrive_dir_chooser_dialog import GDriveDirChooserDialog
from fe.gtk.dialog.local_dir_chooser_dialog import LocalRootDirChooserDialog
from util import file_util
from util.has_lifecycle import HasLifecycle

import gi
gi.require_version("Gtk", "3.0")
from gi.repository import Gtk, Gdk, GLib

logger = logging.getLogger(__name__)


class RootPathPanel(HasLifecycle):
    """
    ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
    CLASS RootPathPanel
    ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼
    """
    def __init__(self, parent_win, controller, can_change_root):
        HasLifecycle.__init__(self)
        self.parent_win: BaseDialog = parent_win
        self.con = controller
        self.content_box = Gtk.Box(spacing=0, orientation=Gtk.Orientation.HORIZONTAL)

        self.can_change_root = can_change_root
        self._ui_enabled = can_change_root
        """If editable, toggled via Signal.TOGGLE_UI_ENABLEMENT. If not, always false"""

        self.path_icon = Gtk.Image()
        self.refresh_icon = Gtk.Image()
        self.refresh_icon.set_from_file(self.parent_win.app.assets.get_path(IconId.ICON_REFRESH))
        if self.can_change_root:
            self.change_btn = Gtk.MenuButton()
            self.change_btn.set_image(image=self.path_icon)
            self.content_box.pack_start(self.change_btn, expand=False, fill=False, padding=0)
            self.change_btn.connect("clicked", self._on_change_btn_clicked)
            self.source_menu = self._build_source_menu()
            self.change_btn.set_popup(self.source_menu)

            def on_key_pressed(widget, event):
                if self._ui_enabled and event.keyval == Gdk.KEY_Escape and self.path_entry:
                    # cancel
                    logger.debug(f'Escape pressed! Cancelling root path path_entry box')
                    self._redraw_root_display()
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

        self.alert_image_box = Gtk.Box(spacing=H_PAD, orientation=Gtk.Orientation.HORIZONTAL)
        self.path_box.pack_start(self.alert_image_box, expand=False, fill=False, padding=0)

        self.alert_image = Gtk.Image()
        self.alert_image.set_from_file(self.parent_win.app.assets.get_path(IconId.ICON_ALERT))

        self.entry_box_focus_eid = None
        self.path_entry = None
        self.label_event_box = None
        self.label = None
        self.toolbar = None
        self.refresh_button = None

        display_tree: DisplayTree = self.con.get_tree()
        if display_tree:
            # Do the initial UI draw (only if we already have a display tree)
            logger.debug(f'[{self.con.tree_id}] Building panel with current root {display_tree.get_root_spid()}')
            GLib.idle_add(self._redraw_root_display)

        self.start()

    def start(self):
        HasLifecycle.start(self)
        self.connect_dispatch_listener(signal=Signal.TOGGLE_UI_ENABLEMENT, receiver=self._on_enable_ui_toggled)
        self.connect_dispatch_listener(signal=Signal.TREE_LOAD_STATE_UPDATED, receiver=self._on_load_state_updated)
        self.connect_dispatch_listener(signal=Signal.DISPLAY_TREE_CHANGED, receiver=self._on_display_tree_changed_rootpanel)
        logger.debug(f'[{self.con.tree_id}] RootPathPanel listeners connected')

    def shutdown(self):
        HasLifecycle.shutdown(self)

        # Disconnect GTK3 listeners:
        if self.entry_box_focus_eid:
            if self.path_entry:
                self.path_entry.disconnect(self.entry_box_focus_eid)
                self.entry_box_focus_eid = None

        if self.key_press_event_eid:
            if self.parent_win:
                self.parent_win.disconnect(self.key_press_event_eid)
            self.key_press_event_eid = None

        if self.con:
            self.con = None

        self.parent_win = None

    # TODO: this is temp code. Need a way to actually distinguish between different devices
    def get_default_local_device_uid(self) -> UID:
        device_uid = None
        for device in self.con.backend.get_device_list():
            if device.tree_type == TreeType.LOCAL_DISK:
                if device_uid:
                    raise RuntimeError('(FIXME): multiple devices of type LOCAL_DISK found! We cannot support this presently')
                device_uid = device.uid
        return device_uid

    # TODO: this is temp code. Need a way to actually distinguish between different devices
    def get_default_gdrive_device_uid(self) -> UID:
        device_uid = None
        for device in self.con.backend.get_device_list():
            if device.tree_type == TreeType.GDRIVE:
                if device_uid:
                    raise RuntimeError('(FIXME): multiple devices of type GDRIVE found! We cannot support this presently')
                device_uid = device.uid
        return device_uid

    def _redraw_root_display(self, new_tree=None):
        """Updates the UI to reflect the new root and tree type.
        Expected to be called from the UI thread.
        For markup options, see: https://developer.gnome.org/pygtk/stable/pango-markup-language.html
        """
        if not new_tree:
            new_tree: DisplayTree = self.con.get_tree()
        new_root = new_tree.get_root_spid()
        logger.debug(f'[{self.con.tree_id}] Redrawing root display for new_root={new_root}')
        if self.path_entry:
            if self.entry_box_focus_eid:
                self.path_entry.disconnect(self.entry_box_focus_eid)
                self.entry_box_focus_eid = None
            # Remove path_entry box (we were probably called by it to do cleanup in fact)
            self.path_box.remove(self.path_entry)
            self.path_entry = None
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

        if new_tree.is_needs_manual_load():
            # Note: good example of toolbar here:
            # https://github.com/kyleuckert/LaserTOF/blob/master/labTOF_main.backend/Contents/Resources/lib/python2.7/matplotlib/backends/backend_gtk3.py
            self.toolbar = Gtk.Toolbar()
            self.toolbar.set_style(Gtk.ToolbarStyle.ICONS)
            self.refresh_button = Gtk.ToolButton()
            self.refresh_button.set_icon_widget(self.refresh_icon)
            self.refresh_button.set_tooltip_text('Load meta for tree')
            self.refresh_button.connect('clicked', self._on_refresh_button_clicked)
            self.toolbar.insert(self.refresh_button, -1)
            self.path_box.pack_end(self.toolbar, expand=False, fill=True, padding=H_PAD)
            self.toolbar.show_all()

        if self.can_change_root:
            self.label_event_box.connect('button_press_event', self._on_label_clicked)

        if new_root.tree_type == TreeType.LOCAL_DISK:
            self.path_icon.set_from_file(self.parent_win.app.assets.get_path(IconId.BTN_LOCAL_DISK_LINUX))
        elif new_root.tree_type == TreeType.GDRIVE:
            self.path_icon.set_from_file(self.parent_win.app.assets.get_path(IconId.BTN_GDRIVE))
        elif new_root.tree_type == TreeType.MIXED:
            self.path_icon.set_from_file(self.parent_win.app.assets.get_path(IconId.BTN_LOCAL_DISK_LINUX))
        else:
            raise RuntimeError(f'Unrecognized tree type: {new_root.tree_type}')

        root_exists = new_tree.is_root_exists() and new_root.node_uid != NULL_UID
        if root_exists:
            pre = ''
            color = ''
            root_part_regular, root_part_bold = os.path.split(new_root.get_single_path())
            if len(self.alert_image_box.get_children()) > 0:
                self.alert_image_box.remove(self.alert_image)
        else:
            root_part_regular, root_part_bold = os.path.split(new_root.get_single_path())
            if not new_tree.is_root_exists() and new_tree.get_offending_path():
                root_part_regular = new_tree.get_offending_path()
                root_part_bold = file_util.strip_root(new_root.get_single_path(), new_tree.get_offending_path())
            if not self.alert_image_box.get_children():
                self.alert_image_box.pack_start(self.alert_image, expand=False, fill=False, padding=0)
            color = f"foreground='gray'"
            pre = f"<span foreground='red' size='medium'>Not found:  </span>"
            self.alert_image.show()

        if root_part_regular != '/':
            root_part_regular = root_part_regular + '/'
        self._set_label_markup(pre, color, root_part_regular, root_part_bold)

    def _on_root_text_entry_submitted(self, widget, tree_id):
        if self.path_entry and self.entry_box_focus_eid:
            self.path_entry.disconnect(self.entry_box_focus_eid)
            self.entry_box_focus_eid = None
        # Triggered when the user submits a root via the text path_entry box
        new_root_path: str = self.path_entry.get_text()
        logger.info(f'[{tree_id}] User entered root path: "{new_root_path}"')

        # Call into backend to update display tree. We'll get updated via the dispatcher
        # FIXME: add switch for device_uid. This is broken!
        device_uid = 99
        self.con.app.backend.create_display_tree_from_user_path(self.con.tree_id, new_root_path, device_uid)

    def _on_change_btn_clicked(self, widget):
        if self._ui_enabled:
            self.source_menu.popup_at_widget(widget, Gdk.Gravity.SOUTH_WEST, Gdk.Gravity.NORTH_WEST, None)
        return True

    def _on_label_clicked(self, widget, event):
        """User clicked on the root label: toggle it to show the text path_entry box"""
        if not self._ui_enabled:
            logger.debug('Ignoring button press - UI is disabled')
            return False

        if not event.button == 1:
            # Left click only
            return False

        # Remove alert image if present; only show path_entry box
        if len(self.alert_image_box.get_children()) > 0:
            self.alert_image_box.remove(self.alert_image)

        self.path_entry = Gtk.Entry()

        root_spid: SinglePathNodeIdentifier = self.con.get_tree().get_root_spid()
        path = root_spid.get_single_path()
        if root_spid.tree_type == TreeType.GDRIVE:
            path = GDRIVE_PATH_PREFIX + path
        self.path_entry.set_text(path)
        self.path_entry.connect('activate', self._on_root_text_entry_submitted, self.con.tree_id)
        self.path_box.remove(self.label_event_box)
        if self.toolbar:
            self.path_box.remove(self.toolbar)
            self.toolbar = None
        self.path_box.pack_start(self.path_entry, expand=True, fill=True, padding=0)

        def cancel_edit(widget, event):
            if self.path_entry and self.entry_box_focus_eid:
                self.path_entry.disconnect(self.entry_box_focus_eid)
                self.entry_box_focus_eid = None
            logger.debug(f'Focus lost! Cancelling root path path_entry box')
            self._redraw_root_display()

        self.entry_box_focus_eid = self.path_entry.connect('focus-out-event', cancel_edit)
        self.path_entry.show()
        self.path_entry.grab_focus()
        return False

    def _set_label_markup(self, pre, color, root_part_regular, root_part_bold):
        """Sets the content of the label only. Expected to be called from the UI thread"""
        root_part_regular = GLib.markup_escape_text(root_part_regular)
        root_part_bold = GLib.markup_escape_text(root_part_bold)
        self.label.set_markup(f"{pre}<span font_family='monospace' size='medium' {color}><i>{root_part_regular}\n<b>{root_part_bold}</b></i></span>")
        self.label.show()
        self.label_event_box.show()

    def _open_localdisk_root_chooser_dialog(self, menu_item, device_uid: UID):
        """Creates and displays a LocalRootDirChooserDialog.
        # the arguments are: title of the window, parent_window, action,
        # (buttons, response)"""
        logger.debug('Creating and displaying LocalRootDirChooserDialog')

        open_dialog = LocalRootDirChooserDialog(title="Pick a directory", parent_win=self.parent_win, tree_id=self.con.tree_id,
                                                current_dir=self.con.get_tree().get_root_spid().get_single_path(), device_uid=device_uid)

        # show the dialog
        open_dialog.show()

    def _open_gdrive_root_chooser_dialog(self, menu_item, device_uid: UID):
        spid = self.con.get_tree().get_root_spid()
        if spid.tree_type != TreeType.GDRIVE:
            spid = None
        logger.debug(f'[{self.con.tree_id}] Displaying GDrive root chooser dialog with current_selection={spid}')

        def open_dialog():
            try:
                # Preview ops in UI pop-up. Change tree_id so that listeners don't step on existing trees
                # Dialog will display itself when ready
                GDriveDirChooserDialog(self.parent_win, device_uid=device_uid, current_selection=spid, target_tree_id=self.con.tree_id)
            except Exception as err:
                self.parent_win.show_error_ui('GDriveDirChooserDialog failed due to unexpected error', repr(err))
                raise

        GLib.idle_add(open_dialog)

    def _build_source_menu(self):
        device_list: List[Device] = self.con.backend.get_device_list()

        source_menu = Gtk.Menu()

        for device in device_list:
            if device.tree_type == TreeType.LOCAL_DISK:
                label = f'Local disk: {device.friendly_name}'
                item_select_local = Gtk.ImageMenuItem(label=label)
                image = Gtk.Image()
                # TODO: these are too large
                image.set_from_file(self.parent_win.app.assets.get_path(IconId.BTN_LOCAL_DISK_LINUX))
                item_select_local.set_image(image)
                item_select_local.connect('activate', self._open_localdisk_root_chooser_dialog, device.uid)
                source_menu.append(item_select_local)

            elif device.tree_type == TreeType.GDRIVE:
                label = f'Google Drive: {device.friendly_name}'
                item_gdrive = Gtk.ImageMenuItem(label=label)
                image = Gtk.Image()
                image.set_from_file(self.parent_win.app.assets.get_path(IconId.BTN_GDRIVE))
                item_gdrive.set_image(image)
                source_menu.append(item_gdrive)
                item_gdrive.connect('activate', self._open_gdrive_root_chooser_dialog, device.uid)
        source_menu.show_all()
        return source_menu

    def _on_refresh_button_clicked(self, widget):
        logger.debug('The Refresh button was clicked!')

        def send_load_signal():
            self.con.app.backend.start_subtree_load(self.con.tree_id)

        GLib.idle_add(send_load_signal)

    # PyDispatch listeners
    # ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼

    def _on_enable_ui_toggled(self, sender, enable):
        # Callback for Signal.TOGGLE_UI_ENABLEMENT
        if not self.can_change_root:
            assert not self._ui_enabled
            return

        self._ui_enabled = enable
        # TODO: what if root text path_entry is showing?

        def change_button():
            self.change_btn.set_sensitive(enable)
        GLib.idle_add(change_button)

    def _on_load_state_updated(self, sender, tree_load_state: TreeLoadState, status_msg: str, dir_stats_dict_by_guid: Dict,
                               dir_stats_dict_by_uid: Dict):
        if sender != self.con.tree_id:
            return

        logger.debug(f'[{self.con.tree_id}] Got signal "{Signal.TREE_LOAD_STATE_UPDATED.name} with state={tree_load_state.name}"')
        if tree_load_state == TreeLoadState.LOAD_STARTED and self.con.get_tree().is_needs_manual_load():
            self.con.get_tree().set_needs_manual_load(False)
            # Hide Refresh button
            GLib.idle_add(self._redraw_root_display)

    def _on_display_tree_changed_rootpanel(self, sender: str, tree: Optional[DisplayTree]):
        """Callback for Signal.DISPLAY_TREE_CHANGED"""
        if sender != self.con.tree_id:
            return
        logger.debug(f'[{sender}] Received signal "{Signal.DISPLAY_TREE_CHANGED.name}" with new root: {tree.get_root_spid()}')

        # Send the new tree directly to _redraw_root_display(). Do not allow it to fall back to querying the controller for the tree,
        # because that would be a race condition:
        GLib.idle_add(self._redraw_root_display, tree)
