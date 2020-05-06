import logging
import os
from pydispatch import dispatcher

import gi

from ui.dialog.local_dir_chooser_dialog import LocalRootDirChooserDialog

gi.require_version("Gtk", "3.0")
from gi.repository import Gtk, Gdk, GLib

from constants import GDRIVE_PATH_PREFIX, OBJ_TYPE_GDRIVE, OBJ_TYPE_LOCAL_DISK
from model.display_id import GDriveIdentifier, Identifier, LocalFsIdentifier
from ui.dialog.base_dialog import BaseDialog
import ui.actions as actions
from ui.assets import ALERT_ICON_PATH, CHOOSE_ROOT_ICON_PATH, GDRIVE_ICON_PATH


logger = logging.getLogger(__name__)


#    CLASS RootDirPanel
# ⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟


class RootDirPanel:
    def __init__(self, parent_win, tree_id, current_root: Identifier, editable):
        self.parent_win: BaseDialog = parent_win
        assert type(tree_id) == str
        self.tree_id = tree_id
        self.content_box = Gtk.Box(spacing=6, orientation=Gtk.Orientation.HORIZONTAL)
        self.current_root: Identifier = current_root
        self.editable = editable
        self.ui_enabled = editable
        """If editable, toggled via actions.TOGGLE_UI_ENABLEMENT. If not, always false"""

        self.icon = Gtk.Image()
        if self.editable:
            self.change_btn = Gtk.MenuButton()
            self.change_btn.set_image(image=self.icon)
            self.content_box.pack_start(self.change_btn, expand=False, fill=False, padding=5)
            self.change_btn.connect("clicked", self._on_change_btn_clicked, parent_win)
            self.source_menu = self._build_source_menu()
            self.change_btn.set_popup(self.source_menu)

            def on_key_pressed(widget, event):
                if self.ui_enabled and event.keyval == Gdk.KEY_Escape and self.entry:
                    # cancel
                    logger.debug(f'Escape pressed! Cancelling root path entry box')
                    self._update_root_label(self.current_root)
                    return True
                return False

            # Connect Escape key listener to parent window so it can be heard everywhere
            self.parent_win.connect('key-press-event', on_key_pressed)
        else:
            self.change_btn = None
            self.content_box.pack_start(self.icon, expand=False, fill=False, padding=0)

        self.path_box = Gtk.Box(spacing=0, orientation=Gtk.Orientation.HORIZONTAL)
        self.content_box.pack_start(self.path_box, expand=True, fill=True, padding=0)

        self.alert_image_box = Gtk.Box(spacing=6, orientation=Gtk.Orientation.HORIZONTAL)
        self.path_box.pack_start(self.alert_image_box, expand=False, fill=False, padding=0)

        self.alert_image = Gtk.Image()
        self.alert_image.set_from_file(ALERT_ICON_PATH)

        self.entry = None
        self.label_event_box = None
        self.label = None

        actions.connect(actions.TOGGLE_UI_ENABLEMENT, self._on_enable_ui_toggled)
        actions.connect(actions.ROOT_PATH_UPDATED, self._on_root_path_updated, self.tree_id)

        # Need to call this to do the initial UI draw:
        GLib.idle_add(self._update_root_label, current_root)

    def _on_root_text_entry_submitted(self, widget, tree_id):
        self.entry.disconnect(self.entry_box_focus_eid)
        # Triggered when the user submits a root via the text entry box
        new_root_path: str = self.entry.get_text()
        logger.info(f'User entered root path: "{new_root_path}" for tree_id={tree_id}')
        new_root = None
        try:
            identifiers = self.parent_win.application.cache_manager.get_all_for_path(new_root_path)
            assert len(identifiers) > 0, f'Got no identifiers (not even NULL) for path: {new_root_path}'
            new_root = identifiers[0]
        except FileNotFoundError as err:
            # currently only GDrive does this.
            # TODO: create GDrive-specific exception class which strips out the gdrive prefix and returns offending dir
            gdrive_path = new_root_path[len(GDRIVE_PATH_PREFIX):]
            new_root = GDriveIdentifier('NULL', gdrive_path)

        dispatcher.send(signal=actions.ROOT_PATH_UPDATED, sender=tree_id, new_root=new_root)

    def _on_change_btn_clicked(self, widget, parent_win):
        if self.ui_enabled:
            self.source_menu.popup_at_widget(widget, Gdk.Gravity.SOUTH_WEST, Gdk.Gravity.NORTH_WEST, None)
        return True

    def _on_label_clicked(self, widget, event):
        """User clicked on the root label: toggle it to show the text entry box"""
        if not self.ui_enabled:
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
        if self.current_root.tree_type == OBJ_TYPE_GDRIVE:
            path = GDRIVE_PATH_PREFIX + path
        self.entry.set_text(path)
        self.entry.connect('activate', self._on_root_text_entry_submitted, self.tree_id)
        self.path_box.remove(self.label_event_box)
        self.path_box.pack_start(self.entry, expand=True, fill=True, padding=0)

        def cancel_edit(widget, event):
            if self.entry:
                self.entry.disconnect(self.entry_box_focus_eid)
            logger.debug(f'Focus lost! Cancelling root path entry box')
            self._update_root_label(self.current_root)

        self.entry_box_focus_eid = self.entry.connect('focus-out-event', cancel_edit)
        self.entry.show()
        self.entry.grab_focus()
        return False

    def _on_enable_ui_toggled(self, sender, enable):
        # Callback for actions.TOGGLE_UI_ENABLEMENT
        if not self.editable:
            self.ui_enabled = False
            return

        self.ui_enabled = enable
        # TODO: what if root text entry is showing?

        def change_button():
            self.change_btn.set_sensitive(enable)
        GLib.idle_add(change_button)

    def _update_root_label(self, new_root: Identifier):
        """Updates the UI to reflect the new root and tree type.
        Expected to be called from the UI thread.
        """

        self.label = Gtk.Label(label='')
        self.label.set_justify(Gtk.Justification.LEFT)
        self.label.set_xalign(0)
        self.label.set_line_wrap(True)
        if self.entry:
            # Remove entry box (we were probably called by it to do cleanup in fact)
            self.path_box.remove(self.entry)
            self.entry = None
        elif self.label_event_box:
            # Remove label even if it's found. Simpler just to redraw the whole thing
            self.path_box.remove(self.label_event_box)
            self.label_event_box = None
        self.label_event_box = Gtk.EventBox()
        self.path_box.pack_start(self.label_event_box, expand=True, fill=True, padding=0)
        self.label_event_box.add(self.label)
        self.label_event_box.show()

        if self.editable:
            self.label_event_box.connect('button_press_event', self._on_label_clicked)

        if new_root.tree_type == OBJ_TYPE_LOCAL_DISK:
            self.icon.set_from_file(CHOOSE_ROOT_ICON_PATH)
            root_exists = os.path.exists(new_root.full_path)
        elif new_root.tree_type == OBJ_TYPE_GDRIVE:
            self.icon.set_from_file(GDRIVE_ICON_PATH)
            root_exists = new_root.uid != 'NULL'
        else:
            raise RuntimeError(f'Unrecognized tree type: {new_root.tree_type}')

        if root_exists:
            pre = ''
            color = ''
            root_part_regular, root_part_bold = os.path.split(new_root.full_path)
            root_part_regular = root_part_regular + '/'
            if len(self.alert_image_box.get_children()) > 0:
                self.alert_image_box.remove(self.alert_image)
        else:
            # TODO: determine offensive parent
            root_part_regular, root_part_bold = os.path.split(new_root.full_path)
            root_part_regular = root_part_regular + '/'
            if len(self.alert_image_box.get_children()) == 0:
                self.alert_image_box.pack_start(self.alert_image, expand=False, fill=False, padding=0)
            color = f"foreground='gray'"
            pre = f"<span foreground='red' size='small'>Not found:  </span>"
            self.alert_image.show()

        self._set_label_markup(pre, color, root_part_regular, root_part_bold)

    def _set_label_markup(self, pre, color, root_part_regular, root_part_bold):
        """Sets the content of the label only. Expected to be called from the UI thread"""
        self.label.set_markup(f"{pre}<span font_family='monospace' size='medium' {color}><i>{root_part_regular}\n<b>{root_part_bold}</b></i></span>")
        self.label.show()
        self.label_event_box.show()

    def _on_root_path_updated(self, sender, new_root: Identifier):
        """Callback for actions.ROOT_PATH_UPDATED"""
        logger.debug(f'Received a new root: type={new_root.tree_type} path="{new_root.full_path}"')
        if not new_root or not new_root.full_path:
            raise RuntimeError(f'Root path cannot be empty! (tree_id={sender})')
        self.current_root = new_root

        # For markup options, see: https://developer.gnome.org/pygtk/stable/pango-markup-language.html
        GLib.idle_add(self._update_root_label, new_root)

    def _open_localfs_root_chooser_dialog(self, menu_item):
        """Creates and displays a LocalRootDirChooserDialog.
        # the arguments are: title of the window, parent_window, action,
        # (buttons, response)"""
        logger.debug('Creating and displaying LocalRootDirChooserDialog')
        open_dialog = LocalRootDirChooserDialog(title="Pick a directory", parent_win=self.parent_win, tree_id=self.tree_id, current_dir=self.current_root.full_path)

        # show the dialog
        open_dialog.show()

    def _open_gdrive_root_chooser_dialog(self, menu_item):
        actions.send_signal(signal=actions.SHOW_GDRIVE_ROOT_DIALOG, sender=self.tree_id)

    def _build_source_menu(self):
        source_menu = Gtk.Menu()
        item_select_local = Gtk.MenuItem(label="Local filesystem subtree...")
        item_select_local.connect('activate', self._open_localfs_root_chooser_dialog)
        source_menu.append(item_select_local)
        item_gdrive = Gtk.MenuItem(label="Google Drive subtree...")
        source_menu.append(item_gdrive)
        item_gdrive.connect('activate', self._open_gdrive_root_chooser_dialog)
        source_menu.show_all()
        return source_menu


