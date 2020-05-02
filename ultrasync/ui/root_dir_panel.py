import asyncio
import logging
import os
import threading

import gi
from pydispatch import dispatcher

from constants import OBJ_TYPE_GDRIVE, OBJ_TYPE_LOCAL_DISK
from ui.base_dialog import BaseDialog

gi.require_version("Gtk", "3.0")
from gi.repository import Gtk, Gdk, GLib

import ui.actions as actions
from ui.assets import ALERT_ICON_PATH, CHOOSE_ROOT_ICON_PATH, GDRIVE_ICON_PATH


logger = logging.getLogger(__name__)


def _on_root_dir_selected(dialog, response_id, root_dir_panel):
    """Called after a directory is chosen via the RootDirChooserDialog"""
    open_dialog = dialog
    # if response is "ACCEPT" (the button "Open" has been clicked)
    if response_id == Gtk.ResponseType.OK:
        filename = open_dialog.get_filename()
        logger.info(f'User selected dir: {filename}')
        dispatcher.send(signal=actions.ROOT_PATH_UPDATED, sender=root_dir_panel.tree_id, new_root=filename, tree_type=OBJ_TYPE_LOCAL_DISK)
    # if response is "CANCEL" (the button "Cancel" has been clicked)
    elif response_id == Gtk.ResponseType.CANCEL:
        logger.debug("Cancelled: RootDirChooserDialog")
    elif response_id == Gtk.ResponseType.CLOSE:
        logger.debug("Closed: RootDirChooserDialog")
    elif response_id == Gtk.ResponseType.DELETE_EVENT:
        logger.debug("Deleted: RootDirChooserDialog")
    else:
        logger.error(f'Unrecognized response: {response_id}')
    # destroy the FileChooserDialog
    dialog.destroy()


class RootDirChooserDialog(Gtk.FileChooserDialog):
    def __init__(self, title, parent_win, tree_id, current_dir):
        Gtk.FileChooserDialog.__init__(self, title=title, parent=parent_win, action=Gtk.FileChooserAction.SELECT_FOLDER)
        self.tree_id = tree_id
        self.add_button(Gtk.STOCK_CANCEL, Gtk.ResponseType.CANCEL)
        self.add_button(Gtk.STOCK_OPEN, Gtk.ResponseType.OK)

        if current_dir is not None:
            self.set_current_folder(current_dir)

    def __call__(self):
        resp = self.run()
        self.hide()

        file_name = self.get_filename()

        d = self.get_current_folder()
        if d:
            self.set_current_folder(d)

        if resp == Gtk.ResponseType.OK:
            return file_name
        else:
            return None


class RootDirPanel:
    def __init__(self, parent_win, tree_id, current_root, tree_type, editable):
        self.parent_win: BaseDialog = parent_win
        assert type(tree_id) == str
        self.tree_id = tree_id
        self.content_box = Gtk.Box(spacing=6, orientation=Gtk.Orientation.HORIZONTAL)
        self.current_root = current_root
        self.current_tree_type = tree_type
        self.ui_enabled = True

        self.icon = Gtk.Image()
        if editable:
            self.change_btn = Gtk.MenuButton()
            self.change_btn.set_image(image=self.icon)
            self.content_box.pack_start(self.change_btn, expand=False, fill=False, padding=5)
            self.change_btn.connect("clicked", self._on_change_btn_clicked, parent_win)
            self.source_menu = self._build_source_menu()
            self.change_btn.set_popup(self.source_menu)
        else:
            self.change_btn = None
            self.content_box.pack_start(self.icon, expand=False, fill=False, padding=0)

        self.alert_image_box = Gtk.Box(spacing=6, orientation=Gtk.Orientation.HORIZONTAL)
        self.content_box.pack_start(self.alert_image_box, expand=False, fill=False, padding=0)

        self.alert_image = Gtk.Image()
        self.alert_image.set_from_file(ALERT_ICON_PATH)

        self.event_box = Gtk.EventBox()
        self.content_box.pack_start(self.event_box, expand=True, fill=True, padding=0)

        self.entry = None
        self.label = self._add_label()

        self.event_box.connect('button_press_event', self._on_label_clicked)

        actions.connect(actions.TOGGLE_UI_ENABLEMENT, self._on_enable_ui_toggled)
        actions.connect(actions.ROOT_PATH_UPDATED, self._on_root_path_updated, self.tree_id)

        # Need to call this to do the initial UI draw:
        self._on_root_path_updated(self.tree_id, current_root, tree_type)

    def _add_label(self):
        self.label = Gtk.Label(label='')
        self.label.set_justify(Gtk.Justification.LEFT)
        self.label.set_xalign(0)
        self.label.set_line_wrap(True)
        if self.entry:
            self.event_box.remove(self.entry)
        self.event_box.add(self.label)
        return self.label

    def _on_change_btn_clicked(self, widget, parent_win):
        self.source_menu.popup_at_widget(widget, Gdk.Gravity.SOUTH_WEST, Gdk.Gravity.NORTH_WEST, None)

    def _on_label_clicked(self, widget, event):
        if not self.ui_enabled:
            logger.debug('Ignoring button press - UI is disabled')
            return True

        if not event.button == 1:  # left click
            return

        def on_root_entry_submitted(widget, tree_id):
            new_root = self.entry.get_text()
            # TODO: parse tree type
            logger.info(f'User entered root path: {new_root}')
            self._add_label()
            dispatcher.send(signal=actions.ROOT_PATH_UPDATED, sender=tree_id, new_root=new_root, tree_type=self.current_tree_type)

        self.entry = Gtk.Entry()
        self.entry.set_text(self.current_root)

        self.entry.connect("activate", on_root_entry_submitted, self.tree_id)
        self.event_box.remove(self.label)
        self.event_box.add(self.entry)
        # self.content_box.pack_start(self.entry, expand=True, fill=True, padding=0)
        self.entry.show()

    def _on_enable_ui_toggled(self, sender, enable):
        # Callback for actions.TOGGLE_UI_ENABLEMENT
        self.ui_enabled = enable
        # TODO: what if root entry is showing?

        if not self.change_btn:
            # Not editable
            return

        def change_button():
            self.change_btn.set_sensitive(enable)
        GLib.idle_add(change_button)

    def _set_gdrive_path(self, new_root):
        self.parent_win.application.cache_manager.all_caches_loaded.wait()

        try:
            root_part_regular = self.parent_win.application.cache_manager.get_gdrive_path_for_id(new_root)
            GLib.idle_add(self._set_label_markup, '', '', root_part_regular, '')
        except Exception:
            GLib.idle_add(self._set_label_markup, '', "foreground='red'", '', 'ERROR')
            raise

    def _update_root_label(self, new_root, tree_type):
        """Updates the UI to reflect the new root and tree type.
        Expected to be called from the UI thread.
        """
        if tree_type == OBJ_TYPE_LOCAL_DISK:
            self.icon.set_from_file(CHOOSE_ROOT_ICON_PATH)

            if os.path.exists(new_root):
                pre = ''
                color = ''
                root_part_regular, root_part_bold = os.path.split(new_root)
                root_part_regular = root_part_regular + '/'
                if len(self.alert_image_box.get_children()) > 0:
                    self.alert_image_box.remove(self.alert_image)
            else:
                # TODO: determine offensive parent
                root_part_regular, root_part_bold = os.path.split(new_root)
                root_part_regular = root_part_regular + '/'
                if len(self.alert_image_box.get_children()) == 0:
                    self.alert_image_box.pack_start(self.alert_image, expand=False, fill=False, padding=0)
                color = f"foreground='gray'"
                pre = f"<span foreground='red' size='small'>Not found:  </span>"
                self.alert_image.show()

            self._set_label_markup(pre, color, root_part_regular, root_part_bold)

        elif tree_type == OBJ_TYPE_GDRIVE:
            self.icon.set_from_file(GDRIVE_ICON_PATH)

            thread = threading.Thread(target=self._set_gdrive_path, args=(new_root,))
            self._set_label_markup('', "foreground='gray'", 'Loading...', '')
            thread.start()

        else:
            raise RuntimeError(f'Unrecognized tree type: {tree_type}')

    def _set_label_markup(self, pre, color, root_part_regular, root_part_bold):
        """Sets the content of the label only. Expected to be called from the UI thread"""
        self.label.set_markup(f"{pre}<span font_family='monospace' size='medium' {color}><i>{root_part_regular}\n<b>{root_part_bold}</b></i></span>")
        self.label.show()

    def _on_root_path_updated(self, sender, new_root, tree_type):
        """Callback for actions.ROOT_PATH_UPDATED"""
        logger.debug(f'Received a new root: type={tree_type} path="{new_root}"')
        if not new_root:
            raise RuntimeError(f'Root path cannot be empty! (tree_id={sender})')
        self.current_root = new_root
        self.current_tree_type = tree_type

        # For markup options, see: https://developer.gnome.org/pygtk/stable/pango-markup-language.html
        GLib.idle_add(self._update_root_label, new_root, tree_type)

    def _open_localfs_root_chooser_dialog(self, menu_item):
        """Creates and displays a RootDirChooserDialog.
        # the arguments are: title of the window, parent_window, action,
        # (buttons, response)"""
        logger.debug('Creating and displaying RootDirChooserDialog')
        open_dialog = RootDirChooserDialog(title="Pick a directory", parent_win=self.parent_win, tree_id=self.tree_id, current_dir=self.current_root)

        # not only local files can be selected in the file selector
        open_dialog.set_local_only(False)
        # dialog always on top of the parent window
        open_dialog.set_modal(True)
        # connect the dialog with the callback function open_response_cb()
        open_dialog.connect("response", _on_root_dir_selected, self)
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


