import logging
import os

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

        # TODO: make Label editable (maybe switch it to an Entry) on click
        self.label = Gtk.Label(label='')
        self.label.set_justify(Gtk.Justification.LEFT)
        self.label.set_xalign(0)
        self.label.set_line_wrap(True)

        self.icon = Gtk.Image()

        if editable:
            self.change_btn = Gtk.MenuButton()
            self.change_btn.set_image(image=self.icon)
            self.content_box.pack_start(self.change_btn, expand=False, fill=False, padding=5)
            self.change_btn.connect("clicked", self._on_change_btn_clicked, parent_win)
            self.source_menu = self.build_source_menu()
            self.change_btn.set_popup(self.source_menu)
        else:
            self.change_btn = None
            self.content_box.pack_start(self.icon, expand=False, fill=False, padding=0)

        self.alert_image = Gtk.Image()
        self.alert_image.set_from_file(ALERT_ICON_PATH)

        self.alert_image_box = Gtk.Box(spacing=6, orientation=Gtk.Orientation.HORIZONTAL)
        self.content_box.pack_start(self.alert_image_box, expand=False, fill=False, padding=0)
        self.content_box.pack_start(self.label, expand=True, fill=True, padding=0)

        actions.connect(actions.TOGGLE_UI_ENABLEMENT, self._on_enable_ui_toggled)
        actions.connect(actions.ROOT_PATH_UPDATED, self._on_root_path_updated, self.tree_id)

        # Need to call this to do the initial UI draw:
        self._on_root_path_updated(self.tree_id, current_root, tree_type)

    def _on_change_btn_clicked(self, widget, parent_win):
        self.source_menu.popup_at_widget(widget, Gdk.Gravity.SOUTH_WEST, Gdk.Gravity.NORTH_WEST, None)

    def _on_enable_ui_toggled(self, sender, enable):
        if not self.change_btn:
            # Not editable
            return

        def change_button():
            self.change_btn.set_sensitive(enable)
        GLib.idle_add(change_button)

    def _on_root_path_updated(self, sender, new_root, tree_type):
        if new_root is None:
            raise RuntimeError(f'Root path cannot be None! (tree_id={sender})')

        # For markup options, see: https://developer.gnome.org/pygtk/stable/pango-markup-language.html
        def update_root_label(root):
            self.current_root = root

            color = ''
            pre = ''
            if tree_type == OBJ_TYPE_LOCAL_DISK:
                self.icon.set_from_file(CHOOSE_ROOT_ICON_PATH)

                if os.path.exists(root):
                    if len(self.alert_image_box.get_children()) > 0:
                        self.alert_image_box.remove(self.alert_image)
                else:
                    if len(self.alert_image_box.get_children()) == 0:
                        self.alert_image_box.pack_start(self.alert_image, expand=False, fill=False, padding=0)
                    color = f"foreground='gray'"
                    pre = f"<span foreground='red' size='small'>Not found:  </span>"
                    self.alert_image.show()
            elif tree_type == OBJ_TYPE_GDRIVE:
                self.icon.set_from_file(GDRIVE_ICON_PATH)

                # root = self.parent_win.application.cache_manager.get_gdrive_path_for_id(root)
            else:
                raise RuntimeError(f'Unrecognized tree type: {tree_type}')

            self.label.set_markup(f"{pre}<span font_family='monospace' size='medium' {color}><i>{root}</i></span>")
        GLib.idle_add(update_root_label, new_root)

    def select_local_path(self, menu_item):
        # create a RootDirChooserDialog to open:
        # the arguments are: title of the window, parent_window, action,
        # (buttons, response)
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

    def select_gdrive_path(self, menu_item):
        actions.send_signal(signal=actions.SHOW_GDRIVE_ROOT_DIALOG, sender=self.tree_id)
        pass

    def build_source_menu(self):
        source_menu = Gtk.Menu()
        item_select_local = Gtk.MenuItem(label="Local filesystem subtree...")
        item_select_local.connect('activate', self.select_local_path)
        source_menu.append(item_select_local)
        item_gdrive = Gtk.MenuItem(label="Google Drive subtree...")
        source_menu.append(item_gdrive)
        item_gdrive.connect('activate', self.select_gdrive_path)
        source_menu.show_all()
        return source_menu

