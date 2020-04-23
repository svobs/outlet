import logging
import os

import file_util
import ui.actions as actions

import gi
gi.require_version("Gtk", "3.0")
from gi.repository import Gtk, GLib

# TODO: constants file
ALERT_ICON_PATH = file_util.get_resource_path("resources/dialog-error-icon-24px.png")
CHOOSE_ROOT_ICON_PATH = file_util.get_resource_path("resources/Folder-tree-flat-40px.png")


logger = logging.getLogger(__name__)


def _on_root_dir_selected(dialog, response_id, root_dir_panel):
    """Called after a directory is chosen via the RootDirChooserDialog"""
    open_dialog = dialog
    # if response is "ACCEPT" (the button "Open" has been clicked)
    if response_id == Gtk.ResponseType.OK:
        filename = open_dialog.get_filename()
        logger.info(f'User selected dir: {filename}')
        actions.get_dispatcher().send(signal=actions.ROOT_PATH_UPDATED, sender=root_dir_panel.data_store.tree_id, new_root=filename)
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
    def __init__(self, title, parent, current_dir):
        Gtk.FileChooserDialog.__init__(self, title=title, parent=parent, action=Gtk.FileChooserAction.SELECT_FOLDER)
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
    def __init__(self, parent_win, data_store, editable):
        self.content_box = Gtk.Box(spacing=6, orientation=Gtk.Orientation.HORIZONTAL)

        self.data_store = data_store

        # TODO: make Label editable (maybe switch it to an Entry) on click
        self.label = Gtk.Label(label='')
        self.label.set_justify(Gtk.Justification.LEFT)
        self.label.set_xalign(0)
        self.label.set_line_wrap(True)

        if editable:
            self.change_btn = Gtk.Button()
            icon = Gtk.Image()
            icon.set_from_file(CHOOSE_ROOT_ICON_PATH)
            self.change_btn.set_image(image=icon)
            self.content_box.pack_start(self.change_btn, expand=False, fill=False, padding=5)
            self.change_btn.connect("clicked", self._on_change_btn_clicked, parent_win)

        self.alert_image = Gtk.Image()
        self.alert_image.set_from_file(ALERT_ICON_PATH)

        self.alert_image_box = Gtk.Box(spacing=6, orientation=Gtk.Orientation.HORIZONTAL)
        self.content_box.pack_start(self.alert_image_box, expand=False, fill=False, padding=0)
        self.content_box.pack_start(self.label, expand=True, fill=True, padding=0)

        actions.connect(actions.TOGGLE_UI_ENABLEMENT, self._on_enable_ui_toggled)
        actions.connect(actions.ROOT_PATH_UPDATED, self._on_root_path_updated, self.data_store.tree_id)

        # Need to call this to do the initial UI draw:
        self._on_root_path_updated(None, self.data_store.get_root_path())

    def _on_change_btn_clicked(self, widget, parent_win):
        # create a RootDirChooserDialog to open:
        # the arguments are: title of the window, parent_window, action,
        # (buttons, response)
        open_dialog = RootDirChooserDialog(title="Pick a directory", parent=parent_win, current_dir=self.data_store.get_root_path())

        # not only local files can be selected in the file selector
        open_dialog.set_local_only(False)
        # dialog always on top of the parent window
        open_dialog.set_modal(True)
        # connect the dialog with the callback function open_response_cb()
        open_dialog.connect("response", _on_root_dir_selected, self)
        # show the dialog
        open_dialog.show()

    def _on_enable_ui_toggled(self, sender, enable):
        def change_button():
            self.change_btn.set_sensitive(enable)
        GLib.idle_add(change_button)

    def _on_root_path_updated(self, sender, new_root):
        if new_root is None:
            raise RuntimeError(f'Root path cannot be None! (tree_id={self.data_store.tree_id})')

        # For markup options, see: https://developer.gnome.org/pygtk/stable/pango-markup-language.html
        def update_root_label():
            if os.path.exists(new_root):
                self.alert_image_box.remove(self.alert_image)
                color = ''
                pre = ''
            else:
                self.alert_image_box.pack_start(self.alert_image, expand=False, fill=False, padding=0)
                color = f"foreground='gray'"
                pre = f"<span foreground='red' size='small'>Not found:  </span>"
            self.label.set_markup(f"{pre}<span font_family='monospace' size='medium' {color}><i>{new_root}</i></span>")
        GLib.idle_add(update_root_label)

