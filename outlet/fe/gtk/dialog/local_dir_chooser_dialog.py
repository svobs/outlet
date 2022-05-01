import logging

import gi

from model.uid import UID

gi.require_version("Gtk", "3.0")
from gi.repository import Gtk

logger = logging.getLogger(__name__)


class LocalRootDirChooserDialog(Gtk.FileChooserDialog):
    """
    ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
    CLASS LocalRootDirChooserDialog
    ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼
    """
    def __init__(self, title, parent_win, tree_id, device_uid, current_dir):
        Gtk.FileChooserDialog.__init__(self, title=title, transient_for=parent_win, action=Gtk.FileChooserAction.SELECT_FOLDER)
        self.tree_id = tree_id
        self.device_uid: UID = device_uid
        self.parent_win = parent_win
        self.add_button(Gtk.STOCK_CANCEL, Gtk.ResponseType.CANCEL)
        self.add_button(Gtk.STOCK_OPEN, Gtk.ResponseType.OK)

        # not only local files can be selected in the file selector
        self.set_local_only(False)
        # dialog always on top of the parent window
        self.set_modal(True)
        # connect the dialog with the callback function open_response_cb()
        self.connect("response", self._on_response)

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

    def _on_response(self, dialog, response_id):
        """Called after a directory is chosen via the LocalRootDirChooserDialog, or if user clicked a close or cancel button"""
        open_dialog = dialog
        # if response is "ACCEPT" (the button "Open" has been clicked)
        if response_id == Gtk.ResponseType.OK:
            full_path = open_dialog.get_filename()
            logger.info(f'User selected dir: {full_path}')

            open_dialog.parent_win.app.backend.create_display_tree_from_user_path(self.tree_id, user_path=full_path, device_uid=self.device_uid)
        # if response is "CANCEL" (the button "Cancel" has been clicked)
        elif response_id == Gtk.ResponseType.CANCEL:
            logger.debug("Cancelled: LocalRootDirChooserDialog")
        elif response_id == Gtk.ResponseType.CLOSE:
            logger.debug("Closed: LocalRootDirChooserDialog")
        elif response_id == Gtk.ResponseType.DELETE_EVENT:
            logger.debug("Deleted: LocalRootDirChooserDialog")
        else:
            logger.error(f'Unrecognized response: {response_id}')
        # destroy the FileChooserDialog
        dialog.destroy()

