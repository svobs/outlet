import logging

import gi
from pydispatch import dispatcher

from util.root_path_meta import RootPathMeta

gi.require_version("Gtk", "3.0")
from gi.repository import Gtk

from model.node_identifier import LocalNodeIdentifier
import ui.actions as actions

logger = logging.getLogger(__name__)


def _on_root_dir_selected(dialog, response_id, root_dir_panel):
    """Called after a directory is chosen via the LocalRootDirChooserDialog"""
    open_dialog = dialog
    # if response is "ACCEPT" (the button "Open" has been clicked)
    if response_id == Gtk.ResponseType.OK:
        filename = open_dialog.get_filename()
        logger.info(f'User selected dir: {filename}')
        uid = open_dialog.parent_win.app.cacheman.get_uid_for_path(filename)
        node_identifier = LocalNodeIdentifier(uid=uid, path_list=filename)
        new_root_meta = RootPathMeta(node_identifier, is_found=True)
        dispatcher.send(signal=actions.ROOT_PATH_UPDATED, sender=root_dir_panel.tree_id, new_root_meta=new_root_meta)
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


#    CLASS LocalRootDirChooserDialog
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼


class LocalRootDirChooserDialog(Gtk.FileChooserDialog):
    def __init__(self, title, parent_win, tree_id, current_dir):
        Gtk.FileChooserDialog.__init__(self, title=title, transient_for=parent_win, action=Gtk.FileChooserAction.SELECT_FOLDER)
        self.tree_id = tree_id
        self.parent_win = parent_win
        self.add_button(Gtk.STOCK_CANCEL, Gtk.ResponseType.CANCEL)
        self.add_button(Gtk.STOCK_OPEN, Gtk.ResponseType.OK)

        # not only local files can be selected in the file selector
        self.set_local_only(False)
        # dialog always on top of the parent window
        self.set_modal(True)
        # connect the dialog with the callback function open_response_cb()
        self.connect("response", _on_root_dir_selected, self)

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
