import logging

import gi

from gdrive.gdrive_data_store import GDriveDataStore
from ui.tree import tree_factory

gi.require_version("Gtk", "3.0")
from gi.repository import Gtk

from ui.base_dialog import BaseDialog

logger = logging.getLogger(__name__)


class GDriveDirSelectionDialog(Gtk.Dialog, BaseDialog):

    def __init__(self, parent, gdrive_meta, tree_id):
        Gtk.Dialog.__init__(self, "Select GDrive Root", parent, 0)
        BaseDialog.__init__(self, parent.config)
        tree_id = tree_id
        self.add_button(Gtk.STOCK_CANCEL, Gtk.ResponseType.CANCEL)
        self.add_button(Gtk.STOCK_OK, Gtk.ResponseType.OK)

        self.set_default_size(1000, 800)

        box = self.get_content_area()
        self.content_box = Gtk.Box(spacing=6, orientation=Gtk.Orientation.VERTICAL)
        box.add(self.content_box)

        label = Gtk.Label(label="Select the Google Drive folder to use as the root for comparison:")
        self.content_box.add(label)

        data_store = GDriveDataStore(tree_id='gdrive_dir_selection', config=parent.config, gdrive_meta=gdrive_meta, root_path='/')
        self.tree_controller = tree_factory.build_gdrive(parent_win=self, data_store=data_store)

        # actions.set_status(sender=data_store.tree_id, status_msg=self.fmeta_tree.get_summary())
        self.content_box.pack_start(self.tree_controller.content_box, True, True, 0)

        self.tree_controller.load()

        self.connect("response", self.on_response)
        self.show_all()

    def on_response(self, dialog, response_id):
        # destroy the widget (the dialog) when the function on_response() is called
        # (that is, when the button of the dialog has been clicked)

        try:
            if response_id == Gtk.ResponseType.OK:
                logger.debug("The OK button was clicked")
                self.tree_controller.display_store.get_selection()
                filename = dialog.get_filename() # FIXME
                logger.info(f'User selected dir: {filename}')
                # TODO: swap out data store if needed?
            #    actions.get_dispatcher().send(signal=actions.ROOT_PATH_UPDATED, sender=self.tree_id, new_root=filename)
                self.on_ok_clicked()
            elif response_id == Gtk.ResponseType.CANCEL:
                logger.debug("The Cancel button was clicked")
            elif response_id == Gtk.ResponseType.CLOSE:
                logger.debug("GDrive dialog was closed")
            elif response_id == Gtk.ResponseType.DELETE_EVENT:
                logger.debug("GDrive dialog was deleted")
            else:
                logger.debug(f'Unexpected response_id: {response_id}')
        except FileNotFoundError as err:
            self.show_error_ui('File not found: ' + err.filename)
            raise
        except Exception as err:
            logger.exception(err)
            detail = f'{repr(err)}]'
            self.show_error_ui('Diff task failed due to unexpected error', detail)
            raise
        finally:
            dialog.destroy()
