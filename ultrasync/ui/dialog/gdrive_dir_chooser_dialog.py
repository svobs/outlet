import logging

import gi
from pydispatch import dispatcher

from model.display_id import Identifier
from model.display_node import DisplayNode
from ui import actions
from ui.tree import tree_factory

gi.require_version("Gtk", "3.0")
from gi.repository import Gtk

from ui.dialog.base_dialog import BaseDialog

logger = logging.getLogger(__name__)


class GDriveDirChooserDialog(Gtk.Dialog, BaseDialog):

    def __init__(self, parent_win: BaseDialog, meta_store, tree_id: str):
        Gtk.Dialog.__init__(self, "Select GDrive Root", parent_win, 0)
        BaseDialog.__init__(self, application=parent_win.application)
        self.tree_id = tree_id
        self.add_button(Gtk.STOCK_CANCEL, Gtk.ResponseType.CANCEL)
        self.add_button(Gtk.STOCK_OK, Gtk.ResponseType.OK)

        self.set_default_size(1000, 800)

        box = self.get_content_area()
        self.content_box = Gtk.Box(spacing=6, orientation=Gtk.Orientation.VERTICAL)
        box.add(self.content_box)

        label = Gtk.Label(label="Select the Google Drive folder to use as the root for comparison:")
        self.content_box.add(label)

        # Prevent dialog from stepping on existing trees by giving it its own ID:
        meta_store.tree_id = actions.ID_GDRIVE_DIR_SELECT
        self.tree_controller = tree_factory.build_gdrive(parent_win=self, meta_store=meta_store)

        self.content_box.pack_start(self.tree_controller.content_box, True, True, 0)

        self.tree_controller.load()

        self.connect("response", self.on_response)
        self.show_all()

    def on_ok_clicked(self, identifier: Identifier):
        # TODO: disallow selection of files
        logger.info(f'User selected dir id="{identifier.uid}"')
        dispatcher.send(signal=actions.ROOT_PATH_UPDATED, sender=self.tree_id, new_root=identifier)

    def on_response(self, dialog, response_id):
        # destroy the widget (the dialog) when the function on_response() is called
        # (that is, when the button of the dialog has been clicked)

        try:
            if response_id == Gtk.ResponseType.OK:
                logger.debug("The OK button was clicked")
                item: DisplayNode = self.tree_controller.get_single_selection()
                assert item.identifier
                # Ensure item has a full_path (not filled in by default in GDriveWholeTree)
                item.identifier.full_path = self.tree_controller.meta_store.get_model().get_full_path_for_item(item)
                self.on_ok_clicked(item.identifier)
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
