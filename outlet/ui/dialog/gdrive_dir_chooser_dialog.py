import logging

import gi
from pydispatch import dispatcher

from constants import TREE_TYPE_GDRIVE
from model.node_identifier import NodeIdentifier
from model.node.display_node import DisplayNode
from model.node_identifier_factory import NodeIdentifierFactory
from ui import actions
from ui.tree import tree_factory

gi.require_version("Gtk", "3.0")
from gi.repository import Gtk

from ui.dialog.base_dialog import BaseDialog

logger = logging.getLogger(__name__)


class GDriveDirChooserDialog(Gtk.Dialog, BaseDialog):

    def __init__(self, parent_win: BaseDialog, tree, tree_id: str, current_selection: NodeIdentifier):
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
        self.tree_controller = tree_factory.build_gdrive_root_chooser(parent_win=self, tree_id=actions.ID_GDRIVE_DIR_SELECT, tree=tree)

        self.content_box.pack_start(self.tree_controller.content_box, True, True, 0)

        self.tree_controller.load()
        if current_selection.tree_type == TREE_TYPE_GDRIVE:
            # TODO: turn this into an action
            self.tree_controller.display_mutator.expand_and_select_node(current_selection)

        self.connect("response", self.on_response)
        self.show_all()

    def on_ok_clicked(self, node_identifier: NodeIdentifier):
        # TODO: disallow selection of files
        logger.info(f'User selected dir "{node_identifier}"')
        dispatcher.send(signal=actions.ROOT_PATH_UPDATED, sender=self.tree_id, new_root=node_identifier)

    def on_response(self, dialog, response_id):
        # destroy the widget (the dialog) when the function on_response() is called
        # (that is, when the button of the dialog has been clicked)

        try:
            if response_id == Gtk.ResponseType.OK:
                logger.debug("The OK button was clicked")
                item: DisplayNode = self.tree_controller.get_single_selection()
                if not item:
                    self.on_ok_clicked(NodeIdentifierFactory.get_gdrive_root_constant_identifier())
                else:
                    self.on_ok_clicked(item.node_identifier)
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
            # Clean up:
            self.tree_controller.destroy()
            self.tree_controller = None
            dialog.destroy()