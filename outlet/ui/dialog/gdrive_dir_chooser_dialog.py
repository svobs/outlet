import logging
from typing import List

from pydispatch import dispatcher
from pydispatch.errors import DispatcherKeyError

from model.display_tree.display_tree import DisplayTree
from model.node.node import Node, SPIDNodePair
from model.node_identifier import SinglePathNodeIdentifier
from model.node_identifier_factory import NodeIdentifierFactory
from ui import actions
from ui.tree import tree_factory

import gi
gi.require_version("Gtk", "3.0")
from gi.repository import Gtk

from ui.dialog.base_dialog import BaseDialog

logger = logging.getLogger(__name__)

GDRIVE_DIR_CHOOSER_DIALOG_DEFAULT_WIDTH = 1000
GDRIVE_DIR_CHOOSER_DIALOG_DEFAULT_HEIGHT = 800


class GDriveDirChooserDialog(Gtk.Dialog, BaseDialog):

    def __init__(self, parent_win: BaseDialog, tree: DisplayTree, current_selection: SinglePathNodeIdentifier, target_tree_id: str):
        Gtk.Dialog.__init__(self, title="Select GDrive Root", transient_for=parent_win, flags=0)
        BaseDialog.__init__(self, app=parent_win.app)

        self.target_tree_id = actions.ID_GDRIVE_DIR_SELECT
        """Note: this is the ID of the tree for which this dialog is ultimately selecting for, not this dialog's tree (see tree_controller below)"""

        self.add_button(Gtk.STOCK_CANCEL, Gtk.ResponseType.CANCEL)
        self.ok_button = self.add_button(Gtk.STOCK_OK, Gtk.ResponseType.OK)

        self.set_default_size(GDRIVE_DIR_CHOOSER_DIALOG_DEFAULT_WIDTH, GDRIVE_DIR_CHOOSER_DIALOG_DEFAULT_HEIGHT)

        box = self.get_content_area()
        self.content_box = Gtk.Box(spacing=6, orientation=Gtk.Orientation.VERTICAL)
        box.add(self.content_box)

        label = Gtk.Label(label="Select the Google Drive folder to use as the root for comparison:")
        self.content_box.add(label)

        # Prevent dialog from stepping on existing trees by giving it its own ID:
        self.con = tree_factory.build_gdrive_root_chooser(parent_win=self, tree=tree)

        self.content_box.pack_start(self.con.content_box, True, True, 0)

        assert isinstance(current_selection, SinglePathNodeIdentifier), \
            f'Expected instance of SinglePathNodeIdentifier but got: {type(current_selection)}'
        self._initial_selection_nid: SinglePathNodeIdentifier = current_selection

        dispatcher.connect(signal=actions.TREE_SELECTION_CHANGED, sender=actions.ID_GDRIVE_DIR_SELECT, receiver=self._on_selected_changed)

        logger.debug(f'[{actions.ID_GDRIVE_DIR_SELECT}] Connecting listener to signal: {actions.LOAD_UI_TREE_DONE}')
        dispatcher.connect(signal=actions.LOAD_UI_TREE_DONE, sender=actions.ID_GDRIVE_DIR_SELECT, receiver=self._on_load_complete)

        self.connect("response", self.on_response)
        self.show_all()

    def destroy(self):
        """Overrides Gtk.Dialog.destroy()"""
        logger.debug(f'GDriveDirChooserDialog.destroy() called')
        # Clean up:
        try:
            dispatcher.disconnect(signal=actions.LOAD_UI_TREE_DONE, sender=actions.ID_GDRIVE_DIR_SELECT, receiver=self._on_load_complete)
        except DispatcherKeyError:
            pass
        try:
            dispatcher.disconnect(signal=actions.TREE_SELECTION_CHANGED, sender=actions.ID_GDRIVE_DIR_SELECT, receiver=self._on_selected_changed)
        except DispatcherKeyError:
            pass
        self.con.shutdown()
        self.con = None

        # call super method
        Gtk.Dialog.destroy(self)

    def _on_selected_changed(self, sender, node_list: List[Node]):
        # In weird GTK-speak, "sensitive" means enabled
        self.ok_button.set_sensitive(node_list and node_list[0].is_dir())

    def _on_load_complete(self, sender):
        logger.debug(f'[{actions.ID_GDRIVE_DIR_SELECT}] Load complete! Sending signal: {actions.EXPAND_AND_SELECT_NODE}')
        dispatcher.send(actions.EXPAND_AND_SELECT_NODE, sender=actions.ID_GDRIVE_DIR_SELECT, nid=self._initial_selection_nid)

    def on_ok_clicked(self, spid: SinglePathNodeIdentifier):
        logger.info(f'[{self.target_tree_id}] User selected dir "{spid}"')
        # This will send a signal to everyone who needs to know:
        self.app.backend.create_display_tree_from_spid(self.target_tree_id, spid)

    def on_response(self, dialog, response_id):
        # destroy the widget (the dialog) when the function on_response() is called
        # (that is, when the button of the dialog has been clicked)

        try:
            if response_id == Gtk.ResponseType.OK:
                logger.debug("The OK button was clicked")
                sn: SPIDNodePair = self.con.display_store.get_single_selection_sn()
                if not sn:
                    spid = NodeIdentifierFactory.get_gdrive_root_constant_single_path_identifier()
                else:
                    if sn.node.is_dir():
                        spid = sn.spid
                    else:
                        self.show_error_ui('Not a directory: ' + sn.node.name)
                        return
                if spid:
                    self.on_ok_clicked(spid)
            elif response_id == Gtk.ResponseType.CANCEL:
                logger.debug("The Cancel button was clicked")
            elif response_id == Gtk.ResponseType.CLOSE:
                logger.debug("GDrive dialog was closed")
            elif response_id == Gtk.ResponseType.DELETE_EVENT:
                logger.debug("GDrive dialog was deleted")
            else:
                logger.debug(f'Unexpected response_id: {response_id}')

            dialog.destroy()
        except FileNotFoundError as err:
            self.show_error_ui('File not found: ' + err.filename)
            raise
        except Exception as err:
            logger.exception(err)
            detail = f'{repr(err)}]'
            self.show_error_ui('Unexpected error', detail)
            raise
