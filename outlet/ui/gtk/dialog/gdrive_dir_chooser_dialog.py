import logging
from typing import List

from pydispatch import dispatcher

from model.display_tree.display_tree import DisplayTree
from model.node.node import Node, SPIDNodePair
from model.node_identifier import SinglePathNodeIdentifier
from model.node_identifier_factory import NodeIdentifierFactory
from signal_constants import ID_GDRIVE_DIR_SELECT, Signal
from ui.gtk.tree import tree_factory

import gi
gi.require_version("Gtk", "3.0")
from gi.repository import Gtk, GLib

from ui.gtk.dialog.base_dialog import BaseDialog

logger = logging.getLogger(__name__)

GDRIVE_DIR_CHOOSER_DIALOG_DEFAULT_WIDTH = 1000
GDRIVE_DIR_CHOOSER_DIALOG_DEFAULT_HEIGHT = 800


class GDriveDirChooserDialog(Gtk.Dialog, BaseDialog):
    """
    ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
    CLASS GDriveDirChooserDialog
    ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼
    """

    def __init__(self, parent_win: BaseDialog, current_selection: SinglePathNodeIdentifier, target_tree_id: str):
        Gtk.Dialog.__init__(self, title="Select GDrive Root", transient_for=parent_win, flags=0)
        BaseDialog.__init__(self, app=parent_win.app)

        self.tree_id = ID_GDRIVE_DIR_SELECT
        self.target_tree_id = target_tree_id
        """Note: this is the ID of the tree for which this dialog is ultimately selecting for, not this dialog's tree (see tree_controller below)"""

        self.add_button(Gtk.STOCK_CANCEL, Gtk.ResponseType.CANCEL)
        self.ok_button = self.add_button(Gtk.STOCK_OK, Gtk.ResponseType.OK)

        self.set_default_size(GDRIVE_DIR_CHOOSER_DIALOG_DEFAULT_WIDTH, GDRIVE_DIR_CHOOSER_DIALOG_DEFAULT_HEIGHT)

        box = self.get_content_area()
        self.content_box = Gtk.Box(spacing=6, orientation=Gtk.Orientation.VERTICAL)
        box.add(self.content_box)

        label = Gtk.Label(label="Select the Google Drive folder to use as the root for comparison:")
        self.content_box.add(label)

        self.connect("response", self.on_response)

        # Start listening BEFORE calling backend to do the load
        dispatcher.connect(signal=Signal.TREE_SELECTION_CHANGED, receiver=self._on_selected_changed)
        dispatcher.connect(signal=Signal.LOAD_SUBTREE_DONE, receiver=self._on_backend_ready)
        dispatcher.connect(signal=Signal.POPULATE_UI_TREE_DONE, receiver=self._on_populate_complete)

        # This will start the load process and send us a Signal.LOAD_SUBTREE_DONE when done:
        tree: DisplayTree = parent_win.app.backend.create_display_tree_for_gdrive_select()
        assert tree, 'create_display_tree_for_gdrive_select() returned None for tree!'
        # Prevent dialog from stepping on existing trees by giving it its own ID:
        self.con = tree_factory.build_gdrive_root_chooser(parent_win=self, tree=tree)

        self.content_box.pack_start(self.con.content_box, True, True, 0)

        assert isinstance(current_selection, SinglePathNodeIdentifier), \
            f'Expected instance of SinglePathNodeIdentifier but got: {type(current_selection)}'
        self._initial_selection_spid: SinglePathNodeIdentifier = current_selection

    def shutdown(self):
        if self.con:
            self.con.shutdown()
            self.con = None

        dispatcher.disconnect(signal=Signal.TREE_SELECTION_CHANGED, receiver=self._on_selected_changed)
        dispatcher.disconnect(signal=Signal.LOAD_SUBTREE_DONE, receiver=self._on_backend_ready)
        dispatcher.disconnect(signal=Signal.POPULATE_UI_TREE_DONE, receiver=self._on_populate_complete)

        # call super method to destroy dialog
        Gtk.Dialog.destroy(self)
        self.close()

    def _on_backend_ready(self, sender):
        if sender != self.tree_id:
            return
        logger.debug(f'[{ID_GDRIVE_DIR_SELECT}] Backend load complete! Showing dialog')
        GLib.idle_add(self.show_all)

    def _on_selected_changed(self, sender, node_list: List[Node]):
        if sender != self.tree_id:
            return
        # In weird GTK-speak, "sensitive" means enabled
        self.ok_button.set_sensitive(node_list and node_list[0].is_dir())

    def _on_populate_complete(self, sender):
        if sender != self.tree_id:
            return
        logger.debug(f'[{ID_GDRIVE_DIR_SELECT}] Populate complete! Sending signal: {Signal.EXPAND_AND_SELECT_NODE}')
        dispatcher.send(Signal.EXPAND_AND_SELECT_NODE, sender=ID_GDRIVE_DIR_SELECT, spid=self._initial_selection_spid)

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
                    spid = NodeIdentifierFactory.get_root_constant_gdrive_spid()
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

            self.shutdown()
        except FileNotFoundError as err:
            self.show_error_ui('File not found: ' + err.filename)
            raise
        except Exception as err:
            logger.exception(err)
            detail = f'{repr(err)}]'
            self.show_error_ui('Unexpected error', detail)
            raise
