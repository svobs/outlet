import logging

import gi

from gdrive.GDriveDataStore import GDriveDataStore
from ui.lazy_tree.lazy_tree import LazyTree

gi.require_version("Gtk", "3.0")
from gi.repository import Gtk

from ui.base_dialog import BaseDialog

logger = logging.getLogger(__name__)


class GDriveDirSelectionDialog(Gtk.Dialog, BaseDialog):

    def __init__(self, parent, gdrive_meta):
        Gtk.Dialog.__init__(self, "Select GDrive Root", parent, 0)
        BaseDialog.__init__(self, parent.config)
        self.add_button(Gtk.STOCK_CANCEL, Gtk.ResponseType.CANCEL)
        self.add_button(Gtk.STOCK_OK, Gtk.ResponseType.OK)

        self.set_default_size(1000, 800)

        box = self.get_content_area()
        self.content_box = Gtk.Box(spacing=6, orientation=Gtk.Orientation.VERTICAL)
        box.add(self.content_box)

        label = Gtk.Label(label="Select the Google Drive folder to use as the root for comparison:")
        self.content_box.add(label)

        store = GDriveDataStore(tree_id='gdrive_dir_selection', config=parent.config, gdrive_meta=gdrive_meta)
        self.lazy_tree = LazyTree(store=store, parent_win=self, editable=False, is_display_persisted=False)
        # actions.set_status(sender=store.tree_id, status_msg=self.fmeta_tree.get_summary())
        self.content_box.pack_start(self.lazy_tree.content_box, True, True, 0)

        self.lazy_tree.populate_root()

        self.connect("response", self.on_response)
        self.show_all()

    def on_response(self, dialog, response_id):
        # destroy the widget (the dialog) when the function on_response() is called
        # (that is, when the button of the dialog has been clicked)

        try:
            if response_id == Gtk.ResponseType.APPLY:
                logger.debug("The APPLY button was clicked")
                self.on_apply_clicked()
            elif response_id == Gtk.ResponseType.CANCEL:
                logger.debug("The Cancel button was clicked")
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
