import logging

import gi
gi.require_version("Gtk", "3.0")
from gi.repository import Gtk

from ui.base_dialog import BaseDialog

logger = logging.getLogger(__name__)


class GDriveDirSelectionDialog(Gtk.Dialog, BaseDialog):

    def __init__(self, parent, meta):
        Gtk.Dialog.__init__(self, "Confirm Merge", parent, 0)
        BaseDialog.__init__(self, parent.config)
        self.add_button(Gtk.STOCK_CANCEL, Gtk.ResponseType.CANCEL)
        self.add_button(Gtk.STOCK_OK, Gtk.ResponseType.OK)

        self.set_default_size(700, 700)

        box = self.get_content_area()
        self.content_box = Gtk.Box(spacing=6, orientation=Gtk.Orientation.VERTICAL)
        box.add(self.content_box)

        label = Gtk.Label(label="Select the Google Drive folder to use as the root for comparison:")
        self.content_box.add(label)
        #
        # store = SimpleDataStore(tree_id='merge_tree', fmeta_tree=self.fmeta_tree)
        # self.lazy_tree = LazyTree(store=store, parent_win=self)
        # actions.set_status(sender=store.tree_id, status_msg=self.fmeta_tree.get_summary())
        # self.content_box.pack_start(self.lazy_tree.content_box, True, True, 0)
        #
        # diff_tree_populator.repopulate_diff_tree(self.diff_tree)

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
                logger.debug("response_id: ", response_id)
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
