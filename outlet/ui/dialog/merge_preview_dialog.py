import logging

from pydispatch import dispatcher

from ui.actions import ID_MERGE_TREE
from ui.tree import tree_factory
from ui.tree.category_display_tree import CategoryDisplayTree

from ui import actions
from ui.dialog.base_dialog import BaseDialog

import gi
gi.require_version("Gtk", "3.0")
from gi.repository import Gtk

logger = logging.getLogger(__name__)


class MergePreviewDialog(Gtk.Dialog, BaseDialog):

    def __init__(self, parent_win, tree):
        Gtk.Dialog.__init__(self, "Confirm Merge", parent_win, 0)
        BaseDialog.__init__(self, parent_win.application)
        self.parent_win = parent_win
        self.add_button(Gtk.STOCK_CANCEL, Gtk.ResponseType.CANCEL)
        self.add_button(Gtk.STOCK_APPLY, Gtk.ResponseType.APPLY)

        self.set_default_size(700, 700)

        box = self.get_content_area()
        self.content_box = Gtk.Box(spacing=6, orientation=Gtk.Orientation.VERTICAL)
        box.add(self.content_box)

        label = Gtk.Label(label="The following changes will be made:")
        self.content_box.add(label)

        self.tree: CategoryDisplayTree = tree
        self.tree_con = tree_factory.build_static_category_file_tree(parent_win=self, tree_id=ID_MERGE_TREE, tree=self.tree)
        actions.set_status(sender=ID_MERGE_TREE, status_msg=self.tree.get_summary())
        self.content_box.pack_start(self.tree_con.content_box, True, True, 0)
        self.tree_con.reload()

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
            elif response_id == -4:
                logger.debug("Dialog was closed")
            else:
                logger.debug(f'response_id: {response_id}')
        except FileNotFoundError as err:
            self.show_error_ui('File not found: ' + err.filename)
            raise
        except Exception as err:
            logger.exception(err)
            detail = f'{repr(err)}]'
            self.show_error_ui('Diff task failed due to unexpected error', detail)
        finally:
            # Clean up:
            self.tree_con.destroy()
            self.tree_con = None
            self.tree = None
            dialog.destroy()

    def on_apply_clicked(self):
        self.parent_win.application.cache_manager.enqueue_op_list(op_list=self.tree.get_ops())
        dispatcher.send(signal=actions.EXIT_DIFF_MODE, sender=actions.ID_MERGE_TREE)
