import logging

import gi

from ui.actions import ID_MERGE_TREE

gi.require_version("Gtk", "3.0")
from gi.repository import Gtk

from ui import actions
from ui.tree.data_store import StaticWholeTreeStore
import fmeta.fmeta_file_util
from file_util import get_resource_path
from fmeta.fmeta_file_util import FMetaError
from fmeta.fmeta import Category
from ui.diff_tree.diff_tree_panel import DiffTreePanel
from ui.base_dialog import BaseDialog
import ui.diff_tree.fmeta_change_strategy as diff_tree_populator

STAGING_DIR_PATH = get_resource_path("temp")

logger = logging.getLogger(__name__)


class MergePreviewDialog(Gtk.Dialog, BaseDialog):

    def __init__(self, parent, fmeta_tree):
        Gtk.Dialog.__init__(self, "Confirm Merge", parent, 0)
        BaseDialog.__init__(self, parent.config)
        self.add_button(Gtk.STOCK_CANCEL, Gtk.ResponseType.CANCEL)
        self.add_button(Gtk.STOCK_APPLY, Gtk.ResponseType.APPLY)

        self.set_default_size(700, 700)

        self.fmeta_tree = fmeta_tree

        box = self.get_content_area()
        self.content_box = Gtk.Box(spacing=6, orientation=Gtk.Orientation.VERTICAL)
        box.add(self.content_box)

        label = Gtk.Label(label="The following changes will be made:")
        self.content_box.add(label)

        data_store = StaticWholeTreeStore(tree_id=ID_MERGE_TREE, config=self.config, tree=self.fmeta_tree)
        self.diff_tree = DiffTreePanel(data_store=data_store, parent_win=self, editable=False, is_display_persisted=False)
        actions.set_status(sender=data_store.tree_id, status_msg=self.fmeta_tree.get_summary())
        self.content_box.pack_start(self.diff_tree.content_box, True, True, 0)

        diff_tree_populator.repopulate_diff_tree(self.diff_tree)

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

    def on_apply_clicked(self):
        staging_dir = STAGING_DIR_PATH
        # TODO: clear dir after use

        error_collection = []
        fmeta.fmeta_file_util.apply_changes_atomically(tree_id=self.diff_tree.data_store.tree_id, tree=self.fmeta_tree, staging_dir=staging_dir,
                                                       continue_on_error=True, error_collector=error_collection)

        if len(error_collection) > 0:
            # TODO: create a much better UI here
            noop_adds = 0
            noop_dels = 0
            noop_movs = 0
            err_adds = 0
            err_dels = 0
            err_movs = 0
            for err in error_collection:
                if err.fm.category == Category.Added:
                    if type(err) == FMetaError:
                        err_adds += 1
                    else:
                        noop_adds += 1
                elif err.fm.category == Category.Deleted:
                    if type(err) == FMetaError:
                        err_dels += 1
                    else:
                        noop_dels += 1
                elif err.fm.category == Category.Moved:
                    if type(err) == FMetaError:
                        err_movs += 1
                    else:
                        noop_movs += 1
            self.show_error_ui(f'{len(error_collection)} Errors occurred',
                               f'Collected the following errors while applying changes: adds={err_adds} dels={err_dels} movs={err_movs} noops={noop_adds + noop_dels + noop_movs}')
