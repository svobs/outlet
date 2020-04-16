import logging

import gi
gi.require_version("Gtk", "3.0")
from gi.repository import GLib, Gtk, Gio, GObject

import file_util
from file_util import get_resource_path, FMetaNoOp, FMetaError
from fmeta.fmeta import FMetaTree, Category
from ui.progress_meter import ProgressMeter
from ui.diff_tree import DiffTree
from ui.base_dialog import BaseDialog
import ui.diff_tree_populator as diff_tree_populator


STAGING_DIR_PATH = get_resource_path("temp")

logger = logging.getLogger(__name__)


class SimpleDataSource:
    def __init__(self, fmeta_tree):
        self._fmeta_tree = fmeta_tree

    def get_root_path(self):
        return self._fmeta_tree.root_path

    def set_root_path(self, new_root_path):
        if self._fmeta_tree.root_path != new_root_path:
            raise RuntimeError('Root path cannot be changed for this tree!')

    def get_fmeta_tree(self):
        return self._fmeta_tree


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

        self.diff_tree = DiffTree(parent_win=self, data_source=SimpleDataSource(self.fmeta_tree), editable=False)
        self.diff_tree.set_status(self.fmeta_tree.get_summary())
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

        def on_progress_made(this, progress, total):
            self.diff_tree.set_status(f'Copied {progress} bytes of {total}')

        progress_meter = ProgressMeter(on_progress_made, self.diff_tree)
        file_util.apply_changes_atomically(tree=self.fmeta_tree, staging_dir=staging_dir,
                                           continue_on_error=True, error_collector=error_collection,
                                           progress_meter=progress_meter)
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
                               f'Collected the following errors while applying changes: adds={err_adds} dels={err_dels} movs={err_movs} noops={noop_adds+noop_dels+noop_movs}')
