import logging
import os

import gi
gi.require_version("Gtk", "3.0")
from gi.repository import GLib, Gtk

from stopwatch import Stopwatch

import ui.actions as actions
from ui.gdrive_dir_selection_dialog import GDriveDirSelectionDialog


from gdrive.tree_builder import GDriveTreeLoader
from file_util import get_resource_path
from fmeta import diff_content_first

logger = logging.getLogger(__name__)


class GlobalActions:
    def __init__(self, application):
        self.application = application

    def init(self):
        actions.connect(signal=actions.DO_DIFF, handler=self.on_diff_requested)
        actions.connect(signal=actions.DOWNLOAD_GDRIVE_META, handler=self.on_gdrive_requested)
        actions.connect(signal=actions.GDRIVE_DOWNLOAD_COMPLETE, handler=self.on_gdrive_download_complete)

    def show_error_ui(self, *args, **kwargs):
        self.application.window.show_error_ui(*args, *kwargs)

    def on_gdrive_requested(self, sender):
        """Callback for signal DOWNLOAD_GDRIVE_META"""
        self.application.task_runner.enqueue(self.download_gdrive_meta, sender)

    def download_gdrive_meta(self, tree_id):
        actions.disable_ui(sender=tree_id)
        try:
            cache_path = get_resource_path('gdrive.db')
            tree_builder = GDriveTreeLoader(config=self.application.config, cache_path=cache_path, tree_id=tree_id)
            meta = tree_builder.load_all(invalidate_cache=False)
            actions.get_dispatcher().send(signal=actions.GDRIVE_DOWNLOAD_COMPLETE, sender=tree_id, meta=meta)
        except Exception as err:
            self.show_error_ui('Download from GDrive failed due to unexpected error', repr(err))
            logger.exception(err)
        finally:
            actions.enable_ui(sender=tree_id)

    def on_gdrive_download_complete(self, sender, meta):
        """Callback for signal GDRIVE_DOWNLOAD_COMPLETE"""
        assert type(sender) == str

        def open_dialog():
            try:
                # Preview changes in UI pop-up
                dialog = GDriveDirSelectionDialog(self, meta, sender)
                response_id = dialog.run()
                if response_id == Gtk.ResponseType.OK:
                    logger.debug('User clicked OK!')

            except Exception as err:
                self.show_error_ui('GDriveDirSelectionDialog failed due to unexpected error', repr(err))
                raise

        GLib.idle_add(open_dialog)

    def on_diff_requested(self, sender, tree_con_left, tree_con_right):
        """Callback for signal DO_DIFF"""
        self.application.task_runner.enqueue(self.do_tree_diff, sender, tree_con_left, tree_con_right)

    # TODO: change DB path whenever root is changed
    def do_tree_diff(self, sender, tree_con_left, tree_con_right):
        stopwatch_diff_total = Stopwatch()
        actions.disable_ui(sender=sender)
        try:
            left_root = tree_con_left.data_store.get_root_path()
            right_root = tree_con_right.data_store.get_root_path()
            if not os.path.exists(left_root) or not os.path.exists(right_root):
                logger.info('Skipping diff because one of the paths does not exist')
                actions.enable_ui(sender=self)
                return

            actions.set_status(sender=actions.ID_RIGHT_TREE, status_msg='Waiting...')

            # Load trees if not loaded - may be a long operation

            left_fmeta_tree = tree_con_left.data_store.get_whole_tree()
            right_fmeta_tree = tree_con_right.data_store.get_whole_tree()

            logger.debug(f'Sending START_PROGRESS_INDETERMINATE for ID: {actions.ID_DIFF_WINDOW}')
            actions.get_dispatcher().send(actions.START_PROGRESS_INDETERMINATE, sender=actions.ID_DIFF_WINDOW)
            msg = 'Computing bidrectional content-first diff...'
            actions.get_dispatcher().send(actions.SET_PROGRESS_TEXT, sender=actions.ID_DIFF_WINDOW, msg=msg)

            stopwatch_diff = Stopwatch()
            diff_content_first.diff(left_fmeta_tree, right_fmeta_tree, compare_paths_also=True, use_modify_times=False)
            logger.info(f'Diff completed in: {stopwatch_diff}')

            actions.get_dispatcher().send(actions.SET_PROGRESS_TEXT, sender=actions.ID_DIFF_WINDOW, msg='Populating UI trees...')
            tree_con_left.load()
            tree_con_right.load()

            actions.get_dispatcher().send(signal=actions.DIFF_DID_COMPLETE, sender=sender, stopwatch=stopwatch_diff_total)
        except Exception as err:
            actions.enable_ui(sender=self)
            self.show_error_ui('Diff task failed due to unexpected error', repr(err))
            logger.exception(err)

