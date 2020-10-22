import logging
import os

import gi
from pydispatch import dispatcher

from constants import TREE_TYPE_LOCAL_DISK, TreeDisplayMode
from diff.diff_content_first import ContentFirstDiffer
from model.node_identifier import NodeIdentifier
from model.display_tree.display_tree import DisplayTree
from util.has_lifecycle import HasLifecycle

gi.require_version("Gtk", "3.0")
from gi.repository import GLib, Gtk

from util.stopwatch_sec import Stopwatch

import ui.actions as actions
from ui.dialog.gdrive_dir_chooser_dialog import GDriveDirChooserDialog

logger = logging.getLogger(__name__)


# CLASS GlobalActions
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼

class GlobalActions(HasLifecycle):
    def __init__(self, app):
        HasLifecycle.__init__(self)
        self.app = app

    def shutdown(self):
        HasLifecycle.shutdown(self)
        logger.debug('GlobalActions shut down')

    """
    🡻🡻🡻 ① Connect Listeners 🡻🡻🡻
    """

    def start(self):
        logger.debug('Starting GlobalActions listeners')
        HasLifecycle.start(self)
        self.connect_dispatch_listener(signal=actions.START_DIFF_TREES, receiver=self._on_diff_requested)
        self.connect_dispatch_listener(signal=actions.SYNC_GDRIVE_CHANGES, receiver=self._on_gdrive_sync_changes_requested)
        self.connect_dispatch_listener(signal=actions.DOWNLOAD_ALL_GDRIVE_META, receiver=self._on_download_all_gdrive_meta_requested)
        self.connect_dispatch_listener(signal=actions.SHOW_GDRIVE_CHOOSER_DIALOG, receiver=self._on_gdrive_root_dialog_requested)
        self.connect_dispatch_listener(signal=actions.GDRIVE_CHOOSER_DIALOG_LOAD_DONE, receiver=self._on_gdrive_chooser_dialog_load_complete)

    """
    🡻🡻🡻 ② Utility functions 🡻🡻🡻
    """

    def show_error_ui(self, *args, **kwargs):
        self.app.window.show_error_ui(*args, *kwargs)

    """
    🡻🡻🡻 ③ Actions 🡻🡻🡻
    """

    def _on_download_all_gdrive_meta_requested(self, sender):
        """See below. Invalidates the GDrive cache and starts a new download of all the GDrive metadata"""
        logger.debug(f'Received signal: "{actions.DOWNLOAD_ALL_GDRIVE_META}"')
        self.app.executor.submit_async_task(self.download_all_gdrive_meta, sender)

    def download_all_gdrive_meta(self, tree_id):
        """See above. Executed by Task Runner. NOT UI thread"""
        actions.disable_ui(sender=tree_id)
        try:
            self.app.cacheman.download_all_gdrive_meta(tree_id)
        except Exception as err:
            self.show_error_ui('Download from GDrive failed due to unexpected error', repr(err))
            logger.exception(err)
        finally:
            actions.enable_ui(sender=tree_id)

    def _on_gdrive_sync_changes_requested(self, sender):
        """See below. This will load the GDrive tree (if it is not loaded already), then sync to the latest changes from GDrive"""
        logger.debug(f'Received signal: "{actions.SYNC_GDRIVE_CHANGES}"')
        self.app.executor.submit_async_task(self.sync_gdrive_changes, sender)

    def sync_gdrive_changes(self, tree_id: str):
        """See above. Executed by Task Runner. NOT UI thread"""
        try:
            # This will send out the necessary notifications if anything has changed
            self.app.cacheman.get_synced_gdrive_master_tree(tree_id)
        except Exception as err:
            self.show_error_ui('Sync from GDrive failed due to unexpected error', repr(err))
            logger.exception(err)

    def _on_gdrive_root_dialog_requested(self, sender: str, current_selection: NodeIdentifier):
        """See below."""
        logger.debug(f'Received signal: "{actions.SHOW_GDRIVE_CHOOSER_DIALOG}"')
        self.app.executor.submit_async_task(self.load_data_for_gdrive_dir_chooser_dialog, sender, current_selection)

    def load_data_for_gdrive_dir_chooser_dialog(self, tree_id: str, current_selection: NodeIdentifier):
        """See above. Executed by Task Runner. NOT UI thread"""
        actions.disable_ui(sender=tree_id)
        try:
            tree = self.app.cacheman.get_synced_gdrive_master_tree(tree_id)
            dispatcher.send(signal=actions.GDRIVE_CHOOSER_DIALOG_LOAD_DONE, sender=tree_id, tree=tree, current_selection=current_selection)
        except Exception as err:
            self.show_error_ui('Download from GDrive failed due to unexpected error', repr(err))
            logger.exception(err)
        finally:
            actions.enable_ui(sender=tree_id)

    def _on_gdrive_chooser_dialog_load_complete(self, sender, tree: DisplayTree, current_selection: NodeIdentifier):
        logger.debug(f'Received signal: "{actions.GDRIVE_CHOOSER_DIALOG_LOAD_DONE}"')
        assert type(sender) == str

        def open_dialog():
            try:
                # Preview ops in UI pop-up. Change tree_id so that listeners don't step on existing trees
                dialog = GDriveDirChooserDialog(self.app.window, tree, sender, current_selection)
                response_id = dialog.run()
                if response_id == Gtk.ResponseType.OK:
                    logger.debug('User clicked OK!')

            except Exception as err:
                self.show_error_ui('GDriveDirChooserDialog failed due to unexpected error', repr(err))
                raise

        GLib.idle_add(open_dialog)

    def _on_diff_requested(self, sender, tree_con_left, tree_con_right):
        logger.debug(f'Received signal: "{actions.START_DIFF_TREES}"')
        self.app.executor.submit_async_task(self.do_tree_diff, sender, tree_con_left, tree_con_right)

    def do_tree_diff(self, sender, tree_con_left, tree_con_right):
        stopwatch_diff_total = Stopwatch()
        actions.disable_ui(sender=sender)
        try:
            left_id: NodeIdentifier = tree_con_left.get_root_identifier()
            right_id: NodeIdentifier = tree_con_right.get_root_identifier()
            if left_id.tree_type == TREE_TYPE_LOCAL_DISK and not os.path.exists(left_id.full_path):
                logger.info(f'Skipping diff because the left path does not exist: "{left_id.full_path}"')
                actions.enable_ui(sender=self)
                return
            elif right_id.tree_type == TREE_TYPE_LOCAL_DISK and not os.path.exists(right_id.full_path):
                logger.info(f'Skipping diff because the right path does not exist: "{right_id.full_path}"')
                actions.enable_ui(sender=self)
                return

            # Load trees if not loaded - may be a long operation
            left_fmeta_tree = tree_con_left.get_tree()
            right_fmeta_tree = tree_con_right.get_tree()

            logger.debug(f'Sending START_PROGRESS_INDETERMINATE for ID: {actions.ID_DIFF_WINDOW}')
            dispatcher.send(actions.START_PROGRESS_INDETERMINATE, sender=actions.ID_DIFF_WINDOW)
            msg = 'Computing bidrectional content-first diff...'
            dispatcher.send(actions.SET_PROGRESS_TEXT, sender=actions.ID_DIFF_WINDOW, msg=msg)

            stopwatch_diff = Stopwatch()
            differ = ContentFirstDiffer(left_fmeta_tree, right_fmeta_tree, self.app)
            op_tree_left, op_tree_right, = differ.diff(compare_paths_also=True)
            logger.info(f'{stopwatch_diff} Diff completed')

            dispatcher.send(actions.SET_PROGRESS_TEXT, sender=actions.ID_DIFF_WINDOW, msg='Populating UI trees...')

            tree_con_left.reload(new_tree=op_tree_left, tree_display_mode=TreeDisplayMode.CHANGES_ONE_TREE_PER_CATEGORY,
                                 show_checkboxes=True)
            tree_con_right.reload(new_tree=op_tree_right, tree_display_mode=TreeDisplayMode.CHANGES_ONE_TREE_PER_CATEGORY,
                                  show_checkboxes=True)

            dispatcher.send(actions.STOP_PROGRESS, sender=actions.ID_DIFF_WINDOW)
            dispatcher.send(signal=actions.DIFF_TREES_DONE, sender=sender, stopwatch=stopwatch_diff_total)
        except Exception as err:
            # Clean up progress bar:
            dispatcher.send(actions.STOP_PROGRESS, sender=actions.ID_DIFF_WINDOW)
            actions.enable_ui(sender=self)
            self.show_error_ui('Diff task failed due to unexpected error', repr(err))
            logger.exception(err)

