import logging
import os

import gi
from pydispatch import dispatcher

from constants import TREE_TYPE_LOCAL_DISK, TreeDisplayMode
from diff.diff_content_first import ContentFirstDiffer
from model.node_identifier import NodeIdentifier
from model.display_tree.display_tree import DisplayTree

gi.require_version("Gtk", "3.0")
from gi.repository import GLib, Gtk

from stopwatch_sec import Stopwatch

import ui.actions as actions
from ui.dialog.gdrive_dir_chooser_dialog import GDriveDirChooserDialog

logger = logging.getLogger(__name__)


# CLASS GlobalActions
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼

class GlobalActions:
    def __init__(self, application):
        self.application = application

    """
    🡻🡻🡻 ① Connect Listeners 🡻🡻🡻
    """

    def init(self):
        logger.debug('Init global actions')
        actions.connect(signal=actions.START_DIFF_TREES, handler=self.on_diff_requested)
        actions.connect(signal=actions.DOWNLOAD_GDRIVE_META, handler=self.on_gdrive_requested)
        actions.connect(signal=actions.SHOW_GDRIVE_ROOT_DIALOG, handler=self.on_gdrive_root_dialog_requested)
        actions.connect(signal=actions.GDRIVE_DOWNLOAD_COMPLETE, handler=self.on_gdrive_download_complete)
        actions.connect(signal=actions.LOAD_ALL_CACHES, handler=self.on_load_all_caches_requested)

    """
    🡻🡻🡻 ② Utility functions 🡻🡻🡻
    """

    def show_error_ui(self, *args, **kwargs):
        self.application.window.show_error_ui(*args, *kwargs)

    """
    🡻🡻🡻 ③ Actions 🡻🡻🡻
    """

    def on_load_all_caches_requested(self, sender):
        logger.debug(f'Received signal: "{actions.LOAD_ALL_CACHES}"')
        self.application.executor.submit_async_task(self.application.cache_manager.load_all_caches, sender)

    def on_gdrive_requested(self, sender):
        logger.debug(f'Received signal: "{actions.DOWNLOAD_GDRIVE_META}"')
        self.application.executor.submit_async_task(self.download_all_gdrive_meta, sender)

    def download_all_gdrive_meta(self, tree_id):
        """Executed by Task Runner. NOT UI thread"""
        actions.disable_ui(sender=tree_id)
        try:
            self.application.cache_manager.download_all_gdrive_meta(tree_id)

            tree = self.application.cache_manager.get_gdrive_whole_tree(tree_id)
            actions.get_dispatcher().send(signal=actions.GDRIVE_DOWNLOAD_COMPLETE, sender=tree_id, tree=tree)
        except Exception as err:
            self.show_error_ui('Download from GDrive failed due to unexpected error', repr(err))
            logger.exception(err)
        finally:
            actions.enable_ui(sender=tree_id)

    def load_gdrive_root_meta(self, tree_id: str, current_selection: NodeIdentifier):
        """Executed by Task Runner. NOT UI thread"""
        actions.disable_ui(sender=tree_id)
        try:
            tree = self.application.cache_manager.get_gdrive_whole_tree(tree_id)
            dispatcher.send(signal=actions.GDRIVE_DOWNLOAD_COMPLETE, sender=tree_id, tree=tree, current_selection=current_selection)
        except Exception as err:
            self.show_error_ui('Download from GDrive failed due to unexpected error', repr(err))
            logger.exception(err)
        finally:
            actions.enable_ui(sender=tree_id)

    def on_gdrive_root_dialog_requested(self, sender, current_selection: NodeIdentifier):
        logger.debug(f'Received signal: "{actions.SHOW_GDRIVE_ROOT_DIALOG}"')
        self.application.executor.submit_async_task(self.load_gdrive_root_meta, sender, current_selection)

    def on_gdrive_download_complete(self, sender, tree: DisplayTree, current_selection: NodeIdentifier):
        logger.debug(f'Received signal: "{actions.GDRIVE_DOWNLOAD_COMPLETE}"')
        assert type(sender) == str

        def open_dialog():
            try:
                # Preview changes in UI pop-up. Change tree_id so that listeners don't step on existing trees
                dialog = GDriveDirChooserDialog(self.application.window, tree, sender, current_selection)
                response_id = dialog.run()
                if response_id == Gtk.ResponseType.OK:
                    logger.debug('User clicked OK!')

            except Exception as err:
                self.show_error_ui('GDriveDirChooserDialog failed due to unexpected error', repr(err))
                raise

        GLib.idle_add(open_dialog)

    def on_diff_requested(self, sender, tree_con_left, tree_con_right):
        logger.debug(f'Received signal: "{actions.START_DIFF_TREES}"')
        self.application.executor.submit_async_task(self.do_tree_diff, sender, tree_con_left, tree_con_right)

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
            actions.get_dispatcher().send(actions.START_PROGRESS_INDETERMINATE, sender=actions.ID_DIFF_WINDOW)
            msg = 'Computing bidrectional content-first diff...'
            actions.get_dispatcher().send(actions.SET_PROGRESS_TEXT, sender=actions.ID_DIFF_WINDOW, msg=msg)

            stopwatch_diff = Stopwatch()
            differ = ContentFirstDiffer(left_fmeta_tree, right_fmeta_tree, self.application)
            change_tree_left, change_tree_right, = differ.diff(compare_paths_also=True)
            logger.info(f'{stopwatch_diff} Diff completed')

            actions.get_dispatcher().send(actions.SET_PROGRESS_TEXT, sender=actions.ID_DIFF_WINDOW, msg='Populating UI trees...')

            tree_con_left.reload(new_tree=change_tree_left, tree_display_mode=TreeDisplayMode.CHANGES_ONE_TREE_PER_CATEGORY,
                                 show_checkboxes=True)
            tree_con_right.reload(new_tree=change_tree_right, tree_display_mode=TreeDisplayMode.CHANGES_ONE_TREE_PER_CATEGORY,
                                  show_checkboxes=True)

            actions.get_dispatcher().send(actions.STOP_PROGRESS, sender=actions.ID_DIFF_WINDOW)
            actions.get_dispatcher().send(signal=actions.DIFF_TREES_DONE, sender=sender, stopwatch=stopwatch_diff_total)
        except Exception as err:
            # Clean up progress bar:
            actions.get_dispatcher().send(actions.STOP_PROGRESS, sender=actions.ID_DIFF_WINDOW)
            actions.enable_ui(sender=self)
            self.show_error_ui('Diff task failed due to unexpected error', repr(err))
            logger.exception(err)

