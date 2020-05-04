import logging
import os
import uuid

import gi

from constants import OBJ_TYPE_LOCAL_DISK, ROOT
from model.display_id import Identifier
from ui.tree.meta_store import BaseMetaStore

gi.require_version("Gtk", "3.0")
from gi.repository import GLib, Gtk

from stopwatch_sec import Stopwatch

import ui.actions as actions
from ui.gdrive_dir_selection_dialog import GDriveDirSelectionDialog

from fmeta import diff_content_first

logger = logging.getLogger(__name__)


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
        self.application.task_runner.enqueue(self.application.cache_manager.load_all_caches, sender)

    def on_gdrive_requested(self, sender):
        logger.debug(f'Received signal: "{actions.DOWNLOAD_GDRIVE_META}"')
        self.application.task_runner.enqueue(self.download_gdrive_meta, sender)

    def download_gdrive_meta(self, tree_id):
        """Executed by Task Runner. NOT UI thread"""
        actions.disable_ui(sender=tree_id)
        try:
            self.application.cache_manager.download_all_gdrive_meta(tree_id)
            meta_store = self.application.cache_manager.get_metastore_for_gdrive_subtree(ROOT, tree_id)
            actions.get_dispatcher().send(signal=actions.GDRIVE_DOWNLOAD_COMPLETE, sender=tree_id, meta_store=meta_store)
        except Exception as err:
            self.show_error_ui('Download from GDrive failed due to unexpected error', repr(err))
            logger.exception(err)
        finally:
            actions.enable_ui(sender=tree_id)

    def load_gdrive_root_meta(self, tree_id):
        """Executed by Task Runner. NOT UI thread"""
        actions.disable_ui(sender=tree_id)
        try:
            meta_store = self.application.cache_manager.get_metastore_for_gdrive_subtree(ROOT, tree_id)
            actions.get_dispatcher().send(signal=actions.GDRIVE_DOWNLOAD_COMPLETE, sender=tree_id, meta_store=meta_store)
        except Exception as err:
            self.show_error_ui('Download from GDrive failed due to unexpected error', repr(err))
            logger.exception(err)
        finally:
            actions.enable_ui(sender=tree_id)

    def on_gdrive_root_dialog_requested(self, sender):
        logger.debug(f'Received signal: "{actions.SHOW_GDRIVE_ROOT_DIALOG}"')
        self.application.task_runner.enqueue(self.load_gdrive_root_meta, sender)

    def on_gdrive_download_complete(self, sender, meta_store: BaseMetaStore):
        logger.debug(f'Received signal: "{actions.GDRIVE_DOWNLOAD_COMPLETE}"')
        assert type(sender) == str

        def open_dialog():
            try:
                # Preview changes in UI pop-up. Change tree_id so that listeners don't step on existing trees
                dialog = GDriveDirSelectionDialog(self.application.window, meta_store, sender)
                response_id = dialog.run()
                if response_id == Gtk.ResponseType.OK:
                    logger.debug('User clicked OK!')

            except Exception as err:
                self.show_error_ui('GDriveDirSelectionDialog failed due to unexpected error', repr(err))
                raise

        GLib.idle_add(open_dialog)

    def on_diff_requested(self, sender, tree_con_left, tree_con_right):
        logger.debug(f'Received signal: "{actions.START_DIFF_TREES}"')
        self.application.task_runner.enqueue(self.do_tree_diff, sender, tree_con_left, tree_con_right)

    def do_tree_diff(self, sender, tree_con_left, tree_con_right):
        stopwatch_diff_total = Stopwatch()
        tx_id = uuid.uuid1()
        actions.disable_ui(sender=sender)
        try:
            left_id: Identifier = tree_con_left.meta_store.get_root_identifier()
            right_id: Identifier = tree_con_right.meta_store.get_root_identifier()
            left_root = left_id.full_path
            right_root = right_id.full_path
            if left_id.tree_type == OBJ_TYPE_LOCAL_DISK and not os.path.exists(left_root):
                logger.info(f'Skipping diff because the left path does not exist: "{left_root}"')
                actions.enable_ui(sender=self)
                return
            elif right_id.tree_type == OBJ_TYPE_LOCAL_DISK and not os.path.exists(right_root):
                logger.info(f'Skipping diff because the right path does not exist: "{right_root}"')
                actions.enable_ui(sender=self)
                return

            # Load trees if not loaded - may be a long operation
            # TODO: turn dummy data store into a lazy-load data store
            left_fmeta_tree = tree_con_left.meta_store.get_whole_tree()
            right_fmeta_tree = tree_con_right.meta_store.get_whole_tree()

            logger.debug(f'Sending START_PROGRESS_INDETERMINATE for ID: {actions.ID_DIFF_WINDOW}')
            actions.get_dispatcher().send(actions.START_PROGRESS_INDETERMINATE, sender=actions.ID_DIFF_WINDOW, tx_id=tx_id)
            msg = 'Computing bidrectional content-first diff...'
            actions.get_dispatcher().send(actions.SET_PROGRESS_TEXT, sender=actions.ID_DIFF_WINDOW, tx_id=tx_id, msg=msg)

            stopwatch_diff = Stopwatch()
            diff_content_first.diff(left_fmeta_tree, right_fmeta_tree, compare_paths_also=True)
            logger.info(f'Diff completed in: {stopwatch_diff}')

            actions.get_dispatcher().send(actions.SET_PROGRESS_TEXT, sender=actions.ID_DIFF_WINDOW, tx_id=tx_id, msg='Populating UI trees...')
            tree_con_left.load()
            tree_con_right.load()

            actions.get_dispatcher().send(actions.STOP_PROGRESS, sender=actions.ID_DIFF_WINDOW, tx_id=tx_id)
            actions.get_dispatcher().send(signal=actions.DIFF_TREES_DONE, sender=sender, stopwatch=stopwatch_diff_total)
        except Exception as err:
            # Clean up progress bar:
            actions.get_dispatcher().send(actions.STOP_PROGRESS, sender=actions.ID_DIFF_WINDOW, tx_id=tx_id)
            actions.enable_ui(sender=self)
            self.show_error_ui('Diff task failed due to unexpected error', repr(err))
            logger.exception(err)

