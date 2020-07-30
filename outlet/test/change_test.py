import os
import shutil
import threading
import time
import unittest
import logging

from py7zr import SevenZipFile
from pydispatch import dispatcher

from app_config import AppConfig
from index.uid.uid import UID
from model.node.display_node import DisplayNode
from outlet_app import OutletApplication
from ui import actions
from ui.actions import DRAG_AND_DROP_DIRECT
from ui.tree.controller import TreePanelController
from ui.tree.ui_listeners import DragAndDropData
from util import file_util

import gi
gi.require_version("Gtk", "3.0")
from gi.repository import Gtk

logger = logging.getLogger(__name__)

LOAD_TIMEOUT_SEC = 15
ENABLE_CHANGE_EXECUTION_THREAD = True

TEST_BASE_DIR = file_util.get_resource_path('test')
TEST_ARCHIVE = 'ChangeTest.7z'
TEST_ARCHIVE_PATH = os.path.join(TEST_BASE_DIR, TEST_ARCHIVE)
TEST_TARGET_DIR = os.path.join(TEST_BASE_DIR, 'ChangeTest')


class ChangeTest(unittest.TestCase):
    def setUp(self) -> None:
        # Remove test files and replace with freshly extracted files
        shutil.rmtree(TEST_TARGET_DIR)
        logger.debug(f'Deleted dir: {TEST_TARGET_DIR}')
        with SevenZipFile(file=TEST_ARCHIVE_PATH) as archive:
            archive.extractall(TEST_BASE_DIR)
        logger.debug(f'Extracted: {TEST_ARCHIVE_PATH} to {TEST_BASE_DIR}')

        config = AppConfig()
        self.app = OutletApplication(config)
        # Disable execution so we can study the state of the OpTree:
        self.app.executor.enable_change_thread = ENABLE_CHANGE_EXECUTION_THREAD

        load_left_done = threading.Event()
        load_right_done = threading.Event()

        def run_thread():
            # this starts the executor, which inits the CacheManager
            # This does not return until the program exits
            self.app.run([])

        def after_left_tree_loaded(sender):
            logger.debug(f'Received signal: "{actions.LOAD_UI_TREE_DONE}" for "{sender}"')
            load_left_done.set()

        def after_right_tree_loaded(sender):
            logger.debug(f'Received signal: "{actions.LOAD_UI_TREE_DONE}" for "{sender}"')
            load_right_done.set()

        dispatcher.connect(signal=actions.LOAD_UI_TREE_DONE, sender=actions.ID_LEFT_TREE, receiver=after_left_tree_loaded)
        dispatcher.connect(signal=actions.LOAD_UI_TREE_DONE, sender=actions.ID_RIGHT_TREE, receiver=after_right_tree_loaded)
        thread = threading.Thread(target=run_thread, daemon=True)
        thread.start()

        # wait for both sides to load before returning:
        if not load_left_done.wait(LOAD_TIMEOUT_SEC):
            raise RuntimeError('Timed out waiting for left to load!')
        if not load_right_done.wait(LOAD_TIMEOUT_SEC):
            raise RuntimeError('Timed out waiting for right to load!')
        self.left_con: TreePanelController = self.app.cache_manager.get_tree_controller(actions.ID_LEFT_TREE)
        self.right_con: TreePanelController = self.app.cache_manager.get_tree_controller(actions.ID_RIGHT_TREE)
        logger.info(f'LOAD COMPLETE')

    def test_single_file_cp(self):
        logger.info('Doing drop test')
        # Offset from 0:
        src_tree_path = Gtk.TreePath.new_from_string('1')
        node: DisplayNode = self.right_con.display_store.get_node_data(src_tree_path)

        nodes = [node]
        dd_data = DragAndDropData(dd_uid=UID(100), src_tree_controller=self.right_con, nodes=nodes)
        dst_tree_path = Gtk.TreePath.new_from_string('1')
        dispatcher.send(signal=DRAG_AND_DROP_DIRECT, sender=actions.ID_LEFT_TREE, drag_data=dd_data, tree_path=dst_tree_path, is_into=False)
        logger.info('Sleeping')
        time.sleep(10) # in seconds
        logger.info('Done!')
