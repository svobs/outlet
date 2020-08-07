import os
import shutil
import threading
import time
import unittest
import logging
from functools import partial
from typing import Callable, List

from py7zr import SevenZipFile
from pydispatch import dispatcher

from app_config import AppConfig
from cmd.cmd_interface import Command
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

LOAD_TIMEOUT_SEC = None
ENABLE_CHANGE_EXECUTION_THREAD = True

TEST_BASE_DIR = file_util.get_resource_path('test')
TEST_ARCHIVE = 'ChangeTest.7z'
TEST_ARCHIVE_PATH = os.path.join(TEST_BASE_DIR, TEST_ARCHIVE)
TEST_TARGET_DIR = os.path.join(TEST_BASE_DIR, 'ChangeTest')


class ChangeTest(unittest.TestCase):
    def setUp(self) -> None:
        # Remove test files and replace with freshly extracted files
        if os.path.exists(TEST_TARGET_DIR):
            shutil.rmtree(TEST_TARGET_DIR)
            logger.debug(f'Deleted dir: {TEST_TARGET_DIR}')

        with SevenZipFile(file=TEST_ARCHIVE_PATH) as archive:
            archive.extractall(TEST_BASE_DIR)
        logger.debug(f'Extracted: {TEST_ARCHIVE_PATH} to {TEST_BASE_DIR}')

        config = AppConfig()
        self.app = OutletApplication(config)
        # Disable execution so we can study the state of the OpTree:
        self.app.executor.enable_op_execution_thread = False

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

    def test_dd_single_file_cp(self):
        logger.info('Testing drag & drop copy of single file local to local')
        self.app.executor.start_op_execution_thread()
        # Offset from 0:
        src_tree_path = Gtk.TreePath.new_from_string('1')
        node: DisplayNode = self.right_con.display_store.get_node_data(src_tree_path)
        logger.info(f'CP "{node.name}" from right root to left root')

        nodes = [node]
        dd_data = DragAndDropData(dd_uid=UID(100), src_tree_controller=self.right_con, nodes=nodes)
        dst_tree_path = Gtk.TreePath.new_from_string('1')
        dispatcher.send(signal=DRAG_AND_DROP_DIRECT, sender=actions.ID_LEFT_TREE, drag_data=dd_data, tree_path=dst_tree_path, is_into=False)
        logger.info('Sleeping')
        time.sleep(10) # in seconds
        logger.info('Done!')

    def test_dd_multi_file_cp(self):
        logger.info('Testing drag & drop copy of multiple files local to local')
        self.app.executor.start_op_execution_thread()
        # Offset from 0:

        # Simulate drag & drop based on position in list:
        nodes = []
        for num in range(0, 3):
            node: DisplayNode = self.right_con.display_store.get_node_data(Gtk.TreePath.new_from_string(f'{num}'))
            self.assertIsNotNone(node, f'Expected to find node at index {num}')
            nodes.append(node)
            logger.warning(f'CP "{node.name}" (#{num}) from right root to left root')

        dd_data = DragAndDropData(dd_uid=UID(100), src_tree_controller=self.right_con, nodes=nodes)
        dst_tree_path = Gtk.TreePath.new_from_string('1')
        dispatcher.send(signal=DRAG_AND_DROP_DIRECT, sender=actions.ID_LEFT_TREE, drag_data=dd_data, tree_path=dst_tree_path, is_into=False)
        logger.info('Sleeping')
        time.sleep(10) # in seconds
        logger.info('Done!')

    def test_bad_dd_dir_tree_cp(self):
        logger.info('Testing negative case: drag & drop copy of duplicate nodes local to local')
        self.app.executor.start_op_execution_thread()
        # Offset from 0:
        node_name = 'Art'

        name_equals_func: Callable = partial(_name_equals_func, node_name)

        nodes = []
        # Duplicate the node 3 times. This is a good test of our reduction logic
        for num in range(0, 3):
            tree_iter = self.left_con.display_store.find_in_tree(name_equals_func)
            node = None
            if tree_iter:
                node = self.left_con.display_store.get_node_data(tree_iter)
            self.assertIsNotNone(node, f'Expected to find node named "{node_name}"')
            nodes.append(node)
            logger.warning(f'CP "{node.name}" (#{num}) from left root to right root')

        dd_data = DragAndDropData(dd_uid=UID(100), src_tree_controller=self.left_con, nodes=nodes)
        dst_tree_path = Gtk.TreePath.new_from_string('1')

        # Verify that 3 identical nodes causes an error:
        with self.assertRaises(RuntimeError) as context:
            dispatcher.send(signal=DRAG_AND_DROP_DIRECT, sender=actions.ID_RIGHT_TREE, drag_data=dd_data, tree_path=dst_tree_path, is_into=False)
            logger.info('Sleeping')
            time.sleep(10)  # in seconds
            self.assertFalse(True, 'If we got here we failed!')

    def _find_iter_by_name_in_left_tree(self, node_name):
        name_equals_func: Callable = partial(_name_equals_func, node_name)
        tree_iter = self.left_con.display_store.find_in_tree(name_equals_func)
        if not tree_iter:
            self.fail(f'Expected to find node named "{node_name}"')
        return tree_iter

    def _find_node_by_name_im_left_tree(self, node_name):
        tree_iter = self._find_iter_by_name_in_left_tree(node_name)
        node = self.left_con.display_store.get_node_data(tree_iter)
        logger.warning(f'CP "{node.name}" from left to right root')
        return node

    def test_dd_dir_tree_cp(self):
        logger.info('Testing drag & drop copy of dir tree local to local')
        self.app.executor.start_op_execution_thread()
        expected_count = 12

        nodes = []
        nodes.append(self._find_node_by_name_im_left_tree('Art'))

        dd_data = DragAndDropData(dd_uid=UID(100), src_tree_controller=self.left_con, nodes=nodes)
        dst_tree_path = Gtk.TreePath.new_from_string('1')

        # Drag & drop 1 node, which represents a tree of 10 files and 2 dirs:
        completed_cmds: List[Command] = []
        all_complete = threading.Event()

        def on_command_complete(command: Command):
            completed_cmds.append(command)
            logger.info(f'Got a completed command (total: {len(completed_cmds)}, expecting: {expected_count})')
            if len(completed_cmds) >= expected_count:
                all_complete.set()

        dispatcher.connect(signal=actions.COMMAND_COMPLETE, receiver=on_command_complete)

        logger.info('Submitting drag & drop signal')
        dispatcher.send(signal=DRAG_AND_DROP_DIRECT, sender=actions.ID_RIGHT_TREE, drag_data=dd_data, tree_path=dst_tree_path, is_into=False)

        logger.info('Sleeping until we get what we want')
        if not all_complete.wait():
            raise RuntimeError('Timed out waiting for all commands to complete!')
        logger.info('Done!')

    def _do_and_wait_for_signal(self, action_func, signal, tree_id):
        received = threading.Event()

        def on_received():
            logger.debug(f'Received signal: {signal} from tree {tree_id}')
            received.set()

        dispatcher.connect(signal=signal, receiver=on_received, sender=tree_id)

        action_func()
        logger.debug(f'Waiting for signal: {signal} from tree {tree_id}')
        if not received.wait():
            raise RuntimeError(f'Timed out waiting for signal: {signal} from tree: {tree_id}')

    def test_dd_two_dir_trees_cp(self):
        logger.info('Testing drag & drop copy of 2 dir trees local to local')
        # Offset from 0:

        expected_count = 18

        # Need to first expand tree in order to find child nodes
        tree_iter = self._find_iter_by_name_in_left_tree('Art')
        tree_path = self.left_con.display_store.model.get_path(tree_iter)

        def action_func():
            self.left_con.tree_view.expand_row(path=tree_path, open_all=True)

        self._do_and_wait_for_signal(action_func, actions.NODE_EXPANSION_DONE, actions.ID_LEFT_TREE)

        nodes = []
        nodes.append(self._find_node_by_name_im_left_tree('Modern'))
        nodes.append(self._find_node_by_name_im_left_tree('Art'))

        dd_data = DragAndDropData(dd_uid=UID(100), src_tree_controller=self.left_con, nodes=nodes)
        dst_tree_path = Gtk.TreePath.new_from_string('1')

        # Drag & drop 1 node, which represents a tree of 10 files and 2 dirs:
        completed_cmds: List[Command] = []
        all_complete = threading.Event()

        def on_command_complete(command: Command):
            completed_cmds.append(command)
            logger.info(f'Got a completed command (total: {len(completed_cmds)}, expecting: {expected_count})')
            if len(completed_cmds) >= expected_count:
                all_complete.set()

        dispatcher.connect(signal=actions.COMMAND_COMPLETE, receiver=on_command_complete)

        logger.info('Submitting drag & drop signal')
        dispatcher.send(signal=DRAG_AND_DROP_DIRECT, sender=actions.ID_RIGHT_TREE, drag_data=dd_data, tree_path=dst_tree_path, is_into=False)

        self.app.executor.start_op_execution_thread()
        logger.info('Sleeping until we get what we want')
        if not all_complete.wait():
            raise RuntimeError('Timed out waiting for all commands to complete!')
        # TODO: verify correct nodes were copied
        logger.info('Done!')


def _name_equals_func(node_name, node) -> bool:
    if logger.isEnabledFor(logging.DEBUG) and not node.is_ephemereal():
        logger.debug(f'Examining node uid={node.uid} name={node.name} (looking for: {node_name})')
    return not node.is_ephemereal() and node.name == node_name
