import collections
import os
import shutil
import threading
import time
import unittest
import logging
from functools import partial
from typing import Callable, Deque, Iterable, List, Optional, Tuple

from py7zr import SevenZipFile
from pydispatch import dispatcher

from app_config import AppConfig
from cmd.cmd_interface import Command
from constants import TREE_TYPE_LOCAL_DISK
from index.uid.uid import UID
from model.display_tree.display_tree import DisplayTree
from model.node.display_node import DisplayNode
from outlet_app import OutletApplication
from ui import actions
from ui.actions import DELETE_SUBTREE, DRAG_AND_DROP_DIRECT
from ui.tree import root_path_config
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


# MOCK CLASS FNode
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼

class FNode:
    def __init__(self, name: str, size_bytes: int):
        self.name: str = name
        self.size_bytes: int = size_bytes

    @classmethod
    def is_dir(cls):
        return False

    def __repr__(self):
        return f'File("{self.name}" size={self.size_bytes})'


# MOCK CLASS DNode
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼

class DNode(FNode):
    def __init__(self, name: str, size_bytes: int, children: Optional[List] = None):
        super().__init__(name, size_bytes)
        if children is None:
            children = list()
        self.children: List[FNode] = children

    @classmethod
    def is_dir(cls):
        return True

    def __repr__(self):
        return f'Dir("{self.name}" size={self.size_bytes} children={len(self.children)})'


# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
# Static stuff
# ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼


def _get_name_lower(display_node: DisplayNode):
    return display_node.name.lower()


def _do_and_wait_for_signal(action_func, signal, tree_id):
    received = threading.Event()

    def on_received():
        logger.debug(f'Received signal: {signal} from tree {tree_id}')
        received.set()

    dispatcher.connect(signal=signal, receiver=on_received, sender=tree_id)

    action_func()
    logger.debug(f'Waiting for signal: {signal} from tree {tree_id}')
    if not received.wait():
        raise RuntimeError(f'Timed out waiting for signal: {signal} from tree: {tree_id}')


def _name_equals_func(node_name, node) -> bool:
    if logger.isEnabledFor(logging.DEBUG) and not node.is_ephemereal():
        logger.debug(f'Examining node uid={node.uid} name={node.name} (looking for: {node_name})')
    return not node.is_ephemereal() and node.name == node_name


INITIAL_TREE_LEFT = [
    FNode('American_Gothic.jpg', 2061397),
    FNode('Angry-Clown.jpg', 824641),
    DNode('Art', (88259+652220+239739+44487+479124)+(147975+275771+8098+247023+36344), [
        FNode('Dark-Art.png', 147975),
        FNode('Hokusai_Great-Wave.jpg', 275771),
        DNode('Modern', (88259+652220+239739+44487+479124), [
            FNode('1923-art.jpeg', 88259),
            FNode('43548-forbidden_planet.jpg', 652220),
            FNode('Dunno.jpg', 239739),
            FNode('felix-the-cat.jpg', 44487),
            FNode('Glow-Cat.png', 479124),
        ]),
        FNode('Mona-Lisa.jpeg', 8098),
        FNode('william-shakespeare.jpg', 247023),
        FNode('WTF.jpg', 36344),
    ]),
    FNode('Egypt.jpg', 154564),
    FNode('George-Floyd.png', 27601),
    FNode('Geriatric-Clown.jpg', 89182),
    FNode('Keep-calm-and-carry-on.jpg', 745698),
]


INITIAL_TREE_RIGHT = [
    FNode('Edvard-Munch-The-Scream.jpg', 114082),
    FNode('M83.jpg', 17329),
    FNode('oak-tree-sunset.jpg', 386888),
    FNode('Ocean-Wave.jpg', 83713),
    FNode('Starry-Night.jpg', 91699),
    FNode('we-can-do-it-poster.jpg', 390093),
]


# CLASS ChangeTest
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼

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
        config.write(root_path_config.make_tree_type_config_key(actions.ID_LEFT_TREE), TREE_TYPE_LOCAL_DISK)
        config.write(root_path_config.make_root_path_config_key(actions.ID_LEFT_TREE), os.path.join(TEST_TARGET_DIR, 'Left-Root'))
        # TODO: craft some kind of strategy for looking up UID for dir

        config.write(root_path_config.make_tree_type_config_key(actions.ID_RIGHT_TREE), TREE_TYPE_LOCAL_DISK)
        config.write(root_path_config.make_root_path_config_key(actions.ID_RIGHT_TREE), os.path.join(TEST_TARGET_DIR, 'Right-Root'))
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

        self._verify(self.left_con, INITIAL_TREE_LEFT)
        self._verify(self.right_con, INITIAL_TREE_RIGHT)
        logger.info(f'LOAD COMPLETE')

    def _verify_one_memcache_dir(self, tree_con, expected: List[FNode], actual: Iterable[DisplayNode],
                                 dir_deque: Deque[Tuple[List[FNode], Iterable[DisplayNode]]]):
        actual_iter = iter(actual)
        for i in range(0, len(expected)):
            expected_node: FNode = expected[i]
            try:
                actual_node: DisplayNode = next(actual_iter)
                logger.info(f'Examining: {actual_node} (expecting: {expected_node})')
                self.assertEqual(expected_node.name, actual_node.name)
                self.assertEqual(expected_node.size_bytes, actual_node.get_size_bytes())
                self.assertEqual(expected_node.is_dir(), actual_node.is_dir())
                logger.info(f'OK: {expected_node.name}')

                if expected_node.is_dir():
                    assert isinstance(expected_node, DNode)
                    expected_list: List[FNode] = expected_node.children
                    actual_list: Iterable[DisplayNode] = tree_con.get_tree().get_children(actual_node)
                    dir_deque.append((expected_list, actual_list))

            except StopIteration:
                self.fail(f'Tree "{tree_con.tree_id}" is missing node: {expected_node}')

        try:
            actual_node: DisplayNode = next(actual_iter)
            self.fail(f'Tree "{tree_con.tree_id}" has unexpected node: {actual_node}')
        except StopIteration:
            pass

    def _verify_one_display_dir(self, tree_con, expected: List[FNode], actual: List[DisplayNode],
                                dir_deque: Deque[Tuple[List[FNode], Iterable[DisplayNode]]]):
        """Displayed directories may not all be loaded. But we will _verify the ones that are"""
        actual_iter = iter(actual)
        for i in range(0, len(expected)):
            expected_node: FNode = expected[i]
            try:
                actual_node: DisplayNode = next(actual_iter)
                logger.info(f'Examining: {actual_node} (expecting: {expected_node})')
                self.assertEqual(expected_node.name, actual_node.name)
                self.assertEqual(expected_node.size_bytes, actual_node.get_size_bytes())
                self.assertEqual(expected_node.is_dir(), actual_node.is_dir())
                logger.info(f'OK: {expected_node.name}')

                if expected_node.is_dir():
                    assert isinstance(expected_node, DNode)
                    expected_list: List[FNode] = expected_node.children
                    actual_list: Iterable[DisplayNode] = tree_con.display_store.get_displayed_children_of(actual_node.uid)
                    if actual_list:
                        dir_deque.append((expected_list, actual_list))

            except StopIteration:
                self.fail(f'Tree "{tree_con.tree_id}" is missing displayed node: {expected_node}')

        try:
            actual_node: DisplayNode = next(actual_iter)
            self.fail(f'Tree "{tree_con.tree_id}" has unexpected displayed node: {actual_node}')
        except StopIteration:
            pass

    def _verify(self, tree_con: TreePanelController, expected_list_root: List[FNode]):
        logger.info(f'Verifying "{tree_con.tree_id}"')

        # Verify that all nodes loaded correctly into the cache, which will be reflected by the state of the DisplayTree:
        backing_tree: DisplayTree = tree_con.get_tree()

        dir_deque: Deque[Tuple[List[FNode], Iterable[DisplayNode]]] = collections.deque()
        """Each entry contains the expected and actual contents of a single dir"""

        actual_list: Iterable[DisplayNode] = backing_tree.get_children_for_root()

        # Cached nodes (in tree model)
        count_dir = 0
        dir_deque.append((expected_list_root, actual_list))
        while len(dir_deque) > 0:
            count_dir += 1
            expected_list, actual_list = dir_deque.popleft()
            # sort the actual list by name, since it is not required to be sorted
            actual_list: List[DisplayNode] = list(actual_list)
            actual_list.sort(key=_get_name_lower)
            self._verify_one_memcache_dir(tree_con, expected_list, actual_list, dir_deque)

        logger.info(f'Verified {count_dir} dirs in memcache for "{tree_con.tree_id}"')

        # Displayed nodes
        count_dir = 0
        actual_list: List[DisplayNode] = tree_con.display_store.get_displayed_children_of(None)
        dir_deque.append((expected_list_root, actual_list))
        while len(dir_deque) > 0:
            count_dir += 1
            expected_list, actual_list = dir_deque.popleft()
            # sort the actual list by name, since it is not required to be sorted
            actual_list: List[DisplayNode] = list(actual_list)
            actual_list.sort(key=_get_name_lower)
            self._verify_one_display_dir(tree_con, expected_list, actual_list, dir_deque)
        logger.info(f'Verified {count_dir} display dirs for "{tree_con.tree_id}"')

    def _find_iter_by_name_in_left_tree(self, node_name):
        name_equals_func: Callable = partial(_name_equals_func, node_name)
        tree_iter = self.left_con.display_store.find_in_tree(name_equals_func)
        if not tree_iter:
            self.fail(f'Expected to find node named "{node_name}"')
        return tree_iter

    def _find_node_by_name_im_left_tree(self, node_name):
        tree_iter = self._find_iter_by_name_in_left_tree(node_name)
        node = self.left_con.display_store.get_node_data(tree_iter)
        logger.info(f'Found "{node.name}"')
        return node

    def _do_and_verify(self, do_func: Callable, count_expected_cmds: int, wait_for_left: bool, wait_for_right: bool,
                       expected_left: List, expected_right: List):
        # Drag & drop 1 node, which represents a tree of 10 files and 2 dirs:
        completed_cmds: List[Command] = []
        all_commands_complete = threading.Event()
        left_stats_updated = threading.Event()
        right_stats_updated = threading.Event()

        def on_command_complete(sender, command: Command):
            completed_cmds.append(command)
            logger.info(f'Got a completed command (total: {len(completed_cmds)}, expecting: {count_expected_cmds})')
            if len(completed_cmds) >= count_expected_cmds:
                all_commands_complete.set()

        def on_stats_updated(sender):
            logger.info(f'Got signal: {actions.SUBTREE_STATS_UPDATED} for "{sender}"')
            if sender == self.left_con.tree_id:
                left_stats_updated.set()
            elif sender == self.right_con.tree_id:
                right_stats_updated.set()

        dispatcher.connect(signal=actions.COMMAND_COMPLETE, receiver=on_command_complete)
        dispatcher.connect(signal=actions.SUBTREE_STATS_UPDATED, receiver=on_stats_updated)

        do_func()

        self.app.executor.start_op_execution_thread()
        logger.info('Sleeping until we get what we want')
        if not all_commands_complete.wait():
            raise RuntimeError('Timed out waiting for all commands to complete!')
        if wait_for_left:
            if not left_stats_updated.wait():
                raise RuntimeError('Timed out waiting for Left stats to update!')
        if wait_for_right:
            if not right_stats_updated.wait():
                raise RuntimeError('Timed out waiting for Right stats to update!')

        self._verify(self.left_con, expected_left)
        self._verify(self.right_con, expected_right)
        logger.info('Done!')

    # TESTS
    # ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼

    def test_dd_single_file_cp(self):
        logger.info('Testing drag & drop copy of single file local right to local left')
        self.app.executor.start_op_execution_thread()
        # Offset from 0:
        src_tree_path = Gtk.TreePath.new_from_string('1')
        node: DisplayNode = self.right_con.display_store.get_node_data(src_tree_path)
        logger.info(f'CP "{node.name}" from right root to left root')

        nodes = [node]
        dd_data = DragAndDropData(dd_uid=UID(100), src_tree_controller=self.right_con, nodes=nodes)
        dst_tree_path = Gtk.TreePath.new_from_string('1')

        def drop():
            logger.info('Submitting drag & drop signal')
            dispatcher.send(signal=DRAG_AND_DROP_DIRECT, sender=actions.ID_LEFT_TREE, drag_data=dd_data, tree_path=dst_tree_path, is_into=False)

        final_tree_left = [
            FNode('American_Gothic.jpg', 2061397),
            FNode('Angry-Clown.jpg', 824641),
            DNode('Art', (88259 + 652220 + 239739 + 44487 + 479124) + (147975 + 275771 + 8098 + 247023 + 36344), [
                FNode('Dark-Art.png', 147975),
                FNode('Hokusai_Great-Wave.jpg', 275771),
                DNode('Modern', (88259 + 652220 + 239739 + 44487 + 479124), [
                    FNode('1923-art.jpeg', 88259),
                    FNode('43548-forbidden_planet.jpg', 652220),
                    FNode('Dunno.jpg', 239739),
                    FNode('felix-the-cat.jpg', 44487),
                    FNode('Glow-Cat.png', 479124),
                ]),
                FNode('Mona-Lisa.jpeg', 8098),
                FNode('william-shakespeare.jpg', 247023),
                FNode('WTF.jpg', 36344),
            ]),
            FNode('Egypt.jpg', 154564),
            FNode('George-Floyd.png', 27601),
            FNode('Geriatric-Clown.jpg', 89182),
            FNode('Keep-calm-and-carry-on.jpg', 745698),
            FNode('M83.jpg', 17329),
        ]

        self._do_and_verify(drop, count_expected_cmds=1, wait_for_left=True, wait_for_right=False,
                            expected_left=final_tree_left, expected_right=INITIAL_TREE_RIGHT)

    def test_dd_multi_file_cp(self):
        logger.info('Testing drag & drop copy of 4 files local right to local left')
        self.app.executor.start_op_execution_thread()

        # Simulate drag & drop based on position in list:
        nodes = []
        for num in range(0, 4):
            node: DisplayNode = self.right_con.display_store.get_node_data(Gtk.TreePath.new_from_string(f'{num}'))
            self.assertIsNotNone(node, f'Expected to find node at index {num}')
            nodes.append(node)
            logger.warning(f'CP "{node.name}" (#{num}) from right root to left root')

        dd_data = DragAndDropData(dd_uid=UID(100), src_tree_controller=self.right_con, nodes=nodes)
        dst_tree_path = Gtk.TreePath.new_from_string('1')
        dispatcher.send(signal=DRAG_AND_DROP_DIRECT, sender=actions.ID_LEFT_TREE, drag_data=dd_data, tree_path=dst_tree_path, is_into=False)

        def drop():
            logger.info('Submitting drag & drop signal')
            dispatcher.send(signal=DRAG_AND_DROP_DIRECT, sender=actions.ID_LEFT_TREE, drag_data=dd_data, tree_path=dst_tree_path, is_into=False)

        final_tree_left = [
            FNode('American_Gothic.jpg', 2061397),
            FNode('Angry-Clown.jpg', 824641),
            DNode('Art', (88259 + 652220 + 239739 + 44487 + 479124) + (147975 + 275771 + 8098 + 247023 + 36344), [
                FNode('Dark-Art.png', 147975),
                FNode('Hokusai_Great-Wave.jpg', 275771),
                DNode('Modern', (88259 + 652220 + 239739 + 44487 + 479124), [
                    FNode('1923-art.jpeg', 88259),
                    FNode('43548-forbidden_planet.jpg', 652220),
                    FNode('Dunno.jpg', 239739),
                    FNode('felix-the-cat.jpg', 44487),
                    FNode('Glow-Cat.png', 479124),
                ]),
                FNode('Mona-Lisa.jpeg', 8098),
                FNode('william-shakespeare.jpg', 247023),
                FNode('WTF.jpg', 36344),
            ]),
            FNode('Edvard-Munch-The-Scream.jpg', 114082),
            FNode('Egypt.jpg', 154564),
            FNode('George-Floyd.png', 27601),
            FNode('Geriatric-Clown.jpg', 89182),
            FNode('Keep-calm-and-carry-on.jpg', 745698),
            FNode('M83.jpg', 17329),
            FNode('oak-tree-sunset.jpg', 386888),
            FNode('Ocean-Wave.jpg', 83713),
        ]

        self._do_and_verify(drop, count_expected_cmds=4, wait_for_left=True, wait_for_right=False,
                            expected_left=final_tree_left, expected_right=INITIAL_TREE_RIGHT)

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

    def test_dd_one_dir_tree_cp(self):
        logger.info('Testing drag & drop copy of 1 dir tree local left to local right')
        self.app.executor.start_op_execution_thread()

        nodes = [
            self._find_node_by_name_im_left_tree('Art')
        ]

        dd_data = DragAndDropData(dd_uid=UID(100), src_tree_controller=self.left_con, nodes=nodes)
        dst_tree_path = Gtk.TreePath.new_from_string('1')

        # Drag & drop 1 node, which represents a tree of 10 files and 2 dirs:

        def drop():
            logger.info('Submitting drag & drop signal')
            dispatcher.send(signal=DRAG_AND_DROP_DIRECT, sender=actions.ID_RIGHT_TREE, drag_data=dd_data, tree_path=dst_tree_path, is_into=False)

        final_tree_right = [
            DNode('Art', (88259 + 652220 + 239739 + 44487 + 479124) + (147975 + 275771 + 8098 + 247023 + 36344), [
                FNode('Dark-Art.png', 147975),
                FNode('Hokusai_Great-Wave.jpg', 275771),
                DNode('Modern', (88259 + 652220 + 239739 + 44487 + 479124), [
                    FNode('1923-art.jpeg', 88259),
                    FNode('43548-forbidden_planet.jpg', 652220),
                    FNode('Dunno.jpg', 239739),
                    FNode('felix-the-cat.jpg', 44487),
                    FNode('Glow-Cat.png', 479124),
                ]),
                FNode('Mona-Lisa.jpeg', 8098),
                FNode('william-shakespeare.jpg', 247023),
                FNode('WTF.jpg', 36344),
            ]),
            FNode('Edvard-Munch-The-Scream.jpg', 114082),
            FNode('M83.jpg', 17329),
            FNode('oak-tree-sunset.jpg', 386888),
            FNode('Ocean-Wave.jpg', 83713),
            FNode('Starry-Night.jpg', 91699),
            FNode('we-can-do-it-poster.jpg', 390093),
        ]

        self._do_and_verify(drop, count_expected_cmds=12, wait_for_left=False, wait_for_right=True,
                            expected_left=INITIAL_TREE_LEFT, expected_right=final_tree_right)

    def test_dd_two_dir_trees_cp(self):
        logger.info('Testing drag & drop copy of 2 dir trees local left to local right')

        # Need to first expand tree in order to find child nodes
        tree_iter = self._find_iter_by_name_in_left_tree('Art')
        tree_path = self.left_con.display_store.model.get_path(tree_iter)

        def action_func():
            self.left_con.tree_view.expand_row(path=tree_path, open_all=True)

        _do_and_wait_for_signal(action_func, actions.NODE_EXPANSION_DONE, actions.ID_LEFT_TREE)

        nodes = [
            self._find_node_by_name_im_left_tree('Modern'),
            self._find_node_by_name_im_left_tree('Art')
        ]

        dd_data = DragAndDropData(dd_uid=UID(100), src_tree_controller=self.left_con, nodes=nodes)
        dst_tree_path = Gtk.TreePath.new_from_string('1')

        def drop():
            logger.info('Submitting drag & drop signal')
            dispatcher.send(signal=DRAG_AND_DROP_DIRECT, sender=actions.ID_RIGHT_TREE, drag_data=dd_data, tree_path=dst_tree_path, is_into=False)

        final_tree_right = [
            DNode('Art', (88259 + 652220 + 239739 + 44487 + 479124) + (147975 + 275771 + 8098 + 247023 + 36344), [
                FNode('Dark-Art.png', 147975),
                FNode('Hokusai_Great-Wave.jpg', 275771),
                DNode('Modern', (88259 + 652220 + 239739 + 44487 + 479124), [
                    FNode('1923-art.jpeg', 88259),
                    FNode('43548-forbidden_planet.jpg', 652220),
                    FNode('Dunno.jpg', 239739),
                    FNode('felix-the-cat.jpg', 44487),
                    FNode('Glow-Cat.png', 479124),
                ]),
                FNode('Mona-Lisa.jpeg', 8098),
                FNode('william-shakespeare.jpg', 247023),
                FNode('WTF.jpg', 36344),
            ]),
            FNode('Edvard-Munch-The-Scream.jpg', 114082),
            FNode('M83.jpg', 17329),
            DNode('Modern', (88259 + 652220 + 239739 + 44487 + 479124), [
                FNode('1923-art.jpeg', 88259),
                FNode('43548-forbidden_planet.jpg', 652220),
                FNode('Dunno.jpg', 239739),
                FNode('felix-the-cat.jpg', 44487),
                FNode('Glow-Cat.png', 479124),
            ]),
            FNode('oak-tree-sunset.jpg', 386888),
            FNode('Ocean-Wave.jpg', 83713),
            FNode('Starry-Night.jpg', 91699),
            FNode('we-can-do-it-poster.jpg', 390093),
        ]

        self._do_and_verify(drop, count_expected_cmds=18, wait_for_left=False, wait_for_right=True,
                            expected_left=INITIAL_TREE_LEFT, expected_right=final_tree_right)

    # TODO: test drop INTO

    def test_delete_subtree(self):
        logger.info('Testing delete subtree on left')

        # Need to first expand tree in order to find child nodes
        tree_iter = self._find_iter_by_name_in_left_tree('Art')
        tree_path = self.left_con.display_store.model.get_path(tree_iter)

        def action_func():
            self.left_con.tree_view.expand_row(path=tree_path, open_all=True)

        _do_and_wait_for_signal(action_func, actions.NODE_EXPANSION_DONE, actions.ID_LEFT_TREE)

        nodes = [
            self._find_node_by_name_im_left_tree('Art')
        ]

        def delete():
            logger.info('Submitting delete signal')
            dispatcher.send(signal=DELETE_SUBTREE, sender=actions.ID_LEFT_TREE, node_list=nodes)

        final_tree_left = [
            FNode('American_Gothic.jpg', 2061397),
            FNode('Angry-Clown.jpg', 824641),
            FNode('Egypt.jpg', 154564),
            FNode('George-Floyd.png', 27601),
            FNode('Geriatric-Clown.jpg', 89182),
            FNode('Keep-calm-and-carry-on.jpg', 745698),
        ]

        self._do_and_verify(delete, count_expected_cmds=18, wait_for_left=False, wait_for_right=True,
                            expected_left=final_tree_left, expected_right=INITIAL_TREE_RIGHT)

        # TODO: test delete overlapping subtrees (before starting execution thread)
