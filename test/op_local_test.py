import logging
import os
import threading
import time
from functools import partial
from typing import Callable

from pydispatch import dispatcher

from constants import TreeType
from model.node.node import Node, SPIDNodePair
from model.uid import UID
from signal_constants import ID_CENTRAL_EXEC, ID_LEFT_TREE, ID_RIGHT_TREE, Signal
from test import op_test_base
from test.op_test_base import DNode, FNode, INITIAL_LOCAL_TREE_LEFT, INITIAL_LOCAL_TREE_RIGHT, LOAD_TIMEOUT_SEC, OpTestBase, TEST_TARGET_DIR
from ui.gtk.tree.ui_listeners import DragAndDropData

import gi
gi.require_version("Gtk", "3.0")
from gi.repository import Gtk

logger = logging.getLogger(__name__)


class OpLocalTest(OpTestBase):
    """
    ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
    CLASS OpLocalTest

    WARNING! RUNNING THIS TEST WILL MODIFY/DELETE RUNTIME FILES!

    Currently, each test must be run in its own Python interpreter, due to Python/GTK3 failing to dispose of the app after each test.
    This means that each test must be run separately.

    # TODO: investigate using pytest-forked [https://github.com/pytest-dev/pytest-forked]
    ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼
    """

    def setUp(self) -> None:
        super().setUp()

        self.left_tree_initial = INITIAL_LOCAL_TREE_LEFT
        self.left_tree_type = TreeType.LOCAL_DISK
        self.left_tree_root_path = os.path.join(TEST_TARGET_DIR, 'Left-Root')

        self.right_tree_initial = INITIAL_LOCAL_TREE_RIGHT
        self.right_tree_type = TreeType.LOCAL_DISK
        self.right_tree_root_path = os.path.join(TEST_TARGET_DIR, 'Right-Root')

        self.do_setup()

    # TESTS
    # ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼

    def test_dd_single_file_cp(self):
        logger.info('Testing drag & drop copy of single file local right to local left')
        dispatcher.send(signal=Signal.RESUME_OP_EXECUTION, sender=ID_CENTRAL_EXEC)
        # Offset from 0:
        src_tree_path = Gtk.TreePath.new_from_string('1')
        sn: SPIDNodePair = self.right_con.display_store.build_sn_from_tree_path(src_tree_path)
        logger.info(f'CP "{sn.node.name}" from right root to left root')

        sn_list = [sn]
        dd_data = DragAndDropData(dd_uid=UID(100), src_treecon=self.right_con, sn_list=sn_list)
        dst_tree_path = Gtk.TreePath.new_from_string('1')

        def drop():
            logger.info('Submitting drag & drop signal')
            dispatcher.send(signal=Signal.DRAG_AND_DROP_DIRECT, sender=ID_LEFT_TREE, drag_data=dd_data, tree_path=dst_tree_path, is_into=False)

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

        self.do_and_verify(drop, count_expected_cmds=1, wait_for_left=True, wait_for_right=False,
                           expected_left=final_tree_left, expected_right=INITIAL_LOCAL_TREE_RIGHT)

    def test_dd_multi_file_cp(self):
        logger.info('Testing drag & drop copy of 4 files local right to local left')
        dispatcher.send(signal=Signal.RESUME_OP_EXECUTION, sender=ID_CENTRAL_EXEC)

        # Simulate drag & drop based on position in list:
        sn_list = []
        for num in range(0, 4):
            sn: SPIDNodePair = self.right_con.display_store.build_sn_from_tree_path(Gtk.TreePath.new_from_string(f'{num}'))
            self.assertIsNotNone(sn, f'Expected to find node at index {num}')
            sn_list.append(sn)
            logger.warning(f'CP "{sn.node.name}" (#{num}) from right root to left root')

        dd_data = DragAndDropData(dd_uid=UID(100), src_treecon=self.right_con, sn_list=sn_list)
        dst_tree_path = Gtk.TreePath.new_from_string('1')
        dispatcher.send(signal=Signal.DRAG_AND_DROP_DIRECT, sender=ID_LEFT_TREE, drag_data=dd_data, tree_path=dst_tree_path, is_into=False)

        def drop():
            logger.info('Submitting drag & drop signal')
            dispatcher.send(signal=Signal.DRAG_AND_DROP_DIRECT, sender=ID_LEFT_TREE, drag_data=dd_data, tree_path=dst_tree_path, is_into=False)

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

        self.do_and_verify(drop, count_expected_cmds=4, wait_for_left=True, wait_for_right=False,
                           expected_left=final_tree_left, expected_right=INITIAL_LOCAL_TREE_RIGHT)

    def test_bad_dd_dir_tree_cp(self):
        logger.info('Testing negative case: drag & drop copy of duplicate nodes local to local')
        dispatcher.send(signal=Signal.RESUME_OP_EXECUTION, sender=ID_CENTRAL_EXEC)
        node_name = 'Art'

        name_equals_func_bound: Callable = partial(op_test_base.name_equals_func, node_name)

        sn_list = []
        # Duplicate the node 3 times. This is a good test of our reduction logic
        for num in range(0, 3):
            tree_iter = self.left_con.display_store.find_in_tree(name_equals_func_bound)
            sn = None
            if tree_iter:
                sn = self.left_con.display_store.build_sn_from_tree_path(tree_iter)
            self.assertIsNotNone(sn, f'Expected to find node named "{node_name}"')
            sn_list.append(sn)
            logger.warning(f'CP "{sn.node.name}" (#{num}) from left root to right root')

        dd_data = DragAndDropData(dd_uid=UID(100), src_treecon=self.left_con, sn_list=sn_list)
        dst_tree_path = Gtk.TreePath.new_from_string('1')

        # Verify that 3 identical nodes causes an error:
        with self.assertRaises(RuntimeError) as context:
            dispatcher.send(signal=Signal.DRAG_AND_DROP_DIRECT, sender=ID_RIGHT_TREE, drag_data=dd_data, tree_path=dst_tree_path, is_into=False)
            logger.info('Sleeping')
            time.sleep(10)  # in seconds
            self.assertFalse(True, 'If we got here we failed!')

    def test_dd_one_dir_tree_cp(self):
        logger.info('Testing drag & drop copy of 1 dir tree local left to local right')
        dispatcher.send(signal=Signal.RESUME_OP_EXECUTION, sender=ID_CENTRAL_EXEC)

        sn_list = [
            self.find_node_by_name_in_left_tree('Art')
        ]

        dd_data = DragAndDropData(dd_uid=UID(100), src_treecon=self.left_con, sn_list=sn_list)
        dst_tree_path = Gtk.TreePath.new_from_string('1')

        # Drag & drop 1 node, which represents a tree of 10 files and 2 dirs:

        def drop():
            logger.info('Submitting drag & drop signal')
            dispatcher.send(signal=Signal.DRAG_AND_DROP_DIRECT, sender=ID_RIGHT_TREE, drag_data=dd_data, tree_path=dst_tree_path, is_into=False)

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

        self.do_and_verify(drop, count_expected_cmds=12, wait_for_left=False, wait_for_right=True,
                           expected_left=INITIAL_LOCAL_TREE_LEFT, expected_right=final_tree_right)

    def test_dd_two_dir_trees_cp(self):
        logger.info('Testing drag & drop copy of 2 dir trees local left to local right')

        self.expand_visible_node(self.left_con, 'Art')

        sn_list = [
            self.find_node_by_name_in_left_tree('Modern'),
            self.find_node_by_name_in_left_tree('Art')
        ]

        dd_data = DragAndDropData(dd_uid=UID(100), src_treecon=self.left_con, sn_list=sn_list)
        dst_tree_path = Gtk.TreePath.new_from_string('1')

        def drop():
            logger.info('Submitting drag & drop signal')
            dispatcher.send(signal=Signal.DRAG_AND_DROP_DIRECT, sender=ID_RIGHT_TREE, drag_data=dd_data, tree_path=dst_tree_path, is_into=False)

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

        self.do_and_verify(drop, count_expected_cmds=18, wait_for_left=False, wait_for_right=True,
                           expected_left=INITIAL_LOCAL_TREE_LEFT, expected_right=final_tree_right)

    def test_dd_into(self):
        logger.info('Testing drag & drop copy of 3 files into dir node')

        sn_list = [
            self.find_node_by_name(self.right_con, 'M83.jpg'),
            self.find_node_by_name(self.right_con, 'Ocean-Wave.jpg'),
            self.find_node_by_name(self.right_con, 'Starry-Night.jpg'),
        ]

        dd_data = DragAndDropData(dd_uid=UID(100), src_treecon=self.right_con, sn_list=sn_list)

        node_iter = self.find_iter_by_name(self.left_con, 'Art')
        dst_tree_path = self.left_con.display_store.model.get_path(node_iter)

        def drop():
            logger.info('Submitting drag & drop signal')
            dispatcher.send(signal=Signal.DRAG_AND_DROP_DIRECT, sender=ID_LEFT_TREE, drag_data=dd_data, tree_path=dst_tree_path, is_into=True)

        final_tree_left = [
            FNode('American_Gothic.jpg', 2061397),
            FNode('Angry-Clown.jpg', 824641),
            DNode('Art', (88259 + 652220 + 239739 + 44487 + 479124) + (147975 + 275771 + 8098 + 247023 + 36344), [
                FNode('Dark-Art.png', 147975),
                FNode('Hokusai_Great-Wave.jpg', 275771),
                FNode('M83.jpg', 17329),
                DNode('Modern', (88259 + 652220 + 239739 + 44487 + 479124), [
                    FNode('1923-art.jpeg', 88259),
                    FNode('43548-forbidden_planet.jpg', 652220),
                    FNode('Dunno.jpg', 239739),
                    FNode('felix-the-cat.jpg', 44487),
                    FNode('Glow-Cat.png', 479124),
                ]),
                FNode('Mona-Lisa.jpeg', 8098),
                FNode('Ocean-Wave.jpg', 83713),
                FNode('Starry-Night.jpg', 91699),
                FNode('william-shakespeare.jpg', 247023),
                FNode('WTF.jpg', 36344),
            ]),
            FNode('Egypt.jpg', 154564),
            FNode('George-Floyd.png', 27601),
            FNode('Geriatric-Clown.jpg', 89182),
            FNode('Keep-calm-and-carry-on.jpg', 745698),
        ]

        # Do not wait for Left stats to be updated; the node we are dropping into is not expanded so there is no need for it to be called
        self.do_and_verify(drop, count_expected_cmds=3, wait_for_left=False, wait_for_right=False,
                           expected_left=final_tree_left, expected_right=INITIAL_LOCAL_TREE_RIGHT)

    def test_dd_left_then_dd_right(self):
        # FIXME
        logger.info('Testing CP tree to right followed by CP of same tree to left')

        self.expand_visible_node(self.left_con, 'Art')

        sn_list_batch_1 = [
            self.find_node_by_name(self.left_con, 'Modern')
        ]

        def drop_both_sides():
            logger.info('Submitting first drag & drop signal')
            dd_data = DragAndDropData(dd_uid=UID(100), src_treecon=self.left_con, sn_list=sn_list_batch_1)
            dst_tree_path = Gtk.TreePath.new_from_string('2')

            drop_complete = threading.Event()
            expected_count = 6

            right_stats_updated = threading.Event()

            def on_stats_updated(sender):
                logger.info(f'Got signal: {Signal.REFRESH_SUBTREE_STATS_DONE} for "{sender}"')
                right_stats_updated.set()

            dispatcher.connect(signal=Signal.REFRESH_SUBTREE_STATS_DONE, receiver=on_stats_updated)

            def on_node_upserted(sender: str, node: Node):
                on_node_upserted.count += 1
                logger.info(f'Got upserted node (total: {on_node_upserted.count}, expecting: {expected_count})')
                if on_node_upserted.count >= expected_count:
                    drop_complete.set()
                    # disconnect this listener; it will cause confusion in the next stage
                    dispatcher.disconnect(signal=Signal.NODE_UPSERTED, receiver=on_node_upserted)

            on_node_upserted.count = 0

            dispatcher.connect(signal=Signal.NODE_UPSERTED, receiver=on_node_upserted)

            dispatcher.send(signal=Signal.DRAG_AND_DROP_DIRECT, sender=ID_RIGHT_TREE, drag_data=dd_data, tree_path=dst_tree_path, is_into=False)

            logger.info('Waiting for drop to complete...')

            if not drop_complete.wait(LOAD_TIMEOUT_SEC):
                raise RuntimeError('Timed out waiting for drag to complete!')

            # Waiting for the upsert signal is not quite enough, because it does not guarantee that the node has been populated.
            # Waiting until stats update should get us there:
            if not right_stats_updated.wait(LOAD_TIMEOUT_SEC):
                raise RuntimeError('Timed out waiting for stats to update!')

            # Now find the dropped nodes in right tree...
            sn_list_batch_2 = [
                self.find_node_by_name(self.right_con, 'Modern')
            ]

            dd_data = DragAndDropData(dd_uid=UID(100), src_treecon=self.right_con, sn_list=sn_list_batch_2)
            dst_tree_path = Gtk.TreePath.new_from_string('1')
            # Drop into left tree:
            logger.info('Submitting second drag & drop signal')
            dispatcher.send(signal=Signal.DRAG_AND_DROP_DIRECT, sender=ID_LEFT_TREE, drag_data=dd_data, tree_path=dst_tree_path, is_into=False)

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
            DNode('Modern', (88259 + 652220 + 239739 + 44487 + 479124), [
                FNode('1923-art.jpeg', 88259),
                FNode('43548-forbidden_planet.jpg', 652220),
                FNode('Dunno.jpg', 239739),
                FNode('felix-the-cat.jpg', 44487),
                FNode('Glow-Cat.png', 479124),
            ]),
        ]

        final_tree_right = [
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

        self.do_and_verify(drop_both_sides, count_expected_cmds=12, wait_for_left=True, wait_for_right=True,
                           expected_left=final_tree_left, expected_right=final_tree_right)

    def test_dd_then_rm(self):
        logger.info('Testing CP tree to right followed by RM of copied nodes')

        self.expand_visible_node(self.left_con, 'Art')

        sn_list_batch_1 = [
            self.find_node_by_name(self.left_con, 'Modern')
        ]

        def drop_then_delete():
            logger.info('Submitting drag & drop signal')
            dd_data = DragAndDropData(dd_uid=UID(100), src_treecon=self.left_con, sn_list=sn_list_batch_1)
            dst_tree_path = Gtk.TreePath.new_from_string('2')

            drop_complete = threading.Event()
            expected_count = 6

            right_stats_updated = threading.Event()

            def on_stats_updated(sender):
                logger.info(f'Got signal: {Signal.REFRESH_SUBTREE_STATS_DONE} for "{sender}"')
                right_stats_updated.set()

            dispatcher.connect(signal=Signal.REFRESH_SUBTREE_STATS_DONE, receiver=on_stats_updated)

            def on_node_upserted(sender: str, node: Node):
                on_node_upserted.count += 1
                logger.info(f'Got upserted node (total: {on_node_upserted.count}, expecting: {expected_count})')
                if on_node_upserted.count >= expected_count:
                    drop_complete.set()

            on_node_upserted.count = 0

            dispatcher.connect(signal=Signal.NODE_UPSERTED, receiver=on_node_upserted)

            dispatcher.send(signal=Signal.DRAG_AND_DROP_DIRECT, sender=ID_RIGHT_TREE, drag_data=dd_data, tree_path=dst_tree_path, is_into=False)

            logger.info('Waiting for drop to complete...')

            if not drop_complete.wait(LOAD_TIMEOUT_SEC):
                raise RuntimeError('Timed out waiting for drag to complete!')

            # Waiting for the upsert signal is not quite enough, because it does not guarantee that the node has been populated.
            # Waiting until stats update should get us there:
            if not right_stats_updated.wait(LOAD_TIMEOUT_SEC):
                raise RuntimeError('Timed out waiting for stats to update!')

            # Now find the dropped nodes in right tree...
            node_list_batch_2 = [
                self.find_node_by_name(self.right_con, 'Modern').node
            ]

            # Now delete the nodes which were just dropped
            logger.info('Submitting delete signal')
            dispatcher.send(signal=Signal.DELETE_SUBTREE, sender=ID_RIGHT_TREE, node_list=node_list_batch_2)

        # The end result should be that nothing has changed
        self.do_and_verify(drop_then_delete, count_expected_cmds=12, wait_for_left=False, wait_for_right=True,
                           expected_left=INITIAL_LOCAL_TREE_LEFT, expected_right=INITIAL_LOCAL_TREE_RIGHT)

    def test_delete_subtree(self):
        logger.info('Testing delete subtree on left')

        # Need to first expand tree in order to find child nodes
        tree_iter = self.find_iter_by_name_in_left_tree('Art')
        tree_path = self.left_con.display_store.model.get_path(tree_iter)

        def action_func():
            self.left_con.tree_view.expand_row(path=tree_path, open_all=True)

        op_test_base.do_and_wait_for_signal(action_func, Signal.NODE_EXPANSION_DONE, ID_LEFT_TREE)

        nodes = [
            self.find_node_by_name_in_left_tree('Art')
        ]

        def delete():
            logger.info('Submitting delete signal')
            dispatcher.send(signal=DELETE_SUBTREE, sender=ID_LEFT_TREE, node_list=nodes)

        final_tree_left = [
            FNode('American_Gothic.jpg', 2061397),
            FNode('Angry-Clown.jpg', 824641),
            FNode('Egypt.jpg', 154564),
            FNode('George-Floyd.png', 27601),
            FNode('Geriatric-Clown.jpg', 89182),
            FNode('Keep-calm-and-carry-on.jpg', 745698),
        ]

        self.do_and_verify(delete, count_expected_cmds=12, wait_for_left=True, wait_for_right=False,
                           expected_left=final_tree_left, expected_right=INITIAL_LOCAL_TREE_RIGHT)

    def test_delete_superset(self):
        logger.info('Testing delete tree followed by superset of tree (on left)')

        self.expand_visible_node(self.left_con, 'Art')

        nodes_1 = [
            self.find_node_by_name_in_left_tree('Modern').node
        ]
        nodes_2 = [
            self.find_node_by_name_in_left_tree('Art').node
        ]

        def delete():
            logger.info('Submitting delete signal 1: RM "Modern"')
            dispatcher.send(signal=Signal.DELETE_SUBTREE, sender=ID_LEFT_TREE, node_list=nodes_1)
            logger.info('Submitting delete signal 2: RM "Art"')
            dispatcher.send(signal=Signal.DELETE_SUBTREE, sender=ID_LEFT_TREE, node_list=nodes_2)

        final_tree_left = [
            FNode('American_Gothic.jpg', 2061397),
            FNode('Angry-Clown.jpg', 824641),
            FNode('Egypt.jpg', 154564),
            FNode('George-Floyd.png', 27601),
            FNode('Geriatric-Clown.jpg', 89182),
            FNode('Keep-calm-and-carry-on.jpg', 745698),
        ]

        self.do_and_verify(delete, count_expected_cmds=12, wait_for_left=True, wait_for_right=False,
                           expected_left=final_tree_left, expected_right=INITIAL_LOCAL_TREE_RIGHT)

    def test_delete_subset(self):
        logger.info('Testing delete tree followed by subset of tree (on left)')

        self.expand_visible_node(self.left_con, 'Art')

        nodes_batch_1 = [
            self.find_node_by_name_in_left_tree('Art').node
        ]
        nodes_batch_2 = [
            self.find_node_by_name_in_left_tree('Modern').node
        ]

        def delete():
            logger.info('Submitting delete signal 1: RM "Art"')
            dispatcher.send(signal=Signal.DELETE_SUBTREE, sender=ID_LEFT_TREE, node_list=nodes_batch_1)
            logger.info('Submitting delete signal 1: RM "Modern"')
            dispatcher.send(signal=Signal.DELETE_SUBTREE, sender=ID_LEFT_TREE, node_list=nodes_batch_2)

        final_tree_left = [
            FNode('American_Gothic.jpg', 2061397),
            FNode('Angry-Clown.jpg', 824641),
            FNode('Egypt.jpg', 154564),
            FNode('George-Floyd.png', 27601),
            FNode('Geriatric-Clown.jpg', 89182),
            FNode('Keep-calm-and-carry-on.jpg', 745698),
        ]

        self.do_and_verify(delete, count_expected_cmds=12, wait_for_left=True, wait_for_right=False,
                           expected_left=final_tree_left, expected_right=INITIAL_LOCAL_TREE_RIGHT)

    # TODO: Test: delete tree, then copy onto the deleted nodes

    # TODO: Test: Copy 3-level tree, then copy new version of 2 levels of that tree

    # TODO: MV op tests

    # TODO: UP op tests

    # TODO: combinations of op types
