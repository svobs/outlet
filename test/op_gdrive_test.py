import logging
import os
import threading
import time
import warnings
from functools import partial
from typing import Callable, List

from pydispatch import dispatcher

from constants import TreeType
from model.node.gdrive_node import GDriveNode
from model.node.node import TNode, SPIDNodePair
from model.uid import UID
from signal_constants import ID_CENTRAL_EXEC, ID_GLOBAL_CACHE, ID_LEFT_TREE, ID_RIGHT_TREE, Signal
from test import op_test_base
from test.op_test_base import DNode, FNode, INITIAL_LOCAL_TREE_LEFT, LOAD_TIMEOUT_SEC, OpTestBase, TEST_TARGET_DIR
from ui.gtk.tree.ui_listeners import DragAndDropData

import gi
gi.require_version("Gtk", "3.0")
from gi.repository import Gtk

logger = logging.getLogger(__name__)


# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
# Static stuff
# ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼

INITIAL_GDRIVE_TREE_RIGHT = [
]

# Suppress spurious Google Drive API warning.
# See https://stackoverflow.com/questions/26563711/disabling-python-3-2-resourcewarning/26620811
warnings.simplefilter("ignore", ResourceWarning)


class OpGDriveTest(OpTestBase):
    """
    ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
    CLASS OpGDriveTest

    WARNING! RUNNING THIS TEST WILL MODIFY/DELETE RUNTIME FILES!

    Currently, each test must be run in its own Python interpreter, due to Python/GTK3 failing to dispose of the app after each test.
    This means that each test must be run separately.
    ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼
    """

    def setUp(self) -> None:
        super().setUp()

        self.left_tree_initial = INITIAL_LOCAL_TREE_LEFT
        self.left_tree_type = TreeType.LOCAL_DISK
        self.left_tree_root_path = os.path.join(TEST_TARGET_DIR, 'Left-Root')

        self.right_tree_initial = INITIAL_GDRIVE_TREE_RIGHT
        self.right_tree_type = TreeType.GDRIVE
        self.right_tree_root_path = '/My Drive/Test'
        # IMPORTANT NOTE: currently need to manually inspect the GDrive database and get UID of 'Test' folder
        self.right_tree_root_uid = 8847219

        self.do_setup(do_before_verify_func=self._cleanup_gdrive_local_and_remote)

    def tearDown(self) -> None:
        # self._cleanup_gdrive_local_and_remote()
        super(OpGDriveTest, self).tearDown()

    def _cleanup_gdrive_local_and_remote(self):
        # self._delete_all_right_tree_displayed_rows_from_cacheman()

        self._delete_all_files_in_gdrive_test_folder()

    def _delete_all_right_tree_displayed_rows_from_cacheman(self):
        displayed_row_list: List[TNode] = list(self.right_con.display_store.displayed_guid_dict.values())
        if not displayed_row_list:
            logger.info('No displayed rows found in right tree; nothing to clean up')
            return

        logger.info(f'Found {len(displayed_row_list)} displayed rows (will delete) for right tree: {self.right_tree_root_path}')

        # If we have displayed rows to delete, then need to wait for StatsUpdated signal before we proceed
        right_stats_updated = threading.Event()

        def on_stats_updated(sender):
            logger.info(f'Got signal: {Signal.REFRESH_SUBTREE_STATS_COMPLETELY_DONE} for "{sender}"')
            if sender == self.right_con.tree_id:
                right_stats_updated.set()

        dispatcher.connect(signal=Signal.REFRESH_SUBTREE_STATS_COMPLETELY_DONE, receiver=on_stats_updated, sender=ID_RIGHT_TREE)

        for node in displayed_row_list:
            logger.warning(f'Deleting node via cacheman: {node}')
            assert isinstance(node, GDriveNode)
            self.backend.cacheman.remove_subtree(node, to_trash=False)

        logger.info('Waiting for Right tree stats to be completely done...')
        if not right_stats_updated.wait(LOAD_TIMEOUT_SEC):
            raise RuntimeError('Timed out waiting for Right stats to update!')

        logger.info('Done with displayed rows cleanup')

    def _delete_all_files_in_gdrive_test_folder(self):
        # delete all files which may have been uploaded to GDrive. Goes around the program cache
        logger.info('Connecting to GDrive to find files in remote test folder')

        parent_node: TNode = self.backend.cacheman.get_node_for_uid(self.right_tree_root_uid)
        assert isinstance(parent_node, GDriveNode)
        # VERY IMPORTANT: fail if name doesn't match - don't want to delete the wrong folder!
        self.assertEqual(parent_node.name, 'Test')

        # FIXME: this is broken
        client = self.backend.cacheman.get_gdrive_client()
        children = client.get_all_children_for_parent(parent_node.goog_id)
        logger.info(f'GDrive server query found {len(children)} child nodes for parent: {parent_node.name}')

        for child in children:
            logger.warning(f'Deleting node via GDrive API: {child}')
            client.hard_delete(child.goog_id)
        logger.info('Done with GDrive remote cleanup')

        # Now download latest changes from server so our local cacheman is up-to-date
        logger.info('Syncing latest changes from GDrive server...')
        self.backend.cacheman.sync_and_get_gdrive_master_tree(ID_GLOBAL_CACHE)

    # TESTS
    # ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼

    def test_dd_single_file_cp(self):
        logger.info('Testing drag & drop copy of single file local Left to GDrive Right')
        dispatcher.send(signal=Signal.RESUME_OP_EXECUTION, sender=ID_CENTRAL_EXEC)
        # Offset from 0:
        src_tree_path = Gtk.TreePath.new_from_string('1')
        sn: SPIDNodePair = self.left_con.display_store.get_node_data(src_tree_path)
        logger.info(f'CP "{sn.node.name}" from left root to left root')

        sn_list = [sn]
        dd_data = DragAndDropData(dd_uid=UID(100), src_treecon=self.right_con, sn_list=sn_list)
        dst_tree_path = None    # top-level drop

        def drop():
            logger.info('Submitting drag & drop signal')
            dispatcher.send(signal=Signal.DRAG_AND_DROP_DIRECT, sender=ID_RIGHT_TREE, drag_data=dd_data, tree_path=dst_tree_path, is_into=False)

        final_tree_right = [
            FNode('Angry-Clown.jpg', 824641),
        ]

        self.do_and_verify(drop, count_expected_cmds=1, wait_for_left=False, wait_for_right=True,
                           expected_left=INITIAL_LOCAL_TREE_LEFT, expected_right=final_tree_right)

    def test_dd_multi_file_cp(self):
        logger.info('Testing drag & drop copy of 4 files local left to local right')
        dispatcher.send(signal=Signal.RESUME_OP_EXECUTION, sender=ID_CENTRAL_EXEC)

        # Simulate drag & drop based on position in list:
        sn_list = []
        for num in range(3, 7):
            sn: SPIDNodePair = self.left_con.display_store.get_node_data(Gtk.TreePath.new_from_string(f'{num}'))
            self.assertIsNotNone(sn, f'Expected to find node at index {num}')
            sn_list.append(sn)
            logger.warning(f'CP "{sn.node.name}" (#{num}) from left root to right root')

        dd_data = DragAndDropData(dd_uid=UID(100), src_treecon=self.left_con, sn_list=sn_list)
        dst_tree_path = None
        dispatcher.send(signal=Signal.DRAG_AND_DROP_DIRECT, sender=ID_RIGHT_TREE, drag_data=dd_data, tree_path=dst_tree_path, is_into=False)

        def drop():
            logger.info('Submitting drag & drop signal')
            dispatcher.send(signal=Signal.DRAG_AND_DROP_DIRECT, sender=ID_LEFT_TREE, drag_data=dd_data, tree_path=dst_tree_path, is_into=False)

        final_tree_right = [
            FNode('Egypt.jpg', 154564),
            FNode('George-Floyd.png', 27601),
            FNode('Geriatric-Clown.jpg', 89182),
            FNode('Keep-calm-and-carry-on.jpg', 745698),
        ]

        self.do_and_verify(drop, count_expected_cmds=4, wait_for_left=False, wait_for_right=True,
                           expected_left=INITIAL_LOCAL_TREE_LEFT, expected_right=final_tree_right)

    def test_bad_dd_dir_tree_cp(self):
        logger.info('Testing negative case: drag & drop copy of duplicate nodes local to GDrive')
        dispatcher.send(signal=Signal.RESUME_OP_EXECUTION, sender=ID_CENTRAL_EXEC)
        node_name = 'Art'

        name_equals_func_bound: Callable = partial(op_test_base.name_equals_func, node_name)

        sn_list = []
        # Duplicate the node 3 times. This is a good test of our reduction logic
        for num in range(0, 3):
            tree_iter = self.left_con.display_store.find_in_tree(name_equals_func_bound)
            sn = self.left_con.display_store.get_node_data(tree_iter)
            self.assertIsNotNone(sn, f'Expected to find node named "{node_name}"')
            sn_list.append(sn)
            logger.warning(f'CP "{sn.node.name}" (#{num}) from left root to right root')

        dd_data = DragAndDropData(dd_uid=UID(100), src_treecon=self.left_con, sn_list=sn_list)
        dst_tree_path = None

        # Verify that 3 identical nodes causes an error:
        with self.assertRaises(RuntimeError) as context:
            dispatcher.send(signal=Signal.DRAG_AND_DROP_DIRECT, sender=ID_RIGHT_TREE, drag_data=dd_data, tree_path=dst_tree_path, is_into=False)
            logger.info('Sleeping')
            time.sleep(10)  # in seconds
            self.assertFalse(True, 'If we got here we failed!')

    def _cp_single_tree_into_right(self):
        sn_list = [
            self.find_node_by_name_in_left_tree('Art')
        ]

        dd_data = DragAndDropData(dd_uid=UID(100), src_treecon=self.left_con, sn_list=sn_list)
        dst_tree_path = None

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
        ]

        self.do_and_verify(drop, count_expected_cmds=12, wait_for_left=False, wait_for_right=True,
                           expected_left=INITIAL_LOCAL_TREE_LEFT, expected_right=final_tree_right)

    def test_dd_one_dir_tree_cp(self):
        """INTERESTING NOTE: try alternating execution of test_dd_one_dir_tree_cp() and test_dd_two_dir_trees_cp()
        to illuminate an interesting GDrive sync race condition"""
        logger.info('Testing drag & drop copy of 1 dir tree local left to GDrive right')
        dispatcher.send(signal=Signal.RESUME_OP_EXECUTION, sender=ID_CENTRAL_EXEC)

        self._cp_single_tree_into_right()

    def test_dd_two_dir_trees_cp(self):
        logger.info('Testing drag & drop copy of 2 dir trees local left to local right')

        self.expand_visible_node(self.left_con, 'Art')

        sn_list = [
            self.find_node_by_name_in_left_tree('Modern'),
            self.find_node_by_name_in_left_tree('Art')
        ]

        dd_data = DragAndDropData(dd_uid=UID(100), src_treecon=self.left_con, sn_list=sn_list)
        dst_tree_path = None

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
            DNode('Modern', (88259 + 652220 + 239739 + 44487 + 479124), [
                FNode('1923-art.jpeg', 88259),
                FNode('43548-forbidden_planet.jpg', 652220),
                FNode('Dunno.jpg', 239739),
                FNode('felix-the-cat.jpg', 44487),
                FNode('Glow-Cat.png', 479124),
            ]),
        ]

        self.do_and_verify(drop, count_expected_cmds=18, wait_for_left=False, wait_for_right=True,
                           expected_left=INITIAL_LOCAL_TREE_LEFT, expected_right=final_tree_right)

    def test_dd_into(self):
        logger.info('Testing drag & drop copy of 2 files into dir node')

        # Setup: cp 'Art' tree to right:
        self._cp_single_tree_into_right()

        # Note: the test code currently will hang if the user has already expanded the node
        self.expand_visible_node(self.right_con, 'Art')

        sn_list = [
            self.find_node_by_name(self.left_con, 'American_Gothic.jpg'),
            self.find_node_by_name(self.left_con, 'George-Floyd.png'),
        ]

        dd_data = DragAndDropData(dd_uid=UID(100), src_treecon=self.right_con, sn_list=sn_list)

        node_iter = self.find_iter_by_name(self.right_con, 'Modern')
        dst_tree_path = self.right_con.display_store.model.get_path(node_iter)

        def drop():
            logger.info('Submitting drag & drop signal')
            dispatcher.send(signal=Signal.DRAG_AND_DROP_DIRECT, sender=ID_RIGHT_TREE, drag_data=dd_data, tree_path=dst_tree_path, is_into=True)

        final_tree_right = [
            DNode('Art', (88259 + 652220 + 239739 + 44487 + 479124) + (147975 + 275771 + 8098 + 247023 + 36344 + 27601 + 2061397), [
                FNode('Dark-Art.png', 147975),
                FNode('Hokusai_Great-Wave.jpg', 275771),
                DNode('Modern', (88259 + 652220 + 239739 + 44487 + 479124 + 27601 + 2061397), [
                    FNode('1923-art.jpeg', 88259),
                    FNode('43548-forbidden_planet.jpg', 652220),
                    FNode('American_Gothic.jpg', 2061397),
                    FNode('Dunno.jpg', 239739),
                    FNode('felix-the-cat.jpg', 44487),
                    FNode('George-Floyd.png', 27601),
                    FNode('Glow-Cat.png', 479124),
                ]),
                FNode('Mona-Lisa.jpeg', 8098),
                FNode('william-shakespeare.jpg', 247023),
                FNode('WTF.jpg', 36344),
            ]),
        ]

        # Do not wait for Left stats to be updated; the node we are dropping into is not expanded so there is no need for it to be called
        self.do_and_verify(drop, count_expected_cmds=2, wait_for_left=False, wait_for_right=True,
                           expected_left=INITIAL_LOCAL_TREE_LEFT, expected_right=final_tree_right)

    def test_dd_left_then_dd_right(self):
        # FIXME: it seems this test is completely broken. Figure out why
        logger.info('Testing CP tree to right followed by CP of same tree to left')

        self.expand_visible_node(self.left_con, 'Art')

        nodes_batch_1 = [
            self.find_node_by_name(self.left_con, 'Modern')
        ]

        def drop_both_sides():
            logger.info('[0] Submitting first drag & drop signal: 6 nodes from left to right')
            dd_data = DragAndDropData(dd_uid=UID(100), src_treecon=self.left_con, sn_list=nodes_batch_1)
            dst_tree_path = None

            drop_complete = threading.Event()
            expected_count = 6

            right_stats_updated = threading.Event()

            def on_stats_updated(sender):
                logger.info(f'Got signal: {Signal.REFRESH_SUBTREE_STATS_COMPLETELY_DONE} for "{sender}"')
                right_stats_updated.set()

            dispatcher.connect(signal=Signal.REFRESH_SUBTREE_STATS_COMPLETELY_DONE, receiver=on_stats_updated, sender=ID_RIGHT_TREE)

            def on_node_upserted(sender: str, node: TNode):
                on_node_upserted.count += 1
                logger.info(f'Got upserted node "{node.name}" (total: {on_node_upserted.count}, expecting: {expected_count})')
                if on_node_upserted.count >= expected_count:
                    drop_complete.set()
                    dispatcher.disconnect(signal=Signal.NODE_UPSERTED, receiver=on_node_upserted)

            on_node_upserted.count = 0

            dispatcher.connect(signal=Signal.NODE_UPSERTED, receiver=on_node_upserted)

            dispatcher.send(signal=Signal.DRAG_AND_DROP_DIRECT, sender=ID_RIGHT_TREE, drag_data=dd_data, tree_path=dst_tree_path, is_into=False)

            logger.info('[1] Waiting for first drop to complete...')

            if not drop_complete.wait(LOAD_TIMEOUT_SEC):
                raise RuntimeError('Timed out waiting for drag to complete!')

            logger.info('[2] First drop complete!')

            # Waiting for the upsert signal is not quite enough, because it does not guarantee that the node has been populated.
            # Waiting until stats update should get us there:
            if not right_stats_updated.wait(LOAD_TIMEOUT_SEC):
                raise RuntimeError('Timed out waiting for stats to update!')

            logger.info('[3] First drop stats updated!')

            # Now find the dropped nodes in right tree...
            nodes_batch_2 = [
                self.find_node_by_name(self.right_con, 'Modern')
            ]

            dd_data = DragAndDropData(dd_uid=UID(100), src_treecon=self.right_con, sn_list=nodes_batch_2)
            dst_tree_path = Gtk.TreePath.new_from_string('1')
            # Drop into left tree:
            logger.info('[4] Submitting second drag & drop signal')
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
            DNode('Modern', (88259 + 652220 + 239739 + 44487 + 479124), [
                FNode('1923-art.jpeg', 88259),
                FNode('43548-forbidden_planet.jpg', 652220),
                FNode('Dunno.jpg', 239739),
                FNode('felix-the-cat.jpg', 44487),
                FNode('Glow-Cat.png', 479124),
            ]),
        ]

        # this waits for both the 1st and 2nd drops: 12 commands total
        self.do_and_verify(drop_both_sides, count_expected_cmds=12, wait_for_left=True, wait_for_right=True,
                           expected_left=final_tree_left, expected_right=final_tree_right)

    def test_dd_then_rm_subtree(self):
        logger.info('Testing CP subtree to right followed by RM of copied nodes')

        self.expand_visible_node(self.left_con, 'Art')

        nodes_batch_1 = [
            self.find_node_by_name(self.left_con, 'Modern')
        ]

        def drop_both_sides():
            logger.info('Submitting first drag & drop signal of "Modern" dir')
            dd_data = DragAndDropData(dd_uid=UID(100), src_treecon=self.left_con, sn_list=nodes_batch_1)
            dst_tree_path = None

            drop_complete = threading.Event()
            expected_count = 6

            right_stats_updated = threading.Event()

            # FIXME
            def on_stats_updated(sender):
                logger.info(f'Got signal: {Signal.REFRESH_SUBTREE_STATS_DONE} for "{sender}"')
                right_stats_updated.set()

            dispatcher.connect(signal=Signal.REFRESH_SUBTREE_STATS_DONE, receiver=on_stats_updated)

            def on_node_upserted(sender: str, node: TNode):
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
            nodes_batch_2 = [
                self.find_node_by_name(self.right_con, 'Modern').node
            ]

            # Now delete the nodes which were just dropped
            logger.info('Submitting delete signal of "Modern" dir')
            dispatcher.send(signal=Signal.DELETE_SUBTREE, sender=ID_RIGHT_TREE, node_list=nodes_batch_2)

        # The end result should be that nothing has changed
        self.do_and_verify(drop_both_sides, count_expected_cmds=12, wait_for_left=False, wait_for_right=True,
                           expected_left=INITIAL_LOCAL_TREE_LEFT, expected_right=INITIAL_GDRIVE_TREE_RIGHT)

    def test_delete_subtree(self):
        logger.info('Testing delete subtree on right')

        # Setup: cp 'Art' tree to right:
        self._cp_single_tree_into_right()
        self.expand_visible_node(self.right_con, 'Art')

        nodes = [
            self.find_node_by_name(self.right_con, 'Modern').node
        ]

        def delete():
            logger.info('Submitting delete signal for "Modern" dir')
            dispatcher.send(signal=Signal.DELETE_SUBTREE, sender=ID_RIGHT_TREE, node_list=nodes)

        final_tree_right = [
            DNode('Art', (147975 + 275771 + 8098 + 247023 + 36344), [
                FNode('Dark-Art.png', 147975),
                FNode('Hokusai_Great-Wave.jpg', 275771),
                FNode('Mona-Lisa.jpeg', 8098),
                FNode('william-shakespeare.jpg', 247023),
                FNode('WTF.jpg', 36344),
            ]),
        ]

        self.do_and_verify(delete, count_expected_cmds=6, wait_for_left=False, wait_for_right=True,
                           expected_left=INITIAL_LOCAL_TREE_LEFT, expected_right=final_tree_right)

    def test_delete_superset(self):
        logger.info('Testing delete tree followed by delete superset of tree (on right)')

        # Setup: cp 'Art' tree to right and expand node:
        self._cp_single_tree_into_right()
        self.expand_visible_node(self.right_con, 'Art')

        nodes_1 = [
            self.find_node_by_name(self.right_con, 'Modern')
        ]
        nodes_2 = [
            self.find_node_by_name(self.right_con, 'Art')
        ]

        def delete():
            logger.info('Submitting delete signal 1: RM "Modern"')
            dispatcher.send(signal=Signal.DELETE_SUBTREE, sender=ID_RIGHT_TREE, node_list=nodes_1)
            logger.info('Submitting delete signal 2: RM "Art"')
            dispatcher.send(signal=Signal.DELETE_SUBTREE, sender=ID_RIGHT_TREE, node_list=nodes_2)

        self.do_and_verify(delete, count_expected_cmds=12, wait_for_left=False, wait_for_right=True,
                           expected_left=INITIAL_LOCAL_TREE_LEFT, expected_right=INITIAL_GDRIVE_TREE_RIGHT)

    def test_delete_subset(self):
        logger.info('Testing delete tree followed by delete subset of tree (on right)')

        # Setup: cp 'Art' tree to right and expand node:
        self._cp_single_tree_into_right()
        self.expand_visible_node(self.right_con, 'Art')

        nodes_batch_1 = [
            self.find_node_by_name(self.right_con, 'Art')
        ]
        nodes_batch_2 = [
            self.find_node_by_name(self.right_con, 'Modern')
        ]

        def delete():
            logger.info('Submitting delete signal 1: RM "Art"')
            dispatcher.send(signal=Signal.DELETE_SUBTREE, sender=ID_RIGHT_TREE, node_list=nodes_batch_1)
            logger.info('Submitting delete signal 1: RM "Modern"')
            dispatcher.send(signal=Signal.DELETE_SUBTREE, sender=ID_RIGHT_TREE, node_list=nodes_batch_2)

        self.do_and_verify(delete, count_expected_cmds=12, wait_for_left=False, wait_for_right=True,
                           expected_left=INITIAL_LOCAL_TREE_LEFT, expected_right=INITIAL_GDRIVE_TREE_RIGHT)
