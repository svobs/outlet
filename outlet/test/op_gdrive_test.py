import copy
import logging
import os
import threading
import time
import warnings
from functools import partial
from typing import Callable, List

import gi
from pydispatch import dispatcher

from constants import TREE_TYPE_GDRIVE, TREE_TYPE_LOCAL_DISK
from gdrive.client import GDriveClient
from index.uid.uid import UID
from model.node.display_node import DisplayNode
from model.node.gdrive_node import GDriveNode
from test import op_test_base
from test.op_test_base import DNode, FNode, INITIAL_LOCAL_TREE_LEFT, INITIAL_LOCAL_TREE_RIGHT, LOAD_TIMEOUT_SEC, OpTestBase, TEST_TARGET_DIR
from ui import actions
from ui.actions import DELETE_SUBTREE, DRAG_AND_DROP_DIRECT, ID_RIGHT_TREE
from ui.tree.ui_listeners import DragAndDropData

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


# CLASS OpGDriveTest
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼

class OpGDriveTest(OpTestBase):
    """WARNING! RUNNING THIS TEST WILL MODIFY/DELETE RUNTIME FILES!

    Currently, each test must be run in its own Python interpreter, due to Python/GTK3 failing to dispose of the app after each test.
    This means that each test must be run separately.
    """

    def setUp(self) -> None:
        super().setUp()

        self.left_tree_initial = INITIAL_LOCAL_TREE_LEFT
        self.left_tree_type = TREE_TYPE_LOCAL_DISK
        self.left_tree_root_path = os.path.join(TEST_TARGET_DIR, 'Left-Root')

        self.right_tree_initial = INITIAL_GDRIVE_TREE_RIGHT
        self.right_tree_type = TREE_TYPE_GDRIVE
        self.right_tree_root_path = '/My Drive/Test'
        self.right_tree_root_uid = 5800000

        self.do_setup(do_before_verify_func=self._cleanup_gdrive_local_and_remote)

    def tearDown(self) -> None:
        # self._cleanup_gdrive_local_and_remote()
        super(OpGDriveTest, self).tearDown()

    def _cleanup_gdrive_local_and_remote(self):
        displayed_rows: List[DisplayNode] = list(self.right_con.display_store.displayed_rows.values())
        if displayed_rows:
            logger.info(f'Found {len(displayed_rows)} displayed rows (will delete) for right tree: {self.right_tree_root_path}')

            # If we have displayed rows to delete, then need to wait for StatsUpdated signal before we proceed
            right_stats_updated = threading.Event()

            def on_stats_updated(sender):
                logger.info(f'Got signal: {actions.REFRESH_SUBTREE_STATS_COMPLETELY_DONE} for "{sender}"')
                if sender == self.right_con.tree_id:
                    right_stats_updated.set()

            dispatcher.connect(signal=actions.REFRESH_SUBTREE_STATS_COMPLETELY_DONE, receiver=on_stats_updated, sender=ID_RIGHT_TREE)

            for node in displayed_rows:
                logger.warning(f'Deleting node via cacheman: {node}')
                self.app.cache_manager.remove_gdrive_subtree(node, to_trash=False)

            logger.info('Waiting for Right tree stats to be completely done...')
            if not right_stats_updated.wait(LOAD_TIMEOUT_SEC):
                raise RuntimeError('Timed out waiting for Right stats to update!')

            logger.info('Done with displayed rows cleanup')

        self._delete_all_files_in_gdrive_test_folder()

    def _delete_all_files_in_gdrive_test_folder(self):
        # delete all files which may have been uploaded to GDrive. Goes around the program cache
        logger.info('Connecting to GDrive to find files in remote test folder')
        client = GDriveClient(self.app)
        parent_node: DisplayNode = self.app.cache_manager.get_item_for_uid(self.right_tree_root_uid, TREE_TYPE_GDRIVE)
        assert isinstance(parent_node, GDriveNode)
        children = client.get_all_children_for_parent(parent_node.goog_id)
        logger.info(f'Found {len(children)} child nodes for parent: {parent_node.name}')

        for child in children:
            logger.warning(f'Deleting node via GDrive API: {child}')
            client.hard_delete(child.goog_id)
        logger.info('Done with GDrive remote cleanup')

    # TESTS
    # ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼

    def test_dd_single_file_cp(self):
        logger.info('Testing drag & drop copy of single file local Left to GDrive Right')
        self.app.executor.start_op_execution_thread()
        # Offset from 0:
        src_tree_path = Gtk.TreePath.new_from_string('1')
        node: DisplayNode = self.left_con.display_store.get_node_data(src_tree_path)
        logger.info(f'CP "{node.name}" from left root to left root')

        nodes = [node]
        dd_data = DragAndDropData(dd_uid=UID(100), src_tree_controller=self.right_con, nodes=nodes)
        dst_tree_path = None    # top-level drop

        def drop():
            logger.info('Submitting drag & drop signal')
            dispatcher.send(signal=DRAG_AND_DROP_DIRECT, sender=actions.ID_RIGHT_TREE, drag_data=dd_data, tree_path=dst_tree_path, is_into=False)

        final_tree_right = [
            FNode('Angry-Clown.jpg', 824641),
        ]

        self.do_and_verify(drop, count_expected_cmds=1, wait_for_left=False, wait_for_right=True,
                           expected_left=INITIAL_LOCAL_TREE_LEFT, expected_right=final_tree_right)

    def test_dd_multi_file_cp(self):
        logger.info('Testing drag & drop copy of 4 files local left to local right')
        self.app.executor.start_op_execution_thread()

        # Simulate drag & drop based on position in list:
        nodes = []
        for num in range(3, 7):
            node: DisplayNode = self.left_con.display_store.get_node_data(Gtk.TreePath.new_from_string(f'{num}'))
            self.assertIsNotNone(node, f'Expected to find node at index {num}')
            nodes.append(node)
            logger.warning(f'CP "{node.name}" (#{num}) from left root to right root')

        dd_data = DragAndDropData(dd_uid=UID(100), src_tree_controller=self.left_con, nodes=nodes)
        dst_tree_path = None
        dispatcher.send(signal=DRAG_AND_DROP_DIRECT, sender=actions.ID_RIGHT_TREE, drag_data=dd_data, tree_path=dst_tree_path, is_into=False)

        def drop():
            logger.info('Submitting drag & drop signal')
            dispatcher.send(signal=DRAG_AND_DROP_DIRECT, sender=actions.ID_LEFT_TREE, drag_data=dd_data, tree_path=dst_tree_path, is_into=False)

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
        self.app.executor.start_op_execution_thread()
        node_name = 'Art'

        name_equals_func_bound: Callable = partial(op_test_base.name_equals_func, node_name)

        nodes = []
        # Duplicate the node 3 times. This is a good test of our reduction logic
        for num in range(0, 3):
            tree_iter = self.left_con.display_store.find_in_tree(name_equals_func_bound)
            node = None
            if tree_iter:
                node = self.left_con.display_store.get_node_data(tree_iter)
            self.assertIsNotNone(node, f'Expected to find node named "{node_name}"')
            nodes.append(node)
            logger.warning(f'CP "{node.name}" (#{num}) from left root to right root')

        dd_data = DragAndDropData(dd_uid=UID(100), src_tree_controller=self.left_con, nodes=nodes)
        dst_tree_path = None

        # Verify that 3 identical nodes causes an error:
        with self.assertRaises(RuntimeError) as context:
            dispatcher.send(signal=DRAG_AND_DROP_DIRECT, sender=actions.ID_RIGHT_TREE, drag_data=dd_data, tree_path=dst_tree_path, is_into=False)
            logger.info('Sleeping')
            time.sleep(10)  # in seconds
            self.assertFalse(True, 'If we got here we failed!')

    def test_dd_one_dir_tree_cp(self):
        logger.info('Testing drag & drop copy of 1 dir tree local left to GDrive right')
        self.app.executor.start_op_execution_thread()

        nodes = [
            self.find_node_by_name_in_left_tree('Art')
        ]

        dd_data = DragAndDropData(dd_uid=UID(100), src_tree_controller=self.left_con, nodes=nodes)
        dst_tree_path = None

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
        ]

        self.do_and_verify(drop, count_expected_cmds=12, wait_for_left=False, wait_for_right=True,
                           expected_left=INITIAL_LOCAL_TREE_LEFT, expected_right=final_tree_right)
