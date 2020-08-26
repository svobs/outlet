import logging
import os
import threading
import time
from functools import partial
from typing import Callable

import gi
from pydispatch import dispatcher

from constants import TREE_TYPE_GDRIVE, TREE_TYPE_LOCAL_DISK
from index.uid.uid import UID
from model.node.display_node import DisplayNode
from test import op_test_base
from test.op_test_base import DNode, FNode, INITIAL_LOCAL_TREE_LEFT, INITIAL_LOCAL_TREE_RIGHT, LOAD_TIMEOUT_SEC, OpTestBase, TEST_TARGET_DIR
from ui import actions
from ui.actions import DELETE_SUBTREE, DRAG_AND_DROP_DIRECT
from ui.tree.ui_listeners import DragAndDropData

gi.require_version("Gtk", "3.0")
from gi.repository import Gtk

logger = logging.getLogger(__name__)


# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
# Static stuff
# ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼

INITIAL_GDRIVE_TREE_RIGHT = [
]


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

        self.right_tree_initial = INITIAL_LOCAL_TREE_RIGHT
        self.right_tree_type = TREE_TYPE_GDRIVE
        self.right_tree_root_path = '/My Drive'
        self.right_tree_root_uid = 5020000

        self.do_setup()

    # TESTS
    # ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼

    def test_dd_single_file_cp(self):
        logger.info('Testing drag & drop copy of single file local right to local left')
        # self.app.executor.start_op_execution_thread()
        # # Offset from 0:
        # src_tree_path = Gtk.TreePath.new_from_string('1')
        # node: DisplayNode = self.right_con.display_store.get_node_data(src_tree_path)
        # logger.info(f'CP "{node.name}" from right root to left root')
        #
        # nodes = [node]
        # dd_data = DragAndDropData(dd_uid=UID(100), src_tree_controller=self.right_con, nodes=nodes)
        # dst_tree_path = Gtk.TreePath.new_from_string('1')
        #
        # def drop():
        #     logger.info('Submitting drag & drop signal')
        #     dispatcher.send(signal=DRAG_AND_DROP_DIRECT, sender=actions.ID_LEFT_TREE, drag_data=dd_data, tree_path=dst_tree_path, is_into=False)
        #
        # final_tree_left = [
        #     FNode('American_Gothic.jpg', 2061397),
        #     FNode('Angry-Clown.jpg', 824641),
        #     DNode('Art', (88259 + 652220 + 239739 + 44487 + 479124) + (147975 + 275771 + 8098 + 247023 + 36344), [
        #         FNode('Dark-Art.png', 147975),
        #         FNode('Hokusai_Great-Wave.jpg', 275771),
        #         DNode('Modern', (88259 + 652220 + 239739 + 44487 + 479124), [
        #             FNode('1923-art.jpeg', 88259),
        #             FNode('43548-forbidden_planet.jpg', 652220),
        #             FNode('Dunno.jpg', 239739),
        #             FNode('felix-the-cat.jpg', 44487),
        #             FNode('Glow-Cat.png', 479124),
        #         ]),
        #         FNode('Mona-Lisa.jpeg', 8098),
        #         FNode('william-shakespeare.jpg', 247023),
        #         FNode('WTF.jpg', 36344),
        #     ]),
        #     FNode('Egypt.jpg', 154564),
        #     FNode('George-Floyd.png', 27601),
        #     FNode('Geriatric-Clown.jpg', 89182),
        #     FNode('Keep-calm-and-carry-on.jpg', 745698),
        #     FNode('M83.jpg', 17329),
        # ]
        #
        # self.do_and_verify(drop, count_expected_cmds=1, wait_for_left=True, wait_for_right=False,
        #                    expected_left=final_tree_left, expected_right=INITIAL_LOCAL_TREE_RIGHT)
