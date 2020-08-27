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

    def _cleanup_gdrive_local_and_remote(self):
        displayed_rows: List[DisplayNode] = list(self.right_con.display_store.displayed_rows.values())
        if displayed_rows:
            logger.info(f'Found {len(displayed_rows)} displayed rows (will delete) for right tree: {self.right_tree_root_path}')
            for node in displayed_rows:
                logger.warning(f'Deleting node via cacheman: {node}')
                self.app.cache_manager.remove_node(node, to_trash=False)

        self._delete_all_files_in_gdrive_test_folder()

    def _delete_all_files_in_gdrive_test_folder(self):
        # delete all files which may have been uploaded to GDrive. Goes around the program cache
        client = GDriveClient(self.app)
        parent_node: DisplayNode = self.app.cache_manager.get_item_for_uid(self.right_tree_root_uid, TREE_TYPE_GDRIVE)
        assert isinstance(parent_node, GDriveNode)
        children = client.get_all_children_for_parent(parent_node.goog_id)
        logger.info(f'Found {len(children)} child nodes for parent: {parent_node.name}')

        for child in children:
            logger.warning(f'Deleting node via GDrive API: {child}')
            client.hard_delete(child.goog_id)

    def tearDown(self) -> None:
        self._cleanup_gdrive_local_and_remote()
        super(OpGDriveTest, self).tearDown()

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
